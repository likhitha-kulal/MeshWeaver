"""
MeshWeaver Consensus Job Orchestrator
High-availability, consensus-backed distributed job scheduler and task state replication.
Provides exactly-once execution semantics, orphan worker recovery, fencing token protection,
and automatic failover across P2P compute mesh clusters.
"""

import asyncio
from dataclasses import dataclass, field
import inspect
import logging
import time
from typing import Any, Callable, Dict, List, Optional, Tuple
import uuid

import cloudpickle

from meshweaver.models import (
    ConsensusJob,
    ConsensusJobStatus,
    LogEntry,
    Message,
    MessageType,
    RaftCommandType,
)
from meshweaver.raft_log import RaftReplicationEngine, ReplicatedStateMachine
from meshweaver.task_serializer import RemoteExecutionError, TaskSerializer

logger = logging.getLogger("meshweaver.orchestrator")


@dataclass
class OrchestratorMetrics:
    """Real-time telemetry snapshot for consensus job orchestration."""
    node_id: str
    is_leader: bool
    total_submitted: int = 0
    total_assigned: int = 0
    total_completed: int = 0
    total_failed: int = 0
    total_cancelled: int = 0
    total_orphans_recovered: int = 0
    pending_jobs_count: int = 0
    assigned_jobs_count: int = 0
    active_workers_count: int = 0

    def to_dict(self) -> Dict[str, Any]:
        return {
            "node_id": self.node_id,
            "is_leader": self.is_leader,
            "total_submitted": self.total_submitted,
            "total_assigned": self.total_assigned,
            "total_completed": self.total_completed,
            "total_failed": self.total_failed,
            "total_cancelled": self.total_cancelled,
            "total_orphans_recovered": self.total_orphans_recovered,
            "pending_jobs_count": self.pending_jobs_count,
            "assigned_jobs_count": self.assigned_jobs_count,
            "active_workers_count": self.active_workers_count,
        }


class ConsensusJobOrchestrator:
    """
    Distributed high-availability job orchestrator coordinated via Raft consensus.
    Guarantees state machine replication for job lifecycle transitions, automated
    orphan task re-assignment on worker death, and fencing token validation.
    """

    def __init__(
        self,
        node_id: str,
        raft_engine: RaftReplicationEngine,
        state_machine: ReplicatedStateMachine,
        get_available_workers_fn: Optional[Callable[[], List[str]]] = None,
        is_leader_fn: Optional[Callable[[], bool]] = None,
        schedule_interval: float = 0.2,
    ) -> None:
        self.node_id = node_id
        self.raft_engine = raft_engine
        self.state_machine = state_machine
        self.get_available_workers = get_available_workers_fn or (lambda: [node_id])
        self.is_leader_fn = is_leader_fn or (lambda: True)
        self.schedule_interval = schedule_interval

        # Local execution tracking
        self._running = False
        self._leader_task: Optional[asyncio.Task] = None
        self._worker_task: Optional[asyncio.Task] = None
        self._local_executing_jobs: Dict[str, asyncio.Task] = {}

        # Telemetry counters
        self.total_submitted = 0
        self.total_assigned = 0
        self.total_completed = 0
        self.total_failed = 0
        self.total_cancelled = 0
        self.total_orphans_recovered = 0

    async def start(self) -> None:
        """Start the background orchestrator leader and worker loops."""
        if self._running:
            return
        self._running = True
        self._leader_task = asyncio.create_task(self._leader_orchestration_loop())
        self._worker_task = asyncio.create_task(self._worker_execution_loop())
        logger.info(f"ConsensusJobOrchestrator started on node {self.node_id[:8]}...")

    async def stop(self) -> None:
        """Gracefully terminate background orchestrator tasks."""
        self._running = False
        if self._leader_task and not self._leader_task.done():
            self._leader_task.cancel()
            try:
                await self._leader_task
            except asyncio.CancelledError:
                pass

        if self._worker_task and not self._worker_task.done():
            self._worker_task.cancel()
            try:
                await self._worker_task
            except asyncio.CancelledError:
                pass

        for job_task in list(self._local_executing_jobs.values()):
            if not job_task.done():
                job_task.cancel()

        self._local_executing_jobs.clear()
        logger.info(f"ConsensusJobOrchestrator stopped on node {self.node_id[:8]}.")

    async def submit_job(
        self,
        func: Callable[..., Any],
        *args: Any,
        priority: int = 2,
        timeout_seconds: float = 60.0,
        max_retries: int = 3,
        job_id: Optional[str] = None,
        **kwargs: Any,
    ) -> str:
        """
        Submit a new distributed compute job to be replicated across the consensus cluster.
        Returns the unique job_id.
        """
        jid = job_id or str(uuid.uuid4())
        func_bytes = cloudpickle.dumps(func).hex()
        args_bytes = cloudpickle.dumps(args).hex()
        kwargs_bytes = cloudpickle.dumps(kwargs).hex()

        self.total_submitted += 1
        await self.raft_engine.propose_command(
            command_type=RaftCommandType.JOB_SUBMIT,
            key=jid,
            client_id=self.node_id,
            extra_data={
                "job_id": jid,
                "func_bytes": func_bytes,
                "args_bytes": args_bytes,
                "kwargs_bytes": kwargs_bytes,
                "priority": priority,
                "timeout_seconds": timeout_seconds,
                "max_retries": max_retries,
            },
        )
        return jid

    def get_job(self, job_id: str) -> Optional[ConsensusJob]:
        """Fetch current state of a replicated consensus job."""
        return self.state_machine.get_job(job_id)

    def list_jobs(self, status: Optional[ConsensusJobStatus] = None) -> List[ConsensusJob]:
        """List all replicated consensus jobs."""
        return self.state_machine.list_jobs(status=status)

    async def cancel_job(self, job_id: str) -> bool:
        """Cancel a pending or running job across the cluster."""
        res = await self.raft_engine.propose_command(
            command_type=RaftCommandType.JOB_CANCEL,
            key=job_id,
            client_id=self.node_id,
        )
        if res:
            self.total_cancelled += 1
        return bool(res)

    async def await_job_result(self, job_id: str, poll_interval: float = 0.05, timeout: float = 30.0) -> Any:
        """Poll consensus state machine until target job finishes, returning unpacked output."""
        start_time = time.time()
        while time.time() - start_time < timeout:
            job = self.get_job(job_id)
            if job is not None:
                if job.status == ConsensusJobStatus.COMPLETED:
                    if job.result_bytes:
                        return cloudpickle.loads(bytes.fromhex(job.result_bytes))
                    return None
                elif job.status == ConsensusJobStatus.FAILED:
                    raise RuntimeError(f"Consensus job {job_id} failed: {job.error_message}")
                elif job.status == ConsensusJobStatus.CANCELLED:
                    raise asyncio.CancelledError(f"Consensus job {job_id} was cancelled")
            await asyncio.sleep(poll_interval)

        raise TimeoutError(f"Timed out waiting for consensus job {job_id} completion after {timeout}s")

    async def _leader_orchestration_loop(self) -> None:
        """
        Background leader scheduler loop:
        1. Identifies submitted jobs and assigns them round-robin / load-balanced to available workers.
        2. Detects expired/orphan tasks from dead workers and re-queues them.
        """
        while self._running:
            try:
                if self.is_leader_fn():
                    await self._schedule_pending_jobs()
                    await self._reap_orphan_jobs()
            except Exception as e:
                logger.debug(f"Error in leader orchestration loop: {e}")
            await asyncio.sleep(self.schedule_interval)

    async def _schedule_pending_jobs(self) -> None:
        """Assign pending SUBMITTED jobs to active workers."""
        pending_jobs = self.state_machine.list_jobs(status=ConsensusJobStatus.SUBMITTED)
        if not pending_jobs:
            return

        # Sort jobs by priority (0=CRITICAL first) then created_at
        pending_jobs.sort(key=lambda j: (j.priority, j.created_at))

        workers = self.get_available_workers()
        if not workers:
            workers = [self.node_id]

        worker_idx = 0
        for job in pending_jobs:
            target_worker = workers[worker_idx % len(workers)]
            worker_idx += 1

            try:
                await self.raft_engine.propose_command(
                    command_type=RaftCommandType.JOB_ASSIGN,
                    key=job.job_id,
                    client_id=self.node_id,
                    extra_data={
                        "job_id": job.job_id,
                        "worker_id": target_worker,
                    },
                )
                self.total_assigned += 1
            except Exception as e:
                logger.debug(f"Failed to assign job {job.job_id} to {target_worker}: {e}")

    async def _reap_orphan_jobs(self) -> None:
        """Detect timed out or orphan assigned jobs and trigger failover re-queueing."""
        assigned_jobs = self.state_machine.list_jobs(status=ConsensusJobStatus.ASSIGNED)
        now = time.time()
        available_workers = set(self.get_available_workers())

        for job in assigned_jobs:
            is_worker_dead = job.assigned_to is not None and job.assigned_to not in available_workers and job.assigned_to != self.node_id
            is_expired = job.is_expired(now)

            if is_expired or is_worker_dead:
                reason = "Worker unreachable/dead" if is_worker_dead else f"Job execution timeout ({job.timeout_seconds}s exceeded)"
                logger.warning(f"Reaping orphan job {job.job_id} (assigned to {job.assigned_to}): {reason}")
                try:
                    await self.raft_engine.propose_command(
                        command_type=RaftCommandType.JOB_FAIL,
                        key=job.job_id,
                        client_id=self.node_id,
                        extra_data={
                            "job_id": job.job_id,
                            "error_message": reason,
                        },
                    )
                    self.total_orphans_recovered += 1
                except Exception as e:
                    logger.debug(f"Failed to failover orphan job {job.job_id}: {e}")

    async def _worker_execution_loop(self) -> None:
        """
        Background worker execution loop:
        Discovers jobs assigned to this local node, runs them asynchronously,
        and proposes completion or failure consensus transactions.
        """
        while self._running:
            try:
                assigned_jobs = self.state_machine.list_jobs(status=ConsensusJobStatus.ASSIGNED)
                my_jobs = [j for j in assigned_jobs if j.assigned_to == self.node_id]

                for job in my_jobs:
                    if job.job_id not in self._local_executing_jobs:
                        task = asyncio.create_task(self._execute_assigned_job(job))
                        self._local_executing_jobs[job.job_id] = task

                # Cleanup completed local tasks
                finished_ids = [jid for jid, t in self._local_executing_jobs.items() if t.done()]
                for jid in finished_ids:
                    self._local_executing_jobs.pop(jid, None)

            except Exception as e:
                logger.debug(f"Error in worker execution loop: {e}")
            await asyncio.sleep(0.05)

    async def _execute_assigned_job(self, job: ConsensusJob) -> None:
        """Execute a single assigned job and publish its result via Raft consensus."""
        try:
            func = cloudpickle.loads(bytes.fromhex(job.func_bytes)) if job.func_bytes else None
            args = cloudpickle.loads(bytes.fromhex(job.args_bytes)) if job.args_bytes else ()
            kwargs = cloudpickle.loads(bytes.fromhex(job.kwargs_bytes)) if job.kwargs_bytes else {}

            if func is None:
                raise ValueError("Job payload contains no executable function")

            # Execute callable with timeout
            if inspect.iscoroutinefunction(func):
                result = await asyncio.wait_for(func(*args, **kwargs), timeout=job.timeout_seconds)
            else:
                loop = asyncio.get_running_loop()
                result = await asyncio.wait_for(
                    loop.run_in_executor(None, lambda: func(*args, **kwargs)),
                    timeout=job.timeout_seconds,
                )

            res_hex = cloudpickle.dumps(result).hex()
            await self.raft_engine.propose_command(
                command_type=RaftCommandType.JOB_COMPLETE,
                key=job.job_id,
                client_id=self.node_id,
                fencing_token=job.fencing_token,
                extra_data={
                    "job_id": job.job_id,
                    "fencing_token": job.fencing_token,
                    "result_bytes": res_hex,
                },
            )
            self.total_completed += 1

        except Exception as e:
            logger.warning(f"Error executing consensus job {job.job_id}: {e}")
            try:
                await self.raft_engine.propose_command(
                    command_type=RaftCommandType.JOB_FAIL,
                    key=job.job_id,
                    client_id=self.node_id,
                    extra_data={
                        "job_id": job.job_id,
                        "error_message": str(e),
                    },
                )
                self.total_failed += 1
            except Exception as prop_err:
                logger.debug(f"Failed to propose JOB_FAIL for {job.job_id}: {prop_err}")

    def get_orchestrator_metrics(self) -> OrchestratorMetrics:
        """Capture real-time orchestration metrics snapshot."""
        pending = len(self.state_machine.list_jobs(status=ConsensusJobStatus.SUBMITTED))
        assigned = len(self.state_machine.list_jobs(status=ConsensusJobStatus.ASSIGNED))
        workers = len(self.get_available_workers())

        return OrchestratorMetrics(
            node_id=self.node_id,
            is_leader=self.is_leader_fn(),
            total_submitted=self.total_submitted,
            total_assigned=self.total_assigned,
            total_completed=self.total_completed,
            total_failed=self.total_failed,
            total_cancelled=self.total_cancelled,
            total_orphans_recovered=self.total_orphans_recovered,
            pending_jobs_count=pending,
            assigned_jobs_count=assigned,
            active_workers_count=workers,
        )
