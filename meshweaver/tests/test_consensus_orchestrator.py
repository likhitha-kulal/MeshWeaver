"""
Unit tests for ConsensusJobOrchestrator and replicated job state lifecycle.
"""

import asyncio
import unittest

from meshweaver.consensus_orchestrator import ConsensusJobOrchestrator, OrchestratorMetrics
from meshweaver.models import ConsensusJob, ConsensusJobStatus, LogEntry, RaftCommandType
from meshweaver.raft_log import RaftLog, RaftReplicationEngine, ReplicatedStateMachine


def sample_multiply(x: int, y: int) -> int:
    return x * y


def sample_failing_task():
    raise ValueError("Intentional task failure for test")


class TestConsensusOrchestratorUnit(unittest.IsolatedAsyncioTestCase):
    async def test_consensus_job_submission_and_query(self):
        log = RaftLog()
        sm = ReplicatedStateMachine()
        engine = RaftReplicationEngine(
            node_id="node_01",
            log=log,
            state_machine=sm,
            get_term_and_role_fn=lambda: (1, "LEADER"),
        )
        orchestrator = ConsensusJobOrchestrator(
            node_id="node_01",
            raft_engine=engine,
            state_machine=sm,
        )

        job_id = await orchestrator.submit_job(sample_multiply, 6, 7, priority=1)
        self.assertIsNotNone(job_id)

        job = orchestrator.get_job(job_id)
        self.assertIsNotNone(job)
        self.assertEqual(job.status, ConsensusJobStatus.SUBMITTED)
        self.assertEqual(job.priority, 1)

        jobs = orchestrator.list_jobs(status=ConsensusJobStatus.SUBMITTED)
        self.assertEqual(len(jobs), 1)
        self.assertEqual(jobs[0].job_id, job_id)

    async def test_consensus_job_execution_and_completion(self):
        log = RaftLog()
        sm = ReplicatedStateMachine()
        engine = RaftReplicationEngine(
            node_id="node_01",
            log=log,
            state_machine=sm,
            get_term_and_role_fn=lambda: (1, "LEADER"),
        )
        orchestrator = ConsensusJobOrchestrator(
            node_id="node_01",
            raft_engine=engine,
            state_machine=sm,
            get_available_workers_fn=lambda: ["node_01"],
            is_leader_fn=lambda: True,
            schedule_interval=0.05,
        )

        await orchestrator.start()
        try:
            job_id = await orchestrator.submit_job(sample_multiply, 8, 9)
            result = await orchestrator.await_job_result(job_id, poll_interval=0.02, timeout=5.0)
            self.assertEqual(result, 72)

            completed_job = orchestrator.get_job(job_id)
            self.assertEqual(completed_job.status, ConsensusJobStatus.COMPLETED)
            self.assertIsNotNone(completed_job.completed_at)

            metrics = orchestrator.get_orchestrator_metrics()
            self.assertTrue(metrics.total_submitted >= 1)
            self.assertTrue(metrics.total_completed >= 1)
        finally:
            await orchestrator.stop()

    async def test_consensus_job_failure_retry_and_reassignment(self):
        log = RaftLog()
        sm = ReplicatedStateMachine()
        engine = RaftReplicationEngine(
            node_id="node_01",
            log=log,
            state_machine=sm,
            get_term_and_role_fn=lambda: (1, "LEADER"),
        )
        orchestrator = ConsensusJobOrchestrator(
            node_id="node_01",
            raft_engine=engine,
            state_machine=sm,
            get_available_workers_fn=lambda: ["node_01"],
            is_leader_fn=lambda: True,
            schedule_interval=0.05,
        )

        await orchestrator.start()
        try:
            # Submit task that will exceed max_retries=1
            job_id = await orchestrator.submit_job(sample_failing_task, max_retries=1)
            with self.assertRaises(RuntimeError) as exc_info:
                await orchestrator.await_job_result(job_id, poll_interval=0.02, timeout=5.0)

            self.assertIn("failed", str(exc_info.exception).lower())
            job = orchestrator.get_job(job_id)
            self.assertEqual(job.status, ConsensusJobStatus.FAILED)
            self.assertTrue(job.retry_count >= 1)
        finally:
            await orchestrator.stop()

    async def test_consensus_job_cancellation(self):
        log = RaftLog()
        sm = ReplicatedStateMachine()
        engine = RaftReplicationEngine(
            node_id="node_01",
            log=log,
            state_machine=sm,
            get_term_and_role_fn=lambda: (1, "LEADER"),
        )
        orchestrator = ConsensusJobOrchestrator(
            node_id="node_01",
            raft_engine=engine,
            state_machine=sm,
            get_available_workers_fn=lambda: ["node_01"],
            is_leader_fn=lambda: False,  # Don't auto-schedule
        )

        job_id = await orchestrator.submit_job(sample_multiply, 5, 5)
        cancelled = await orchestrator.cancel_job(job_id)
        self.assertTrue(cancelled)

        job = orchestrator.get_job(job_id)
        self.assertEqual(job.status, ConsensusJobStatus.CANCELLED)


if __name__ == "__main__":
    unittest.main()
