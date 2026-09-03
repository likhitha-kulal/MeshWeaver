"""
MeshWeaver Multi-Stage Pipeline & DAG Workflow Engine.
Allows composing chained data processing stages (Stage 1 -> Stage 2 -> Stage 3)
with distributed parallel worker execution and stage-level telemetry.
"""

import asyncio
from dataclasses import dataclass, field
import inspect
import logging
import time
from typing import Any, Callable, Dict, List, Optional, Tuple, Union

from meshweaver.scheduler import RetryPolicy, SchedulingPolicy, TaskScheduler

logger = logging.getLogger("meshweaver.pipeline")


@dataclass
class StageMetrics:
    """Performance telemetry for an individual pipeline stage."""
    stage_name: str
    input_count: int = 0
    output_count: int = 0
    duration_seconds: float = 0.0
    status: str = "PENDING"  # PENDING, RUNNING, COMPLETED, FAILED
    error_message: Optional[str] = None


@dataclass
class PipelineMetrics:
    """Aggregated execution telemetry for an entire task pipeline."""
    total_stages: int = 0
    total_duration_seconds: float = 0.0
    stages: List[StageMetrics] = field(default_factory=list)
    is_successful: bool = True
    tripped_nodes: List[str] = field(default_factory=list)


@dataclass
class PipelineStage:
    """
    Represents a single step or transformation in a computation pipeline.
    """
    name: str
    func: Callable[..., Any]
    is_parallel: bool = True  # If True, applies func across each input item; if False, applies func to the whole dataset
    concurrency: int = 8
    retry_policy: Optional[RetryPolicy] = None
    policy: SchedulingPolicy = SchedulingPolicy.LEAST_LOADED


class TaskPipeline:
    """
    Pipeline orchestrator that executes sequential or branched computation stages
    across distributed mesh workers.
    """

    def __init__(self, scheduler: TaskScheduler):
        self.scheduler = scheduler
        self.stages: List[PipelineStage] = []

    def add_stage(
        self,
        name: str,
        func: Callable[..., Any],
        is_parallel: bool = True,
        concurrency: int = 8,
        retry_policy: Optional[RetryPolicy] = None,
        policy: SchedulingPolicy = SchedulingPolicy.LEAST_LOADED,
    ) -> "TaskPipeline":
        """Add a stage to the pipeline."""
        stage = PipelineStage(
            name=name,
            func=func,
            is_parallel=is_parallel,
            concurrency=concurrency,
            retry_policy=retry_policy,
            policy=policy,
        )
        self.stages.append(stage)
        return self

    def pipe(self, name: str, func: Callable[..., Any], is_parallel: bool = True) -> "TaskPipeline":
        """Fluent helper to chain a processing stage."""
        return self.add_stage(name=name, func=func, is_parallel=is_parallel)

    async def execute(self, initial_data: Any) -> Tuple[Any, PipelineMetrics]:
        """
        Execute all configured pipeline stages in order.
        Passes output of each stage as input to the next stage.
        """
        pipeline_start = time.perf_counter()
        current_data = initial_data
        metrics = PipelineMetrics(total_stages=len(self.stages))

        for idx, stage in enumerate(self.stages):
            stage_start = time.perf_counter()
            stage_metric = StageMetrics(
                stage_name=stage.name,
                status="RUNNING",
            )

            # Count input elements if list/iterable
            if isinstance(current_data, list):
                stage_metric.input_count = len(current_data)
            else:
                stage_metric.input_count = 1

            logger.info(f"Executing pipeline stage [{idx + 1}/{len(self.stages)}]: '{stage.name}'")

            try:
                if stage.is_parallel and isinstance(current_data, list):
                    # Parallel item-wise dispatch across mesh workers
                    semaphore = asyncio.Semaphore(stage.concurrency)

                    async def _run_item(item: Any) -> Any:
                        async with semaphore:
                            return await self.scheduler.dispatch_task(
                                stage.func,
                                item,
                                policy=stage.policy,
                                retry_policy=stage.retry_policy,
                            )

                    tasks = [_run_item(item) for item in current_data]
                    stage_output = await asyncio.gather(*tasks)
                else:
                    # Single dataset-level dispatch
                    stage_output = await self.scheduler.dispatch_task(
                        stage.func,
                        current_data,
                        policy=stage.policy,
                        retry_policy=stage.retry_policy,
                    )

                stage_metric.duration_seconds = time.perf_counter() - stage_start
                stage_metric.status = "COMPLETED"
                if isinstance(stage_output, list):
                    stage_metric.output_count = len(stage_output)
                else:
                    stage_metric.output_count = 1

                metrics.stages.append(stage_metric)
                current_data = stage_output

            except Exception as e:
                stage_metric.duration_seconds = time.perf_counter() - stage_start
                stage_metric.status = "FAILED"
                stage_metric.error_message = str(e)
                metrics.stages.append(stage_metric)
                metrics.is_successful = False
                metrics.total_duration_seconds = time.perf_counter() - pipeline_start
                if hasattr(self.scheduler, "circuit_breakers"):
                    metrics.tripped_nodes = self.scheduler.circuit_breakers.get_tripped_nodes()
                logger.error(f"Pipeline stage '{stage.name}' failed: {e}")
                raise

        metrics.total_duration_seconds = time.perf_counter() - pipeline_start
        if hasattr(self.scheduler, "circuit_breakers"):
            metrics.tripped_nodes = self.scheduler.circuit_breakers.get_tripped_nodes()
        return current_data, metrics
