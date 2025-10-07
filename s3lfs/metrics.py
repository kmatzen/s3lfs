"""
Metrics and monitoring for s3lfs pipeline parallelism.

This module provides tools to measure and track parallelism across different
stages of the s3lfs pipeline (hashing, compression, upload/download, decompression).
"""

import threading
import time
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import Dict, List, Optional


@dataclass
class StageMetrics:
    """Metrics for a single pipeline stage."""

    name: str
    start_time: Optional[float] = None
    end_time: Optional[float] = None
    active_workers: int = 0
    max_workers: int = 0
    total_tasks: int = 0
    completed_tasks: int = 0
    # Track when workers are active (timestamp -> worker count)
    worker_timeline: List[tuple] = field(default_factory=list)
    # Track task durations
    task_durations: List[float] = field(default_factory=list)

    def duration(self) -> Optional[float]:
        """Get total duration of this stage."""
        if self.start_time and self.end_time:
            return self.end_time - self.start_time
        return None

    def avg_parallelism(self) -> float:
        """Calculate average parallelism during this stage."""
        if not self.worker_timeline:
            return 0.0

        # Calculate weighted average of workers over time
        total_worker_seconds = 0.0
        for i in range(len(self.worker_timeline) - 1):
            t1, workers1 = self.worker_timeline[i]
            t2, _ = self.worker_timeline[i + 1]
            total_worker_seconds += workers1 * (t2 - t1)

        duration = self.duration()
        if duration and duration > 0:
            return total_worker_seconds / duration
        return 0.0

    def utilization(self) -> float:
        """Calculate worker utilization (0-1) based on max_workers."""
        if self.max_workers == 0:
            return 0.0
        avg = self.avg_parallelism()
        return avg / self.max_workers

    def avg_task_duration(self) -> float:
        """Get average task duration in seconds."""
        if not self.task_durations:
            return 0.0
        return sum(self.task_durations) / len(self.task_durations)


@dataclass
class PipelineMetrics:
    """Container for all pipeline stage metrics."""

    stages: Dict[str, StageMetrics] = field(default_factory=dict)
    pipeline_start: Optional[float] = None
    pipeline_end: Optional[float] = None

    def get_or_create_stage(self, stage_name: str) -> StageMetrics:
        """Get existing stage or create new one."""
        if stage_name not in self.stages:
            self.stages[stage_name] = StageMetrics(name=stage_name)
        return self.stages[stage_name]

    def total_duration(self) -> Optional[float]:
        """Get total pipeline duration."""
        if self.pipeline_start and self.pipeline_end:
            return self.pipeline_end - self.pipeline_start
        return None

    def print_summary(self, verbose: bool = False) -> None:
        """Print a summary of pipeline metrics."""
        print("\n" + "=" * 70)
        print("PIPELINE PARALLELISM METRICS")
        print("=" * 70)

        total_dur = self.total_duration()
        if total_dur:
            print(f"\nTotal Pipeline Duration: {total_dur:.2f}s")

        if not self.stages:
            print("\nNo metrics collected.")
            return

        print("\nStage Summary:")
        print("-" * 70)
        print(
            f"{'Stage':<20} {'Duration':<12} {'Avg Par':<10} {'Util%':<8} {'Tasks':<8}"
        )
        print("-" * 70)

        for stage_name, metrics in sorted(self.stages.items()):
            duration = metrics.duration()
            dur_str = f"{duration:.2f}s" if duration else "N/A"
            avg_par = metrics.avg_parallelism()
            util = metrics.utilization() * 100
            tasks = metrics.completed_tasks

            print(
                f"{stage_name:<20} {dur_str:<12} {avg_par:<10.2f} {util:<8.1f} {tasks:<8}"
            )

        if verbose:
            print("\nDetailed Stage Metrics:")
            print("-" * 70)
            for stage_name, metrics in sorted(self.stages.items()):
                print(f"\n{stage_name}:")
                print(f"  Max concurrent workers: {metrics.max_workers}")
                print(f"  Total tasks: {metrics.total_tasks}")
                print(f"  Completed tasks: {metrics.completed_tasks}")
                if metrics.task_durations:
                    print(f"  Avg task duration: {metrics.avg_task_duration():.2f}s")
                    print(f"  Min task duration: {min(metrics.task_durations):.2f}s")
                    print(f"  Max task duration: {max(metrics.task_durations):.2f}s")

        print("=" * 70 + "\n")


class MetricsTracker:
    """Thread-safe metrics tracker for pipeline operations."""

    def __init__(self):
        self._metrics = PipelineMetrics()
        self._lock = threading.Lock()
        # Track active tasks per stage
        self._active_tasks: Dict[str, Dict[str, float]] = {}

    def start_pipeline(self) -> None:
        """Mark the start of the pipeline."""
        with self._lock:
            self._metrics.pipeline_start = time.time()

    def end_pipeline(self) -> None:
        """Mark the end of the pipeline."""
        with self._lock:
            self._metrics.pipeline_end = time.time()

    def start_stage(self, stage_name: str, max_workers: int = 0) -> None:
        """Mark the start of a pipeline stage."""
        with self._lock:
            stage = self._metrics.get_or_create_stage(stage_name)
            stage.start_time = time.time()
            stage.max_workers = max_workers
            self._active_tasks[stage_name] = {}

    def end_stage(self, stage_name: str) -> None:
        """Mark the end of a pipeline stage."""
        with self._lock:
            stage = self._metrics.get_or_create_stage(stage_name)
            stage.end_time = time.time()
            # Record final worker count as 0
            if stage.start_time:
                stage.worker_timeline.append((stage.end_time, 0))

    @contextmanager
    def track_task(self, stage_name: str, task_id: str):
        """Context manager to track a single task in a stage."""
        task_start = time.time()
        self._task_started(stage_name, task_id)
        try:
            yield
        finally:
            task_duration = time.time() - task_start
            self._task_completed(stage_name, task_id, task_duration)

    def _task_started(self, stage_name: str, task_id: str) -> None:
        """Record task start."""
        with self._lock:
            stage = self._metrics.get_or_create_stage(stage_name)

            # Initialize active tasks dict for this stage if needed
            if stage_name not in self._active_tasks:
                self._active_tasks[stage_name] = {}

            # Record task start time
            self._active_tasks[stage_name][task_id] = time.time()

            # Update worker count
            stage.active_workers = len(self._active_tasks[stage_name])
            stage.max_workers = max(stage.max_workers, stage.active_workers)
            stage.total_tasks += 1

            # Record in timeline
            stage.worker_timeline.append((time.time(), stage.active_workers))

    def _task_completed(self, stage_name: str, task_id: str, duration: float) -> None:
        """Record task completion."""
        with self._lock:
            stage = self._metrics.get_or_create_stage(stage_name)

            # Remove from active tasks
            if (
                stage_name in self._active_tasks
                and task_id in self._active_tasks[stage_name]
            ):
                del self._active_tasks[stage_name][task_id]

            # Update worker count
            stage.active_workers = len(self._active_tasks.get(stage_name, {}))
            stage.completed_tasks += 1
            stage.task_durations.append(duration)

            # Record in timeline
            stage.worker_timeline.append((time.time(), stage.active_workers))

    def get_metrics(self) -> PipelineMetrics:
        """Get a copy of current metrics."""
        with self._lock:
            return self._metrics

    def print_summary(self, verbose: bool = False) -> None:
        """Print metrics summary."""
        self._metrics.print_summary(verbose=verbose)

    def reset(self) -> None:
        """Reset all metrics."""
        with self._lock:
            self._metrics = PipelineMetrics()
            self._active_tasks = {}


# Global metrics tracker instance
_global_tracker: Optional[MetricsTracker] = None


def get_tracker() -> MetricsTracker:
    """Get the global metrics tracker instance."""
    global _global_tracker
    if _global_tracker is None:
        _global_tracker = MetricsTracker()
    return _global_tracker


def enable_metrics() -> MetricsTracker:
    """Enable metrics collection and return the tracker."""
    global _global_tracker
    _global_tracker = MetricsTracker()
    return _global_tracker


def disable_metrics() -> None:
    """Disable metrics collection."""
    global _global_tracker
    _global_tracker = None


def is_enabled() -> bool:
    """Check if metrics collection is enabled."""
    return _global_tracker is not None
