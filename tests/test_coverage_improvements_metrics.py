"""Additional tests to close coverage gaps in s3lfs/metrics.py."""

import io
import unittest
from contextlib import redirect_stdout

from s3lfs import metrics
from s3lfs.metrics import MetricsTracker, PipelineMetrics, StageMetrics


class TestStageMetricsEdgeCases(unittest.TestCase):
    """Cover empty/zero edge cases in StageMetrics (lines 40, 57, 63-65)."""

    def test_avg_parallelism_empty_timeline(self):
        """Line 40: avg_parallelism returns 0.0 when worker_timeline is empty."""
        stage = StageMetrics(name="empty")
        self.assertEqual(stage.avg_parallelism(), 0.0)

    def test_utilization_zero_max_workers(self):
        """Line 57: utilization returns 0.0 when max_workers is 0."""
        stage = StageMetrics(name="nomax")
        self.assertEqual(stage.max_workers, 0)
        self.assertEqual(stage.utilization(), 0.0)

    def test_avg_task_duration_empty(self):
        """Lines 63-65: avg_task_duration returns 0.0 with no durations."""
        stage = StageMetrics(name="notasks")
        self.assertEqual(stage.avg_task_duration(), 0.0)

    def test_avg_task_duration_nonempty(self):
        """Also exercise the non-empty branch of avg_task_duration."""
        stage = StageMetrics(name="withtasks")
        stage.task_durations = [1.0, 2.0, 3.0]
        self.assertEqual(stage.avg_task_duration(), 2.0)


class TestPipelineMetricsSummary(unittest.TestCase):
    """Cover PipelineMetrics.total_duration and print_summary branches
    (lines 86, 99-100, 129-131)."""

    def test_total_duration_none_when_missing_timestamps(self):
        """Line 86: total_duration returns None if start/end not both set."""
        pm = PipelineMetrics()
        self.assertIsNone(pm.total_duration())

        pm2 = PipelineMetrics(pipeline_start=10.0)
        self.assertIsNone(pm2.total_duration())

    def test_print_summary_no_stages(self):
        """Lines 99-100: print_summary prints 'No metrics collected.' and
        returns early when there are no stages."""
        pm = PipelineMetrics()
        buf = io.StringIO()
        with redirect_stdout(buf):
            pm.print_summary()
        output = buf.getvalue()
        self.assertIn("No metrics collected.", output)

    def test_print_summary_verbose_with_task_durations(self):
        """Lines 129-131: verbose print_summary prints avg/min/max task
        duration when task_durations is non-empty."""
        pm = PipelineMetrics()
        pm.pipeline_start = 0.0
        pm.pipeline_end = 5.0
        stage = pm.get_or_create_stage("hashing")
        stage.start_time = 0.0
        stage.end_time = 5.0
        stage.max_workers = 2
        stage.total_tasks = 3
        stage.completed_tasks = 3
        stage.task_durations = [1.0, 2.0, 3.0]
        stage.worker_timeline = [(0.0, 1), (2.5, 2), (5.0, 0)]

        buf = io.StringIO()
        with redirect_stdout(buf):
            pm.print_summary(verbose=True)
        output = buf.getvalue()
        self.assertIn("Avg task duration:", output)
        self.assertIn("Min task duration:", output)
        self.assertIn("Max task duration:", output)


class TestMetricsTrackerLockedMethods(unittest.TestCase):
    """Cover MetricsTracker.get_metrics and reset (lines 225-226, 234-236)."""

    def setUp(self):
        metrics._global_tracker = None

    def tearDown(self):
        metrics._global_tracker = None

    def test_get_metrics_returns_current_metrics(self):
        """Lines 225-226: get_metrics returns the tracked PipelineMetrics."""
        tracker = MetricsTracker()
        tracker.start_pipeline()
        result = tracker.get_metrics()
        self.assertIs(result, tracker._metrics)
        self.assertIsNotNone(result.pipeline_start)

    def test_reset_clears_metrics_and_active_tasks(self):
        """Lines 234-236: reset() replaces metrics with a fresh instance and
        clears active tasks."""
        tracker = MetricsTracker()
        tracker.start_pipeline()
        tracker.start_stage("hashing", max_workers=2)
        with tracker.track_task("hashing", "file1"):
            pass

        old_metrics = tracker._metrics
        self.assertIn("hashing", tracker._active_tasks)

        tracker.reset()

        self.assertIsNot(tracker._metrics, old_metrics)
        self.assertEqual(tracker._metrics.stages, {})
        self.assertEqual(tracker._active_tasks, {})


class TestGlobalTrackerFunctions(unittest.TestCase):
    """Cover get_tracker, enable_metrics, disable_metrics (lines 247, 261)."""

    def setUp(self):
        metrics._global_tracker = None

    def tearDown(self):
        metrics._global_tracker = None

    def test_get_tracker_creates_when_none(self):
        """Line 247: get_tracker() lazily creates a global tracker."""
        self.assertIsNone(metrics._global_tracker)
        tracker = metrics.get_tracker()
        self.assertIsInstance(tracker, MetricsTracker)
        self.assertIs(metrics._global_tracker, tracker)

        # Calling again returns the same instance (branch not re-triggered).
        tracker2 = metrics.get_tracker()
        self.assertIs(tracker, tracker2)

    def test_disable_metrics_clears_global_tracker(self):
        """Line 261: disable_metrics() sets the global tracker to None."""
        metrics.enable_metrics()
        self.assertTrue(metrics.is_enabled())
        metrics.disable_metrics()
        self.assertIsNone(metrics._global_tracker)
        self.assertFalse(metrics.is_enabled())


if __name__ == "__main__":
    unittest.main()
