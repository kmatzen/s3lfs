import unittest

from s3lfs import metrics


class TestMetricsTrack(unittest.TestCase):
    """Tests for the metrics.track() context manager."""

    def setUp(self):
        # Reset global state
        metrics._global_tracker = None

    def tearDown(self):
        metrics._global_tracker = None

    def test_noop_when_disabled(self):
        """When metrics are disabled, track() is a no-op."""
        self.assertFalse(metrics.is_enabled())
        result = None
        with metrics.track("hashing", "test.txt"):
            result = 42
        self.assertEqual(result, 42)

    def test_tracks_when_enabled(self):
        """When metrics are enabled, track() records the task."""
        tracker = metrics.enable_metrics()
        tracker.start_pipeline()
        tracker.start_stage("hashing", max_workers=4)

        with metrics.track("hashing", "test.txt"):
            pass

        stage = tracker._metrics.get_or_create_stage("hashing")
        self.assertEqual(stage.completed_tasks, 1)

    def test_propagates_exceptions(self):
        """Exceptions inside track() propagate normally."""
        with self.assertRaises(ValueError):
            with metrics.track("hashing", "test.txt"):
                raise ValueError("test error")

    def test_return_value_accessible(self):
        """Code inside track() can produce results normally."""
        results = []
        with metrics.track("compression", "file.bin"):
            results.append("compressed")
        self.assertEqual(results, ["compressed"])


if __name__ == "__main__":
    unittest.main()
