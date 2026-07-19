import unittest
from unittest.mock import patch

from s3lfs.core import retry


class TestRetryDecorator(unittest.TestCase):
    def test_no_retry_on_success(self):
        call_count = [0]

        @retry(3, (ValueError,))
        def succeed():
            call_count[0] += 1
            return "ok"

        result = succeed()
        self.assertEqual(result, "ok")
        self.assertEqual(call_count[0], 1)

    def test_retries_on_exception(self):
        call_count = [0]

        @retry(3, (ValueError,))
        def fail_then_succeed():
            call_count[0] += 1
            if call_count[0] < 3:
                raise ValueError("transient")
            return "ok"

        with patch("time.sleep"):
            result = fail_then_succeed()
        self.assertEqual(result, "ok")
        self.assertEqual(call_count[0], 3)

    def test_raises_after_all_retries_exhausted(self):
        call_count = [0]

        @retry(3, (ValueError,))
        def always_fail():
            call_count[0] += 1
            raise ValueError("permanent")

        with patch("time.sleep"):
            with self.assertRaises(ValueError):
                always_fail()
        self.assertEqual(call_count[0], 3)

    def test_does_not_catch_unrelated_exceptions(self):
        @retry(3, (ValueError,))
        def raise_type_error():
            raise TypeError("wrong type")

        with self.assertRaises(TypeError):
            raise_type_error()

    def test_exponential_backoff_delays(self):
        call_count = [0]
        sleep_calls = []

        @retry(4, (ValueError,))
        def always_fail():
            call_count[0] += 1
            raise ValueError("fail")

        with patch("time.sleep", side_effect=lambda d: sleep_calls.append(d)):
            with self.assertRaises(ValueError):
                always_fail()

        # 3 sleeps for 4 attempts (no sleep after final failure)
        self.assertEqual(len(sleep_calls), 3)
        # Full jitter: each delay is drawn from [0, 2**(attempt+1)] rather
        # than being exactly that ceiling. A fixed schedule makes every
        # worker that failed together retry together.
        self.assertGreaterEqual(sleep_calls[0], 0)
        self.assertLessEqual(sleep_calls[0], 2)  # 2^1
        self.assertLessEqual(sleep_calls[1], 4)  # 2^2
        self.assertLessEqual(sleep_calls[2], 8)  # 2^3

    def test_max_delay_cap(self):
        sleep_calls = []

        @retry(5, (ValueError,), max_delay=5)
        def always_fail():
            raise ValueError("fail")

        with patch("time.sleep", side_effect=lambda d: sleep_calls.append(d)):
            with self.assertRaises(ValueError):
                always_fail()

        # All delays should be capped at 5
        for delay in sleep_calls:
            self.assertLessEqual(delay, 5)

    def test_no_sleep_on_first_attempt(self):
        """First attempt should not sleep, even on failure."""
        sleep_calls = []

        @retry(2, (ValueError,))
        def fail_once():
            if not hasattr(fail_once, "_called"):
                fail_once._called = True
                raise ValueError("first fail")
            return "ok"

        with patch("time.sleep", side_effect=lambda d: sleep_calls.append(d)):
            result = fail_once()

        self.assertEqual(result, "ok")
        # One sleep between attempt 1 and attempt 2
        self.assertEqual(len(sleep_calls), 1)


if __name__ == "__main__":
    unittest.main()
