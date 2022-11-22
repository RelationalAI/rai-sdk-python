import unittest

from railib import api


class TestPolling(unittest.TestCase):
    def test_timeout_exception(self):
        try:
            api.poll_with_specified_overhead(lambda: False, overhead_rate=0.1, timeout=1)
        except Exception as e:
            self.assertEqual('timed out after 1 seconds', str(e))

    def test_max_tries_exception(self):
        try:
            api.poll_with_specified_overhead(lambda: False, overhead_rate=0.1, max_tries=1)
        except Exception as e:
            self.assertEqual('max tries 1 exhausted', str(e))

    def test_validation(self):
        api.poll_with_specified_overhead(lambda: True, overhead_rate=0.1)
        api.poll_with_specified_overhead(lambda: True, overhead_rate=0.1, timeout=1)
        api.poll_with_specified_overhead(lambda: True, overhead_rate=0.1, max_tries=1)
        api.poll_with_specified_overhead(lambda: True, overhead_rate=0.1, timeout=1, max_tries=1)


if __name__ == '__main__':
    unittest.main()
