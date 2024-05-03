import socket
import time
import unittest
from unittest.mock import patch, MagicMock
from urllib.error import URLError
from urllib.request import Request

from railib import api
from railib.rest import _urlopen_with_retry


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

    @patch('time.sleep', return_value=None)
    @patch('time.time')
    def test_initial_delay(self, mock_time, mock_sleep):
        start_time = 100  # Fixed start time
        increment_time = 0.0001
        mock_time.side_effect = [start_time, start_time + increment_time]  # Simulate time progression

        try:
            api.poll_with_specified_overhead(lambda: False, overhead_rate=0.1, max_tries=2)
        except Exception as e:
            pass  # Ignore the exception for this test

        expected_sleep_time = (start_time + increment_time - start_time) * 0.1
        mock_sleep.assert_called_with(expected_sleep_time)
    
    def test_max_delay_cap(self):
        with patch('time.sleep') as mock_sleep:
            try:
                api.poll_with_specified_overhead(lambda: False, overhead_rate=1, max_tries=2, start_time=time.time() - 200)
            except Exception:
                pass
            # Ensure that the maximum delay of 120 seconds is not exceeded
            mock_sleep.assert_called_with(120)

    def test_polling_success(self):
        with patch('time.sleep', return_value=None) as mock_sleep:
            api.poll_with_specified_overhead(lambda: True, overhead_rate=0.1)
            mock_sleep.assert_not_called()

    def test_negative_overhead_rate(self):
        with self.assertRaises(ValueError):
            api.poll_with_specified_overhead(lambda: True, overhead_rate=-0.1)

    def test_realistic_scenario(self):
        responses = [False, False, True]
        def side_effect():
            return responses.pop(0)

        with patch('time.sleep') as mock_sleep:
            api.poll_with_specified_overhead(side_effect, overhead_rate=0.1, max_tries=3)
            # Ensure that `time.sleep` was called twice (for two false returns)
            self.assertEqual(mock_sleep.call_count, 2)

@patch('railib.rest.urlopen')
class TestURLOpenWithRetry(unittest.TestCase):

    WARNING_LOG_PREFIX = "URL/Connection error occured"
    ERROR_LOG_PREFIX = "Failed to connect to"

    def test_successful_response(self, mock_urlopen):
        # Set up the mock urlopen to return a successful response
        mock_response = MagicMock()
        mock_urlopen.return_value = mock_response
        mock_response.read.return_value = b'Hello, World!'

        req = Request('https://example.com')

        response = _urlopen_with_retry(req)
        self.assertEqual(response.read(), b'Hello, World!')
        mock_urlopen.assert_called_once_with(req)

        response = _urlopen_with_retry(req, 3)
        self.assertEqual(response.read(), b'Hello, World!')
        self.assertEqual(mock_urlopen.call_count, 2)

    def test_negative_retries(self, _):
        req = Request('https://example.com')

        with self.assertRaises(Exception) as e:
            _urlopen_with_retry(req, -1)

        self.assertIn("Retries must be a non-negative integer", str(e.exception))

    def test_url_error_retry(self, mock_urlopen):
        # Set up the mock urlopen to raise a socket timeout error
        mock_urlopen.side_effect = URLError(socket.timeout())

        req = Request('https://example.com')
        with self.assertLogs() as log:
            with self.assertRaises(Exception):
                _urlopen_with_retry(req, 2)

        self.assertEqual(mock_urlopen.call_count, 3)  # Expect 1 original call and 2 calls for retries
        self.assertEqual(len(log.output), 4)  # Expect 3 log messages for retries and 1 for failure to connect
        self.assertIn(TestURLOpenWithRetry.WARNING_LOG_PREFIX, log.output[0])
        self.assertIn(TestURLOpenWithRetry.WARNING_LOG_PREFIX, log.output[1])
        self.assertIn(TestURLOpenWithRetry.WARNING_LOG_PREFIX, log.output[2])
        self.assertIn(TestURLOpenWithRetry.ERROR_LOG_PREFIX, log.output[3])

    def test_connection_error_retry(self, mock_urlopen):
        # Set up the mock urlopen to raise a error that is subclass of ConnectonError
        mock_urlopen.side_effect = ConnectionResetError("connection reset by peer")

        req = Request('https://example.com')
        with self.assertLogs() as log:
            with self.assertRaises(Exception):
                _urlopen_with_retry(req, 2)

        self.assertEqual(mock_urlopen.call_count, 3)  # Expect 1 original call and 2 calls for retries
        self.assertEqual(len(log.output), 4)  # Expect 3 log messages for retries and 1 for failure to connect
        self.assertIn(TestURLOpenWithRetry.WARNING_LOG_PREFIX, log.output[0])
        self.assertIn(TestURLOpenWithRetry.WARNING_LOG_PREFIX, log.output[1])
        self.assertIn(TestURLOpenWithRetry.WARNING_LOG_PREFIX, log.output[2])
        self.assertIn(TestURLOpenWithRetry.ERROR_LOG_PREFIX, log.output[3])



if __name__ == '__main__':
    unittest.main()
