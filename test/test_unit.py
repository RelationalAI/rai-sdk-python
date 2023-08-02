import socket
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

@patch('railib.rest.urlopen')
class TestURLOpenWithRetry(unittest.TestCase):

    def test_successful_response(self, mock_urlopen):
        # Set up the mock urlopen to return a successful response
        mock_response = MagicMock()
        mock_urlopen.return_value = mock_response
        mock_response.read.return_value = b'Hello, World!'

        req = Request('https://example.com')
        response = _urlopen_with_retry(req)

        self.assertEqual(response.read(), b'Hello, World!')
        mock_urlopen.assert_called_once_with(req)

    def test_timeout_retry(self, mock_urlopen):
        # Set up the mock urlopen to raise a socket timeout error
        mock_urlopen.side_effect = URLError(socket.timeout())

        req = Request('https://example.com')
        with self.assertLogs() as log:
            with self.assertRaises(Exception):
                _urlopen_with_retry(req, 3)

        self.assertEqual(mock_urlopen.call_count, 3)  # Expect 3 calls for retries
        self.assertEqual(len(log.output), 4)  # Expect 3 log messages for retries and 1 for failure to connect
        self.assertIn('Timeout occurred', log.output[0])
        self.assertIn('Timeout occurred', log.output[1])
        self.assertIn('Timeout occurred', log.output[2])
        self.assertIn('Failed to connect to', log.output[3])

    def test_other_error_retry(self, mock_urlopen):
        # Set up the mock urlopen to raise a non-timeout URLError
        mock_urlopen.side_effect = URLError('Some other error')

        req = Request('https://example.com')
        with self.assertLogs() as log:
            with self.assertRaises(Exception):
                _urlopen_with_retry(req, retries=3)

        self.assertEqual(mock_urlopen.call_count, 3)  # Expect 3 calls for retries
        self.assertEqual(len(log.output), 4)  # Expect 3 log messages for retries and 1 for failure to connect
        self.assertIn('URLError occurred', log.output[0])
        self.assertIn('URLError occurred', log.output[1])
        self.assertIn('URLError occurred', log.output[2])
        self.assertIn('Failed to connect to', log.output[3])


if __name__ == '__main__':
    unittest.main()
