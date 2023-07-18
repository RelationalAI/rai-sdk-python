import json
import unittest

from unittest.mock import patch, MagicMock
from railib import api


ctx = MagicMock()

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


class TestEngineAPI(unittest.TestCase):
    @patch('railib.rest.get')
    def test_get_engine(self, mock_get):
        response_json = {
            "computes": [
                {
                    "name": "test-engine"
                }
            ]
        }
        mock_response = MagicMock()
        mock_response.read.return_value = json.dumps(response_json).encode()
        mock_get.return_value = mock_response

        engine = api.get_engine(ctx, "test-engine")

        self.assertEqual(engine, response_json['computes'][0])
        mock_get.assert_called_once()

    @patch('railib.rest.delete')
    def test_delete_engine(self, mock_delete):
        response_json = {
            "status": {
                "name": "test-engine",
                "state": api.EngineState.DELETING.value,
                "message": "engine \"test-engine\" deleted successfully"
            }
        }

        mock_response = MagicMock()
        mock_response.read.return_value = json.dumps(response_json).encode()
        mock_delete.return_value = mock_response

        res = api.delete_engine(ctx, "test-engine")

        self.assertEqual(res, response_json)
        mock_delete.assert_called_once()

    @patch('railib.rest.delete')
    @patch('railib.rest.get')
    def test_delete_engine_wait(self, mock_get, mock_delete):
        # mock response for engine delete
        response_delete_json = {
            "status": {
                "name": "test-engine",
                "state": "DELETING",
                "message": "engine \"test-engine\" deleted successfully"
            }
        }
        mock_response_delete = MagicMock()
        mock_response_delete.read.return_value = json.dumps(response_delete_json).encode()
        mock_delete.return_value = mock_response_delete

        # mock response for engine get and return an engine in DEPROVISIONING state
        response_get_deprovisioning_engine_json = {
            "computes": [
                {
                    "name": "test-engine",
                    "state": api.EngineState.DEPROVISIONING.value
                }
            ]
        }
        mock_response_get_deprovisioning_engine = MagicMock()
        mock_response_get_deprovisioning_engine.read.return_value = json.dumps(response_get_deprovisioning_engine_json).encode()

        # mock response for engine get and return empty list as if the engine has been completely deleted
        response_get_no_engine_json = {
            "computes": []
        }
        mock_response_get_no_engine = MagicMock()
        mock_response_get_no_engine.read.return_value = json.dumps(response_get_no_engine_json).encode()

        mock_get.side_effect = [mock_response_get_deprovisioning_engine, mock_response_get_no_engine]

        res = api.delete_engine_wait(ctx, "test-engine")
        self.assertEqual(res, None)
        self.assertEqual(mock_delete.call_count, 1)
        self.assertEqual(mock_get.call_count, 2)


if __name__ == '__main__':
    unittest.main()
