"""Check bounded fixture startup retries without contacting Azure."""
import importlib.util
import io
from http.client import RemoteDisconnected
from pathlib import Path
import unittest
from unittest.mock import patch
from urllib.error import HTTPError

spec = importlib.util.spec_from_file_location("init_azurite", Path(__file__).with_name("init-azurite.py"))
fixture = importlib.util.module_from_spec(spec)
spec.loader.exec_module(fixture)


class Startup(unittest.TestCase):
    def test_early_disconnect_is_retried(self):
        with patch.object(fixture, "create_container", side_effect=[RemoteDisconnected(), None]) as create, patch.object(fixture.time, "sleep"):
            fixture.wait_for_container()
        self.assertEqual(create.call_count, 2)

    def test_retry_budget_is_bounded(self):
        with patch.object(fixture, "create_container", side_effect=TimeoutError()) as create, patch.object(fixture.time, "sleep"):
            with self.assertRaises(TimeoutError):
                fixture.wait_for_container()
        self.assertEqual(create.call_count, 30)

    def test_authentication_failure_is_not_retried(self):
        error = HTTPError("http://localhost", 403, "Forbidden", {}, io.BytesIO())
        with patch.object(fixture, "create_container", side_effect=error) as create, patch.object(fixture.time, "sleep"):
            with self.assertRaises(HTTPError):
                fixture.wait_for_container()
        self.assertEqual(create.call_count, 1)


if __name__ == "__main__":
    unittest.main()
