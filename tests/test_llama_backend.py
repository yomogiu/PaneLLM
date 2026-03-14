import atexit
import os
import re
import shutil
import tempfile
import unittest
from dataclasses import replace
from urllib.error import URLError
from unittest.mock import patch


IMPORT_DATA_DIR = tempfile.mkdtemp(prefix="assist-test-import-broker-")
atexit.register(shutil.rmtree, IMPORT_DATA_DIR, ignore_errors=True)
os.environ["BROKER_DATA_DIR"] = IMPORT_DATA_DIR

from broker import local_broker


class LlamaBackendHealthTest(unittest.TestCase):
    def setUp(self) -> None:
        self.config = replace(
            local_broker.CONFIG,
            llama_url="http://127.0.0.1:18000/v1/chat/completions",
            llama_model="test-llama-model",
        )

    def test_invalid_llama_url_is_reported(self) -> None:
        health = local_broker.llama_backend_health(replace(self.config, llama_url="not-a-url"))

        self.assertFalse(health["available"])
        self.assertEqual("invalid_url", health["status"])
        self.assertIn("LLAMA_URL is invalid", health["last_error"])

    def test_unreachable_llama_url_is_reported(self) -> None:
        with patch.object(
            local_broker.socket,
            "create_connection",
            side_effect=ConnectionRefusedError(61, "Connection refused"),
        ):
            health = local_broker.llama_backend_health(self.config)

        self.assertFalse(health["available"])
        self.assertEqual("unreachable", health["status"])
        self.assertIn(self.config.llama_url, health["last_error"])
        self.assertIn("Connection refused", health["last_error"])

    def test_models_payload_disables_unreachable_llama_backend(self) -> None:
        manager = local_broker.MlxRuntimeManager(replace(self.config, mlx_model_path=""))

        with patch.object(
            local_broker.socket,
            "create_connection",
            side_effect=ConnectionRefusedError(61, "Connection refused"),
        ):
            payload = manager.models_payload()

        backends = {str(item["id"]): item for item in payload["backends"]}
        self.assertFalse(backends["llama"]["available"])
        self.assertIn("llama", payload)
        self.assertFalse(payload["llama"]["available"])
        self.assertEqual("unreachable", payload["llama"]["status"])

    def test_call_llama_completion_includes_target_url_in_errors(self) -> None:
        with patch.object(local_broker, "CONFIG", self.config):
            with patch.object(
                local_broker,
                "urlopen",
                side_effect=URLError(ConnectionRefusedError(61, "Connection refused")),
            ):
                with self.assertRaisesRegex(RuntimeError, re.escape(self.config.llama_url)):
                    local_broker.call_llama_completion([{"role": "user", "content": "hello"}])


if __name__ == "__main__":
    unittest.main()
