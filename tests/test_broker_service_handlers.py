import atexit
import importlib.util
import os
import shutil
import sys
import tempfile
import unittest
from dataclasses import replace
from pathlib import Path
from unittest.mock import patch


IMPORT_DATA_DIR = tempfile.mkdtemp(prefix="assist-test-import-broker-services-")
atexit.register(shutil.rmtree, IMPORT_DATA_DIR, ignore_errors=True)
os.environ["BROKER_DATA_DIR"] = IMPORT_DATA_DIR

from broker import browser_tools, local_broker


class _FakeThread:
    def __init__(self, *args, **kwargs) -> None:
        self.args = args
        self.kwargs = kwargs
        self.started = False

    def start(self) -> None:
        self.started = True


def _load_browser_use_server():
    server_path = (
        Path(__file__).resolve().parent.parent
        / "tools"
        / "mcp-servers"
        / "browser-use"
        / "server.py"
    )
    spec = importlib.util.spec_from_file_location("assist_browser_use_server_test", server_path)
    if spec is None or spec.loader is None:
        raise RuntimeError("Unable to load browser-use MCP server module for tests.")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


class ReadAssistantBrokerTest(unittest.TestCase):
    def test_normalize_page_context_accepts_rich_read_context(self) -> None:
        normalized = local_broker.normalize_page_context(
            {
                "title": "Attention Is All You Need",
                "url": "https://arxiv.org/html/1706.03762v7",
                "content_kind": "html",
                "selection": "Scaled dot-product attention",
                "text_excerpt": "Transformers replace recurrence with attention.",
                "heading_path": ["Abstract", "Model Architecture"],
                "selection_context": {
                    "before": "The encoder maps an input sequence",
                    "focus": "Scaled dot-product attention",
                    "after": "uses queries, keys, and values",
                },
            }
        )
        self.assertEqual("Attention Is All You Need", normalized["title"])
        self.assertEqual("html", normalized["content_kind"])
        self.assertEqual(["Abstract", "Model Architecture"], normalized["heading_path"])
        self.assertEqual("Scaled dot-product attention", normalized["selection_context"]["focus"])

    def test_format_page_context_renders_reading_fields(self) -> None:
        formatted = local_broker.format_page_context(
            {
                "title": "Example Page",
                "url": "https://example.com/read",
                "selection": "Important sentence",
                "heading_path": ["Intro", "Method"],
                "selection_context": {
                    "before": "Earlier context",
                    "focus": "Important sentence",
                    "after": "Later context",
                },
                "text_excerpt": "Bounded page excerpt",
            }
        )
        self.assertIn("Title: Example Page", formatted)
        self.assertIn("URL: https://example.com/read", formatted)
        self.assertIn("Section: Intro > Method", formatted)
        self.assertIn("Selected text:\nImportant sentence", formatted)
        self.assertIn("Local context:\nEarlier context\nImportant sentence\nLater context", formatted)
        self.assertIn("Page excerpt:\nBounded page excerpt", formatted)

    def test_browser_highlight_registered_in_broker_tool_maps(self) -> None:
        self.assertIn("browser.highlight", local_broker.BROWSER_TOOL_NAMES)
        self.assertEqual("highlight", local_broker.BROWSER_COMMAND_METHODS["browser.highlight"])
        self.assertIn("browser.highlight", local_broker.CODEX_AUTO_APPROVE_TOOLS)
        tool_names = [tool["function"]["name"] for tool in local_broker.LLAMA_BROWSER_TOOLS]
        self.assertIn("browser.highlight", tool_names)


class BrokerContractTest(unittest.TestCase):
    def test_browser_tool_catalog_matches_broker_and_mcp_surfaces(self) -> None:
        server_module = _load_browser_use_server()
        broker_llama_parameters = {
            tool["function"]["name"]: tool["function"]["parameters"]
            for tool in local_broker.LLAMA_BROWSER_TOOLS
        }
        shared_mcp_parameters = {
            tool["name"]: tool["inputSchema"]
            for tool in browser_tools.PROXIED_TOOL_DEFINITIONS
        }
        server_mcp_parameters = {
            tool["name"]: tool["inputSchema"]
            for tool in server_module.PROXIED_TOOL_DEFINITIONS
        }

        self.assertEqual(shared_mcp_parameters, server_mcp_parameters)
        self.assertEqual(set(broker_llama_parameters), set(shared_mcp_parameters))
        for tool_name, parameters in shared_mcp_parameters.items():
            self.assertEqual(parameters, broker_llama_parameters[tool_name])

        get_content = shared_mcp_parameters["browser.get_content"]["properties"]
        self.assertEqual(
            [
                browser_tools.BROWSER_GET_CONTENT_MODE_NAVIGATION,
                browser_tools.BROWSER_GET_CONTENT_MODE_RAW_HTML,
            ],
            get_content["mode"]["enum"],
        )
        self.assertIn("maxItems", get_content)
        self.assertEqual(
            browser_tools.BROWSER_LOCATOR_SCHEMA,
            shared_mcp_parameters["browser.highlight"]["properties"]["locator"],
        )

    def test_health_payload_omits_removed_codex_legacy_fields(self) -> None:
        removed_backend_key = "legacy_" + "command"
        removed_health_key = "codex_" + "legacy_" + "command"
        config = replace(
            local_broker.CONFIG,
            openai_api_key="test-key",
            codex_cli_path=None,
            codex_cli_logged_in=False,
        )
        with patch.object(local_broker, "CONFIG", config):
            with patch.object(local_broker.EXTENSION_RELAY, "health", return_value={"connected_clients": 0}):
                with patch.object(local_broker.BROWSER_AUTOMATION, "health", return_value={"sessions": 0}):
                    with patch.object(local_broker.CODEX_RUNS, "health", return_value={"active_runs": 0}):
                        with patch.object(local_broker, "llama_backend_health", return_value={"available": False}):
                            with patch.object(local_broker.MLX_RUNTIME, "health", return_value={"available": False}):
                                with patch.object(local_broker.EXPERIMENTS, "health", return_value={"jobs": 0}):
                                    with patch.object(local_broker.TRAININGS, "health", return_value={"jobs": 0}):
                                        payload = local_broker.build_health_payload()

        self.assertEqual("responses_ready", payload["codex_backend"])
        self.assertNotIn(removed_backend_key, payload)
        self.assertNotIn(removed_health_key, payload)

    def test_load_config_ignores_removed_codex_command_env(self) -> None:
        removed_env = "CODEX_" + "COMMAND"
        with patch.dict(
            os.environ,
            {
                "BROKER_DATA_DIR": IMPORT_DATA_DIR,
                removed_env: "/bin/echo legacy",
            },
            clear=False,
        ):
            with patch.object(local_broker.shutil, "which", return_value=None):
                config = local_broker.load_config()

        self.assertFalse(hasattr(config, "codex_command"))
        self.assertEqual(IMPORT_DATA_DIR, str(config.data_dir))

    def test_handle_run_start_supports_codex_llama_and_mlx(self) -> None:
        for backend in ("codex", "llama", "mlx"):
            with self.subTest(backend=backend):
                manager_root = Path(IMPORT_DATA_DIR) / f"runs_{backend}"
                manager = local_broker.CodexRunManager(manager_root)
                config = replace(
                    local_broker.CONFIG,
                    openai_api_key="test-key" if backend == "codex" else None,
                    codex_cli_path=None,
                    codex_cli_logged_in=False,
                )
                with patch.object(local_broker, "CONFIG", config):
                    with patch.object(local_broker, "CODEX_RUNS", manager):
                        with patch.object(local_broker.threading, "Thread", return_value=_FakeThread()):
                            result = local_broker.handle_run_start(
                                {
                                    "session_id": f"run_backend_{backend}",
                                    "backend": backend,
                                    "prompt": "hello",
                                }
                            )

                self.assertEqual(backend, result["backend"])
                self.assertEqual("thinking", result["status"])
                self.assertTrue(result["run_id"].startswith("run_"))
                self.assertEqual(backend, manager._runs[result["run_id"]]["backend"])


if __name__ == "__main__":
    unittest.main()
