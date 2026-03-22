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
    def test_discover_new_codex_session_id_requires_a_fresh_index_entry(self) -> None:
        previous = {"id": "session-old", "updated_at": "2026-03-21T00:00:00Z"}
        with patch.object(local_broker, "read_codex_session_index", return_value=[previous]):
            self.assertEqual("", local_broker.discover_new_codex_session_id(previous))

        fresh = {"id": "session-new", "updated_at": "2026-03-21T00:00:01Z"}
        with patch.object(local_broker, "read_codex_session_index", return_value=[previous, fresh]):
            self.assertEqual("session-new", local_broker.discover_new_codex_session_id(previous))

    def test_extract_arxiv_paper_normalizes_abs_pdf_and_versioned_urls(self) -> None:
        cases = [
            "https://arxiv.org/abs/1706.03762v7",
            "https://arxiv.org/pdf/1706.03762v7.pdf",
            "https://www.arxiv.org/html/1706.03762v7",
        ]

        for url in cases:
            with self.subTest(url=url):
                paper = local_broker.extract_arxiv_paper(url, "Attention Is All You Need")
                self.assertIsNotNone(paper)
                self.assertEqual("arxiv", paper["source"])
                self.assertEqual("1706.03762", paper["paper_id"])
                self.assertEqual("https://arxiv.org/abs/1706.03762", paper["canonical_url"])

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

    def test_conversation_paper_context_keeps_saved_paper_when_page_payload_mismatches(self) -> None:
        paper = local_broker.conversation_paper_context(
            {
                "codex": {
                    "paper_source": "arxiv",
                    "paper_id": "2601.20245",
                    "paper_url": "https://arxiv.org/abs/2601.20245",
                    "paper_title": "How AI Impacts Skill Formation",
                    "page_context_payload": {
                        "url": "https://arxiv.org/html/2507.20534#S2.SS3",
                        "title": "Kimi K2: Open Agentic Intelligence",
                    },
                }
            }
        )
        self.assertIsNotNone(paper)
        self.assertEqual("arxiv", paper["source"])
        self.assertEqual("2601.20245", paper["paper_id"])
        self.assertEqual("How AI Impacts Skill Formation", paper["title"])

    def test_paper_workspace_links_related_conversations_and_summary_requests(self) -> None:
        conversation_id = "paper_workspace_test"
        local_broker.CONVERSATIONS.append_message(conversation_id, "user", "Summarize the transformer paper.")
        local_broker.CONVERSATIONS.update_codex_state(
            conversation_id,
            {
                "paper_source": "arxiv",
                "paper_id": "1706.03762",
                "paper_url": "https://arxiv.org/abs/1706.03762",
                "paper_title": "Attention Is All You Need",
            },
        )

        workspace = local_broker.build_paper_workspace("arxiv", "1706.03762")
        conversation_ids = [item["id"] for item in workspace["conversations"]]
        self.assertIn(conversation_id, conversation_ids)
        self.assertEqual("Attention Is All You Need", workspace["paper"]["title"])

        requested = local_broker.handle_paper_summary_request(
            {
                "paper": {
                    "source": "arxiv",
                    "paper_id": "1706.03762",
                    "canonical_url": "https://arxiv.org/abs/1706.03762",
                    "title": "Attention Is All You Need",
                },
                "conversation_id": conversation_id,
            }
        )
        self.assertEqual("requested", requested["paper"]["summary_status"])
        self.assertEqual(conversation_id, requested["paper"]["last_summary_conversation_id"])

    def test_explain_selection_run_persists_highlight_capture_on_conversation(self) -> None:
        root = Path(tempfile.mkdtemp(prefix="paper-highlight-run-", dir=IMPORT_DATA_DIR))
        manager = local_broker.CodexRunManager(root)
        conversations = local_broker.ConversationStore(root)
        config = replace(
            local_broker.CONFIG,
            data_dir=root,
            openai_api_key="test-key",
            codex_cli_path=None,
            codex_cli_logged_in=False,
        )

        with patch.object(local_broker, "CONFIG", config):
            with patch.object(local_broker, "CONVERSATIONS", conversations):
                with patch.object(local_broker.threading, "Thread", return_value=_FakeThread()):
                    result = manager.start_run(
                        {
                            "session_id": "paper_highlight_run",
                            "backend": "codex",
                            "prompt": "Explain why this paragraph matters.",
                            "confirmed": True,
                            "include_page_context": True,
                            "page_context": {
                                "title": "Kimi K2: Open Agentic Intelligence",
                                "url": "https://arxiv.org/html/2507.20534#S2.SS3",
                                "selection": "Kimi K2 uses sparse expert routing.",
                                "text_excerpt": "Kimi K2 uses sparse expert routing to scale compute and capacity.",
                            },
                            "paper_context": {
                                "source": "arxiv",
                                "paper_id": "2507.20534",
                                "canonical_url": "https://arxiv.org/abs/2507.20534",
                                "title": "Kimi K2: Open Agentic Intelligence",
                            },
                            "highlight_context": {
                                "kind": "explain_selection",
                                "selection": "Kimi K2 uses sparse expert routing.",
                                "prompt": "Explain why this paragraph matters.",
                            },
                        }
                    )

                    run = manager._runs[result["run_id"]]
                    with manager._condition:
                        manager._finish_run_locked(
                            run,
                            "completed",
                            assistant_text="It explains how the model scales capacity without activating the full network on every token.",
                            emit_type="completed",
                            emit_message="Run completed.",
                        )

        conversation = conversations.get("paper_highlight_run")
        highlight_captures = conversation["codex"]["highlight_captures"]
        self.assertEqual(1, len(highlight_captures))
        self.assertEqual("explain_selection", highlight_captures[0]["kind"])
        self.assertEqual(
            "Kimi K2 uses sparse expert routing.",
            highlight_captures[0]["selection"],
        )
        self.assertEqual(
            "Explain why this paragraph matters.",
            highlight_captures[0]["prompt"],
        )
        self.assertIn(
            "scale",
            highlight_captures[0]["response"],
        )

    def test_paper_highlights_capture_saves_explain_selection_pairs_to_paper_record(self) -> None:
        root = Path(tempfile.mkdtemp(prefix="paper-highlights-capture-", dir=IMPORT_DATA_DIR))
        conversations = local_broker.ConversationStore(root)
        papers = local_broker.PaperStateStore(root)

        conversation_id = "paper_highlight_test"
        conversations.append_message(conversation_id, "user", "Explain the selected passage.")
        conversations.append_message(
            conversation_id,
            "assistant",
            "The authors are emphasizing the sparse expert routing mechanism and why it matters for scaling.",
        )
        conversations.update_codex_state(
            conversation_id,
            {
                "paper_source": "arxiv",
                "paper_id": "2507.20534",
                "paper_url": "https://arxiv.org/abs/2507.20534",
                "paper_title": "Kimi K2: Open Agentic Intelligence",
                "highlight_captures": [
                    {
                        "kind": "explain_selection",
                        "selection": "Kimi K2 uses sparse expert routing to scale capacity.",
                        "prompt": "Explain the selected passage.",
                        "response": "The authors are emphasizing the sparse expert routing mechanism and why it matters for scaling.",
                    }
                ],
            },
        )

        with patch.object(local_broker, "CONVERSATIONS", conversations):
            with patch.object(local_broker, "PAPERS", papers):
                result = local_broker.handle_paper_highlights_capture(
                    {
                        "paper": {
                            "source": "arxiv",
                            "paper_id": "2507.20534",
                            "canonical_url": "https://arxiv.org/abs/2507.20534",
                            "title": "Kimi K2: Open Agentic Intelligence",
                        },
                        "conversation_id": conversation_id,
                    }
                )

        self.assertTrue(result["saved"])
        self.assertEqual("explain_selection", result["highlight"]["kind"])
        self.assertEqual(
            "Kimi K2 uses sparse expert routing to scale capacity.",
            result["paper"]["highlights"][0]["selection"],
        )
        self.assertIn(
            "sparse expert routing mechanism",
            result["paper"]["highlights"][0]["response"],
        )

    def test_paper_summary_generate_uses_hidden_run_and_persists_summary(self) -> None:
        root = Path(tempfile.mkdtemp(prefix="paper-summary-generate-", dir=IMPORT_DATA_DIR))
        manager = local_broker.CodexRunManager(root)
        conversations = local_broker.ConversationStore(root)
        papers = local_broker.PaperStateStore(root)
        config = replace(
            local_broker.CONFIG,
            data_dir=root,
            openai_api_key="test-key",
            codex_cli_path=None,
            codex_cli_logged_in=False,
        )

        with patch.object(local_broker, "CONFIG", config):
            with patch.object(local_broker, "CODEX_RUNS", manager):
                with patch.object(local_broker, "CONVERSATIONS", conversations):
                    with patch.object(local_broker, "PAPERS", papers):
                        with patch.object(local_broker.threading, "Thread", return_value=_FakeThread()):
                            result = local_broker.handle_paper_summary_generate(
                                {
                                    "session_id": "paper_summary_hidden",
                                    "backend": "codex",
                                    "paper": {
                                        "source": "arxiv",
                                        "paper_id": "1706.03762",
                                        "canonical_url": "https://arxiv.org/abs/1706.03762",
                                        "title": "Attention Is All You Need",
                                    },
                                    "page_context": {
                                        "title": "Attention Is All You Need",
                                        "url": "https://arxiv.org/html/1706.03762v7",
                                        "text_excerpt": "Transformers replace recurrence with attention.",
                                    },
                                }
                            )

                        self.assertTrue(result["run_id"].startswith("run_"))
                        self.assertEqual("requested", result["paper"]["summary_status"])
                        run = manager._runs[result["run_id"]]
                        self.assertFalse(run["_append_assistant_message"])
                        self.assertFalse(run["_persist_backend_session"])
                        self.assertEqual("1706.03762", run["_paper_summary_target"]["paper_id"])

                        conversation = conversations.get_or_create("paper_summary_hidden")
                        self.assertEqual([], conversation["messages"])
                        self.assertEqual([], conversations.list_metadata())

                        with manager._condition:
                            manager._finish_run_locked(
                                run,
                                "completed",
                                assistant_text="Short paper summary",
                                emit_type="completed",
                                emit_message="Run completed.",
                            )

                        updated_workspace = local_broker.build_paper_workspace("arxiv", "1706.03762")
                        self.assertEqual("ready", updated_workspace["paper"]["summary_status"])
                        self.assertEqual("Short paper summary", updated_workspace["paper"]["summary"])
                        self.assertEqual("paper_summary_hidden", updated_workspace["paper"]["last_summary_conversation_id"])

    def test_hidden_summary_run_does_not_resume_or_persist_cli_session_state(self) -> None:
        root = Path(tempfile.mkdtemp(prefix="paper-summary-cli-isolation-", dir=IMPORT_DATA_DIR))
        manager = local_broker.CodexRunManager(root)
        conversations = local_broker.ConversationStore(root)
        config = replace(
            local_broker.CONFIG,
            data_dir=root,
            openai_api_key=None,
            codex_cli_path="/usr/bin/codex",
            codex_cli_logged_in=True,
        )

        session_id = "paper_summary_hidden_cli"
        conversations.update_codex_state(
            session_id,
            {
                "cli_session_id": "stale-cli-session",
                "last_response_message_count": 7,
                "last_page_context_fingerprint": "existing-fingerprint",
            },
        )

        with patch.object(local_broker, "CONFIG", config):
            with patch.object(local_broker, "CONVERSATIONS", conversations):
                with patch.object(local_broker.threading, "Thread", return_value=_FakeThread()):
                    result = manager.start_run(
                        {
                            "session_id": session_id,
                            "backend": "codex",
                            "prompt": "Summarize this paper.",
                            "confirmed": True,
                            "include_page_context": True,
                            "page_context": {
                                "title": "Attention Is All You Need",
                                "url": "https://arxiv.org/html/1706.03762v7",
                                "text_excerpt": "Transformers replace recurrence with attention.",
                            },
                            "paper_context": {
                                "source": "arxiv",
                                "paper_id": "1706.03762",
                                "canonical_url": "https://arxiv.org/abs/1706.03762",
                                "title": "Attention Is All You Need",
                            },
                            "store_user_message": False,
                            "append_assistant_message": False,
                        }
                    )

                    run = manager._runs[result["run_id"]]
                    self.assertFalse(run["_persist_backend_session"])

                    captured: dict[str, str] = {}

                    def _fake_call_codex_cli(prompt, messages, cli_session_id="", **kwargs):
                        captured["cli_session_id"] = cli_session_id
                        captured["prompt"] = prompt
                        captured["message_count"] = str(len(messages))
                        return "Hidden summary", "new-cli-session"

                    with patch.object(local_broker, "call_codex_cli", side_effect=_fake_call_codex_cli):
                        answer, reasoning = manager._run_codex_cli_loop(result["run_id"])

                    self.assertEqual("Hidden summary", answer)
                    self.assertEqual("", reasoning)
                    self.assertEqual("", captured["cli_session_id"])

                    conversation = conversations.get(session_id)
                    self.assertEqual("stale-cli-session", conversation["codex"]["cli_session_id"])
                    self.assertEqual(7, conversation["codex"]["last_response_message_count"])
                    self.assertEqual("existing-fingerprint", conversation["codex"]["last_page_context_fingerprint"])


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
                            with patch.object(local_broker, "ensure_llama_backend_available", return_value=None):
                                with patch.object(local_broker, "ensure_mlx_backend_available", return_value=None):
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
