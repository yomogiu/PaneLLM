import atexit
import itertools
import json
import os
import shutil
import tempfile
import unittest
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch


IMPORT_DATA_DIR = tempfile.mkdtemp(prefix="assist-test-import-paper-history-")
atexit.register(shutil.rmtree, IMPORT_DATA_DIR, ignore_errors=True)
os.environ["BROKER_DATA_DIR"] = IMPORT_DATA_DIR

from broker import local_broker


class _FakeThread:
    def __init__(self, *args, **kwargs) -> None:
        self.args = args
        self.kwargs = kwargs
        self.started = False

    def start(self) -> None:
        self.started = True


class PaperHistoryMetadataBrokerTest(unittest.TestCase):
    def _make_root(self, prefix: str) -> Path:
        root = Path(tempfile.mkdtemp(prefix=prefix, dir=IMPORT_DATA_DIR))
        self.addCleanup(shutil.rmtree, root, ignore_errors=True)
        return root

    def _monotonic_now_iso(self, start: datetime | None = None):
        current = start or datetime(2026, 3, 21, 12, 0, 0, tzinfo=timezone.utc)
        offset = itertools.count()

        def _next() -> str:
            return (current + timedelta(seconds=next(offset))).isoformat().replace("+00:00", "Z")

        return _next

    def _page_context(self, paper_id: str, version: str | None = None) -> dict[str, str]:
        suffix = version or ""
        return {
            "title": "Kimi K2: Open Agentic Intelligence",
            "url": f"https://arxiv.org/html/{paper_id}{suffix}#S2.SS3",
            "selection": "Kimi K2 uses sparse expert routing.",
            "text_excerpt": "Kimi K2 uses sparse expert routing to scale compute and capacity.",
        }

    def _seed_versioned_conversation(
        self,
        conversations: local_broker.ConversationStore,
        session_id: str,
        paper_id: str,
        version: str,
        *,
        title: str = "Kimi K2: Open Agentic Intelligence",
        focus_text: str = "Kimi K2 uses sparse expert routing.",
    ) -> None:
        conversations.append_message(session_id, "user", f"Discuss {title} {version}.")
        conversations.update_codex_state(
            session_id,
            {
                "paper_source": "arxiv",
                "paper_id": paper_id,
                "paper_url": f"https://arxiv.org/abs/{paper_id}{version}",
                "paper_title": title,
                "paper_version": version,
                "paper_version_url": f"https://arxiv.org/abs/{paper_id}{version}",
                "paper_chat_kind": "general",
                "paper_history_label": f"{title} {version}",
                "paper_focus_text": focus_text,
            },
        )

    def test_extract_arxiv_paper_preserves_version_details_from_versioned_urls(self) -> None:
        cases = [
            (
                "https://arxiv.org/abs/1706.03762v7",
                "1706.03762",
                "v7",
                "https://arxiv.org/abs/1706.03762v7",
            ),
            (
                "https://arxiv.org/pdf/1706.03762v7.pdf",
                "1706.03762",
                "v7",
                "https://arxiv.org/abs/1706.03762v7",
            ),
            (
                "https://www.arxiv.org/html/1706.03762v7",
                "1706.03762",
                "v7",
                "https://arxiv.org/abs/1706.03762v7",
            ),
        ]

        for url, paper_id, paper_version, versioned_url in cases:
            with self.subTest(url=url):
                paper = local_broker.extract_arxiv_paper(url, "Attention Is All You Need")
                self.assertIsNotNone(paper)
                self.assertEqual("arxiv", paper["source"])
                self.assertEqual(paper_id, paper["paper_id"])
                self.assertEqual("https://arxiv.org/abs/1706.03762", paper["canonical_url"])
                self.assertEqual(paper_version, paper["paper_version"])
                self.assertEqual(versioned_url, paper["versioned_url"])

    def test_conversation_paper_context_prefers_saved_paper_and_merges_same_base_page_context(self) -> None:
        mismatched = local_broker.conversation_paper_context(
            {
                "codex": {
                    "paper_source": "arxiv",
                    "paper_id": "2601.20245",
                    "paper_url": "https://arxiv.org/abs/2601.20245v1",
                    "paper_title": "How AI Impacts Skill Formation",
                    "paper_version": "v1",
                    "paper_version_url": "https://arxiv.org/abs/2601.20245v1",
                    "page_context_payload": {
                        "url": "https://arxiv.org/html/2507.20534v2#S2.SS3",
                        "title": "Kimi K2: Open Agentic Intelligence",
                    },
                }
            }
        )
        self.assertIsNotNone(mismatched)
        self.assertEqual("2601.20245", mismatched["paper_id"])
        self.assertEqual("How AI Impacts Skill Formation", mismatched["title"])
        self.assertEqual("v1", mismatched["paper_version"])
        self.assertEqual("https://arxiv.org/abs/2601.20245v1", mismatched["versioned_url"])

        merged = local_broker.conversation_paper_context(
            {
                "codex": {
                    "paper_source": "arxiv",
                    "paper_id": "2507.20534",
                    "paper_url": "https://arxiv.org/abs/2507.20534",
                    "paper_title": "",
                    "paper_version": "",
                    "paper_version_url": "",
                    "page_context_payload": {
                        "url": "https://arxiv.org/html/2507.20534v2#S2.SS3",
                        "title": "Kimi K2: Open Agentic Intelligence",
                    },
                }
            }
        )
        self.assertIsNotNone(merged)
        self.assertEqual("2507.20534", merged["paper_id"])
        self.assertEqual("Kimi K2: Open Agentic Intelligence", merged["title"])
        self.assertEqual("v2", merged["paper_version"])
        self.assertEqual("https://arxiv.org/abs/2507.20534v2", merged["versioned_url"])

    def test_versioned_explain_selection_run_persists_metadata_and_versioned_highlight(self) -> None:
        root = self._make_root("paper-history-run-")
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
            with patch.object(local_broker, "CONVERSATIONS", conversations):
                with patch.object(local_broker, "PAPERS", papers):
                    with patch.object(local_broker.EXTENSION_RELAY, "health", return_value={"connected_clients": 0}):
                        with patch.object(local_broker.threading, "Thread", return_value=_FakeThread()):
                            result = manager.start_run(
                                {
                                    "session_id": "paper_history_run",
                                    "backend": "codex",
                                    "prompt": "Explain why this paragraph matters.",
                                    "confirmed": True,
                                    "include_page_context": True,
                                    "page_context": self._page_context("2507.20534", "v2"),
                                    "paper_context": {
                                        "source": "arxiv",
                                        "paper_id": "2507.20534",
                                        "canonical_url": "https://arxiv.org/abs/2507.20534v2",
                                        "title": "Kimi K2: Open Agentic Intelligence",
                                        "paper_version": "v2",
                                        "versioned_url": "https://arxiv.org/abs/2507.20534v2",
                                    },
                                    "highlight_context": {
                                        "kind": "explain_selection",
                                        "selection": "Kimi K2 uses sparse expert routing.",
                                        "prompt": "Explain why this paragraph matters.",
                                        "paper_version": "v2",
                                    },
                                }
                            )

                            run = manager._runs[result["run_id"]]
                            with manager._condition:
                                manager._finish_run_locked(
                                    run,
                                    "completed",
                                    assistant_text=(
                                        "It explains how the model scales capacity without activating the full network on every token."
                                    ),
                                    emit_type="completed",
                                    emit_message="Run completed.",
                                )

        conversation = conversations.get("paper_history_run")
        codex = conversation["codex"]
        self.assertEqual("v2", codex["paper_version"])
        self.assertEqual("https://arxiv.org/abs/2507.20534v2", codex["paper_version_url"])
        self.assertEqual("explain_selection", codex["paper_chat_kind"])
        self.assertEqual("Explain Selection", codex["paper_history_label"])
        self.assertEqual("Kimi K2 uses sparse expert routing.", codex["paper_focus_text"])
        self.assertEqual(1, len(codex["highlight_captures"]))
        self.assertEqual("v2", codex["highlight_captures"][0]["paper_version"])
        self.assertIn("scales capacity", codex["highlight_captures"][0]["response"])

        paper_path = papers._path("arxiv", "2507.20534")
        paper_record = json.loads(paper_path.read_text(encoding="utf-8"))
        self.assertEqual(1, len(paper_record["highlights"]))
        self.assertEqual("v2", paper_record["highlights"][0]["paper_version"])
        self.assertEqual("2507.20534", paper_record["paper_id"])

    def test_paper_workspace_groups_versions_and_dedupes_observed_versions_newest_first(self) -> None:
        root = self._make_root("paper-history-workspace-")
        conversations = local_broker.ConversationStore(root)
        papers = local_broker.PaperStateStore(root)
        config = replace(local_broker.CONFIG, data_dir=root)

        with patch.object(local_broker, "CONFIG", config):
            with patch.object(local_broker, "CONVERSATIONS", conversations):
                with patch.object(local_broker, "PAPERS", papers):
                    with patch.object(local_broker, "now_iso", side_effect=self._monotonic_now_iso()):
                        self._seed_versioned_conversation(
                            conversations,
                            "paper_v1_old",
                            "2507.20534",
                            "v1",
                        )
                        self._seed_versioned_conversation(
                            conversations,
                            "paper_v2_middle",
                            "2507.20534",
                            "v2",
                        )
                        self._seed_versioned_conversation(
                            conversations,
                            "paper_v2_newest",
                            "2507.20534",
                            "v2",
                        )

                        workspace = local_broker.build_paper_workspace("arxiv", "2507.20534")

        conversation_ids = [item["id"] for item in workspace["conversations"]]
        self.assertEqual(
            ["paper_v2_newest", "paper_v2_middle", "paper_v1_old"],
            conversation_ids,
        )
        self.assertEqual(["v2", "v1"], workspace["paper"]["observed_versions"])
        self.assertEqual("2507.20534", workspace["paper"]["paper_id"])
        self.assertEqual("Kimi K2: Open Agentic Intelligence", workspace["paper"]["title"])
        self.assertIn("memory", workspace)

    def test_paper_workspace_exposes_memory_metadata_by_version(self) -> None:
        root = self._make_root("paper-memory-metadata-")
        conversations = local_broker.ConversationStore(root)
        papers = local_broker.PaperStateStore(root)
        config = replace(local_broker.CONFIG, data_dir=root)

        with patch.object(local_broker, "CONFIG", config):
            with patch.object(local_broker, "CONVERSATIONS", conversations):
                with patch.object(local_broker, "PAPERS", papers):
                    with patch.object(local_broker, "now_iso", side_effect=self._monotonic_now_iso()):
                        self._seed_versioned_conversation(conversations, "paper_v1", "2507.20534", "v1")
                        self._seed_versioned_conversation(conversations, "paper_v2", "2507.20534", "v2")
                        self._seed_versioned_conversation(conversations, "paper_unversioned", "2507.20534", "")
                        papers.store_summary_result(
                            "arxiv",
                            "2507.20534",
                            canonical_url="https://arxiv.org/abs/2507.20534",
                            title="Kimi K2: Open Agentic Intelligence",
                            conversation_id="paper_v2",
                            paper_version="v2",
                            summary="Sparse expert routing drives the main scaling story.",
                        )
                        papers.add_highlight(
                            "arxiv",
                            "2507.20534",
                            canonical_url="https://arxiv.org/abs/2507.20534",
                            title="Kimi K2: Open Agentic Intelligence",
                            highlight={
                                "kind": "explain_selection",
                                "selection": "Kimi K2 uses sparse expert routing to scale capacity.",
                                "prompt": "Explain the selected passage.",
                                "response": "The routing mechanism preserves capacity without activating the full network.",
                                "paper_version": "v2",
                                "conversation_id": "paper_v2",
                                "created_at": local_broker.now_iso(),
                            },
                        )

                        workspace = local_broker.build_paper_workspace("arxiv", "2507.20534")

        self.assertEqual("v2", workspace["memory"]["default_version"])
        self.assertTrue(workspace["memory"]["has_unversioned"])
        self.assertGreaterEqual(workspace["memory"]["counts_by_version"]["v2"], 3)
        self.assertTrue(workspace["memory"]["latest_updated_at"])

    def test_paper_memory_query_filters_versions_and_orders_results(self) -> None:
        root = self._make_root("paper-memory-query-")
        conversations = local_broker.ConversationStore(root)
        papers = local_broker.PaperStateStore(root)
        config = replace(local_broker.CONFIG, data_dir=root)

        with patch.object(local_broker, "CONFIG", config):
            with patch.object(local_broker, "CONVERSATIONS", conversations):
                with patch.object(local_broker, "PAPERS", papers):
                    with patch.object(local_broker, "now_iso", side_effect=self._monotonic_now_iso()):
                        self._seed_versioned_conversation(
                            conversations,
                            "paper_v1",
                            "2507.20534",
                            "v1",
                            focus_text="Legacy dense routing baseline.",
                        )
                        self._seed_versioned_conversation(
                            conversations,
                            "paper_v2",
                            "2507.20534",
                            "v2",
                            focus_text="Sparse expert routing keeps compute efficient.",
                        )
                        self._seed_versioned_conversation(
                            conversations,
                            "paper_unversioned",
                            "2507.20534",
                            "",
                            focus_text="General note about Kimi K2 routing.",
                        )
                        papers.store_summary_result(
                            "arxiv",
                            "2507.20534",
                            canonical_url="https://arxiv.org/abs/2507.20534",
                            title="Kimi K2: Open Agentic Intelligence",
                            conversation_id="paper_v2",
                            paper_version="v2",
                            summary="Sparse expert routing is the key scaling mechanism in v2.",
                        )
                        papers.add_highlight(
                            "arxiv",
                            "2507.20534",
                            canonical_url="https://arxiv.org/abs/2507.20534",
                            title="Kimi K2: Open Agentic Intelligence",
                            highlight={
                                "kind": "explain_selection",
                                "selection": "Sparse expert routing keeps compute efficient.",
                                "prompt": "Why does sparse expert routing matter?",
                                "response": "It routes tokens through a smaller active subset of the network.",
                                "paper_version": "v2",
                                "conversation_id": "paper_v2",
                                "created_at": local_broker.now_iso(),
                            },
                        )

                        result = local_broker.handle_paper_memory_query(
                            {
                                "paper": {
                                    "source": "arxiv",
                                    "paper_id": "2507.20534",
                                    "canonical_url": "https://arxiv.org/abs/2507.20534",
                                    "title": "Kimi K2: Open Agentic Intelligence",
                                    "paper_version": "v2",
                                    "versioned_url": "https://arxiv.org/abs/2507.20534v2",
                                },
                                "query": "sparse expert routing",
                                "limit": 10,
                            }
                        )

        self.assertEqual("v2", result["memory_version"])
        self.assertEqual("summary", result["results"][0]["kind"])
        self.assertEqual("highlight", result["results"][1]["kind"])
        self.assertEqual(3, result["counts"]["exact_version_count"])
        self.assertEqual(1, result["counts"]["unversioned_fallback_count"])
        conversation_ids = [item["conversation_id"] for item in result["results"] if item["kind"] == "conversation"]
        self.assertIn("paper_v2", conversation_ids)
        self.assertIn("paper_unversioned", conversation_ids)
        self.assertNotIn("paper_v1", conversation_ids)

    def test_paper_memory_query_filters_to_matching_entries(self) -> None:
        root = self._make_root("paper-memory-match-filter-")
        conversations = local_broker.ConversationStore(root)
        papers = local_broker.PaperStateStore(root)
        config = replace(local_broker.CONFIG, data_dir=root)

        with patch.object(local_broker, "CONFIG", config):
            with patch.object(local_broker, "CONVERSATIONS", conversations):
                with patch.object(local_broker, "PAPERS", papers):
                    with patch.object(local_broker, "now_iso", side_effect=self._monotonic_now_iso()):
                        self._seed_versioned_conversation(
                            conversations,
                            "paper_v2",
                            "2507.20534",
                            "v2",
                            focus_text="Sparse expert routing keeps compute efficient.",
                        )
                        self._seed_versioned_conversation(
                            conversations,
                            "paper_unversioned",
                            "2507.20534",
                            "",
                            focus_text="General note about Kimi K2 routing.",
                        )
                        papers.store_summary_result(
                            "arxiv",
                            "2507.20534",
                            canonical_url="https://arxiv.org/abs/2507.20534",
                            title="Kimi K2: Open Agentic Intelligence",
                            conversation_id="paper_v2",
                            paper_version="v2",
                            summary="Sparse expert routing is the key scaling mechanism in v2.",
                        )
                        papers.add_highlight(
                            "arxiv",
                            "2507.20534",
                            canonical_url="https://arxiv.org/abs/2507.20534",
                            title="Kimi K2: Open Agentic Intelligence",
                            highlight={
                                "kind": "explain_selection",
                                "selection": "Sparse expert routing keeps compute efficient.",
                                "prompt": "Why does sparse expert routing matter?",
                                "response": "It routes tokens through a smaller active subset of the network.",
                                "paper_version": "v2",
                                "conversation_id": "paper_v2",
                                "created_at": local_broker.now_iso(),
                            },
                        )

                        result = local_broker.handle_paper_memory_query(
                            {
                                "paper": {
                                    "source": "arxiv",
                                    "paper_id": "2507.20534",
                                    "canonical_url": "https://arxiv.org/abs/2507.20534",
                                    "title": "Kimi K2: Open Agentic Intelligence",
                                    "paper_version": "v2",
                                    "versioned_url": "https://arxiv.org/abs/2507.20534v2",
                                },
                                "query": "smaller active subset",
                                "limit": 10,
                            }
                        )

        self.assertEqual(["highlight"], [item["kind"] for item in result["results"]])
        self.assertEqual(
            "Sparse expert routing keeps compute efficient. It routes tokens through a smaller active subset of the network.",
            result["results"][0]["snippet"],
        )

    def test_paper_memory_query_excludes_requested_conversation(self) -> None:
        root = self._make_root("paper-memory-exclude-")
        conversations = local_broker.ConversationStore(root)
        papers = local_broker.PaperStateStore(root)
        config = replace(local_broker.CONFIG, data_dir=root)

        with patch.object(local_broker, "CONFIG", config):
            with patch.object(local_broker, "CONVERSATIONS", conversations):
                with patch.object(local_broker, "PAPERS", papers):
                    with patch.object(local_broker, "now_iso", side_effect=self._monotonic_now_iso()):
                        self._seed_versioned_conversation(conversations, "paper_v2_a", "2507.20534", "v2")
                        self._seed_versioned_conversation(conversations, "paper_v2_b", "2507.20534", "v2")

                        result = local_broker.handle_paper_memory_query(
                            {
                                "paper": {
                                    "source": "arxiv",
                                    "paper_id": "2507.20534",
                                    "canonical_url": "https://arxiv.org/abs/2507.20534",
                                    "title": "Kimi K2: Open Agentic Intelligence",
                                    "paper_version": "v2",
                                },
                                "exclude_conversation_id": "paper_v2_b",
                            }
                        )

        conversation_ids = [item["conversation_id"] for item in result["results"] if item["kind"] == "conversation"]
        self.assertIn("paper_v2_a", conversation_ids)
        self.assertNotIn("paper_v2_b", conversation_ids)

    def test_run_start_injects_same_version_paper_memory_only(self) -> None:
        root = self._make_root("paper-memory-run-")
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
            with patch.object(local_broker, "CONVERSATIONS", conversations):
                with patch.object(local_broker, "PAPERS", papers):
                    with patch.object(local_broker.EXTENSION_RELAY, "health", return_value={"connected_clients": 0}):
                        with patch.object(local_broker.threading, "Thread", return_value=_FakeThread()):
                            with patch.object(local_broker, "now_iso", side_effect=self._monotonic_now_iso()):
                                self._seed_versioned_conversation(
                                    conversations,
                                    "paper_v1",
                                    "2507.20534",
                                    "v1",
                                    focus_text="Legacy dense routing baseline.",
                                )
                                self._seed_versioned_conversation(
                                    conversations,
                                    "paper_v2",
                                    "2507.20534",
                                    "v2",
                                    focus_text="Sparse expert routing keeps compute efficient.",
                                )
                                papers.store_summary_result(
                                    "arxiv",
                                    "2507.20534",
                                    canonical_url="https://arxiv.org/abs/2507.20534",
                                    title="Kimi K2: Open Agentic Intelligence",
                                    conversation_id="paper_v2",
                                    paper_version="v2",
                                    summary="Sparse expert routing drives efficient scaling in v2.",
                                )

                                result = manager.start_run(
                                    {
                                        "session_id": "paper_memory_new_chat",
                                        "backend": "codex",
                                        "prompt": "How does sparse expert routing work?",
                                        "confirmed": True,
                                        "paper_context": {
                                            "source": "arxiv",
                                            "paper_id": "2507.20534",
                                            "canonical_url": "https://arxiv.org/abs/2507.20534",
                                            "title": "Kimi K2: Open Agentic Intelligence",
                                            "paper_version": "v2",
                                            "versioned_url": "https://arxiv.org/abs/2507.20534v2",
                                        },
                                    }
                                )

        run = manager._runs[result["run_id"]]
        suffix = run["_request_prompt_suffix"]
        self.assertIn("[Paper Memory]", suffix)
        self.assertIn("Sparse expert routing", suffix)
        self.assertNotIn("Legacy dense routing baseline.", suffix)

    def test_paper_highlights_capture_remains_idempotent_after_immediate_persistence(self) -> None:
        root = self._make_root("paper-history-highlights-")
        conversations = local_broker.ConversationStore(root)
        papers = local_broker.PaperStateStore(root)

        conversation_id = "paper_highlight_idempotent"
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
                "paper_url": "https://arxiv.org/abs/2507.20534v2",
                "paper_title": "Kimi K2: Open Agentic Intelligence",
                "paper_version": "v2",
                "paper_version_url": "https://arxiv.org/abs/2507.20534v2",
                "highlight_captures": [
                    {
                        "kind": "explain_selection",
                        "selection": "Kimi K2 uses sparse expert routing.",
                        "prompt": "Explain the selected passage.",
                        "response": (
                            "The authors are emphasizing the sparse expert routing mechanism and why it matters for scaling."
                        ),
                        "conversation_id": conversation_id,
                        "created_at": "2026-03-21T12:00:00Z",
                        "paper_version": "v2",
                    }
                ],
            },
        )

        initial_record = {
            "source": "arxiv",
            "paper_id": "2507.20534",
            "canonical_url": "https://arxiv.org/abs/2507.20534v2",
            "title": "Kimi K2: Open Agentic Intelligence",
            "summary": "",
            "summary_status": "idle",
            "summary_requested_at": "",
            "last_summary_conversation_id": "",
            "summary_error": "",
            "highlights": [
                {
                    "kind": "explain_selection",
                    "selection": "Kimi K2 uses sparse expert routing.",
                    "prompt": "Explain the selected passage.",
                    "response": (
                        "The authors are emphasizing the sparse expert routing mechanism and why it matters for scaling."
                    ),
                    "conversation_id": conversation_id,
                    "created_at": "2026-03-21T12:00:00Z",
                    "paper_version": "v2",
                }
            ],
            "observed_versions": ["v2"],
        }
        papers._write(papers._path("arxiv", "2507.20534"), initial_record)

        with patch.object(local_broker, "CONVERSATIONS", conversations):
            with patch.object(local_broker, "PAPERS", papers):
                first = local_broker.handle_paper_highlights_capture(
                    {
                        "paper": {
                            "source": "arxiv",
                            "paper_id": "2507.20534",
                            "canonical_url": "https://arxiv.org/abs/2507.20534v2",
                            "title": "Kimi K2: Open Agentic Intelligence",
                            "paper_version": "v2",
                            "versioned_url": "https://arxiv.org/abs/2507.20534v2",
                        },
                        "conversation_id": conversation_id,
                    }
                )
                second = local_broker.handle_paper_highlights_capture(
                    {
                        "paper": {
                            "source": "arxiv",
                            "paper_id": "2507.20534",
                            "canonical_url": "https://arxiv.org/abs/2507.20534v2",
                            "title": "Kimi K2: Open Agentic Intelligence",
                            "paper_version": "v2",
                            "versioned_url": "https://arxiv.org/abs/2507.20534v2",
                        },
                        "conversation_id": conversation_id,
                    }
                )

        self.assertTrue(first["saved"])
        self.assertTrue(second["saved"])
        self.assertEqual(1, len(first["highlights"]))
        self.assertEqual(1, len(second["highlights"]))
        self.assertEqual("v2", first["highlights"][0]["paper_version"])
        self.assertEqual("v2", second["highlights"][0]["paper_version"])

        paper_record = json.loads(papers._path("arxiv", "2507.20534").read_text(encoding="utf-8"))
        self.assertEqual(1, len(paper_record["highlights"]))
        self.assertEqual("v2", paper_record["highlights"][0]["paper_version"])


if __name__ == "__main__":
    unittest.main()
