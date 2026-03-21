import atexit
import json
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


class _FakeJsonResponse:
    def __init__(self, payload: dict) -> None:
        self._body = json.dumps(payload).encode("utf-8")
        self.headers = {"Content-Type": "application/json"}

    def read(self) -> bytes:
        return self._body

    def __enter__(self) -> "_FakeJsonResponse":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        return None


class _FakeSseResponse:
    def __init__(self, events: list[object]) -> None:
        self.headers = {"Content-Type": "text/event-stream"}
        self._index = 0
        self._lines: list[bytes] = []
        for event in events:
            payload = event if isinstance(event, str) else json.dumps(event)
            self._lines.append(f"data: {payload}\n".encode("utf-8"))
            self._lines.append(b"\n")

    def readline(self) -> bytes:
        if self._index >= len(self._lines):
            return b""
        line = self._lines[self._index]
        self._index += 1
        return line

    def close(self) -> None:
        return None

    def __enter__(self) -> "_FakeSseResponse":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        return None


class _FakeSocketConnection:
    def __enter__(self) -> "_FakeSocketConnection":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        return None


class _FakeThread:
    def __init__(self, *args, **kwargs) -> None:
        self.args = args
        self.kwargs = kwargs
        self.started = False

    def start(self) -> None:
        self.started = True


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

    def test_llama_backend_health_autodetects_single_advertised_model(self) -> None:
        config = replace(self.config, llama_model=local_broker.DEFAULT_LLAMA_MODEL)

        def fake_urlopen(request, timeout=120):
            self.assertEqual("http://127.0.0.1:18000/v1/models", request.full_url)
            return _FakeJsonResponse({"data": [{"id": "qwen3.5-35b-a3b-q3_k_m"}]})

        with patch.object(
            local_broker.socket,
            "create_connection",
            return_value=_FakeSocketConnection(),
        ):
            with patch.object(local_broker, "urlopen", side_effect=fake_urlopen):
                health = local_broker.llama_backend_health(config)

        self.assertTrue(health["available"])
        self.assertEqual("ready", health["status"])
        self.assertEqual("qwen3.5-35b-a3b-q3_k_m", health["model"])
        self.assertEqual(local_broker.DEFAULT_LLAMA_MODEL, health["configured_model"])
        self.assertEqual(["qwen3.5-35b-a3b-q3_k_m"], health["advertised_models"])
        self.assertEqual("auto_detected", health["model_source"])

    def test_call_llama_completion_includes_target_url_in_errors(self) -> None:
        with patch.object(local_broker, "CONFIG", self.config):
            with patch.object(
                local_broker,
                "urlopen",
                side_effect=URLError(ConnectionRefusedError(61, "Connection refused")),
            ):
                with self.assertRaisesRegex(RuntimeError, re.escape(self.config.llama_url)):
                    local_broker.call_llama_completion([{"role": "user", "content": "hello"}])

    def test_call_llama_completion_includes_reasoning_controls(self) -> None:
        captured: dict[str, object] = {}

        def fake_urlopen(request, timeout=120):
            if request.full_url.endswith("/v1/models"):
                return _FakeJsonResponse({"data": [{"id": self.config.llama_model}]})
            captured["payload"] = json.loads(request.data.decode("utf-8"))
            return _FakeJsonResponse({"choices": [{"message": {"content": "ok"}}]})

        with patch.object(local_broker, "CONFIG", self.config):
            with patch.object(local_broker, "urlopen", side_effect=fake_urlopen):
                local_broker.call_llama_completion(
                    [{"role": "user", "content": "hello"}],
                    chat_template_kwargs={"enable_thinking": True, "clear_thinking": False},
                    reasoning_budget=0,
                )

        payload = captured["payload"]
        self.assertIsInstance(payload, dict)
        self.assertEqual(
            {"enable_thinking": True, "clear_thinking": False},
            payload["chat_template_kwargs"],
        )
        self.assertEqual(0, payload["reasoning_budget"])

    def test_call_llama_completion_omits_max_tokens_by_default(self) -> None:
        captured: dict[str, object] = {}

        def fake_urlopen(request, timeout=120):
            if request.full_url.endswith("/v1/models"):
                return _FakeJsonResponse({"data": [{"id": self.config.llama_model}]})
            captured["payload"] = json.loads(request.data.decode("utf-8"))
            return _FakeJsonResponse({"choices": [{"message": {"content": "ok"}}]})

        with patch.object(local_broker, "CONFIG", self.config):
            with patch.object(local_broker, "urlopen", side_effect=fake_urlopen):
                local_broker.call_llama_completion([{"role": "user", "content": "hello"}])

        payload = captured["payload"]
        self.assertIsInstance(payload, dict)
        self.assertNotIn("max_tokens", payload)

    def test_call_llama_completion_autodetects_model_when_configured_model_is_stale(self) -> None:
        captured: dict[str, object] = {}
        config = replace(self.config, llama_model=local_broker.DEFAULT_LLAMA_MODEL)

        def fake_urlopen(request, timeout=120):
            if request.full_url.endswith("/v1/models"):
                return _FakeJsonResponse({"data": [{"id": "qwen3.5-35b-a3b-q3_k_m"}]})
            captured["payload"] = json.loads(request.data.decode("utf-8"))
            return _FakeJsonResponse({"choices": [{"message": {"content": "ok"}}]})

        with patch.object(local_broker, "CONFIG", config):
            with patch.object(local_broker, "urlopen", side_effect=fake_urlopen):
                local_broker.call_llama_completion([{"role": "user", "content": "hello"}])

        payload = captured["payload"]
        self.assertIsInstance(payload, dict)
        self.assertEqual("qwen3.5-35b-a3b-q3_k_m", payload["model"])


class ThinkingParsingTest(unittest.TestCase):
    def test_split_stream_text_hides_explicit_think_tags(self) -> None:
        answer, reasoning = local_broker.split_stream_text(
            "<think>Need to compare the options.</think>\nFinal answer."
        )

        self.assertEqual("Final answer.", answer)
        self.assertEqual("Need to compare the options.", reasoning)

    def test_split_stream_text_keeps_plain_analysis_headers_by_default(self) -> None:
        raw_text = "Analysis:\nNeed to compare the options.\n\nFinal answer."

        answer, reasoning = local_broker.split_stream_text(raw_text)

        self.assertEqual(raw_text, answer)
        self.assertEqual("", reasoning)

    def test_split_stream_text_keeps_unmarked_first_person_paragraphs_by_default(self) -> None:
        raw_text = "I should compare the options first.\n\nThe answer is to use the cheaper plan."

        answer, reasoning = local_broker.split_stream_text(raw_text)

        self.assertEqual(raw_text, answer)
        self.assertEqual("", reasoning)

    def test_call_llama_uses_server_reasoning_content(self) -> None:
        payload = {
            "choices": [
                {
                    "message": {
                        "content": "Final answer.",
                        "reasoning_content": "Need to compare the options.",
                    }
                }
            ]
        }

        def fake_urlopen(request, timeout=120):
            if request.full_url.endswith("/v1/models"):
                return _FakeJsonResponse({"data": [{"id": local_broker.DEFAULT_LLAMA_MODEL}]})
            return _FakeJsonResponse(payload)

        with patch.object(local_broker, "CONFIG", replace(local_broker.CONFIG, llama_url="http://127.0.0.1:18000/v1/chat/completions")):
            with patch.object(
                local_broker.socket,
                "create_connection",
                return_value=_FakeSocketConnection(),
            ):
                with patch.object(local_broker, "urlopen", side_effect=fake_urlopen):
                    answer, reasoning = local_broker.call_llama([{"role": "user", "content": "hello"}])

        self.assertEqual("Final answer.", answer)
        self.assertEqual("Need to compare the options.", reasoning)

    def test_call_llama_stream_uses_server_reasoning_deltas(self) -> None:
        events = [
            {"choices": [{"delta": {"reasoning_content": "Need to "}}]},
            {"choices": [{"delta": {"reasoning_content": "compare the options."}}]},
            {"choices": [{"delta": {"content": "Final "}}]},
            {"choices": [{"delta": {"content": "answer."}}]},
            "[DONE]",
        ]
        states: list[tuple[str, str]] = []

        def fake_urlopen(request, timeout=120):
            if request.full_url.endswith("/v1/models"):
                return _FakeJsonResponse({"data": [{"id": local_broker.DEFAULT_LLAMA_MODEL}]})
            return _FakeSseResponse(events)

        with patch.object(local_broker, "CONFIG", replace(local_broker.CONFIG, llama_url="http://127.0.0.1:18000/v1/chat/completions")):
            with patch.object(
                local_broker.socket,
                "create_connection",
                return_value=_FakeSocketConnection(),
            ):
                with patch.object(local_broker, "urlopen", side_effect=fake_urlopen):
                    answer, reasoning = local_broker.call_llama_stream(
                        [{"role": "user", "content": "hello"}],
                        on_state_delta=lambda current_answer, current_reasoning: states.append(
                            (current_answer, current_reasoning)
                        ),
                    )

        self.assertEqual("Final answer.", answer)
        self.assertEqual("Need to compare the options.", reasoning)
        self.assertIn(("", "Need to compare the options."), states)
        self.assertEqual(("Final answer.", "Need to compare the options."), states[-1])

    def test_start_run_persists_llama_reasoning_controls(self) -> None:
        manager = local_broker.CodexRunManager(local_broker.CONFIG.data_dir)

        with patch.object(local_broker.threading, "Thread", return_value=_FakeThread()):
            with patch.object(local_broker, "ensure_llama_backend_available", return_value=None):
                result = manager.start_run(
                    {
                        "session_id": "run_llama_controls",
                        "backend": "llama",
                        "prompt": "hello",
                        "chat_template_kwargs": "{\"enable_thinking\":false,\"clear_thinking\":false}",
                        "reasoning_budget": -1,
                    }
                )

        run = manager._runs[result["run_id"]]
        self.assertEqual(
            {
                "chat_template_kwargs": {
                    "enable_thinking": False,
                    "clear_thinking": False,
                },
                "reasoning_budget": -1,
            },
            run["_llama_request_options"],
        )
        self.assertEqual(
            {
                "chat_template_kwargs": {
                    "enable_thinking": False,
                    "clear_thinking": False,
                },
                "reasoning_budget": -1,
            },
            result["backend_metadata"]["llama_request_options"],
        )


class CodexRunPersistenceTest(unittest.TestCase):
    def test_run_llama_loop_uses_hidden_prompt_suffix_without_mutating_conversation(self) -> None:
        manager = local_broker.CodexRunManager(local_broker.CONFIG.data_dir)
        session_id = "conv_llama_hidden_suffix"
        prompt = "Where is the main claim?"
        suffix = "Selected passage:\n> The paper introduces a new method."
        conversation = local_broker.CONVERSATIONS.get_or_create(session_id)
        conversation["messages"] = [{"role": "user", "content": prompt}]
        stamp = local_broker.now_iso()
        run = {
            "run_id": "run_llama_hidden_suffix",
            "conversation_id": session_id,
            "backend": "llama",
            "status": "thinking",
            "created_at": stamp,
            "updated_at": stamp,
            "completed_at": None,
            "assistant_text": "",
            "reasoning_text": "",
            "risk_flags": [],
            "backend_metadata": {},
            "pending_approval": None,
            "events": [],
            "next_seq": 1,
            "last_error": None,
            "cancel_requested": False,
            "_approval_decision": None,
            "_prompt": prompt,
            "_request_prompt_suffix": suffix,
            "_page_context": None,
            "_force_browser_action": False,
            "_allowed_hosts": [],
            "_llama_request_options": {},
        }

        with manager._condition:
            manager._runs[run["run_id"]] = run

        with patch.object(local_broker, "call_llama_stream", return_value=("Final answer.", "")) as mock_call:
            answer, reasoning = manager._run_llama_loop(run["run_id"])

        self.assertEqual("Final answer.", answer)
        self.assertEqual("", reasoning)
        self.assertEqual(prompt, conversation["messages"][-1]["content"])
        self.assertEqual(
            f"{prompt}\n\n{suffix}",
            mock_call.call_args.args[0][-1]["content"],
        )

    def test_run_llama_loop_wraps_browser_agent_result(self) -> None:
        manager = local_broker.CodexRunManager(local_broker.CONFIG.data_dir)
        conversation = local_broker.CONVERSATIONS.get_or_create("conv_llama_browser_action")
        conversation["messages"] = [{"role": "user", "content": "show me where the page explains the claim"}]
        stamp = local_broker.now_iso()
        run = {
            "run_id": "run_llama_browser_action",
            "conversation_id": "conv_llama_browser_action",
            "backend": "llama",
            "status": "thinking",
            "created_at": stamp,
            "updated_at": stamp,
            "completed_at": None,
            "assistant_text": "",
            "reasoning_text": "",
            "risk_flags": [],
            "backend_metadata": {},
            "pending_approval": None,
            "events": [],
            "next_seq": 1,
            "last_error": None,
            "cancel_requested": False,
            "_approval_decision": None,
            "_prompt": "show me where the page explains the claim",
            "_page_context": None,
            "_force_browser_action": True,
            "_allowed_hosts": ["example.com"],
            "_llama_request_options": {},
        }

        with manager._condition:
            manager._runs[run["run_id"]] = run

        with patch.object(local_broker.EXTENSION_RELAY, "health", return_value={"connected_clients": 1}):
            with patch.object(
                local_broker,
                "run_llama_browser_agent",
                return_value="Highlighted the relevant section.",
            ) as mock_agent:
                answer, reasoning = manager._run_llama_loop(run["run_id"])

        self.assertEqual("Highlighted the relevant section.", answer)
        self.assertEqual("", reasoning)
        self.assertEqual("conv_llama_browser_action", mock_agent.call_args.args[0])

    def test_finish_run_persists_reasoning_only_assistant_message(self) -> None:
        manager = local_broker.CodexRunManager(local_broker.CONFIG.data_dir)
        stamp = local_broker.now_iso()
        run = {
            "run_id": "run_reasoning_only",
            "conversation_id": "conv_reasoning_only",
            "backend": "llama",
            "status": "thinking",
            "created_at": stamp,
            "updated_at": stamp,
            "completed_at": None,
            "assistant_text": "",
            "reasoning_text": "",
            "risk_flags": [],
            "backend_metadata": {},
            "pending_approval": None,
            "events": [],
            "next_seq": 1,
            "last_error": None,
            "cancel_requested": False,
            "_approval_decision": None,
        }

        with manager._condition:
            manager._runs[run["run_id"]] = run
            manager._finish_run_locked(
                run,
                "completed",
                assistant_text="",
                reasoning_text="First thought.\n\nSecond thought.",
                emit_type="completed",
                emit_message="Run completed.",
            )

        conversation = local_broker.CONVERSATIONS.get_or_create("conv_reasoning_only")
        assistant_message = conversation["messages"][-1]
        self.assertEqual("assistant", assistant_message["role"])
        self.assertEqual("", assistant_message["content"])
        self.assertEqual(
            ["First thought.", "Second thought."],
            assistant_message["reasoning_blocks"],
        )


if __name__ == "__main__":
    unittest.main()
