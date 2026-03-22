import atexit
import io
import json
import os
import re
import shutil
import tempfile
from pathlib import Path
import unittest
from dataclasses import replace
from urllib.error import HTTPError, URLError
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


def _http_error(url: str, status: int, payload: dict[str, object] | str) -> HTTPError:
    if isinstance(payload, str):
        body = payload.encode("utf-8")
    else:
        body = json.dumps(payload).encode("utf-8")
    return HTTPError(url, status, "error", hdrs=None, fp=io.BytesIO(body))


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
        mlx_config = replace(
            self.config,
            mlx_url="http://127.0.0.1:18001/v1/chat/completions",
            mlx_model="test-mlx-model",
        )

        with patch.object(local_broker, "CONFIG", mlx_config):
            with patch.object(
                local_broker.socket,
                "create_connection",
                side_effect=ConnectionRefusedError(61, "Connection refused"),
            ):
                payload = local_broker.build_models_payload()

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

    def test_llama_backend_health_retries_model_discovery_without_auth_for_loopback_invalid_api_key(self) -> None:
        config = replace(
            self.config,
            llama_model=local_broker.DEFAULT_LLAMA_MODEL,
            llama_api_key="bad-key",
        )
        auth_headers: list[str | None] = []

        def fake_urlopen(request, timeout=120):
            auth_headers.append(request.get_header("Authorization"))
            if request.get_header("Authorization"):
                raise _http_error(
                    request.full_url,
                    401,
                    {"error": {"message": "Invalid API Key"}},
                )
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
        self.assertEqual([f"Bearer bad-key", None], auth_headers)
        self.assertEqual("", health["last_error"])

    def test_llama_backend_health_reports_auth_error_when_invalid_key_persists(self) -> None:
        config = replace(
            self.config,
            llama_model=local_broker.DEFAULT_LLAMA_MODEL,
            llama_api_key="bad-key",
        )

        def fake_urlopen(request, timeout=120):
            raise _http_error(
                request.full_url,
                401,
                {"error": {"message": "Invalid API Key"}},
            )

        with patch.object(
            local_broker.socket,
            "create_connection",
            return_value=_FakeSocketConnection(),
        ):
            with patch.object(local_broker, "urlopen", side_effect=fake_urlopen):
                health = local_broker.llama_backend_health(config)

        self.assertFalse(health["available"])
        self.assertEqual("auth_error", health["status"])
        self.assertIn("Invalid API Key", health["last_error"])
        self.assertIn("Clear LLAMA_API_KEY", health["last_error"])

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

    def test_call_llama_completion_retries_without_auth_for_loopback_invalid_api_key(self) -> None:
        captured_auth_headers: list[str | None] = []

        def fake_urlopen(request, timeout=120):
            captured_auth_headers.append(request.get_header("Authorization"))
            if request.get_header("Authorization"):
                raise _http_error(
                    request.full_url,
                    401,
                    {"error": {"message": "Invalid API Key"}},
                )
            return _FakeJsonResponse({"choices": [{"message": {"content": "ok"}}]})

        config = replace(self.config, llama_api_key="bad-key")

        with patch.object(local_broker, "CONFIG", config):
            with patch.object(local_broker, "urlopen", side_effect=fake_urlopen):
                parsed = local_broker.call_llama_completion(
                    [{"role": "user", "content": "hello"}],
                    resolved_model=self.config.llama_model,
                )

        self.assertEqual("ok", parsed["choices"][0]["message"]["content"])
        self.assertEqual([f"Bearer bad-key", None], captured_auth_headers)

    def test_call_llama_completion_stream_retries_without_auth_for_loopback_invalid_api_key(self) -> None:
        captured_auth_headers: list[str | None] = []

        def fake_urlopen(request, timeout=120):
            captured_auth_headers.append(request.get_header("Authorization"))
            if request.get_header("Authorization"):
                raise _http_error(
                    request.full_url,
                    401,
                    {"error": {"message": "Invalid API Key"}},
                )
            return _FakeSseResponse(
                [
                    {"choices": [{"delta": {"content": "Final "}}]},
                    {"choices": [{"delta": {"content": "answer."}}]},
                    "[DONE]",
                ]
            )

        config = replace(self.config, llama_api_key="bad-key")

        with patch.object(local_broker, "CONFIG", config):
            with patch.object(local_broker, "urlopen", side_effect=fake_urlopen):
                answer, reasoning = local_broker.call_llama_completion_stream(
                    [{"role": "user", "content": "hello"}],
                    resolved_model=self.config.llama_model,
                )

        self.assertEqual("Final answer.", answer)
        self.assertEqual("", reasoning)
        self.assertEqual([f"Bearer bad-key", None], captured_auth_headers)


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


class BrowserConfigTest(unittest.TestCase):
    def setUp(self) -> None:
        self.config_dir = tempfile.mkdtemp(prefix="assist-test-browser-config-")
        self.addCleanup(shutil.rmtree, self.config_dir, ignore_errors=True)
        self.manager = local_broker.BrowserConfigManager(Path(self.config_dir))

    def test_default_browser_config_is_unlimited(self) -> None:
        payload = self.manager.config()
        self.assertEqual(0, payload["agent_max_steps"])
        self.assertEqual(1, payload["limits"]["agent_max_steps"]["min"])
        self.assertIsNone(payload["limits"]["agent_max_steps"]["max"])

    def test_update_browser_agent_steps_accepts_unlimited_and_large_values(self) -> None:
        unlimited = self.manager.update_config({"agent_max_steps": 0})
        self.assertEqual(0, unlimited["agent_max_steps"])

        large = self.manager.update_config({"agent_max_steps": 500})
        self.assertEqual(500, large["agent_max_steps"])
        reloaded = local_broker.BrowserConfigManager(Path(self.config_dir))
        self.assertEqual(500, reloaded.agent_max_steps())

    def test_update_browser_agent_steps_rejects_negative_values(self) -> None:
        with self.assertRaises(ValueError):
            self.manager.update_config({"agent_max_steps": -1})

    def test_invalid_persisted_browser_config_defaults_to_unlimited(self) -> None:
        config_path = Path(self.config_dir) / "browser_config.json"
        config_path.write_text('{"agent_max_steps": -3}', encoding="utf-8")
        reloaded = local_broker.BrowserConfigManager(Path(self.config_dir))
        self.assertEqual(0, reloaded.agent_max_steps())

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


class BrowserToolCatalogTest(unittest.TestCase):
    def test_model_browser_tools_expose_compact_catalog(self) -> None:
        llama_names = {tool["function"]["name"] for tool in local_broker.LLAMA_BROWSER_TOOLS}
        codex_names = {tool["name"] for tool in local_broker.CODEX_BROWSER_TOOLS}

        self.assertEqual(
            {"browser.navigate", "browser.tabs", "browser.read", "browser.interact"},
            llama_names,
        )
        self.assertEqual(llama_names, codex_names)

    def test_low_level_browser_tools_remain_available_for_broker_and_mcp(self) -> None:
        self.assertIn("browser.click", local_broker.BROWSER_COMMAND_METHODS)
        self.assertIn("browser.find_one", local_broker.PROXIED_BROWSER_TOOL_NAMES)
        self.assertIn("browser.session_create", local_broker.BROWSER_TOOL_NAMES)
        self.assertNotIn("browser.tabs", local_broker.BROWSER_TOOL_NAMES)
        self.assertNotIn("browser.read", local_broker.BROWSER_TOOL_NAMES)
        self.assertNotIn("browser.interact", local_broker.BROWSER_TOOL_NAMES)

    def test_translate_model_browser_tool_maps_read_find_to_find_one(self) -> None:
        translated = local_broker.translate_model_browser_tool(
            "browser.read",
            {"action": "find", "selector": "#search"},
        )

        self.assertEqual("browser.find_one", translated["tool_name"])
        self.assertEqual({"locator": {"selector": "#search"}}, translated["args"])
        self.assertEqual("auto", translated["approval"])

    def test_translate_model_browser_tool_maps_navigate_new_tab_to_open_tab(self) -> None:
        translated = local_broker.translate_model_browser_tool(
            "browser.navigate",
            {"url": "https://example.com", "newTab": True},
        )

        self.assertEqual("browser.open_tab", translated["tool_name"])
        self.assertEqual({"url": "https://example.com"}, translated["args"])
        self.assertEqual("manual", translated["approval"])

    def test_translate_model_browser_tool_maps_interact_type_to_type(self) -> None:
        translated = local_broker.translate_model_browser_tool(
            "browser.interact",
            {"action": "type", "selector": "#email", "text": "masked-input", "clear": True},
        )

        self.assertEqual("browser.type", translated["tool_name"])
        self.assertEqual(
            {
                "selector": "#email",
                "text": "masked-input",
                "clear": True,
            },
            translated["args"],
        )
        self.assertEqual("manual", translated["approval"])


class BrowserElementContextTest(unittest.TestCase):
    def test_normalize_browser_element_context_strips_query_and_fragment_from_url(self) -> None:
        normalized = local_broker.normalize_browser_element_context(
            {
                "selector": "#submit",
                "url": "https://example.com/form?email=masked#submit",
            }
        )

        self.assertEqual(
            {
                "selector": "#submit",
                "url": "https://example.com/form",
            },
            normalized,
        )

    def test_normalize_browser_element_context_ignores_textual_payload_fields(self) -> None:
        normalized = local_broker.normalize_browser_element_context(
            {
                "selector": "#submit",
                "xpath": "//button[@id='submit']",
                "tagName": "BUTTON",
                "role": "button",
                "label": "Save draft",
                "name": "submit",
                "placeholder": "Save draft",
                "url": "https://example.com/form",
                "title": "Example Form",
                "tabId": 17,
                "pickedAt": "2026-03-22T12:00:00Z",
                "x": 120,
                "y": 220,
                "width": 80,
                "height": 30,
                "textPreview": "Redacted value",
                "valuePreview": "Redacted value",
                "href": "https://example.com/form#submit",
            }
        )

        self.assertEqual(
            {
                "selector": "#submit",
                "xpath": "//button[@id='submit']",
                "tag_name": "button",
                "role": "button",
                "label": "Save draft",
                "name": "submit",
                "placeholder": "Save draft",
                "url": "https://example.com/form",
                "title": "Example Form",
                "tab_id": 17,
                "picked_at": "2026-03-22T12:00:00Z",
                "x": 120,
                "y": 220,
                "width": 80,
                "height": 30,
            },
            normalized,
        )

    def test_format_browser_element_context_is_structural_only(self) -> None:
        formatted = local_broker.format_browser_element_context(
            {
                "selector": "#submit",
                "xpath": "//button[@id='submit']",
                "tag_name": "button",
                "role": "button",
                "label": "Save draft",
                "name": "submit",
                "placeholder": "Save draft",
                "url": "https://example.com/form",
                "title": "Example Form",
                "x": 120,
                "y": 220,
                "width": 80,
                "height": 30,
                "picked_at": "2026-03-22T12:00:00Z",
            }
        )

        self.assertIn("Page title: Example Form", formatted)
        self.assertIn("Page URL: https://example.com/form", formatted)
        self.assertIn("CSS selector: #submit", formatted)
        self.assertIn("XPath: //button[@id='submit']", formatted)
        self.assertIn("Tag: button", formatted)
        self.assertIn("Role: button", formatted)
        self.assertIn("Label: Save draft", formatted)
        self.assertIn("Name: submit", formatted)
        self.assertIn("Placeholder: Save draft", formatted)
        self.assertIn("Bounds: x=120, y=220, width=80, height=30", formatted)
        self.assertIn("Picked at: 2026-03-22T12:00:00Z", formatted)
        self.assertNotIn("Text:", formatted)
        self.assertNotIn("Value:", formatted)
        self.assertNotIn("Href:", formatted)

    def test_compose_request_prompt_includes_browser_element_context(self) -> None:
        prompt = local_broker.compose_request_prompt(
            "Find the submit button.",
            "Only use the current page.",
            "Page title: Example",
            "CSS selector: #submit",
        )

        self.assertIn("[Selected Browser Element]\nCSS selector: #submit", prompt)

    def test_start_run_stores_normalized_browser_element_context(self) -> None:
        manager = local_broker.CodexRunManager(local_broker.CONFIG.data_dir)

        with patch.object(local_broker.threading, "Thread", return_value=_FakeThread()):
            with patch.object(local_broker, "ensure_llama_backend_available", return_value=None):
                result = manager.start_run(
                    {
                        "session_id": "run_browser_element_context",
                        "backend": "llama",
                        "prompt": "Click the selected element.",
                        "browser_element_context": {
                            "selector": "#submit",
                            "xpath": "//button[@id='submit']",
                            "tagName": "BUTTON",
                            "role": "button",
                            "label": "Save draft",
                            "name": "submit",
                            "placeholder": "Save draft",
                            "url": "https://example.com/form",
                            "title": "Example Form",
                            "tabId": 17,
                            "pickedAt": "2026-03-22T12:00:00Z",
                            "x": 120,
                            "y": 220,
                            "width": 80,
                            "height": 30,
                            "textPreview": "Redacted value",
                            "valuePreview": "Redacted value",
                            "href": "https://example.com/form#submit",
                        },
                    }
                )

        run = manager._runs[result["run_id"]]
        self.assertEqual(
            {
                "title": "Example Form",
                "url": "https://example.com/form",
                "selector": "#submit",
                "xpath": "//button[@id='submit']",
                "tag_name": "button",
                "role": "button",
                "label": "Save draft",
                "name": "submit",
                "placeholder": "Save draft",
                "tab_id": 17,
                "picked_at": "2026-03-22T12:00:00Z",
                "x": 120,
                "y": 220,
                "width": 80,
                "height": 30,
            },
            run["_browser_element_context"],
        )
        self.assertNotIn("text_preview", run["_browser_element_context"])
        self.assertNotIn("value_preview", run["_browser_element_context"])
        self.assertNotIn("href", run["_browser_element_context"])

    def test_start_run_rejects_invalid_browser_element_context(self) -> None:
        manager = local_broker.CodexRunManager(local_broker.CONFIG.data_dir)

        with patch.object(local_broker.threading, "Thread", return_value=_FakeThread()):
            with patch.object(local_broker, "ensure_llama_backend_available", return_value=None):
                with self.assertRaisesRegex(ValueError, "browser_element_context is invalid"):
                    manager.start_run(
                        {
                            "session_id": "run_invalid_browser_element_context",
                            "backend": "llama",
                            "prompt": "Click the selected element.",
                            "browser_element_context": {"foo": "bar"},
                        }
                    )


class BrowserRuntimeContextTest(unittest.TestCase):
    def test_normalize_browser_runtime_context_strips_query_and_fragment_from_url(self) -> None:
        normalized = local_broker.normalize_browser_runtime_context(
            {
                "tabId": "17",
                "url": "https://example.com/form?search=private#results",
                "title": "Example Form",
                "host": "example.com",
            }
        )

        self.assertEqual(
            {
                "tab_id": 17,
                "url": "https://example.com/form",
                "title": "Example Form",
                "host": "example.com",
            },
            normalized,
        )

    def test_normalize_browser_runtime_context_ignores_content_fields(self) -> None:
        normalized = local_broker.normalize_browser_runtime_context(
            {
                "tabId": "17",
                "url": "https://example.com/form",
                "title": "Example Form",
                "host": "example.com",
                "allowlisted": True,
                "active": True,
                "selection": "sensitive selection",
                "text_excerpt": "sensitive page text",
            }
        )

        self.assertEqual(
            {
                "tab_id": 17,
                "url": "https://example.com/form",
                "title": "Example Form",
                "host": "example.com",
                "allowlisted": True,
                "active": True,
            },
            normalized,
        )

    def test_format_browser_runtime_context_is_minimal_metadata_only(self) -> None:
        formatted = local_broker.format_browser_runtime_context(
            {
                "tab_id": 17,
                "url": "https://example.com/form",
                "title": "Example Form",
                "host": "example.com",
                "allowlisted": True,
                "active": True,
            }
        )

        self.assertIn("Tab ID: 17", formatted)
        self.assertIn("URL: https://example.com/form", formatted)
        self.assertIn("Title: Example Form", formatted)
        self.assertIn("Host: example.com", formatted)
        self.assertIn("Allowlisted: true", formatted)
        self.assertIn("Active: true", formatted)
        self.assertNotIn("selection", formatted.lower())
        self.assertNotIn("text excerpt", formatted.lower())

    def test_compose_request_prompt_includes_browser_runtime_context_without_page_context(self) -> None:
        runtime_context = local_broker.format_browser_runtime_context(
            {
                "tab_id": 17,
                "url": "https://example.com/form",
                "title": "Example Form",
                "host": "example.com",
                "allowlisted": True,
                "active": True,
            }
        )
        prompt = local_broker.compose_request_prompt(
            "Inspect the current page.",
            "Only use browser tools when needed.",
            "",
            "",
            runtime_context,
        )

        self.assertIn("https://example.com/form", prompt)
        self.assertIn("example.com", prompt)
        self.assertNotIn("[Page Context]", prompt)

    def test_start_run_stores_browser_runtime_context_without_page_context(self) -> None:
        manager = local_broker.CodexRunManager(local_broker.CONFIG.data_dir)

        with patch.object(local_broker.threading, "Thread", return_value=_FakeThread()):
            with patch.object(local_broker.EXTENSION_RELAY, "health", return_value={"connected_clients": 1}):
                with patch.object(local_broker, "ensure_llama_backend_available", return_value=None):
                    result = manager.start_run(
                        {
                            "session_id": "run_browser_runtime_context",
                            "backend": "llama",
                            "prompt": "Inspect the current page.",
                            "allowed_hosts": ["example.com"],
                            "force_browser_action": True,
                            "browser_runtime_context": {
                                "tabId": "17",
                                "url": "https://example.com/form",
                                "title": "Example Form",
                                "host": "example.com",
                                "allowlisted": True,
                                "active": True,
                                "selection": "sensitive selection",
                                "text_excerpt": "sensitive page text",
                            },
                        }
                    )

        run = manager._runs[result["run_id"]]
        conversation = local_broker.CONVERSATIONS.get("run_browser_runtime_context")

        self.assertIsNone(run["_page_context"])
        self.assertEqual(
            {
                "tab_id": 17,
                "url": "https://example.com/form",
                "title": "Example Form",
                "host": "example.com",
                "allowlisted": True,
                "active": True,
            },
            run["_browser_runtime_context"],
        )
        self.assertNotIn("browser_runtime_context", conversation["codex"])
        self.assertIsNone(conversation["codex"].get("page_context_payload"))


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

    def test_run_llama_loop_includes_browser_element_context_without_mutating_conversation(self) -> None:
        manager = local_broker.CodexRunManager(local_broker.CONFIG.data_dir)
        session_id = "conv_llama_browser_element"
        prompt = "Use the selected element."
        conversation = local_broker.CONVERSATIONS.get_or_create(session_id)
        conversation["messages"] = [{"role": "user", "content": prompt}]
        stamp = local_broker.now_iso()
        run = {
            "run_id": "run_llama_browser_element",
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
            "_request_prompt_suffix": "",
            "_page_context": None,
            "_browser_element_context": {
                "selector": "#submit",
                "xpath": "//button[@id='submit']",
                "tag_name": "button",
                "role": "button",
                "label": "Save draft",
                "name": "submit",
                "placeholder": "Save draft",
                "url": "https://example.com/form",
                "title": "Example Form",
                "tab_id": 17,
                "picked_at": "2026-03-22T12:00:00Z",
                "x": 120,
                "y": 220,
                "width": 80,
                "height": 30,
            },
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
            (
                f"{prompt}\n\n[Selected Browser Element]\n"
                "Tab ID: 17\n"
                "Page title: Example Form\n"
                "Page URL: https://example.com/form\n"
                "CSS selector: #submit\n"
                "XPath: //button[@id='submit']\n"
                "Tag: button\n"
                "Role: button\n"
                "Label: Save draft\n"
                "Name: submit\n"
                "Placeholder: Save draft\n"
                "Bounds: x=120, y=220, width=80, height=30\n"
                "Picked at: 2026-03-22T12:00:00Z"
            ),
            mock_call.call_args.args[0][-1]["content"],
        )

    def test_run_llama_loop_includes_browser_runtime_context_without_mutating_conversation(self) -> None:
        manager = local_broker.CodexRunManager(local_broker.CONFIG.data_dir)
        session_id = "conv_llama_browser_runtime"
        prompt = "Inspect the current browser tab."
        conversation = local_broker.CONVERSATIONS.get_or_create(session_id)
        conversation["messages"] = [{"role": "user", "content": prompt}]
        stamp = local_broker.now_iso()
        run = {
            "run_id": "run_llama_browser_runtime",
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
            "_request_prompt_suffix": "",
            "_page_context": None,
            "_browser_runtime_context": {
                "tab_id": 17,
                "url": "https://example.com/form",
                "title": "Example Form",
                "host": "example.com",
                "allowlisted": True,
                "active": True,
            },
            "_browser_element_context": None,
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
        self.assertIn("Example Form", mock_call.call_args.args[0][-1]["content"])
        self.assertIn("https://example.com/form", mock_call.call_args.args[0][-1]["content"])
        self.assertNotIn("[Page Context]", mock_call.call_args.args[0][-1]["content"])

    def test_run_llama_loop_force_browser_action_does_not_inject_page_context_by_default(self) -> None:
        manager = local_broker.CodexRunManager(local_broker.CONFIG.data_dir)
        session_id = "conv_llama_forced_browser_runtime"
        prompt = "Use browser tools to inspect the current tab."
        conversation = local_broker.CONVERSATIONS.get_or_create(session_id)
        conversation["messages"] = [{"role": "user", "content": prompt}]
        stamp = local_broker.now_iso()
        run = {
            "run_id": "run_llama_forced_browser_runtime",
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
            "_request_prompt_suffix": "",
            "_page_context": None,
            "_browser_runtime_context": {
                "tab_id": 17,
                "url": "https://example.com/form",
                "title": "Example Form",
                "host": "example.com",
                "allowlisted": True,
                "active": True,
            },
            "_browser_element_context": None,
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
        self.assertEqual("conv_llama_forced_browser_runtime", mock_agent.call_args.args[0])
        self.assertIn("Example Form", mock_agent.call_args.args[1][-1]["content"])
        self.assertIn("https://example.com/form", mock_agent.call_args.args[1][-1]["content"])
        self.assertNotIn("[Page Context]", mock_agent.call_args.args[1][-1]["content"])

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

    def test_execute_function_call_skips_manual_approval_when_browser_mode_forced(self) -> None:
        manager = local_broker.CodexRunManager(local_broker.CONFIG.data_dir)
        stamp = local_broker.now_iso()
        run = {
            "run_id": "run_forced_browser_tool",
            "conversation_id": "conv_forced_browser_tool",
            "backend": "codex",
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
            "_force_browser_action": True,
            "_browser_session": {"sessionId": "session_1", "capabilityToken": "capability"},
            "_browser_run": {"runId": "run_remote_1"},
        }

        with manager._condition:
            manager._runs[run["run_id"]] = run

        with patch.object(
            local_broker.BROWSER_AUTOMATION,
            "execute_tool",
            return_value={
                "success": True,
                "tool": "browser.click",
                "data": {"ok": True},
                "error": None,
                "policy": None,
            },
        ) as mock_execute:
            result = manager._execute_function_call(
                run["run_id"],
                {
                    "name": "browser.interact",
                    "arguments": json.dumps({"action": "click", "selector": "#submit"}),
                    "call_id": "call_1",
                },
            )

        self.assertEqual("function_call_output", result["type"])
        self.assertEqual("call_1", result["call_id"])
        self.assertIsNone(run["pending_approval"])
        self.assertFalse(any(event["type"] == "waiting_approval" for event in run["events"]))
        mock_execute.assert_called_once()

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


class BrowserAgentLoopTest(unittest.TestCase):
    def test_run_local_backend_browser_agent_uses_forced_browser_prompt(self) -> None:
        response = {
            "choices": [
                {
                    "message": {
                        "content": "Done.",
                        "tool_calls": [],
                    }
                }
            ]
        }

        with patch.object(local_broker, "ensure_local_backend_available", return_value={"model": "test-model"}):
            with patch.object(local_broker, "local_backend_settings", return_value={"default_model": "test-model"}):
                with patch.object(local_broker, "call_local_backend_completion", return_value=response) as mock_call:
                    with patch.object(local_broker.BROWSER_AUTOMATION, "session_create", return_value={"sessionId": "s1", "capabilityToken": "cap"}):
                        with patch.object(local_broker.BROWSER_AUTOMATION, "run_start", return_value={"runId": "r1"}):
                            with patch.object(local_broker.BROWSER_AUTOMATION, "close_session"):
                                result = local_broker.run_local_backend_browser_agent(
                                    "session-id",
                                    [{"role": "user", "content": "Open the page and click the submit button."}],
                                    ["example.com"],
                                    1,
                                    backend="llama",
                                )

        self.assertEqual("Done.", result)
        self.assertIn(
            "Browser action mode is explicitly enabled for this request.",
            mock_call.call_args.args[1][0]["content"],
        )
        self.assertEqual("required", mock_call.call_args.kwargs["tool_choice"])
        self.assertEqual(
            local_broker.CONFIG.local_backend_browser_timeout_sec,
            mock_call.call_args.kwargs["timeout_sec"],
        )

    def test_run_local_backend_browser_agent_unlimited_mode_allows_multiple_tool_turns(self) -> None:
        responses = [
            {
                "choices": [
                    {
                        "message": {
                            "content": "",
                            "tool_calls": [
                                {
                                    "id": "toolcall_1",
                                    "function": {
                                        "name": "browser.read",
                                        "arguments": json.dumps({"action": "page_digest"}),
                                    },
                                }
                            ],
                        }
                    }
                ]
            },
            {
                "choices": [
                    {
                        "message": {
                            "content": "Done.",
                            "tool_calls": [],
                        }
                    }
                ]
            },
        ]

        with patch.object(local_broker, "ensure_local_backend_available", return_value={"model": "test-model"}):
            with patch.object(local_broker, "local_backend_settings", return_value={"default_model": "test-model"}):
                with patch.object(local_broker, "call_local_backend_completion", side_effect=responses) as mock_call:
                    with patch.object(local_broker.BROWSER_AUTOMATION, "session_create", return_value={"sessionId": "s1", "capabilityToken": "cap"}):
                        with patch.object(local_broker.BROWSER_AUTOMATION, "run_start", return_value={"runId": "r1"}):
                            with patch.object(local_broker.BROWSER_AUTOMATION, "execute_tool", return_value={"success": True, "data": {}, "error": None, "policy": None}):
                                with patch.object(local_broker.BROWSER_AUTOMATION, "close_session"):
                                    result = local_broker.run_local_backend_browser_agent(
                                        "session-id",
                                        [{"role": "user", "content": "Find the page summary."}],
                                        ["example.com"],
                                        0,
                                        backend="llama",
                                    )

        self.assertEqual("Done.", result)
        self.assertEqual(2, mock_call.call_count)
        self.assertEqual("required", mock_call.call_args_list[0].kwargs["tool_choice"])
        self.assertEqual("auto", mock_call.call_args_list[1].kwargs["tool_choice"])

    def test_run_local_backend_browser_agent_finite_mode_stops_after_one_tool_round(self) -> None:
        response = {
            "choices": [
                {
                    "message": {
                        "content": "",
                        "tool_calls": [
                            {
                                "id": "toolcall_1",
                                "function": {
                                    "name": "browser.read",
                                    "arguments": json.dumps({"action": "page_digest"}),
                                },
                            }
                        ],
                    }
                }
            ]
        }

        with patch.object(local_broker, "ensure_local_backend_available", return_value={"model": "test-model"}):
            with patch.object(local_broker, "local_backend_settings", return_value={"default_model": "test-model"}):
                with patch.object(local_broker, "call_local_backend_completion", return_value=response) as mock_call:
                    with patch.object(local_broker.BROWSER_AUTOMATION, "session_create", return_value={"sessionId": "s1", "capabilityToken": "cap"}):
                        with patch.object(local_broker.BROWSER_AUTOMATION, "run_start", return_value={"runId": "r1"}):
                            with patch.object(local_broker.BROWSER_AUTOMATION, "execute_tool", return_value={"success": True, "data": {}, "error": None, "policy": None}):
                                with patch.object(local_broker.BROWSER_AUTOMATION, "close_session"):
                                    result = local_broker.run_local_backend_browser_agent(
                                        "session-id",
                                        [{"role": "user", "content": "Keep reading."}],
                                        ["example.com"],
                                        1,
                                        backend="llama",
                                    )

        self.assertEqual("I could not complete the browser task within the allowed number of steps.", result)
        self.assertEqual(1, mock_call.call_count)

    def test_browser_profile_suffix_reduces_runtime_for_repeated_google_signup_flow(self) -> None:
        profile_task = (
            "Open a new browser tab, go to google.ca, click Create account, "
            "select 'For my personal use', type John Doe, and finish."
        )
        base_messages = [{"role": "user", "content": profile_task}]
        profile_messages = [
            {
                "role": "user",
                "content": (
                    f"{profile_task}\n\n"
                    "Browser workflow profile: Google signup flow [google-signup]\n"
                    "Steps:\n"
                    "1. Open google.ca.\n"
                    "2. Click Create account.\n"
                    "3. Select 'For my personal use'.\n"
                    "4. Enter the name John Doe.\n"
                    "Attached step 4: Enter the name John Doe."
                ),
            }
        ]

        def run_profiled(messages, delays):
            state = {"calls": 0}

            def fake_call_local_backend_completion(
                backend,
                prompt_messages,
                tools=None,
                tool_choice=None,
                resolved_model=None,
                chat_template_kwargs=None,
                reasoning_budget=None,
                temperature=None,
                timeout_sec=None,
            ):
                state["calls"] += 1
                local_broker.time.sleep(delays["latency"])
                tool_turn = (state["calls"] <= delays["tool_turns"]) and (
                    "Browser workflow profile" not in str(prompt_messages[-1].get("content", ""))
                )
                if not tool_turn:
                    return {
                        "choices": [
                            {
                                "message": {
                                    "content": "Done.",
                                    "tool_calls": [],
                                }
                            }
                        ]
                    }

                return {
                    "choices": [
                        {
                            "message": {
                                "content": "",
                                "tool_calls": [
                                    {
                                        "id": "toolcall_1",
                                        "function": {
                                            "name": "browser.read",
                                            "arguments": json.dumps({"action": "page_digest"}),
                                        },
                                    }
                                ],
                            }
                        }
                    ]
                }

            with patch.object(local_broker, "ensure_local_backend_available", return_value={"model": "test-model"}):
                with patch.object(local_broker, "local_backend_settings", return_value={"default_model": "test-model"}):
                    with patch.object(
                        local_broker,
                        "call_local_backend_completion",
                        side_effect=fake_call_local_backend_completion,
                    ) as mock_call:
                        with patch.object(
                            local_broker.BROWSER_AUTOMATION,
                            "session_create",
                            return_value={"sessionId": "s1", "capabilityToken": "cap"},
                        ):
                            with patch.object(local_broker.BROWSER_AUTOMATION, "run_start", return_value={"runId": "r1"}):
                                with patch.object(
                                    local_broker.BROWSER_AUTOMATION,
                                    "execute_tool",
                                    return_value={"success": True, "data": {}, "error": None, "policy": None},
                                ):
                                    with patch.object(local_broker.BROWSER_AUTOMATION, "close_session"):
                                        start = local_broker.time.perf_counter()
                                        result = local_broker.run_local_backend_browser_agent(
                                            "session-id",
                                            messages,
                                            ["google.ca"],
                                            6,
                                            backend="llama",
                                        )
                                        elapsed_ms = (local_broker.time.perf_counter() - start) * 1000

            return result, elapsed_ms, mock_call, state["calls"]

        without_profile, without_time, without_mock, without_turns = run_profiled(
            base_messages,
            {"latency": 0.02, "tool_turns": 3},
        )
        with_profile, with_time, with_mock, with_turns = run_profiled(
            profile_messages,
            {"latency": 0.005, "tool_turns": 0},
        )

        self.assertEqual("Done.", without_profile)
        self.assertEqual("Done.", with_profile)
        self.assertGreater(without_turns, with_turns)
        self.assertGreaterEqual(without_turns, 4)
        self.assertEqual(1, with_turns)
        self.assertLess(with_time, without_time)
        self.assertLess(with_time * 5, without_time)


if __name__ == "__main__":
    unittest.main()
