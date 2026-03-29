from __future__ import annotations

import json
import socket
from typing import Any, Callable
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen


def extract_response_output_text(response: dict[str, Any]) -> str:
    output = response.get("output")
    if not isinstance(output, list):
        return ""
    parts: list[str] = []
    for item in output:
        if not isinstance(item, dict) or item.get("type") != "message":
            continue
        for content in item.get("content", []):
            if not isinstance(content, dict):
                continue
            if content.get("type") in {"output_text", "text"}:
                text = content.get("text")
                if isinstance(text, str) and text:
                    parts.append(text)
    return "".join(parts)


def iter_sse_events(response: Any) -> Any:
    event_name = ""
    data_lines: list[str] = []
    while True:
        line = response.readline()
        if not line:
            if data_lines:
                yield {"event": event_name, "data": "\n".join(data_lines)}
            break
        decoded = line.decode("utf-8")
        if decoded in {"\n", "\r\n"}:
            if data_lines:
                yield {"event": event_name, "data": "\n".join(data_lines)}
            event_name = ""
            data_lines = []
            continue
        if decoded.startswith(":"):
            continue
        field, _, raw_value = decoded.partition(":")
        value = raw_value.lstrip(" ").rstrip("\r\n")
        if field == "event":
            event_name = value
        elif field == "data":
            data_lines.append(value)


def call_openai_responses_stream(
    input_items: list[dict[str, Any]],
    *,
    openai_api_key: str,
    openai_base_url: str,
    openai_codex_model: str,
    max_output_tokens: int,
    reasoning_effort: str,
    codex_run_timeout_sec: int,
    default_instructions: str,
    previous_response_id: str | None = None,
    tools: list[dict[str, Any]] | None = None,
    instructions: str | None = None,
    on_text_delta: Any = None,
    cancel_check: Any = None,
    cancelled_error_cls: type[BaseException] = RuntimeError,
    request_class: Callable[..., Any] = Request,
    urlopen_func: Callable[..., Any] = urlopen,
    socket_module: Any = socket,
) -> tuple[dict[str, Any], str]:
    if not openai_api_key:
        raise RuntimeError("OPENAI_API_KEY is not configured.")

    payload: dict[str, Any] = {
        "model": openai_codex_model,
        "instructions": instructions or default_instructions,
        "input": input_items,
        "stream": True,
        "store": True,
        "parallel_tool_calls": False,
        "max_output_tokens": max_output_tokens,
        "reasoning": {"effort": reasoning_effort},
    }
    if previous_response_id:
        payload["previous_response_id"] = previous_response_id
    if tools:
        payload["tools"] = tools

    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {openai_api_key}",
    }
    request = request_class(
        f"{openai_base_url}/responses",
        method="POST",
        headers=headers,
        data=json.dumps(payload).encode("utf-8"),
    )

    accumulated_text = ""
    final_response: dict[str, Any] | None = None
    try:
        with urlopen_func(request, timeout=max(30, codex_run_timeout_sec)) as response:
            for event in iter_sse_events(response):
                if cancel_check and cancel_check():
                    try:
                        response.close()
                    except Exception:
                        pass
                    raise cancelled_error_cls("Run cancelled by user.")
                raw_data = str(event.get("data", ""))
                if not raw_data or raw_data == "[DONE]":
                    continue
                parsed = json.loads(raw_data)
                event_name = str(event.get("event", "") or "")
                if event_name == "response.output_text.delta":
                    delta = str(parsed.get("delta", "") or "")
                    if delta:
                        accumulated_text += delta
                        if on_text_delta:
                            on_text_delta(delta, accumulated_text)
                elif event_name == "response.completed":
                    candidate = parsed.get("response")
                    if isinstance(candidate, dict):
                        final_response = candidate
                elif event_name in {"response.failed", "error"}:
                    error = parsed.get("error")
                    if not isinstance(error, dict):
                        error = (parsed.get("response") or {}).get("error", {})
                    message = str(error.get("message", "") or "OpenAI Responses request failed.")
                    raise RuntimeError(message)
    except HTTPError as error:
        body = error.read().decode("utf-8", errors="replace")
        try:
            parsed = json.loads(body)
        except json.JSONDecodeError:
            parsed = {}
        message = str(
            ((parsed.get("error") or {}).get("message"))
            or body
            or f"OpenAI request failed with status {error.code}."
        )
        raise RuntimeError(message) from error
    except URLError as error:
        raise RuntimeError(f"OpenAI request failed: {error.reason}") from error
    except socket_module.timeout as error:
        raise RuntimeError("OpenAI Responses request timed out.") from error

    if not isinstance(final_response, dict):
        raise RuntimeError("OpenAI Responses stream ended without a completed response object.")
    if not accumulated_text:
        accumulated_text = extract_response_output_text(final_response)
    return final_response, accumulated_text
