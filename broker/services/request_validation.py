from __future__ import annotations

import json
import re
from http.server import BaseHTTPRequestHandler
from ipaddress import ip_address
from typing import Any
from urllib.parse import urlparse

from broker.common import normalize_codex_bool

HIGH_RISK_PATTERN = re.compile(
    r"\b(delete|transfer|wire|bank|purchase|buy|checkout|submit|password|token|credential|2fa|otp|security code)\b",
    re.IGNORECASE,
)
BROWSER_ACTION_PATTERN = re.compile(
    r"\b(open|navigate|visit|search|google|click|type|press|scroll|tab|page|site|website|url|link|browser)\b",
    re.IGNORECASE,
)
INVALID_API_KEY_PATTERN = re.compile(
    r"(?:invalid|incorrect)\s+api(?:[ _-]?key)|api(?:[ _-]?key).*(?:invalid|incorrect)",
    re.IGNORECASE,
)
MAX_JSON_BODY_BYTES = 3 * 1024 * 1024


def normalize_host(value: str) -> str:
    candidate = value.strip().lower().strip(".")
    if not candidate:
        return ""
    if "://" in candidate:
        try:
            parsed = urlparse(candidate)
            candidate = (parsed.hostname or "").strip().lower().strip(".")
        except Exception:
            return ""
    return candidate


def normalize_domain_allowlist(raw_value: Any) -> list[str]:
    values: list[str] = []
    if isinstance(raw_value, str):
        parts = raw_value.split(",")
    elif isinstance(raw_value, list):
        parts = [str(part) for part in raw_value]
    else:
        parts = []
    for part in parts:
        host = normalize_host(part)
        if host and host not in values:
            values.append(host)
    return values


def url_host_is_allowed(raw_url: str, allowed_hosts: list[str]) -> bool:
    try:
        parsed = urlparse(raw_url)
    except Exception:
        return False
    host = (parsed.hostname or "").strip().lower()
    if not host:
        return False
    for allowed in allowed_hosts:
        if host == allowed or host.endswith(f".{allowed}"):
            return True
    return False


def extract_url_host(raw_url: str) -> str:
    try:
        parsed = urlparse(raw_url)
    except Exception:
        return ""
    return normalize_host(parsed.hostname or "")


def resolve_route_allowlist(
    raw_value: Any,
    page_context: dict[str, Any] | None,
    default_allowlist: list[str] | tuple[str, ...] | None = None,
) -> list[str]:
    allowlist = normalize_domain_allowlist(raw_value)
    if not allowlist:
        allowlist = list(default_allowlist or [])
    page_host = extract_url_host(str((page_context or {}).get("url", "")))
    if page_host and page_host not in allowlist:
        allowlist.append(page_host)
    return allowlist


def is_loopback_host(host: str) -> bool:
    normalized = str(host or "").strip().lower().strip("[]")
    if not normalized:
        return False
    if normalized == "localhost":
        return True
    try:
        return ip_address(normalized).is_loopback
    except ValueError:
        return False


def is_loopback_target_url(target_url: str) -> bool:
    try:
        parsed = urlparse(str(target_url or "").strip())
    except Exception:
        return False
    return is_loopback_host(str(parsed.hostname or ""))


def is_invalid_api_key_message(message: str) -> bool:
    normalized = str(message or "").replace("_", " ").strip()
    return bool(normalized and INVALID_API_KEY_PATTERN.search(normalized))


def should_retry_local_backend_without_auth(
    backend: str,
    target_url: str,
    api_key: str,
    message: str,
) -> bool:
    return (
        str(backend or "").strip().lower() == "llama"
        and bool(str(api_key or "").strip())
        and is_loopback_target_url(target_url)
        and is_invalid_api_key_message(message)
    )


def is_loopback_client(address: str) -> bool:
    return address in {"127.0.0.1", "::1", "localhost"}


def is_extension_origin(origin: str | None) -> bool:
    if not origin:
        return False
    return origin.startswith("chrome-extension://")


def parse_json_body(handler: BaseHTTPRequestHandler) -> dict[str, Any]:
    raw_length = handler.headers.get("Content-Length")
    if not raw_length:
        raise ValueError("Missing Content-Length.")
    length = int(raw_length)
    if length <= 0 or length > MAX_JSON_BODY_BYTES:
        raise ValueError("Body is missing or too large.")
    raw = handler.rfile.read(length)
    parsed = json.loads(raw.decode("utf-8"))
    if not isinstance(parsed, dict):
        raise ValueError("JSON body must be an object.")
    return parsed


def gather_risk_flags(prompt: str, incoming: list[str]) -> list[str]:
    flags: list[str] = []
    if HIGH_RISK_PATTERN.search(prompt):
        flags.append("high_risk_prompt")
    for flag in incoming:
        if flag not in flags:
            flags.append(flag)
    return flags


def prompt_requests_browser_tools(prompt: str) -> bool:
    return bool(BROWSER_ACTION_PATTERN.search(prompt))


class RouteRequestCancelledError(RuntimeError):
    pass


def ensure_rewrite_message_index(value: Any) -> int | None:
    if value is None:
        return None
    if isinstance(value, bool):
        raise ValueError("rewrite_message_index must be an integer when provided.")
    try:
        index = int(value)
    except (TypeError, ValueError) as error:
        raise ValueError("rewrite_message_index must be an integer when provided.") from error
    if index < 0:
        raise ValueError("rewrite_message_index must be >= 0.")
    return index


def ensure_boolean_flag(value: Any, field_name: str, *, default: bool = False) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    raise ValueError(f"{field_name} must be a boolean when provided.")


def normalize_llama_chat_template_kwargs(value: Any) -> dict[str, Any]:
    if value is None:
        return {}
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except json.JSONDecodeError as error:
            raise ValueError(
                "chat_template_kwargs must be a JSON object string when provided as text."
            ) from error
    if not isinstance(value, dict):
        raise ValueError("chat_template_kwargs must be an object when provided.")
    return json.loads(json.dumps(value))


def normalize_llama_reasoning_budget(value: Any) -> int | None:
    if value is None or value == "":
        return None
    if isinstance(value, bool):
        raise ValueError("reasoning_budget must be an integer when provided.")
    try:
        budget = int(value)
    except (TypeError, ValueError) as error:
        raise ValueError("reasoning_budget must be an integer when provided.") from error
    if budget < -1:
        raise ValueError("reasoning_budget must be >= -1 when provided.")
    return budget


def normalize_llama_request_options(data: Any) -> dict[str, Any]:
    if not isinstance(data, dict):
        return {"chat_template_kwargs": {}, "reasoning_budget": None}
    return {
        "chat_template_kwargs": normalize_llama_chat_template_kwargs(
            data.get("chat_template_kwargs", data.get("chatTemplateKwargs"))
        ),
        "reasoning_budget": normalize_llama_reasoning_budget(
            data.get("reasoning_budget", data.get("reasoningBudget"))
        ),
    }
