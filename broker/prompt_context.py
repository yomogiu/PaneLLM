from __future__ import annotations

from typing import Any
from urllib.parse import urlsplit

from broker.common import compact_text_block, normalize_codex_bool
from broker.services import read_assistant as read_assistant_service


PAGE_CONTEXT_FIELD_LIMITS = {
    "title": 240,
    "url": 2000,
    "content_kind": 32,
    "selection": 1200,
    "text_excerpt": 5000,
    "heading_path": 160,
    "selection_context": 700,
}

BROWSER_ELEMENT_CONTEXT_FIELD_LIMITS = {
    "title": 240,
    "url": 2000,
    "selector": 400,
    "xpath": 800,
    "tag_name": 48,
    "role": 120,
    "label": 240,
    "name": 240,
    "placeholder": 240,
}

BROWSER_RUNTIME_CONTEXT_FIELD_LIMITS = {
    "title": 240,
    "url": 2000,
    "host": 255,
}

PAGE_CONTEXT_PROMPT_CHAR_BUDGET = 7200
BROWSER_ELEMENT_CONTEXT_PROMPT_CHAR_BUDGET = 2400
BROWSER_RUNTIME_CONTEXT_PROMPT_CHAR_BUDGET = 1200


def sanitize_browser_context_url(value: Any, limit: int) -> str:
    raw = str(value or "").strip()
    if not raw:
        return ""
    try:
        parsed = urlsplit(raw)
    except Exception:
        return raw[:limit]
    scheme = str(parsed.scheme or "").lower()
    hostname = str(parsed.hostname or "").strip()
    if scheme not in {"http", "https"} or not hostname:
        return raw[:limit]
    host = f"[{hostname}]" if ":" in hostname and not hostname.startswith("[") else hostname
    port = f":{parsed.port}" if parsed.port else ""
    path = parsed.path or ""
    return f"{scheme}://{host}{port}{path}"[:limit]


def normalize_page_context(value: Any) -> dict[str, Any] | None:
    return read_assistant_service.normalize_page_context(value, PAGE_CONTEXT_FIELD_LIMITS)


def _normalize_browser_element_bound(value: Any) -> float | None:
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return None
    if not numeric == numeric or numeric in {float("inf"), float("-inf")}:
        return None
    return round(numeric, 2)


def _format_prompt_number(value: Any) -> str:
    if isinstance(value, (int, float)):
        numeric = float(value)
        if numeric.is_integer():
            return str(int(numeric))
    return str(value)


def _normalize_optional_int(value: Any) -> int | None:
    try:
        numeric = int(value)
    except (TypeError, ValueError):
        return None
    return numeric


def normalize_browser_element_context(value: Any) -> dict[str, Any] | None:
    if not isinstance(value, dict):
        return None
    raw = value
    normalized: dict[str, Any] = {
        "title": compact_text_block(raw.get("title", ""), BROWSER_ELEMENT_CONTEXT_FIELD_LIMITS["title"]),
        "url": sanitize_browser_context_url(
            raw.get("url", ""),
            BROWSER_ELEMENT_CONTEXT_FIELD_LIMITS["url"],
        ),
        "selector": compact_text_block(
            raw.get("selector", ""),
            BROWSER_ELEMENT_CONTEXT_FIELD_LIMITS["selector"],
        ),
        "xpath": compact_text_block(raw.get("xpath", ""), BROWSER_ELEMENT_CONTEXT_FIELD_LIMITS["xpath"]),
        "tag_name": compact_text_block(
            raw.get("tag_name", raw.get("tagName", "")),
            BROWSER_ELEMENT_CONTEXT_FIELD_LIMITS["tag_name"],
        ).lower(),
        "role": compact_text_block(raw.get("role", ""), BROWSER_ELEMENT_CONTEXT_FIELD_LIMITS["role"]),
        "label": compact_text_block(raw.get("label", ""), BROWSER_ELEMENT_CONTEXT_FIELD_LIMITS["label"]),
        "name": compact_text_block(raw.get("name", ""), BROWSER_ELEMENT_CONTEXT_FIELD_LIMITS["name"]),
        "placeholder": compact_text_block(
            raw.get("placeholder", ""),
            BROWSER_ELEMENT_CONTEXT_FIELD_LIMITS["placeholder"],
        ),
    }
    tab_id = _normalize_optional_int(raw.get("tab_id", raw.get("tabId")))
    if tab_id is not None:
        normalized["tab_id"] = tab_id
    if "enabled" in raw:
        normalized["enabled"] = normalize_codex_bool(raw.get("enabled")) == "true"
    if "editable" in raw:
        normalized["editable"] = normalize_codex_bool(raw.get("editable")) == "true"
    bounds = raw.get("bounds") if isinstance(raw.get("bounds"), dict) else {}
    x = _normalize_browser_element_bound(raw.get("x", bounds.get("x")))
    y = _normalize_browser_element_bound(raw.get("y", bounds.get("y")))
    width = _normalize_browser_element_bound(raw.get("width", bounds.get("width")))
    height = _normalize_browser_element_bound(raw.get("height", bounds.get("height")))
    picked_at = str(raw.get("picked_at", raw.get("pickedAt")) or "").strip()[:80]

    if x is not None:
        normalized["x"] = x
    if y is not None:
        normalized["y"] = y
    if width is not None:
        normalized["width"] = width
    if height is not None:
        normalized["height"] = height
    if picked_at:
        normalized["picked_at"] = picked_at

    if not any(
        normalized.get(key)
        for key in ("selector", "xpath", "label", "role", "name", "placeholder", "tag_name", "url", "title")
    ):
        return None
    return {key: value for key, value in normalized.items() if value not in {"", None}}


def normalize_browser_runtime_context(value: Any) -> dict[str, Any] | None:
    if not isinstance(value, dict):
        return None
    raw = value
    normalized = {
        "title": compact_text_block(raw.get("title", ""), BROWSER_RUNTIME_CONTEXT_FIELD_LIMITS["title"]),
        "url": sanitize_browser_context_url(
            raw.get("url", ""),
            BROWSER_RUNTIME_CONTEXT_FIELD_LIMITS["url"],
        ),
        "host": compact_text_block(raw.get("host", ""), BROWSER_RUNTIME_CONTEXT_FIELD_LIMITS["host"]),
    }
    tab_id = _normalize_optional_int(raw.get("tab_id", raw.get("tabId")))
    if tab_id is not None:
        normalized["tab_id"] = tab_id
    if "allowlisted" in raw or "allowListed" in raw:
        normalized["allowlisted"] = normalize_codex_bool(raw.get("allowlisted", raw.get("allowListed"))) == "true"
    if "active" in raw:
        normalized["active"] = normalize_codex_bool(raw.get("active")) == "true"

    if not any(normalized.get(key) for key in ("title", "url", "host", "tab_id")):
        return None
    return {key: value for key, value in normalized.items() if value not in {"", None}}


def format_browser_runtime_context(browser_runtime_context: dict[str, Any] | None) -> str:
    normalized = normalize_browser_runtime_context(browser_runtime_context)
    if normalized is None:
        return ""

    lines: list[str] = []
    tab_id = normalized.get("tab_id")
    if tab_id is not None:
        lines.append(f"Tab ID: {tab_id}")
    title = str(normalized.get("title", "") or "")
    url = str(normalized.get("url", "") or "")
    host = str(normalized.get("host", "") or "")
    if title:
        lines.append(f"Title: {title}")
    if url:
        lines.append(f"URL: {url}")
    if host:
        lines.append(f"Host: {host}")
    if "allowlisted" in normalized:
        lines.append(f"Allowlisted: {'true' if normalized.get('allowlisted') else 'false'}")
    if "active" in normalized:
        lines.append(f"Active: {'true' if normalized.get('active') else 'false'}")
    return "\n".join(lines)[:BROWSER_RUNTIME_CONTEXT_PROMPT_CHAR_BUDGET]


def format_browser_element_context(browser_element_context: dict[str, Any] | None) -> str:
    normalized = normalize_browser_element_context(browser_element_context)
    if normalized is None:
        return ""

    lines: list[str] = []
    tab_id = normalized.get("tab_id")
    if tab_id is not None:
        lines.append(f"Tab ID: {tab_id}")
    title = str(normalized.get("title", "") or "")
    url = str(normalized.get("url", "") or "")
    if title:
        lines.append(f"Page title: {title}")
    if url:
        lines.append(f"Page URL: {url}")
    selector = str(normalized.get("selector", "") or "")
    xpath = str(normalized.get("xpath", "") or "")
    if selector:
        lines.append(f"CSS selector: {selector}")
    if xpath:
        lines.append(f"XPath: {xpath}")
    tag_name = str(normalized.get("tag_name", "") or "")
    role = str(normalized.get("role", "") or "")
    if tag_name:
        lines.append(f"Tag: {tag_name}")
    if role:
        lines.append(f"Role: {role}")
    for label, key in (("Label", "label"), ("Name", "name"), ("Placeholder", "placeholder")):
        value = str(normalized.get(key, "") or "")
        if value:
            lines.append(f"{label}: {value}")
    if "enabled" in normalized:
        lines.append(f"Enabled: {'yes' if normalized.get('enabled') else 'no'}")
    if "editable" in normalized:
        lines.append(f"Editable: {'yes' if normalized.get('editable') else 'no'}")
    bounds_parts = []
    for label, key in (("x", "x"), ("y", "y"), ("width", "width"), ("height", "height")):
        value = normalized.get(key)
        if value is not None:
            bounds_parts.append(f"{label}={_format_prompt_number(value)}")
    if bounds_parts:
        lines.append(f"Bounds: {', '.join(bounds_parts)}")
    picked_at = str(normalized.get("picked_at", "") or "")
    if picked_at:
        lines.append(f"Picked at: {picked_at}")
    return "\n".join(lines)[:BROWSER_ELEMENT_CONTEXT_PROMPT_CHAR_BUDGET]


def format_page_context(page_context: dict[str, Any] | None) -> str:
    return read_assistant_service.format_page_context(
        page_context,
        PAGE_CONTEXT_PROMPT_CHAR_BUDGET,
    )


def inject_page_context(messages: list[dict[str, str]], content: str) -> list[dict[str, str]]:
    updated = list(messages)
    for index in range(len(updated) - 1, -1, -1):
        if updated[index].get("role") == "user":
            updated[index] = {"role": "user", "content": content}
            return updated
    updated.append({"role": "user", "content": content})
    return updated


def compose_request_prompt(
    prompt: str,
    request_prompt_suffix: str = "",
    page_context_text: str = "",
    browser_element_context_text: str = "",
    browser_runtime_context_text: str = "",
) -> str:
    composed = str(prompt or "").strip()
    suffix = str(request_prompt_suffix or "").strip()
    if suffix:
        composed = f"{composed}\n\n{suffix}" if composed else suffix
    runtime_context = str(browser_runtime_context_text or "").strip()
    if runtime_context:
        composed = (
            f"{composed}\n\n[Browser Runtime Context]\n{runtime_context}"
            if composed
            else f"[Browser Runtime Context]\n{runtime_context}"
        )
    context = str(page_context_text or "").strip()
    if context:
        composed = f"{composed}\n\n[Page Context]\n{context}" if composed else f"[Page Context]\n{context}"
    browser_element = str(browser_element_context_text or "").strip()
    if browser_element:
        composed = (
            f"{composed}\n\n[Selected Browser Element]\n{browser_element}"
            if composed
            else f"[Selected Browser Element]\n{browser_element}"
        )
    return composed
