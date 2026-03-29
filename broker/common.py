from __future__ import annotations

import json
import re
from datetime import datetime, timezone
from hashlib import sha1
from typing import Any


def normalize_codex_bool(value: Any) -> str:
    if isinstance(value, bool):
        return "true" if value else ""
    if isinstance(value, str):
        value = value.strip().lower()
        return "true" if value in {"1", "true", "on", "yes"} else ""
    return "true" if bool(value) else ""


def page_context_fingerprint(context: dict[str, Any] | None) -> str:
    if not isinstance(context, dict):
        return ""
    payload = {
        "url": str(context.get("url", "") or ""),
        "title": str(context.get("title", "") or ""),
        "content_kind": str(context.get("content_kind", "") or ""),
        "text_excerpt": str(context.get("text_excerpt", "") or ""),
        "heading_path": [
            str(item)
            for item in (context.get("heading_path") if isinstance(context.get("heading_path"), list) else [])
            if str(item).strip()
        ],
    }
    raw = json.dumps(payload, ensure_ascii=True, sort_keys=True, separators=(",", ":"))
    return sha1(raw.encode("utf-8")).hexdigest()


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def truncate_text(value: Any, limit: int) -> str:
    text = str(value or "")
    if limit <= 0:
        return ""
    if len(text) <= limit:
        return text
    if limit <= 3:
        return text[:limit]
    return text[: limit - 3] + "..."


def sanitize_value_for_model(
    value: Any,
    *,
    depth: int = 0,
    max_items: int = 20,
    max_string_chars: int = 4000,
) -> Any:
    if depth > 5:
        return truncate_text(value, max_string_chars)
    if isinstance(value, dict):
        sanitized: dict[str, Any] = {}
        for index, (key, child) in enumerate(value.items()):
            if index >= max_items:
                sanitized["_truncated"] = True
                break
            sanitized[str(key)] = sanitize_value_for_model(
                child,
                depth=depth + 1,
                max_items=max_items,
                max_string_chars=max_string_chars,
            )
        return sanitized
    if isinstance(value, list):
        return [
            sanitize_value_for_model(
                child,
                depth=depth + 1,
                max_items=max_items,
                max_string_chars=max_string_chars,
            )
            for child in value[:max_items]
        ]
    if isinstance(value, str):
        return truncate_text(value, max_string_chars)
    if isinstance(value, (int, float, bool)) or value is None:
        return value
    return truncate_text(value, max_string_chars)


def compact_whitespace(value: Any, limit: int) -> str:
    cleaned = " ".join(str(value).split())
    return cleaned[:limit]


def compact_text_block(value: Any, limit: int) -> str:
    raw = str(value).replace("\r\n", "\n").replace("\r", "\n")
    paragraphs: list[str] = []
    for part in re.split(r"\n\s*\n", raw):
        cleaned = " ".join(part.split())
        if cleaned:
            paragraphs.append(cleaned)
    cleaned = "\n\n".join(paragraphs)
    return cleaned[:limit]
