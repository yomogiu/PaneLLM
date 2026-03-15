from __future__ import annotations

from typing import Any

_DEPRECATED_PAPERS_ERROR = {
    "code": "deprecated_feature",
    "message": "Paper analysis has been replaced by the read assistant. Use chat with page context enabled.",
}


def deprecated_papers_payload() -> dict[str, Any]:
    return {
        "ok": False,
        "error": dict(_DEPRECATED_PAPERS_ERROR),
    }


def _compact_whitespace(value: Any, limit: int) -> str:
    cleaned = " ".join(str(value or "").split())
    return cleaned[:limit]


def _compact_text_block(value: Any, limit: int) -> str:
    raw = str(value or "").replace("\r\n", "\n").replace("\r", "\n")
    paragraphs: list[str] = []
    for part in raw.split("\n\n"):
        cleaned = " ".join(part.split())
        if cleaned:
            paragraphs.append(cleaned)
    return "\n\n".join(paragraphs)[:limit]


def normalize_page_context(
    value: Any,
    field_limits: dict[str, int],
) -> dict[str, Any] | None:
    if value is None:
        return None
    if not isinstance(value, dict):
        raise ValueError("page_context must be an object.")

    output: dict[str, Any] = {}

    title = _compact_whitespace(value.get("title"), field_limits.get("title", 240))
    if title:
        output["title"] = title

    url = str(value.get("url", "")).strip()[: field_limits.get("url", 2000)]
    if url:
        output["url"] = url

    content_kind = str(value.get("content_kind", "")).strip().lower()
    if content_kind in {"html", "unknown"}:
        output["content_kind"] = content_kind[: field_limits.get("content_kind", 32)]

    selection = _compact_text_block(value.get("selection"), field_limits.get("selection", 1200))
    if selection:
        output["selection"] = selection

    excerpt = _compact_text_block(value.get("text_excerpt"), field_limits.get("text_excerpt", 5000))
    if excerpt:
        output["text_excerpt"] = excerpt

    heading_path_raw = value.get("heading_path")
    if isinstance(heading_path_raw, (list, tuple)):
        heading_limit = field_limits.get("heading_path", 160)
        heading_path = [
            _compact_whitespace(item, heading_limit)
            for item in heading_path_raw
            if _compact_whitespace(item, heading_limit)
        ]
        if heading_path:
            output["heading_path"] = heading_path[:6]

    local_raw = value.get("selection_context")
    if isinstance(local_raw, dict):
        local_limit = field_limits.get("selection_context", 700)
        local = {
            key: _compact_text_block(local_raw.get(key), local_limit)
            for key in ("before", "focus", "after")
        }
        if any(local.values()):
            output["selection_context"] = local

    return output


def format_page_context(page_context: dict[str, Any] | None, budget: int) -> str:
    if not page_context:
        return ""

    parts: list[str] = []

    title = str(page_context.get("title", "")).strip()
    url = str(page_context.get("url", "")).strip()
    selection = str(page_context.get("selection", "")).strip()
    excerpt = str(page_context.get("text_excerpt", "")).strip()
    heading_path = page_context.get("heading_path") or []
    local = page_context.get("selection_context") or {}

    if title:
        parts.append(f"Title: {title}")
    if url:
        parts.append(f"URL: {url}")
    if isinstance(heading_path, list):
        normalized_heading_path = [str(item).strip() for item in heading_path if str(item).strip()]
        if normalized_heading_path:
            parts.append("Section: " + " > ".join(normalized_heading_path))
    if selection:
        parts.append("Selected text:\n" + selection)
    if isinstance(local, dict):
        before = str(local.get("before", "")).strip()
        focus = str(local.get("focus", "")).strip()
        after = str(local.get("after", "")).strip()
        if focus:
            parts.append("Local context:\n" + "\n".join(part for part in (before, focus, after) if part))
    if excerpt:
        parts.append("Page excerpt:\n" + excerpt)

    return "\n\n".join(parts)[:budget]
