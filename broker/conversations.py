from __future__ import annotations

import json
import os
import re
from pathlib import Path
from typing import Any
from typing import Callable

from broker.common import compact_text_block, compact_whitespace, normalize_codex_bool, now_iso, truncate_text
from broker.papers import merge_paper_contexts, normalize_paper_context, papers_equal
from broker.papers import normalize_highlight_capture_list, normalize_paper_id, normalize_paper_source
from broker.papers import normalize_paper_version
from broker.prompt_context import normalize_page_context


THINK_OPEN_TAG_PATTERN = re.compile(r"<(?:think|thinking)\b[^>]*>", re.IGNORECASE)
THINK_CLOSE_TAG_PATTERN = re.compile(r"</(?:think|thinking)\b[^>]*>", re.IGNORECASE)
THINKING_PLAIN_HEADER_PATTERN = re.compile(
    r"(?m)(?:^|\n)\s*(?:#+\s*)?(?:thinking|reasoning|scratchpad)\s*:?\s*"
)
UNMARKED_REASONING_PREFIX_PATTERN = re.compile(
    r"(?i)\b(?:let'?s think|step by step|first[, ]|reasoning|analysis)\b"
)
UNMARKED_REASONING_ANSWER_START_PATTERN = re.compile(
    r"(?i)\b(?:answer|final answer|in summary|overall)\b"
)
FINAL_ANSWER_MARKER_PATTERN = re.compile(r"(?m)(?:^|\n)\s*###\s*FINAL ANSWER\s*:\s*", re.IGNORECASE)
ROLE_HEADER_PATTERN = re.compile(r"^(USER|ASSISTANT|SYSTEM)\s*:\s*", re.IGNORECASE | re.MULTILINE)
LEADING_ROLE_HEADER_PATTERN = re.compile(r"^\s*(USER|ASSISTANT|SYSTEM)\s*:\s*", re.IGNORECASE)
LEADING_ROLE_HEADER_NEWLINE_PATTERN = re.compile(
    r"^\s*(?:USER|ASSISTANT|SYSTEM)\s*:\s*\n", re.IGNORECASE
)
PROMPT_LEAK_MARKERS = (
    "return only the current assistant reply",
    "do not emit user:",
    "you are a broker-managed codex session",
)
TRAILING_PROMPT_LEAK_PATTERN = re.compile(r"(?mi)^\s*(?:user|assistant|system)\s*:")
CONVERSATION_ID_RE = re.compile(r"^[A-Za-z0-9._-]{1,128}$")


def conversation_paper_context(conversation: dict[str, Any]) -> dict[str, Any] | None:
    if not isinstance(conversation, dict):
        return None
    codex = conversation.get("codex")
    if not isinstance(codex, dict):
        return None

    direct_paper = None
    direct_candidate = {
        "source": codex.get("paper_source"),
        "paper_id": codex.get("paper_id"),
        "canonical_url": codex.get("paper_url") or codex.get("page_context_url"),
        "paper_version": codex.get("paper_version"),
        "paper_version_url": codex.get("paper_version_url") or codex.get("versioned_url"),
        "versioned_url": codex.get("paper_version_url") or codex.get("versioned_url"),
        "title": codex.get("paper_title") or codex.get("page_context_title"),
    }
    if any(direct_candidate.values()):
        try:
            direct_paper = normalize_paper_context(direct_candidate)
        except ValueError:
            direct_paper = None

    page_paper = None
    page_payload = codex.get("page_context_payload")
    if isinstance(page_payload, dict):
        try:
            page_paper = normalize_paper_context(
                {
                    "url": page_payload.get("url", ""),
                    "title": page_payload.get("title", ""),
                }
            )
        except ValueError:
            page_paper = None

    if direct_paper and page_paper:
        if papers_equal(direct_paper, page_paper):
            return merge_paper_contexts(direct_paper, page_paper)
        return direct_paper
    return direct_paper or page_paper


def should_reuse_session_page_context(
    codex_state: dict[str, Any],
    run: dict[str, Any],
    conversation_message_count: int,
) -> bool:
    current_fingerprint = str(run.get("_page_context_fingerprint", "") or "")
    stored_fingerprint = str(
        codex_state.get("last_page_context_fingerprint", "") if isinstance(codex_state, dict) else ""
    )
    try:
        stored_message_count = int(
            codex_state.get("last_response_message_count", 0) if isinstance(codex_state, dict) else 0
        )
    except (TypeError, ValueError):
        stored_message_count = 0
    return current_fingerprint == stored_fingerprint and conversation_message_count == stored_message_count + 1


def build_conversation_highlight(conversation: dict[str, Any]) -> str:
    if not isinstance(conversation, dict):
        return ""
    messages = conversation.get("messages")
    if not isinstance(messages, list) or not messages:
        return ""

    title = compact_whitespace(conversation.get("title", ""), 120)
    if title == "New Chat":
        title = ""

    assistant_preview = ""
    user_preview = ""
    for message in reversed(messages):
        if not isinstance(message, dict):
            continue
        role = str(message.get("role", "") or "").strip()
        content = compact_text_block(message.get("content", ""), 280)
        if not content:
            continue
        if role == "assistant" and not assistant_preview:
            assistant_preview = content
        elif role == "user" and not user_preview:
            user_preview = content
        if assistant_preview and user_preview:
            break

    preview = assistant_preview or user_preview
    if title and preview:
        combined = f"{title}: {preview}"
        if combined.startswith(f"{title}: {title}"):
            return truncate_text(title, 400)
        return truncate_text(combined, 400)
    return truncate_text(title or preview, 400)


def summarize_messages(existing: str, extra_messages: list[dict[str, str]], *, max_summary_chars: int) -> str:
    snippets: list[str] = []
    for message in extra_messages:
        role = message.get("role", "assistant")
        prefix = "U" if role == "user" else "A"
        content = str(message.get("content", ""))
        if role == "assistant":
            content = strip_transcript_spillover(content) or content
        cleaned = " ".join(content.split())
        if cleaned:
            snippets.append(f"{prefix}: {cleaned[:180]}")
    if not snippets:
        return existing
    merged = (existing + " " + " | ".join(snippets)).strip()
    if len(merged) > max_summary_chars:
        merged = merged[-max_summary_chars:]
    return merged


def strip_internal_thinking(
    value: Any,
    *,
    allow_plaintext_headers: bool = False,
    allow_unmarked_reasoning: bool = False,
) -> tuple[str, int, list[str]]:
    raw = str(value or "")
    if not raw:
        return "", 0, []

    visible_parts: list[str] = []
    reasoning_blocks: list[str] = []
    hidden_chars = 0

    thinking_header_match = THINKING_PLAIN_HEADER_PATTERN.search(raw) if allow_plaintext_headers else None
    final_answer_match = FINAL_ANSWER_MARKER_PATTERN.search(raw) if allow_plaintext_headers else None

    if allow_unmarked_reasoning and not thinking_header_match and not THINK_OPEN_TAG_PATTERN.search(raw):
        paragraphs = [part.strip() for part in re.split(r"\n\s*\n", raw) if part.strip()]
        if len(paragraphs) >= 2 and UNMARKED_REASONING_PREFIX_PATTERN.search(paragraphs[0]):
            split_index = None
            for index, paragraph in enumerate(paragraphs[1:], start=1):
                if UNMARKED_REASONING_ANSWER_START_PATTERN.search(paragraph):
                    split_index = index
                    break
            if split_index is None:
                for index, paragraph in enumerate(paragraphs[1:], start=1):
                    if not UNMARKED_REASONING_PREFIX_PATTERN.search(paragraph):
                        split_index = index
                        break
            if split_index is not None and split_index > 0:
                reasoning_text = "\n\n".join(paragraphs[:split_index]).strip()
                visible = "\n\n".join(paragraphs[split_index:]).strip()
                if reasoning_text:
                    reasoning_blocks.append(reasoning_text)
                    hidden_chars += len(reasoning_text)
                return visible, hidden_chars, reasoning_blocks

    if thinking_header_match:
        think_close_match = THINK_CLOSE_TAG_PATTERN.search(raw, thinking_header_match.start())
        if think_close_match:
            reasoning_text = raw[thinking_header_match.start(): think_close_match.start()].strip()
            if reasoning_text:
                reasoning_blocks.append(reasoning_text)
            hidden_chars += len(reasoning_text)
            raw = raw[think_close_match.end():]
        elif final_answer_match and final_answer_match.start() > thinking_header_match.start():
            reasoning_text = raw[thinking_header_match.start(): final_answer_match.start()].strip()
            if reasoning_text:
                reasoning_blocks.append(reasoning_text)
            hidden_chars += len(reasoning_text)
            raw = raw[final_answer_match.end():]
        else:
            reasoning_text = raw[thinking_header_match.start():].strip()
            if reasoning_text:
                reasoning_blocks.append(reasoning_text)
            hidden_chars += len(reasoning_text)
            return "", hidden_chars, reasoning_blocks

    cursor = 0
    while cursor < len(raw):
        open_match = THINK_OPEN_TAG_PATTERN.search(raw, cursor)
        if not open_match:
            visible_parts.append(raw[cursor:])
            break

        visible_parts.append(raw[cursor:open_match.start()])
        close_match = THINK_CLOSE_TAG_PATTERN.search(raw, open_match.end())
        if close_match:
            reasoning_text = raw[open_match.end(): close_match.start()].strip()
            if reasoning_text:
                reasoning_blocks.append(reasoning_text)
            hidden_chars += max(0, close_match.start() - open_match.end())
            cursor = close_match.end()
            continue

        hidden_chars += max(0, len(raw) - open_match.end())
        cursor = len(raw)
        break

    visible = "".join(visible_parts)
    visible = THINK_CLOSE_TAG_PATTERN.sub("", visible)
    visible = re.sub(r"\n{3,}", "\n\n", visible).strip()
    return visible, hidden_chars, reasoning_blocks


def split_stream_text(
    raw_text: str,
    *,
    allow_plaintext_headers: bool = False,
    allow_unmarked_reasoning: bool = False,
) -> tuple[str, str]:
    visible, _hidden_chars, reasoning_blocks = strip_internal_thinking(
        raw_text,
        allow_plaintext_headers=allow_plaintext_headers,
        allow_unmarked_reasoning=allow_unmarked_reasoning,
    )
    visible = strip_transcript_spillover(visible)
    reasoning = "\n\n".join(
        str(block or "").strip()
        for block in reasoning_blocks
        if str(block or "").strip()
    ).strip()
    return visible, reasoning


def strip_transcript_spillover(value: Any) -> str:
    text = str(value or "").strip()
    if not text:
        return ""

    role_headers = list(ROLE_HEADER_PATTERN.finditer(text))
    lower_text = text.lower()
    transcript_like = any(marker in lower_text for marker in PROMPT_LEAK_MARKERS)
    if role_headers:
        first_header = role_headers[0]
        transcript_like = transcript_like or first_header.start() > 0
        if first_header.start() == 0 and (
            len(role_headers) > 1 or LEADING_ROLE_HEADER_NEWLINE_PATTERN.match(text)
        ):
            transcript_like = True

    if transcript_like and role_headers:
        first_header = role_headers[0]
        if first_header.start() > 0:
            text = text[: first_header.start()].rstrip()
        else:
            extracted = ""
            for index, match in enumerate(role_headers):
                role = match.group(1).strip().lower()
                next_start = role_headers[index + 1].start() if index + 1 < len(role_headers) else len(text)
                block = text[match.end(): next_start].strip()
                if role in {"assistant", "system"} and block:
                    extracted = block
                    break
            text = extracted

    if not text:
        return ""

    lower_text = text.lower()
    for marker in PROMPT_LEAK_MARKERS:
        position = lower_text.find(marker)
        if position > 0:
            text = text[:position].rstrip()
            break

    trailing_prompt_match = TRAILING_PROMPT_LEAK_PATTERN.search(text)
    if trailing_prompt_match and trailing_prompt_match.start() > 0:
        text = text[: trailing_prompt_match.start()].rstrip()

    if transcript_like:
        while True:
            updated = LEADING_ROLE_HEADER_PATTERN.sub("", text, count=1).lstrip()
            if updated == text:
                break
            text = updated

    normalized = text.strip()
    if not normalized:
        return ""
    if LEADING_ROLE_HEADER_PATTERN.match(normalized):
        return ""
    if re.fullmatch(r"(?:assistant|system|user)\s*:?", normalized, re.IGNORECASE):
        return ""
    return normalized


def build_model_context(
    conversation: dict[str, Any],
    *,
    default_max_context_chars: int,
    max_context_messages: int,
    max_summary_chars: int,
    save_conversation_func: Callable[[dict[str, Any]], None],
    max_context_chars: int | None = None,
) -> list[dict[str, str]]:
    return _build_model_context_with_stats(
        conversation,
        default_max_context_chars=default_max_context_chars,
        max_context_messages=max_context_messages,
        max_summary_chars=max_summary_chars,
        save_conversation_func=save_conversation_func,
        max_context_chars=max_context_chars,
    )[0]


def _build_model_context_with_stats(
    conversation: dict[str, Any],
    *,
    default_max_context_chars: int,
    max_context_messages: int,
    max_summary_chars: int,
    save_conversation_func: Callable[[dict[str, Any]], None],
    max_context_chars: int | None = None,
) -> tuple[list[dict[str, str]], dict[str, Any]]:
    messages: list[dict[str, str]] = []
    for msg in conversation.get("messages", []):
        role = str(msg.get("role", ""))
        if role not in {"user", "assistant"}:
            continue
        content = str(msg.get("content", ""))
        if role == "assistant":
            content = strip_transcript_spillover(content)
            if not content:
                continue
        messages.append({"role": role, "content": content})
    effective_max_context_chars = max(
        2000,
        int(default_max_context_chars if max_context_chars is None else max_context_chars),
    )
    selected: list[dict[str, str]] = []
    total_chars = 0
    for message in reversed(messages):
        content = message["content"]
        msg_chars = len(content)
        if selected and (
            len(selected) >= max_context_messages
            or total_chars + msg_chars > effective_max_context_chars
        ):
            break
        selected.append(message)
        total_chars += msg_chars
    selected.reverse()

    dropped_count = len(messages) - len(selected)
    summary_upto = int(conversation.get("summary_upto", 0))
    if dropped_count > summary_upto:
        newly_dropped = messages[summary_upto:dropped_count]
        conversation["summary"] = summarize_messages(
            str(conversation.get("summary", "")),
            newly_dropped,
            max_summary_chars=max_summary_chars,
        )
        conversation["summary_upto"] = dropped_count
        save_conversation_func(conversation)

    summary = str(conversation.get("summary", "")).strip()
    if summary:
        summary_msg = {
            "role": "system",
            "content": (
                "Conversation summary of older turns (for continuity):\n"
                f"{summary}"
            ),
        }
        context_messages = [summary_msg, *selected]
    else:
        context_messages = selected

    return context_messages, {
        "used_chars": sum(len(message["content"]) for message in context_messages),
        "selected_chars": total_chars,
        "selected_count": len(selected),
        "effective_max_context_chars": effective_max_context_chars,
        "max_context_messages": max_context_messages,
        "messages_available": len(messages),
        "summary_included": bool(summary),
        "summary_chars": len(summary_msg["content"]) if summary else 0,
        "dropped_count": len(messages) - len(selected),
    }


class ConversationStore:
    def __init__(
        self,
        root: Path,
        *,
        now_iso_func: Callable[[], str] = now_iso,
        normalize_codex_bool_func: Callable[[Any], str] = normalize_codex_bool,
        normalize_page_context_func: Callable[[Any], dict[str, Any] | None] = normalize_page_context,
        normalize_highlight_capture_list_func: Callable[[Any], list[dict[str, Any]]] = normalize_highlight_capture_list,
        conversation_paper_context_func: Callable[[dict[str, Any]], dict[str, Any] | None] = conversation_paper_context,
        build_conversation_highlight_func: Callable[[dict[str, Any]], str] = build_conversation_highlight,
        normalize_paper_version_func: Callable[[Any], str] = normalize_paper_version,
    ) -> None:
        self._root = root
        self._dir = root / "conversations"
        self._dir.mkdir(parents=True, exist_ok=True)
        self._now_iso = now_iso_func
        self._normalize_codex_bool = normalize_codex_bool_func
        self._normalize_page_context = normalize_page_context_func
        self._normalize_highlight_capture_list = normalize_highlight_capture_list_func
        self._conversation_paper_context = conversation_paper_context_func
        self._build_conversation_highlight = build_conversation_highlight_func
        self._normalize_paper_version = normalize_paper_version_func
        try:
            os.chmod(self._root, 0o700)
            os.chmod(self._dir, 0o700)
        except OSError:
            pass

    def _validate_id(self, conversation_id: str) -> str:
        if not CONVERSATION_ID_RE.match(conversation_id):
            raise ValueError("Invalid conversation id.")
        return conversation_id

    def _path(self, conversation_id: str) -> Path:
        cid = self._validate_id(conversation_id)
        return self._dir / f"{cid}.json"

    def _write(self, path: Path, payload: dict[str, Any]) -> None:
        raw = json.dumps(payload, ensure_ascii=True, indent=2)
        tmp = path.with_suffix(".json.tmp")
        tmp.write_text(raw, encoding="utf-8")
        try:
            os.chmod(tmp, 0o600)
        except OSError:
            pass
        tmp.replace(path)
        try:
            os.chmod(path, 0o600)
        except OSError:
            pass

    def _normalize_codex_metadata(self, value: Any) -> dict[str, Any]:
        raw = value if isinstance(value, dict) else {}
        return {
            "mode": str(raw.get("mode", "") or ""),
            "model": str(raw.get("model", "") or ""),
            "last_response_id": str(raw.get("last_response_id", "") or ""),
            "active_run_id": str(raw.get("active_run_id", "") or ""),
            "last_run_id": str(raw.get("last_run_id", "") or ""),
            "last_run_status": str(raw.get("last_run_status", "") or ""),
            "last_response_message_count": int(raw.get("last_response_message_count", 0) or 0),
            "cli_session_id": str(raw.get("cli_session_id", "") or ""),
            "page_context_enabled": self._normalize_codex_bool(raw.get("page_context_enabled")),
            "page_context_fingerprint": str(raw.get("page_context_fingerprint", "") or ""),
            "page_context_url": str(raw.get("page_context_url", "") or ""),
            "page_context_title": str(raw.get("page_context_title", "") or ""),
            "page_context_updated_at": str(raw.get("page_context_updated_at", "") or ""),
            "paper_source": str(raw.get("paper_source", "") or ""),
            "paper_id": str(raw.get("paper_id", "") or ""),
            "paper_url": str(raw.get("paper_url", "") or ""),
            "paper_version": self._normalize_paper_version(raw.get("paper_version", "")),
            "paper_version_url": str(raw.get("paper_version_url", "") or ""),
            "paper_chat_kind": str(raw.get("paper_chat_kind", "") or "").strip().lower(),
            "paper_history_label": str(raw.get("paper_history_label", "") or ""),
            "paper_focus_text": str(raw.get("paper_focus_text", "") or ""),
            "paper_title": str(raw.get("paper_title", "") or ""),
            "paper_updated_at": str(raw.get("paper_updated_at", "") or ""),
            "last_page_context_message_count": int(
                raw.get("last_page_context_message_count", 0) or 0
            ),
            "last_page_context_fingerprint": str(raw.get("last_page_context_fingerprint", "") or ""),
            "page_context_payload": self._normalize_page_context(raw.get("page_context_payload")),
            "highlight_captures": self._normalize_highlight_capture_list(raw.get("highlight_captures")),
        }

    def _normalize_reasoning_blocks(self, value: Any) -> list[str]:
        if not isinstance(value, list):
            return []
        blocks: list[str] = []
        for item in value:
            text = str(item or "").strip()
            if text:
                blocks.append(text)
        return blocks

    def _normalize_conversation(
        self,
        value: Any,
        conversation_id: str | None = None,
    ) -> dict[str, Any]:
        raw = value if isinstance(value, dict) else {}
        stamp = self._now_iso()
        normalized = {
            "id": str(raw.get("id") or conversation_id or ""),
            "title": str(raw.get("title") or "New Chat"),
            "created_at": str(raw.get("created_at") or stamp),
            "updated_at": str(raw.get("updated_at") or stamp),
            "summary": str(raw.get("summary") or ""),
            "summary_upto": int(raw.get("summary_upto", 0) or 0),
            "messages": [],
            "codex": self._normalize_codex_metadata(raw.get("codex")),
        }
        for message in raw.get("messages", []):
            if not isinstance(message, dict):
                continue
            role = str(message.get("role", "")).strip()
            if role not in {"user", "assistant"}:
                continue
            entry = {
                "role": role,
                "content": str(message.get("content", "")),
                "created_at": str(message.get("created_at") or stamp),
            }
            if role == "assistant":
                reasoning_value = message.get(
                    "reasoning_blocks",
                    message.get("reasoningBlocks"),
                )
                reasoning_blocks = self._normalize_reasoning_blocks(reasoning_value)
                if reasoning_blocks:
                    entry["reasoning_blocks"] = reasoning_blocks
            normalized["messages"].append(entry)
        if not normalized["id"]:
            normalized["id"] = self._validate_id(str(conversation_id or raw.get("id", "")))
        return normalized

    def get_or_create(self, conversation_id: str) -> dict[str, Any]:
        path = self._path(conversation_id)
        if path.exists():
            return self._normalize_conversation(
                json.loads(path.read_text(encoding="utf-8")),
                conversation_id,
            )
        stamp = self._now_iso()
        convo = self._normalize_conversation(
            {
                "id": conversation_id,
                "title": "New Chat",
                "created_at": stamp,
                "updated_at": stamp,
                "summary": "",
                "summary_upto": 0,
                "messages": [],
                "codex": {},
            },
            conversation_id,
        )
        self._write(path, convo)
        return convo

    def save(self, conversation: dict[str, Any]) -> None:
        normalized = self._normalize_conversation(conversation)
        cid = self._validate_id(str(normalized.get("id", "")))
        normalized["id"] = cid
        normalized["updated_at"] = self._now_iso()
        self._write(self._path(cid), normalized)

    def append_message(
        self,
        conversation_id: str,
        role: str,
        content: str,
        *,
        reasoning_blocks: Any = None,
    ) -> dict[str, Any]:
        if role not in {"user", "assistant"}:
            raise ValueError("Unsupported message role.")
        conversation = self.get_or_create(conversation_id)
        stamp = self._now_iso()
        message = {"role": role, "content": content, "created_at": stamp}
        if role == "assistant":
            normalized_blocks = self._normalize_reasoning_blocks(reasoning_blocks)
            if normalized_blocks:
                message["reasoning_blocks"] = normalized_blocks
        conversation.setdefault("messages", []).append(message)
        if conversation.get("title") in {"", "New Chat"} and role == "user":
            normalized = " ".join(content.split())
            conversation["title"] = normalized[:80] if normalized else "New Chat"
        conversation["updated_at"] = stamp
        self.save(conversation)
        return conversation

    def rewrite_user_message(self, conversation_id: str, message_index: int, content: str) -> dict[str, Any]:
        updated_content = str(content or "").strip()
        if not updated_content:
            raise ValueError("prompt is required.")
        conversation = self.get_or_create(conversation_id)
        messages = conversation.setdefault("messages", [])
        if not isinstance(message_index, int):
            raise ValueError("rewrite_message_index must be an integer.")
        if message_index < 0 or message_index >= len(messages):
            raise ValueError("rewrite_message_index is out of range.")
        target = messages[message_index]
        if not isinstance(target, dict) or str(target.get("role", "")) != "user":
            raise ValueError("rewrite_message_index must target a user message.")

        stamp = self._now_iso()
        rewritten = {"role": "user", "content": updated_content, "created_at": stamp}
        conversation["messages"] = [*messages[:message_index], rewritten]
        if message_index == 0:
            normalized_title = " ".join(updated_content.split())
            conversation["title"] = normalized_title[:80] if normalized_title else "New Chat"
        conversation["summary"] = ""
        conversation["summary_upto"] = 0
        conversation["updated_at"] = stamp

        codex = self._normalize_codex_metadata(conversation.get("codex"))
        codex["last_response_id"] = ""
        codex["active_run_id"] = ""
        codex["last_run_id"] = ""
        codex["last_run_status"] = ""
        codex["last_response_message_count"] = 0
        codex["last_page_context_message_count"] = 0
        codex["last_page_context_fingerprint"] = ""
        codex["cli_session_id"] = ""
        codex["highlight_captures"] = []
        conversation["codex"] = codex

        self.save(conversation)
        return conversation

    def update_codex_state(
        self,
        conversation_id: str,
        updates: dict[str, Any],
    ) -> dict[str, Any]:
        if not isinstance(updates, dict):
            raise ValueError("updates must be an object.")
        conversation = self.get_or_create(conversation_id)
        codex = self._normalize_codex_metadata(conversation.get("codex"))
        for key, value in updates.items():
            if key == "last_response_message_count":
                codex[key] = int(value or 0)
            elif key == "last_page_context_message_count":
                codex[key] = int(value or 0)
            elif key == "last_page_context_fingerprint":
                codex[key] = str(value or "")
            elif key == "page_context_enabled":
                codex[key] = self._normalize_codex_bool(value)
            elif key == "page_context_payload":
                codex[key] = self._normalize_page_context(value)
            elif key == "highlight_captures":
                codex[key] = self._normalize_highlight_capture_list(value)
            else:
                codex[key] = str(value or "")
        conversation["codex"] = codex
        self.save(conversation)
        return conversation

    def list_metadata(
        self,
        *,
        paper_source: str | None = None,
        paper_id: str | None = None,
    ) -> list[dict[str, Any]]:
        required_source = normalize_paper_source(paper_source) if paper_source else ""
        required_paper_id = normalize_paper_id(paper_id) if paper_id else ""
        items: list[dict[str, Any]] = []
        for path in self._dir.glob("*.json"):
            try:
                payload = self._normalize_conversation(json.loads(path.read_text(encoding="utf-8")))
            except Exception:
                continue
            paper = self._conversation_paper_context(payload)
            if required_source:
                if not paper:
                    continue
                if paper["source"] != required_source or paper["paper_id"] != required_paper_id:
                    continue
            codex = payload.get("codex", {})
            messages = payload.get("messages", [])
            if not messages:
                continue
            item = {
                "id": payload.get("id", path.stem),
                "title": payload.get("title", "New Chat"),
                "created_at": payload.get("created_at"),
                "updated_at": payload.get("updated_at"),
                "message_count": len(messages),
                "preview": self._build_conversation_highlight(payload),
                "summary": str(payload.get("summary", "") or ""),
                "paper_chat_kind": str(codex.get("paper_chat_kind", "") or ""),
                "paper_history_label": str(codex.get("paper_history_label", "") or ""),
                "paper_focus_text": str(codex.get("paper_focus_text", "") or ""),
                "paper_version": self._normalize_paper_version(
                    codex.get("paper_version", "") or (paper.get("paper_version", "") if paper else "")
                ),
                "paper_version_url": str(
                    codex.get("paper_version_url", "") or (paper.get("versioned_url", "") if paper else "")
                    or ""
                ),
            }
            if paper:
                item["paper"] = paper
            items.append(item)
        items.sort(key=lambda item: str(item.get("updated_at", "")), reverse=True)
        return items

    def get(self, conversation_id: str) -> dict[str, Any]:
        path = self._path(conversation_id)
        if not path.exists():
            raise FileNotFoundError("Conversation not found.")
        conversation = self._normalize_conversation(
            json.loads(path.read_text(encoding="utf-8")),
            conversation_id,
        )
        paper = self._conversation_paper_context(conversation)
        if paper:
            conversation["paper"] = paper
        return conversation

    def delete(self, conversation_id: str) -> bool:
        path = self._path(conversation_id)
        if not path.exists():
            return False
        path.unlink()
        return True
