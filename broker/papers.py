from __future__ import annotations

import json
import os
import re
from hashlib import sha1
from pathlib import Path
from typing import Any, Callable
from urllib.parse import unquote, urlsplit

from broker.common import compact_text_block, compact_whitespace, now_iso


PAPER_SOURCE_RE = re.compile(r"^[a-z][a-z0-9_-]{0,31}$")
PAPER_ID_RE = re.compile(r"^[A-Za-z0-9._/-]{1,128}$")
PAPER_STATUS_VALUES = {"idle", "requested", "ready", "error"}
ARXIV_HOSTS = {"arxiv.org", "www.arxiv.org"}
ARXIV_ROUTE_PREFIXES = {"abs", "pdf", "html"}
PAGE_CONTEXT_FIELD_LIMITS = {"url": 2000}


def normalize_paper_source(value: Any) -> str:
    source = str(value or "").strip().lower()
    if PAPER_SOURCE_RE.fullmatch(source):
        return source
    raise ValueError("paper_source is invalid.")


def normalize_paper_id(value: Any) -> str:
    paper_id = str(value or "").strip()
    if PAPER_ID_RE.fullmatch(paper_id):
        return paper_id
    raise ValueError("paper_id is invalid.")


def split_arxiv_identifier(value: Any) -> tuple[str, str]:
    identifier = str(value or "").strip().strip("/")
    if identifier.lower().endswith(".pdf"):
        identifier = identifier[:-4]
    version = ""
    match = re.search(r"v(\d+)$", identifier, flags=re.IGNORECASE)
    if match:
        version = f"v{int(match.group(1))}"
        identifier = identifier[: match.start()]
    return identifier.strip("/"), version


def canonicalize_arxiv_identifier(value: Any) -> str:
    identifier, _ = split_arxiv_identifier(value)
    return identifier


def normalize_paper_version(value: Any) -> str:
    version = str(value or "").strip()
    if not version:
        return ""
    if version.lower().startswith("v") and version[1:].isdigit():
        return f"v{int(version[1:])}"
    if version.isdigit():
        return f"v{int(version)}"
    match = re.fullmatch(r"v(\d+)", version, flags=re.IGNORECASE)
    if match:
        return f"v{int(match.group(1))}"
    return version[:16]


def normalize_paper_versions(value: Any, *, limit: int = 16) -> list[str]:
    if value is None:
        return []
    if isinstance(value, (str, int, float)):
        candidates = [value]
    elif isinstance(value, list):
        candidates = value
    else:
        return []
    versions: list[str] = []
    seen: set[str] = set()
    for item in candidates:
        version = normalize_paper_version(item)
        if not version or version in seen:
            continue
        seen.add(version)
        versions.append(version)
    versions.sort(
        key=lambda item: int(item[1:]) if item.startswith("v") and item[1:].isdigit() else -1,
        reverse=True,
    )
    return versions[:limit]


def extract_arxiv_paper(raw_url: Any, title: Any = "") -> dict[str, Any] | None:
    try:
        parsed = urlsplit(str(raw_url or "").strip())
    except Exception:
        return None
    host = str(parsed.hostname or "").strip().lower().strip(".")
    if host not in ARXIV_HOSTS:
        return None
    segments = [unquote(part).strip() for part in (parsed.path or "").split("/") if part.strip()]
    if len(segments) < 2 or segments[0].lower() not in ARXIV_ROUTE_PREFIXES:
        return None
    identifier = "/".join(segments[1:])
    canonical_id, paper_version = split_arxiv_identifier(identifier)
    if not canonical_id or not PAPER_ID_RE.fullmatch(canonical_id):
        return None
    clean_title = " ".join(str(title or "").split())[:240]
    versioned_url = f"https://arxiv.org/abs/{canonical_id}{paper_version}" if paper_version else ""
    return {
        "source": "arxiv",
        "paper_id": canonical_id,
        "canonical_url": f"https://arxiv.org/abs/{canonical_id}",
        "paper_version": paper_version,
        "versioned_url": versioned_url,
        "title": clean_title,
    }


def extract_paper_context(raw_url: Any, title: Any = "") -> dict[str, Any] | None:
    return extract_arxiv_paper(raw_url, title)


def normalize_paper_context(value: Any) -> dict[str, Any] | None:
    if value is None:
        return None
    if not isinstance(value, dict):
        raise ValueError("paper_context must be an object.")

    raw_source = value.get("source")
    raw_paper_id = value.get("paper_id", value.get("paperId"))
    raw_url = value.get("canonical_url", value.get("canonicalUrl", value.get("url")))
    raw_version = normalize_paper_version(value.get("paper_version", value.get("paperVersion")))
    raw_versioned_url = str(
        value.get(
            "versioned_url",
            value.get("versionedUrl", value.get("paper_version_url", value.get("paperVersionUrl"))),
        )
        or ""
    ).strip()[: PAGE_CONTEXT_FIELD_LIMITS["url"]]
    raw_title = value.get("title")

    clean_title = " ".join(str(raw_title or "").split())[:240]
    clean_url = str(raw_url or "").strip()[: PAGE_CONTEXT_FIELD_LIMITS["url"]]

    if raw_source or raw_paper_id:
        source = normalize_paper_source(raw_source)
        paper_id = str(raw_paper_id or "").strip()
        paper_version = raw_version
        versioned_url = raw_versioned_url
        if source == "arxiv":
            paper_id, derived_version = split_arxiv_identifier(paper_id)
            if not paper_id:
                raise ValueError("paper_id is invalid.")
            derived = None
            if clean_url:
                derived = extract_arxiv_paper(clean_url, clean_title)
                if derived is not None and derived["paper_id"] != paper_id:
                    raise ValueError("paper_context url does not match paper_id.")
            if derived is not None and not derived.get("paper_version", "") and versioned_url:
                derived_versioned = extract_arxiv_paper(versioned_url, clean_title)
                if derived_versioned is not None:
                    if derived_versioned["paper_id"] != paper_id:
                        raise ValueError("paper_context url does not match paper_id.")
                    derived = derived_versioned
            elif derived is None and versioned_url:
                derived_versioned = extract_arxiv_paper(versioned_url, clean_title)
                if derived_versioned is not None:
                    if derived_versioned["paper_id"] != paper_id:
                        raise ValueError("paper_context url does not match paper_id.")
                    derived = derived_versioned
            if derived is not None:
                clean_url = derived["canonical_url"]
                if not paper_version:
                    paper_version = derived.get("paper_version", "") or derived_version
                if not versioned_url:
                    versioned_url = derived.get("versioned_url", "")
            elif derived_version and not paper_version:
                paper_version = derived_version
            if not clean_url:
                clean_url = f"https://arxiv.org/abs/{paper_id}"
            if paper_version:
                versioned_url = f"https://arxiv.org/abs/{paper_id}{paper_version}"
        paper_id = normalize_paper_id(paper_id)
        return {
            "source": source,
            "paper_id": paper_id,
            "canonical_url": clean_url,
            "paper_version": paper_version,
            "versioned_url": versioned_url,
            "title": clean_title,
        }

    derived = extract_paper_context(clean_url, clean_title)
    if derived:
        return derived
    raise ValueError("paper_context is invalid.")


def papers_equal(left: Any, right: Any) -> bool:
    left_paper = normalize_paper_context(left)
    right_paper = normalize_paper_context(right)
    if not left_paper and not right_paper:
        return True
    if not left_paper or not right_paper:
        return False
    return left_paper["source"] == right_paper["source"] and left_paper["paper_id"] == right_paper["paper_id"]


def merge_paper_contexts(primary: Any, secondary: Any) -> dict[str, Any] | None:
    primary_paper = normalize_paper_context(primary)
    secondary_paper = normalize_paper_context(secondary)
    if primary_paper and secondary_paper:
        if not papers_equal(primary_paper, secondary_paper):
            return primary_paper
        merged = dict(primary_paper)
        for key in ("title", "canonical_url", "paper_version", "versioned_url"):
            if not merged.get(key) and secondary_paper.get(key):
                merged[key] = secondary_paper.get(key)
        return merged
    return primary_paper or secondary_paper


def default_paper_summary_prompt() -> str:
    return "\n".join(
        [
            "# Paper Summary",
            "",
            "Use only the provided page context.",
            "Write a concise saved summary for this paper page.",
            "",
            "Requirements:",
            "- Start with a 2-4 sentence summary of the paper's core idea.",
            "- Then add short sections named `Key contributions`, `Why it matters`, and `Limits / caveats`.",
            "- Keep the writing factual and grounded in the provided page only.",
            "- If the context is insufficient to support a claim, say so briefly instead of guessing.",
            "- Do not mention these instructions in the answer.",
        ]
    ).strip()


def load_paper_summary_prompt(prompt_path: Path) -> str:
    try:
        raw = prompt_path.read_text(encoding="utf-8")
    except OSError:
        return default_paper_summary_prompt()
    prompt = str(raw or "").strip()
    return prompt or default_paper_summary_prompt()


def normalize_highlight_capture(value: Any) -> dict[str, Any] | None:
    if not isinstance(value, dict):
        return None
    kind = str(value.get("kind", "") or "").strip().lower()
    if kind != "explain_selection":
        return None
    selection = compact_text_block(value.get("selection", ""), 1600)
    if not selection:
        return None
    return {
        "kind": "explain_selection",
        "selection": selection,
        "prompt": compact_text_block(value.get("prompt", ""), 600),
        "response": compact_text_block(value.get("response", ""), 4000),
        "paper_version": normalize_paper_version(value.get("paper_version", value.get("paperVersion"))),
        "conversation_id": str(value.get("conversation_id", value.get("conversationId")) or "").strip()[:120],
        "created_at": str(value.get("created_at", value.get("createdAt")) or "").strip()[:80],
    }


def highlight_capture_signature(value: Any) -> tuple[str, str, str, str, str]:
    capture = normalize_highlight_capture(value)
    if capture is None:
        return ("", "", "", "", "")
    return (
        capture["kind"],
        capture["selection"],
        capture["prompt"],
        capture["response"],
        capture.get("paper_version", ""),
    )


def normalize_highlight_capture_list(value: Any, *, limit: int = 24) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        return []
    captures: list[dict[str, Any]] = []
    seen: set[tuple[str, str, str, str, str]] = set()
    for item in value:
        capture = normalize_highlight_capture(item)
        if capture is None:
            continue
        signature = highlight_capture_signature(capture)
        if signature in seen:
            continue
        seen.add(signature)
        captures.append(capture)
        if len(captures) >= limit:
            break
    return captures


def normalize_paper_highlight_entry(value: Any) -> Any:
    capture = normalize_highlight_capture(value)
    if capture is not None:
        if not capture.get("response", ""):
            return None
        return capture
    text = compact_text_block(value, 400)
    return text or None


def paper_highlight_signature(value: Any) -> tuple[str, str, str, str, str, str]:
    capture = normalize_highlight_capture(value)
    if capture is not None and capture.get("response", ""):
        kind, selection, prompt, response, paper_version = highlight_capture_signature(capture)
        return ("structured", kind, selection, prompt, response, paper_version)
    return ("legacy", compact_text_block(value, 400), "", "", "", "")


def normalize_paper_highlights(value: Any, *, limit: int = 64) -> list[Any]:
    if not isinstance(value, list):
        return []
    highlights: list[Any] = []
    seen: set[tuple[str, str, str, str, str, str]] = set()
    for item in value:
        normalized = normalize_paper_highlight_entry(item)
        if normalized is None:
            continue
        signature = paper_highlight_signature(normalized)
        if signature in seen:
            continue
        seen.add(signature)
        highlights.append(normalized)
        if len(highlights) >= limit:
            break
    return highlights


class PaperStateStore:
    def __init__(self, root: Path, *, now_iso_func: Callable[[], str] = now_iso) -> None:
        self._root = root
        self._dir = root / "papers"
        self._dir.mkdir(parents=True, exist_ok=True)
        self._now_iso = now_iso_func
        try:
            os.chmod(self._root, 0o700)
            os.chmod(self._dir, 0o700)
        except OSError:
            pass

    def _filename(self, source: str, paper_id: str) -> str:
        safe_id = re.sub(r"[^A-Za-z0-9._-]+", "_", paper_id).strip("._")
        if not safe_id:
            safe_id = sha1(paper_id.encode("utf-8")).hexdigest()[:16]
        return f"{source}--{safe_id}.json"

    def _path(self, source: str, paper_id: str) -> Path:
        validated_source = normalize_paper_source(source)
        validated_paper_id = normalize_paper_id(paper_id)
        return self._dir / self._filename(validated_source, validated_paper_id)

    def _normalize_highlights(self, value: Any) -> list[Any]:
        return normalize_paper_highlights(value)

    def _normalize_observed_versions(self, value: Any) -> list[str]:
        return normalize_paper_versions(value)

    def _merge_observed_versions(self, record: dict[str, Any], *version_values: Any) -> dict[str, Any]:
        versions = normalize_paper_versions(record.get("observed_versions"))
        for value in version_values:
            versions = normalize_paper_versions([*versions, *normalize_paper_versions(value)])
        record["observed_versions"] = versions
        return record

    def _normalize_record(
        self,
        value: Any,
        *,
        source: str | None = None,
        paper_id: str | None = None,
    ) -> dict[str, Any]:
        raw = value if isinstance(value, dict) else {}
        normalized_source = normalize_paper_source(source or raw.get("source"))
        raw_identifier = paper_id or raw.get("paper_id")
        normalized_paper_id = normalize_paper_id(
            canonicalize_arxiv_identifier(raw_identifier) if normalized_source == "arxiv" else raw_identifier
        )
        stamp = self._now_iso()
        status = str(raw.get("summary_status", "idle") or "idle").strip().lower()
        if status not in PAPER_STATUS_VALUES:
            status = "idle"
        canonical_url = str(raw.get("canonical_url", "") or "").strip()[: PAGE_CONTEXT_FIELD_LIMITS["url"]]
        if not canonical_url and normalized_source == "arxiv":
            canonical_url = f"https://arxiv.org/abs/{normalized_paper_id}"
        return {
            "source": normalized_source,
            "paper_id": normalized_paper_id,
            "canonical_url": canonical_url,
            "title": " ".join(str(raw.get("title", "") or "").split())[:240],
            "observed_versions": self._normalize_observed_versions(raw.get("observed_versions")),
            "summary": str(raw.get("summary", "") or ""),
            "summary_status": status,
            "summary_requested_at": str(raw.get("summary_requested_at", "") or ""),
            "last_summary_conversation_id": str(raw.get("last_summary_conversation_id", "") or ""),
            "last_summary_version": normalize_paper_version(raw.get("last_summary_version", "")),
            "summary_error": str(raw.get("summary_error", "") or "")[:800],
            "highlights": self._normalize_highlights(raw.get("highlights")),
            "created_at": str(raw.get("created_at", "") or stamp),
            "updated_at": str(raw.get("updated_at", "") or stamp),
        }

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

    def get_or_create(
        self,
        source: str,
        paper_id: str,
        *,
        canonical_url: str = "",
        title: str = "",
    ) -> dict[str, Any]:
        path = self._path(source, paper_id)
        if path.exists():
            return self._normalize_record(
                json.loads(path.read_text(encoding="utf-8")),
                source=source,
                paper_id=paper_id,
            )
        record = self._normalize_record(
            {
                "source": source,
                "paper_id": paper_id,
                "canonical_url": canonical_url,
                "title": title,
                "summary": "",
                "summary_status": "idle",
                "summary_requested_at": "",
                "last_summary_conversation_id": "",
                "last_summary_version": "",
                "summary_error": "",
                "highlights": [],
                "observed_versions": [],
            },
            source=source,
            paper_id=paper_id,
        )
        self._write(path, record)
        return record

    def save(self, record: dict[str, Any]) -> dict[str, Any]:
        normalized = self._normalize_record(record)
        normalized["updated_at"] = self._now_iso()
        self._write(self._path(normalized["source"], normalized["paper_id"]), normalized)
        return normalized

    def add_highlight(
        self,
        source: str,
        paper_id: str,
        *,
        canonical_url: str = "",
        title: str = "",
        highlight: Any = None,
    ) -> dict[str, Any]:
        record = self.get_or_create(source, paper_id, canonical_url=canonical_url, title=title)
        if title:
            record["title"] = " ".join(str(title).split())[:240]
        if canonical_url:
            record["canonical_url"] = str(canonical_url).strip()[: PAGE_CONTEXT_FIELD_LIMITS["url"]]
        normalized_highlights = self._normalize_highlights([highlight])
        if normalized_highlights:
            clean_highlight = normalized_highlights[0]
            clean_signature = paper_highlight_signature(clean_highlight)
            existing = [
                item
                for item in self._normalize_highlights(record.get("highlights"))
                if paper_highlight_signature(item) != clean_signature
            ]
            record["highlights"] = [clean_highlight, *existing][:64]
            record = self._merge_observed_versions(record, clean_highlight.get("paper_version", ""))
        return self.save(record)

    def mark_summary_requested(
        self,
        source: str,
        paper_id: str,
        *,
        canonical_url: str = "",
        title: str = "",
        conversation_id: str = "",
        paper_version: str = "",
    ) -> dict[str, Any]:
        record = self.get_or_create(source, paper_id, canonical_url=canonical_url, title=title)
        if title:
            record["title"] = " ".join(str(title).split())[:240]
        if canonical_url:
            record["canonical_url"] = str(canonical_url).strip()[: PAGE_CONTEXT_FIELD_LIMITS["url"]]
        record["summary_status"] = "requested"
        record["summary_requested_at"] = self._now_iso()
        record["last_summary_conversation_id"] = str(conversation_id or "").strip()
        normalized_version = normalize_paper_version(paper_version)
        if normalized_version:
            record["last_summary_version"] = normalized_version
            record = self._merge_observed_versions(record, normalized_version)
        record["summary_error"] = ""
        return self.save(record)

    def store_summary_result(
        self,
        source: str,
        paper_id: str,
        *,
        canonical_url: str = "",
        title: str = "",
        conversation_id: str = "",
        paper_version: str = "",
        summary: str = "",
        error: str = "",
    ) -> dict[str, Any]:
        record = self.get_or_create(source, paper_id, canonical_url=canonical_url, title=title)
        if title:
            record["title"] = " ".join(str(title).split())[:240]
        if canonical_url:
            record["canonical_url"] = str(canonical_url).strip()[: PAGE_CONTEXT_FIELD_LIMITS["url"]]
        if conversation_id:
            record["last_summary_conversation_id"] = str(conversation_id or "").strip()
        normalized_version = normalize_paper_version(paper_version)
        if normalized_version:
            record["last_summary_version"] = normalized_version
            record = self._merge_observed_versions(record, normalized_version)
        clean_summary = str(summary or "").strip()
        if clean_summary:
            record["summary"] = clean_summary
            record["summary_status"] = "ready"
            record["summary_error"] = ""
        else:
            record["summary_status"] = "error"
            record["summary_error"] = str(error or "Paper summary generation failed.").strip()[:800]
        return self.save(record)


def collect_paper_versions_from_conversations(conversations: list[dict[str, Any]]) -> list[str]:
    versions: list[str] = []
    for conversation in conversations:
        if not isinstance(conversation, dict):
            continue
        paper = conversation.get("paper")
        if not isinstance(paper, dict):
            continue
        version = normalize_paper_version(paper.get("paper_version", ""))
        if version:
            versions.append(version)
    return normalize_paper_versions(versions)


def normalize_paper_memory_limit(
    value: Any,
    *,
    default: int,
    max_limit: int,
) -> int:
    try:
        limit = int(value)
    except (TypeError, ValueError):
        limit = default
    return max(1, min(max_limit, limit))


def paper_memory_kind_rank(kind: str) -> int:
    normalized = str(kind or "").strip().lower()
    if normalized == "summary":
        return 0
    if normalized == "highlight":
        return 1
    return 2


def build_paper_memory_metadata(record: dict[str, Any], conversations: list[dict[str, Any]]) -> dict[str, Any]:
    counts_by_version: dict[str, int] = {}
    latest_updated_at = str(record.get("updated_at", "") or "")
    has_unversioned = False

    summary_text = str(record.get("summary", "") or "").strip()
    summary_version = normalize_paper_version(record.get("last_summary_version", ""))
    if summary_text and summary_version:
        counts_by_version[summary_version] = counts_by_version.get(summary_version, 0) + 1

    for highlight in normalize_paper_highlights(record.get("highlights")):
        version = normalize_paper_version(highlight.get("paper_version", ""))
        created_at = str(highlight.get("created_at", "") or "")
        if created_at > latest_updated_at:
            latest_updated_at = created_at
        if version:
            counts_by_version[version] = counts_by_version.get(version, 0) + 1
        else:
            has_unversioned = True

    for conversation in conversations:
        updated_at = str(conversation.get("updated_at", "") or "")
        if updated_at > latest_updated_at:
            latest_updated_at = updated_at
        paper = conversation.get("paper") if isinstance(conversation.get("paper"), dict) else {}
        version = normalize_paper_version(paper.get("paper_version", ""))
        if version:
            counts_by_version[version] = counts_by_version.get(version, 0) + 1
        else:
            has_unversioned = True

    ordered_versions = normalize_paper_versions(
        [*normalize_paper_versions(record.get("observed_versions")), *counts_by_version.keys()]
    )
    ordered_counts = {
        version: int(counts_by_version.get(version, 0))
        for version in ordered_versions
        if int(counts_by_version.get(version, 0)) > 0
    }
    default_version = ordered_versions[0] if ordered_versions else ""
    return {
        "default_version": default_version,
        "counts_by_version": ordered_counts,
        "has_unversioned": bool(has_unversioned),
        "latest_updated_at": latest_updated_at,
    }


def build_paper_workspace(
    papers_store: Any,
    conversations_store: Any,
    source: str,
    paper_id: str,
) -> dict[str, Any]:
    normalized_source = normalize_paper_source(source)
    normalized_paper_id = normalize_paper_id(
        canonicalize_arxiv_identifier(paper_id) if normalized_source == "arxiv" else paper_id
    )
    conversations = conversations_store.list_metadata(
        paper_source=normalized_source,
        paper_id=normalized_paper_id,
    )
    record = papers_store.get_or_create(normalized_source, normalized_paper_id)

    updated = False
    if not record.get("canonical_url") and normalized_source == "arxiv":
        record["canonical_url"] = f"https://arxiv.org/abs/{normalized_paper_id}"
        updated = True
    if conversations:
        observed_versions = normalize_paper_versions(record.get("observed_versions"))
        observed_versions = normalize_paper_versions(
            [*observed_versions, *collect_paper_versions_from_conversations(conversations)]
        )
        if observed_versions != normalize_paper_versions(record.get("observed_versions")):
            record["observed_versions"] = observed_versions
            updated = True
        latest_paper = conversations[0].get("paper")
        if isinstance(latest_paper, dict):
            latest_url = str(latest_paper.get("canonical_url", "") or "")
            latest_title = str(latest_paper.get("title", "") or "")
            if latest_url and record.get("canonical_url") != latest_url:
                record["canonical_url"] = latest_url
                updated = True
            if latest_title and not record.get("title"):
                record["title"] = latest_title
                updated = True
    if updated:
        record = papers_store.save(record)
    return {
        "paper": record,
        "conversations": conversations,
        "memory": build_paper_memory_metadata(record, conversations),
    }


def build_paper_memory_candidates(
    record: dict[str, Any],
    conversations: list[dict[str, Any]],
    *,
    requested_version: str,
    exclude_conversation_id: str = "",
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    exact_candidates: list[dict[str, Any]] = []
    fallback_candidates: list[dict[str, Any]] = []
    source = str(record.get("source", "") or "")
    paper_id = str(record.get("paper_id", "") or "")
    normalized_requested_version = normalize_paper_version(requested_version)
    excluded_id = str(exclude_conversation_id or "").strip()

    summary_text = compact_text_block(str(record.get("summary", "") or ""), 560)
    summary_version = normalize_paper_version(record.get("last_summary_version", ""))
    if normalized_requested_version and summary_text and summary_version == normalized_requested_version:
        exact_candidates.append(
            {
                "entry": {
                    "id": f"summary:{source}:{paper_id}:{normalized_requested_version}",
                    "kind": "summary",
                    "paper_version": normalized_requested_version,
                    "title": compact_whitespace(
                        str(record.get("title", "") or f"arXiv:{paper_id}"),
                        120,
                    ),
                    "snippet": summary_text,
                    "conversation_id": str(record.get("last_summary_conversation_id", "") or ""),
                    "updated_at": str(record.get("updated_at", "") or ""),
                    "source_label": "Summary",
                },
                "search_text": " ".join(
                    [
                        str(record.get("title", "") or ""),
                        summary_text,
                        "summary",
                        normalized_requested_version,
                    ]
                ).lower(),
                "exact": True,
            }
        )

    for highlight in normalize_paper_highlights(record.get("highlights")):
        highlight_version = normalize_paper_version(highlight.get("paper_version", ""))
        if not highlight_version or highlight_version != normalized_requested_version:
            continue
        selection = compact_whitespace(str(highlight.get("selection", "") or ""), 220)
        response = compact_text_block(str(highlight.get("response", "") or ""), 340)
        snippet = response or selection
        if selection and response and selection.lower() not in response.lower():
            snippet = compact_text_block(f"{selection} {response}", 420)
        exact_candidates.append(
            {
                "entry": {
                    "id": f"highlight:{source}:{paper_id}:{highlight_version}:{sha1(json.dumps(highlight, sort_keys=True).encode('utf-8')).hexdigest()[:12]}",
                    "kind": "highlight",
                    "paper_version": highlight_version,
                    "title": compact_whitespace(
                        str(highlight.get("prompt", "") or highlight.get("selection", "") or "Saved highlight"),
                        120,
                    ),
                    "snippet": snippet,
                    "conversation_id": str(highlight.get("conversation_id", "") or ""),
                    "updated_at": str(highlight.get("created_at", "") or ""),
                    "source_label": "Highlight",
                },
                "search_text": " ".join(
                    [
                        str(highlight.get("prompt", "") or ""),
                        str(highlight.get("selection", "") or ""),
                        str(highlight.get("response", "") or ""),
                        "highlight",
                        highlight_version,
                    ]
                ).lower(),
                "exact": True,
            }
        )

    for conversation in conversations:
        conversation_id = str(conversation.get("id", "") or "").strip()
        if not conversation_id or (excluded_id and conversation_id == excluded_id):
            continue
        paper = conversation.get("paper") if isinstance(conversation.get("paper"), dict) else {}
        conversation_version = normalize_paper_version(paper.get("paper_version", ""))
        if normalized_requested_version:
            if conversation_version == normalized_requested_version:
                target = exact_candidates
                is_exact = True
            elif not conversation_version:
                target = fallback_candidates
                is_exact = False
            else:
                continue
        else:
            if conversation_version:
                continue
            target = fallback_candidates
            is_exact = False
        title = compact_whitespace(
            str(conversation.get("paper_history_label", "") or conversation.get("title", "") or "Prior chat"),
            120,
        )
        preview = compact_text_block(str(conversation.get("preview", "") or ""), 320)
        summary = compact_text_block(str(conversation.get("summary", "") or ""), 320)
        focus_text = compact_whitespace(str(conversation.get("paper_focus_text", "") or ""), 220)
        snippet = summary or preview or focus_text or title
        target.append(
            {
                "entry": {
                    "id": f"conversation:{conversation_id}",
                    "kind": "conversation",
                    "paper_version": conversation_version,
                    "title": title,
                    "snippet": snippet,
                    "conversation_id": conversation_id,
                    "updated_at": str(conversation.get("updated_at", "") or ""),
                    "source_label": "Prior chat",
                },
                "search_text": " ".join(
                    [
                        title,
                        summary,
                        preview,
                        focus_text,
                        str(conversation.get("paper_history_label", "") or ""),
                        "conversation prior chat",
                    ]
                ).lower(),
                "exact": is_exact,
            }
        )

    return exact_candidates, fallback_candidates


def rank_paper_memory_candidates(
    exact_candidates: list[dict[str, Any]],
    fallback_candidates: list[dict[str, Any]],
    *,
    query: str,
    limit: int,
) -> list[dict[str, Any]]:
    normalized_query = compact_whitespace(query, 240).lower()
    if not normalized_query:
        ordered_exact = list(exact_candidates)
        ordered_fallback = list(fallback_candidates)
        ordered_exact.sort(key=lambda item: str(item["entry"].get("id", "") or ""))
        ordered_exact.sort(key=lambda item: str(item["entry"].get("updated_at", "") or ""), reverse=True)
        ordered_exact.sort(key=lambda item: paper_memory_kind_rank(item["entry"].get("kind", "")))
        ordered_fallback.sort(key=lambda item: str(item["entry"].get("id", "") or ""))
        ordered_fallback.sort(key=lambda item: str(item["entry"].get("updated_at", "") or ""), reverse=True)
        ordered_fallback.sort(key=lambda item: paper_memory_kind_rank(item["entry"].get("kind", "")))
        return [item["entry"] for item in [*ordered_exact, *ordered_fallback][:limit]]

    tokens = [token for token in re.split(r"[^a-z0-9]+", normalized_query) if token]

    def _matches(item: dict[str, Any]) -> bool:
        haystack = str(item.get("search_text", "") or "")
        if normalized_query in haystack:
            return True
        return any(token in haystack for token in tokens)

    def _score(item: dict[str, Any]) -> int:
        entry = item["entry"]
        haystack = str(item.get("search_text", "") or "")
        title = str(entry.get("title", "") or "").lower()
        score = 0
        if normalized_query in haystack:
            score += 120
        for token in tokens:
            if token in haystack:
                score += 18
            if token in title:
                score += 6
        if item.get("exact"):
            score += 40
        elif not str(entry.get("paper_version", "") or "").strip():
            score += 8
        kind = str(entry.get("kind", "") or "")
        if kind == "summary":
            score += 48
        elif kind == "highlight":
            score += 16
        else:
            score += 8
        return score

    ranked = [item for item in [*exact_candidates, *fallback_candidates] if _matches(item)]
    ranked.sort(key=lambda item: str(item["entry"].get("id", "") or ""))
    ranked.sort(key=lambda item: str(item["entry"].get("updated_at", "") or ""), reverse=True)
    ranked.sort(key=lambda item: paper_memory_kind_rank(item["entry"].get("kind", "")))
    ranked.sort(key=lambda item: _score(item), reverse=True)
    return [item["entry"] for item in ranked[:limit]]


def query_paper_memory(
    papers_store: Any,
    conversations_store: Any,
    paper_context: dict[str, Any],
    *,
    query: str = "",
    limit: int,
    default_limit: int,
    max_limit: int,
    exclude_conversation_id: str = "",
) -> dict[str, Any]:
    workspace = build_paper_workspace(
        papers_store,
        conversations_store,
        paper_context["source"],
        paper_context["paper_id"],
    )
    requested_version = normalize_paper_version(
        paper_context.get("paper_version", "") or workspace.get("memory", {}).get("default_version", "")
    )
    normalized_limit = normalize_paper_memory_limit(
        limit,
        default=default_limit,
        max_limit=max_limit,
    )
    exact_candidates, fallback_candidates = build_paper_memory_candidates(
        workspace["paper"],
        workspace["conversations"],
        requested_version=requested_version,
        exclude_conversation_id=exclude_conversation_id,
    )
    results = rank_paper_memory_candidates(
        exact_candidates,
        fallback_candidates,
        query=query,
        limit=normalized_limit,
    )
    return {
        "paper": workspace["paper"],
        "memory_version": requested_version,
        "results": results,
        "counts": {
            "exact_version_count": len(exact_candidates),
            "unversioned_fallback_count": len(fallback_candidates),
        },
    }


def format_paper_memory_prompt_block(memory_result: dict[str, Any]) -> str:
    results = memory_result.get("results") if isinstance(memory_result, dict) else None
    if not isinstance(results, list) or not results:
        return ""
    version = normalize_paper_version(memory_result.get("memory_version", ""))
    lines = ["[Paper Memory]"]
    if version:
        lines.append(f"Version: {version}")
    for entry in results:
        if not isinstance(entry, dict):
            continue
        label = str(entry.get("source_label", "") or "").strip() or str(entry.get("kind", "") or "Memory").title()
        title = compact_whitespace(str(entry.get("title", "") or ""), 120)
        snippet = compact_text_block(str(entry.get("snippet", "") or ""), 280)
        entry_version = normalize_paper_version(entry.get("paper_version", ""))
        version_suffix = f" ({entry_version})" if entry_version else ""
        if title and snippet and title.lower() not in snippet.lower():
            lines.append(f"- {label}{version_suffix}: {title}: {snippet}")
        elif snippet:
            lines.append(f"- {label}{version_suffix}: {snippet}")
        elif title:
            lines.append(f"- {label}{version_suffix}: {title}")
    return "\n".join(lines) if len(lines) > 1 else ""
