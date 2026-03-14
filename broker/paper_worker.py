#!/usr/bin/env python3
from __future__ import annotations

import hashlib
import html
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
from datetime import datetime, timezone
from html.parser import HTMLParser
from pathlib import Path
from typing import Any
from urllib.parse import urljoin, urlparse
from urllib.request import Request, urlopen


USER_AGENT = "AssistPaperWorker/0.1 (+http://127.0.0.1)"
MAX_INSPECT_BYTES = 1_500_000
MAX_EXTRACT_BYTES = 10_000_000
MAX_TEXT_CHARS = 200_000
MAX_SECTION_CHARS = 12_000
MAX_SECTIONS = 24
PDF_EXTRACTOR_FIXES = [
    "pip install pypdf",
    "pip install PyPDF2",
    "install pdftotext (Poppler) and ensure it is on PATH",
]


class WorkerError(RuntimeError):
    def __init__(self, code: str, message: str, *, details: dict[str, Any] | None = None) -> None:
        super().__init__(message)
        self.code = str(code or "worker_failed")
        self.details = details or {}


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def emit(payload: dict[str, Any]) -> int:
    sys.stdout.write(json.dumps(payload, ensure_ascii=True) + "\n")
    sys.stdout.flush()
    return 0


def fail(code: str, message: str, *, details: dict[str, Any] | None = None) -> int:
    return emit(
        {
            "ok": False,
            "error": {
                "code": code,
                "message": message,
                "details": details or {},
            },
            "created_at": now_iso(),
        }
    )


def compact_text(value: Any) -> str:
    return " ".join(str(value or "").split()).strip()


def compact_paragraphs(value: Any, limit: int) -> str:
    raw = str(value or "").replace("\r\n", "\n").replace("\r", "\n")
    blocks: list[str] = []
    for part in re.split(r"\n\s*\n", raw):
        cleaned = compact_text(part)
        if cleaned:
            blocks.append(cleaned)
    text = "\n\n".join(blocks)
    return text[:limit]


def coerce_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    return str(value or "").strip().lower() in {"1", "true", "yes", "on"}


def sha1_text(value: str) -> str:
    return hashlib.sha1(value.encode("utf-8", "ignore")).hexdigest()


class HeadMetadataParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.in_title = False
        self.title_parts: list[str] = []
        self.meta: dict[str, list[str]] = {}
        self.links: list[dict[str, str]] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        lowered = tag.lower()
        attr_map = {key.lower(): (value or "") for key, value in attrs}
        if lowered == "title":
            self.in_title = True
            return
        if lowered == "meta":
            key = attr_map.get("name") or attr_map.get("property") or attr_map.get("http-equiv")
            value = attr_map.get("content", "")
            if key and value:
                self.meta.setdefault(key.strip().lower(), []).append(value.strip())
            return
        if lowered == "link":
            href = attr_map.get("href", "").strip()
            rel = attr_map.get("rel", "").strip().lower()
            if href:
                self.links.append({"href": href, "rel": rel})

    def handle_endtag(self, tag: str) -> None:
        if tag.lower() == "title":
            self.in_title = False

    def handle_data(self, data: str) -> None:
        if self.in_title:
            self.title_parts.append(data)

    def single(self, key: str) -> str:
        values = self.meta.get(key.lower(), [])
        return values[0] if values else ""

    def all_values(self, key: str) -> list[str]:
        return list(self.meta.get(key.lower(), []))

    @property
    def title(self) -> str:
        return compact_text("".join(self.title_parts))


class HTMLTextStripper(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.parts: list[str] = []

    def handle_data(self, data: str) -> None:
        if data:
            self.parts.append(data)

    def text(self) -> str:
        return "".join(self.parts)


def normalize_url(value: str) -> str:
    parsed = urlparse(value.strip())
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        raise ValueError("url must be http(s).")
    return parsed.geturl()


def fetch_url(url: str, *, max_bytes: int) -> tuple[bytes, str, str]:
    request = Request(
        url,
        headers={
            "User-Agent": USER_AGENT,
            "Accept": "text/html,application/pdf,text/plain;q=0.9,*/*;q=0.5",
        },
    )
    with urlopen(request, timeout=30) as response:
        data = response.read(max_bytes + 1)
        if len(data) > max_bytes:
            raise ValueError(f"remote payload exceeds {max_bytes} bytes.")
        content_type = str(response.headers.get("Content-Type", "") or "").strip().lower()
        final_url = str(response.geturl() or url)
        return data, content_type, final_url


def resolve_source(payload: dict[str, Any], *, mode: str) -> dict[str, Any]:
    url = str(payload.get("url", "") or "").strip()
    pdf_path = str(payload.get("pdf_path", payload.get("pdfPath", "")) or "").strip()
    text_path = str(payload.get("text_path", payload.get("textPath", "")) or "").strip()
    html_path = str(payload.get("html_path", payload.get("htmlPath", "")) or "").strip()
    max_bytes = MAX_EXTRACT_BYTES if mode == "extract" else MAX_INSPECT_BYTES

    if url:
        normalized_url = normalize_url(url)
        raw, content_type, final_url = fetch_url(normalized_url, max_bytes=max_bytes)
        return {
            "source_type": "url",
            "url": final_url,
            "content_type": content_type,
            "raw_bytes": raw,
            "display_name": Path(urlparse(final_url).path or "/").name or final_url,
        }
    for path_value, source_type in ((pdf_path, "pdf_path"), (html_path, "html_path"), (text_path, "text_path")):
        if not path_value:
            continue
        resolved = Path(path_value).expanduser().resolve()
        if not resolved.exists():
            raise ValueError(f"path does not exist: {resolved}")
        if not resolved.is_file():
            raise ValueError(f"path is not a file: {resolved}")
        raw = resolved.read_bytes()
        if len(raw) > max_bytes:
            raise ValueError(f"file exceeds {max_bytes} bytes: {resolved}")
        if source_type == "pdf_path":
            content_type = "application/pdf"
        elif source_type == "html_path":
            content_type = "text/html"
        else:
            content_type = "text/plain"
        return {
            "source_type": source_type,
            "local_path": str(resolved),
            "content_type": content_type,
            "raw_bytes": raw,
            "display_name": resolved.name,
        }
    raise ValueError("one of url, pdf_path, html_path, or text_path is required.")


def decode_text(raw: bytes) -> str:
    if not raw:
        return ""
    for encoding in ("utf-8", "utf-8-sig", "latin-1"):
        try:
            return raw.decode(encoding)
        except UnicodeDecodeError:
            continue
    return raw.decode("utf-8", "ignore")


def extract_head_metadata(html_text: str) -> HeadMetadataParser:
    parser = HeadMetadataParser()
    parser.feed(html_text)
    parser.close()
    return parser


def detect_source_format(content_type: str, display_name: str) -> str:
    lowered = content_type.lower()
    suffix = Path(display_name).suffix.lower()
    if "pdf" in lowered or suffix == ".pdf":
        return "pdf"
    if "html" in lowered or suffix in {".html", ".htm"}:
        return "html"
    if lowered.startswith("text/") or suffix in {".txt", ".md"}:
        return "text"
    return "html"


def text_from_html_fragment(value: str) -> str:
    stripper = HTMLTextStripper()
    stripper.feed(value)
    stripper.close()
    return compact_text(html.unescape(stripper.text()))


def body_fragment(html_text: str) -> str:
    match = re.search(r"(?is)<body\b[^>]*>(.*)</body>", html_text)
    return match.group(1) if match else html_text


def reindex_sections(sections: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    headings: list[dict[str, Any]] = []
    for index, section in enumerate(sections[:MAX_SECTIONS], start=1):
        text = compact_paragraphs(section.get("text", ""), MAX_SECTION_CHARS)
        section_id = f"sec_{index:03d}"
        section["section_id"] = section_id
        section["heading"] = compact_text(section.get("heading", "")) or f"Section {index}"
        section["level"] = max(1, min(4, int(section.get("level", 1) or 1)))
        section["text"] = text
        section["char_count"] = len(text)
        section["preview"] = compact_text(text[:240])
        headings.append(
            {
                "section_id": section_id,
                "heading": section["heading"],
                "level": section["level"],
            }
        )
    return sections[:MAX_SECTIONS], headings


def merge_redundant_document_section(
    sections: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    if len(sections) < 2:
        return reindex_sections(sections)
    first = sections[0]
    second = sections[1]
    if compact_text(first.get("heading", "")) != "Document":
        return reindex_sections(sections)
    if int(second.get("level", 1) or 1) != 1:
        return reindex_sections(sections)

    intro = compact_paragraphs(first.get("text", ""), MAX_SECTION_CHARS)
    intro_clean = compact_text(intro)
    heading_clean = compact_text(second.get("heading", ""))
    if not heading_clean:
        return reindex_sections(sections)

    intro_lower = intro_clean.lower()
    heading_lower = heading_clean.lower()
    repeated_heading = intro_lower == heading_lower
    should_merge = repeated_heading or len(intro_clean) <= 280
    if not should_merge:
        return reindex_sections(sections)

    second_parts = []
    if intro_clean and not repeated_heading:
        second_parts.append(intro)
    second_parts.append(str(second.get("text", "") or ""))
    second["text"] = compact_paragraphs("\n\n".join(part for part in second_parts if part), MAX_SECTION_CHARS)
    return reindex_sections(sections[1:])


def extract_arxiv_id(url: str) -> str:
    parsed = urlparse(url)
    path = parsed.path.strip("/")
    if path.startswith("abs/"):
        return path.split("/", 1)[1]
    if path.startswith("pdf/"):
        identifier = path.split("/", 1)[1]
        return re.sub(r"\.pdf$", "", identifier)
    return ""


def inspect_html(source: dict[str, Any], html_text: str) -> dict[str, Any]:
    meta = extract_head_metadata(html_text)
    url = str(source.get("url", "") or "")
    arxiv_id = extract_arxiv_id(url)
    authors = [compact_text(value) for value in meta.all_values("citation_author") if compact_text(value)]
    title = (
        compact_text(meta.single("citation_title"))
        or compact_text(meta.single("og:title"))
        or meta.title
        or compact_text(source.get("display_name"))
    )
    abstract = (
        compact_paragraphs(meta.single("citation_abstract"), 4000)
        or compact_paragraphs(meta.single("description"), 4000)
        or compact_paragraphs(meta.single("og:description"), 4000)
    )
    pdf_url = compact_text(meta.single("citation_pdf_url"))
    if pdf_url and url:
        pdf_url = urljoin(url, pdf_url)
    if not pdf_url and arxiv_id:
        pdf_url = f"https://arxiv.org/pdf/{arxiv_id}.pdf"
    return {
        "title": title,
        "authors": authors,
        "abstract": abstract,
        "pdf_url": pdf_url,
        "arxiv_id": arxiv_id,
        "preview_text": abstract or compact_paragraphs(text_from_html_fragment(html_text), 1200),
    }


def html_to_sections(html_text: str) -> tuple[list[dict[str, Any]], list[dict[str, Any]], str]:
    cleaned = body_fragment(html_text)
    cleaned = re.sub(r"(?is)<!--.*?-->", " ", cleaned)
    cleaned = re.sub(r"(?is)<(script|style|noscript|svg|nav|footer|aside|form|template)[^>]*>.*?</\1>", " ", cleaned)
    cleaned = re.sub(r"(?is)<br\s*/?>", "\n", cleaned)
    cleaned = re.sub(
        r"(?is)</(p|div|section|article|header|li|tr|td|th|table|ul|ol|main|blockquote|pre|dl|dt|dd|figure|figcaption)>",
        "\n\n",
        cleaned,
    )

    def heading_marker(match: re.Match[str]) -> str:
        level = match.group(1)
        content = text_from_html_fragment(match.group(2))
        return f"\n\n@@H{level}@@ {content}\n\n"

    cleaned = re.sub(r"(?is)<h([1-4])[^>]*>(.*?)</h\1>", heading_marker, cleaned)
    stripper = HTMLTextStripper()
    stripper.feed(cleaned)
    stripper.close()
    text = html.unescape(stripper.text())
    text = (
        text.replace("\r\n", "\n")
        .replace("\r", "\n")
    )
    text = re.sub(r"[ \t]+\n", "\n", text)
    text = re.sub(r"\n[ \t]+", "\n", text)
    text = re.sub(r"[ \t]{2,}", " ", text)
    text = re.sub(r"\n{3,}", "\n\n", text).strip()
    text = text[:MAX_TEXT_CHARS]
    sections: list[dict[str, Any]] = []

    current_heading = "Document"
    current_level = 1
    buffer: list[str] = []

    def flush() -> None:
        nonlocal buffer, current_heading, current_level
        body = compact_paragraphs("\n\n".join(buffer), MAX_SECTION_CHARS)
        buffer = []
        if not body:
            return
        sections.append(
            {
                "heading": current_heading,
                "level": current_level,
                "text": body,
            }
        )

    for block in re.split(r"\n\s*\n", text):
        line = block.strip()
        if not line:
            continue
        heading_match = re.match(r"^@@H([1-4])@@\s+(.*)$", line)
        if heading_match:
            flush()
            current_level = int(heading_match.group(1))
            current_heading = compact_text(heading_match.group(2)) or f"Section {len(sections) + 1}"
            continue
        buffer.append(line)
    flush()

    if not sections and text:
        sections.append(
            {
                "heading": "Document",
                "level": 1,
                "text": text[:MAX_SECTION_CHARS],
            }
        )

    sections, headings = merge_redundant_document_section(sections)

    full_text = "\n\n".join(str(section.get("text", "")) for section in sections if str(section.get("text", "")).strip())
    full_text = compact_paragraphs(full_text or text, MAX_TEXT_CHARS)
    return sections[:MAX_SECTIONS], headings[:MAX_SECTIONS], full_text


def split_text_sections(text: str) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    paragraphs = [compact_text(part) for part in re.split(r"\n\s*\n", text) if compact_text(part)]
    sections: list[dict[str, Any]] = []
    current_chunks: list[str] = []

    def flush() -> None:
        nonlocal current_chunks
        body = "\n\n".join(current_chunks).strip()
        current_chunks = []
        if not body:
            return
        heading = f"Section {len(sections) + 1}"
        sections.append(
            {
                "heading": heading,
                "level": 1,
                "text": body[:MAX_SECTION_CHARS],
            }
        )

    for paragraph in paragraphs:
        if current_chunks and sum(len(part) for part in current_chunks) + len(paragraph) > MAX_SECTION_CHARS:
            flush()
        current_chunks.append(paragraph)
        if len(sections) >= MAX_SECTIONS:
            break
    flush()
    return reindex_sections(sections)


def extract_pdf_text_from_path(path: Path) -> str:
    attempts: list[str] = []
    errors: list[str] = []
    try:
        from pypdf import PdfReader  # type: ignore

        attempts.append("pypdf")
        reader = PdfReader(str(path))
        text = "\n\n".join(page.extract_text() or "" for page in reader.pages)
        if compact_text(text):
            return text
        errors.append("pypdf returned no extractable text")
    except ImportError:
        pass
    except Exception as error:
        errors.append(f"pypdf: {error}")
    try:
        from PyPDF2 import PdfReader  # type: ignore

        attempts.append("PyPDF2")
        reader = PdfReader(str(path))
        text = "\n\n".join(page.extract_text() or "" for page in reader.pages)
        if compact_text(text):
            return text
        errors.append("PyPDF2 returned no extractable text")
    except ImportError:
        pass
    except Exception as error:
        errors.append(f"PyPDF2: {error}")
    pdftotext = shutil.which("pdftotext")
    if pdftotext:
        attempts.append("pdftotext")
        completed = subprocess.run(
            [pdftotext, "-layout", str(path), "-"],
            text=True,
            capture_output=True,
            timeout=60,
            check=False,
        )
        if completed.returncode == 0 and compact_text(completed.stdout):
            return completed.stdout
        errors.append(completed.stderr.strip() or "pdftotext returned no extractable text")

    if attempts:
        raise WorkerError(
            "pdf_extract_failed",
            f"Unable to extract text from PDF: {path}",
            details={
                "path": str(path),
                "attempts": attempts,
                "errors": errors[:3],
            },
        )
    raise WorkerError(
        "pdf_extractor_unavailable",
        "No PDF text extractor is available. Install pypdf, PyPDF2, or pdftotext.",
        details={
            "path": str(path),
            "suggested_fixes": PDF_EXTRACTOR_FIXES,
        },
    )


def extract_pdf_text(raw_bytes: bytes) -> str:
    with tempfile.NamedTemporaryFile(prefix="assist-paper-", suffix=".pdf", delete=False) as handle:
        handle.write(raw_bytes)
        temp_path = Path(handle.name)
    try:
        return extract_pdf_text_from_path(temp_path)
    finally:
        try:
            os.unlink(temp_path)
        except OSError:
            pass


def build_source_key(source: dict[str, Any]) -> str:
    if source.get("url"):
        return str(source["url"])
    if source.get("local_path"):
        return str(source["local_path"])
    return compact_text(source.get("display_name"))


def build_url_source(*, url: str, raw_bytes: bytes, content_type: str) -> dict[str, Any]:
    final_url = str(url or "")
    return {
        "source_type": "url",
        "url": final_url,
        "content_type": content_type,
        "raw_bytes": raw_bytes,
        "display_name": Path(urlparse(final_url).path or "/").name or final_url,
    }


def default_extraction_path(source_format: str) -> str:
    if source_format == "pdf":
        return "pdf_source"
    if source_format == "text":
        return "text_source"
    return "html_source"


def maybe_promote_url_html_to_pdf(
    source: dict[str, Any],
    inspect_payload: dict[str, Any],
    *,
    allow_html_fallback: bool,
) -> tuple[dict[str, Any], dict[str, Any], str, str]:
    if str(source.get("source_type", "")) != "url" or str(inspect_payload.get("source_format", "")) != "html":
        return source, inspect_payload, default_extraction_path(str(inspect_payload.get("source_format", ""))), ""

    pdf_url = compact_text(inspect_payload.get("pdf_url", ""))
    if not pdf_url:
        return source, inspect_payload, "html_source", ""

    requested_url = str(inspect_payload.get("url", source.get("url", "")) or "")
    promotion_url = normalize_url(urljoin(requested_url, pdf_url))
    try:
        raw_bytes, content_type, final_url = fetch_url(promotion_url, max_bytes=MAX_EXTRACT_BYTES)
        promoted_source = build_url_source(url=final_url, raw_bytes=raw_bytes, content_type=content_type)
        promoted_inspect = inspect_source(promoted_source)
        if str(promoted_inspect.get("source_format", "")) != "pdf":
            raise WorkerError(
                "pdf_promotion_failed",
                "Discovered pdf_url did not resolve to a PDF source.",
                details={
                    "requested_url": requested_url,
                    "pdf_url": promotion_url,
                    "final_url": final_url,
                    "content_type": content_type,
                },
            )
        return promoted_source, promoted_inspect, "promoted_pdf_url", ""
    except WorkerError as error:
        fallback_reason = f"{error.code}: {error}"
        failure_details = dict(error.details)
    except Exception as error:
        fallback_reason = f"pdf_promotion_failed: {error}"
        failure_details = {"requested_url": requested_url, "pdf_url": promotion_url}

    if allow_html_fallback:
        return source, inspect_payload, "html_fallback_after_pdf_promotion_error", fallback_reason[:600]

    raise WorkerError(
        "pdf_promotion_failed",
        "Unable to promote landing page extraction to the discovered PDF.",
        details={
            **failure_details,
            "requested_url": requested_url,
            "pdf_url": promotion_url,
            "fallback_allowed": False,
            "reason": fallback_reason[:600],
        },
    )


def inspect_source(source: dict[str, Any]) -> dict[str, Any]:
    source_format = detect_source_format(str(source.get("content_type", "")), str(source.get("display_name", "")))
    raw_bytes = source.get("raw_bytes", b"")
    if not isinstance(raw_bytes, (bytes, bytearray)):
        raise ValueError("resolved source bytes are invalid.")
    base = {
        "source_type": str(source.get("source_type", "")),
        "content_type": str(source.get("content_type", "")),
        "source_format": source_format,
        "source_key": build_source_key(source),
        "url": str(source.get("url", "")),
        "local_path": str(source.get("local_path", "")),
        "display_name": str(source.get("display_name", "")),
        "inspected_at": now_iso(),
    }
    if source_format == "html":
        html_text = decode_text(bytes(raw_bytes))
        meta = inspect_html(source, html_text)
        return {**base, **meta}
    if source_format == "text":
        text = compact_paragraphs(decode_text(bytes(raw_bytes)), 4000)
        title = compact_text(Path(base["display_name"]).stem or "Text Document")
        return {
            **base,
            "title": title,
            "authors": [],
            "abstract": "",
            "pdf_url": "",
            "arxiv_id": "",
            "preview_text": text[:1200],
        }
    if source_format == "pdf":
        title = compact_text(Path(base["display_name"]).stem or "PDF Document")
        return {
            **base,
            "title": title,
            "authors": [],
            "abstract": "",
            "pdf_url": "",
            "arxiv_id": extract_arxiv_id(base["url"]),
            "preview_text": "",
        }
    raise ValueError(f"Unsupported source format: {source_format}")


def extract_source(source: dict[str, Any], *, allow_html_fallback: bool = False) -> dict[str, Any]:
    inspect_payload = inspect_source(source)
    extraction_source, extraction_inspect, extraction_path, fallback_reason = maybe_promote_url_html_to_pdf(
        source,
        inspect_payload,
        allow_html_fallback=allow_html_fallback,
    )
    source_format = str(extraction_inspect["source_format"])
    raw_bytes = bytes(extraction_source.get("raw_bytes", b""))
    full_text = ""
    sections: list[dict[str, Any]] = []
    headings: list[dict[str, Any]] = []

    if source_format == "html":
        html_text = decode_text(raw_bytes)
        sections, headings, full_text = html_to_sections(html_text)
    elif source_format == "text":
        full_text = compact_paragraphs(decode_text(raw_bytes), MAX_TEXT_CHARS)
        sections, headings = split_text_sections(full_text)
    elif source_format == "pdf":
        try:
            full_text = compact_paragraphs(extract_pdf_text(raw_bytes), MAX_TEXT_CHARS)
            sections, headings = split_text_sections(full_text)
        except Exception as error:
            if extraction_path != "promoted_pdf_url" or not allow_html_fallback:
                raise
            extraction_source = source
            extraction_inspect = inspect_payload
            source_format = str(inspect_payload["source_format"])
            raw_bytes = bytes(source.get("raw_bytes", b""))
            extraction_path = "html_fallback_after_pdf_extract_error"
            if isinstance(error, WorkerError):
                fallback_reason = f"{error.code}: {error}"
            else:
                fallback_reason = f"pdf_extract_failed: {error}"
            html_text = decode_text(raw_bytes)
            sections, headings, full_text = html_to_sections(html_text)
    else:
        raise ValueError(f"Unsupported source format: {source_format}")

    abstract = inspect_payload.get("abstract", "") or ""
    if not abstract and sections:
        abstract = compact_paragraphs(sections[0].get("text", ""), 2400)

    paper_title = (
        inspect_payload.get("title")
        or extraction_inspect.get("title")
        or inspect_payload.get("display_name")
        or extraction_inspect.get("display_name")
        or "Untitled Document"
    )
    paper_id = f"paper_{sha1_text(str(inspect_payload.get('source_key', '')) or paper_title)[:12]}"
    return {
        "paper_id": paper_id,
        "source_key": inspect_payload["source_key"],
        "source_type": inspect_payload["source_type"],
        "source_format": source_format,
        "content_type": extraction_inspect["content_type"],
        "url": extraction_inspect.get("url", ""),
        "local_path": extraction_inspect.get("local_path", ""),
        "display_name": extraction_inspect.get("display_name", ""),
        "requested_url": inspect_payload.get("url", ""),
        "requested_local_path": inspect_payload.get("local_path", ""),
        "requested_display_name": inspect_payload.get("display_name", ""),
        "requested_source_format": inspect_payload["source_format"],
        "extraction_path": extraction_path,
        "extraction_fallback_reason": fallback_reason,
        "title": compact_text(paper_title),
        "authors": inspect_payload.get("authors", []),
        "abstract": compact_paragraphs(abstract, 4000),
        "pdf_url": inspect_payload.get("pdf_url", ""),
        "arxiv_id": inspect_payload.get("arxiv_id", ""),
        "section_count": len(sections),
        "char_count": len(full_text),
        "headings": headings[:MAX_SECTIONS],
        "sections": sections[:MAX_SECTIONS],
        "text_preview": compact_paragraphs(full_text, 2400),
        "extracted_at": now_iso(),
    }


def main() -> int:
    raw = sys.stdin.read()
    if not raw.strip():
        return fail("invalid_input", "Worker input is required.")
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError as error:
        return fail("invalid_json", f"Worker input is not valid JSON: {error}")
    if not isinstance(payload, dict):
        return fail("invalid_input", "Worker input must be a JSON object.")

    mode = str(payload.get("mode", "")).strip().lower()
    if mode not in {"inspect", "extract"}:
        return fail("invalid_mode", "mode must be inspect or extract.")
    allow_html_fallback = coerce_bool(payload.get("allow_html_fallback", payload.get("allowHtmlFallback", False)))

    try:
        source = resolve_source(payload, mode=mode)
        if mode == "inspect":
            data = inspect_source(source)
        else:
            data = extract_source(source, allow_html_fallback=allow_html_fallback)
        return emit({"ok": True, "data": data, "created_at": now_iso()})
    except WorkerError as error:
        return fail(error.code, str(error), details=error.details)
    except Exception as error:
        return fail("worker_failed", str(error))


if __name__ == "__main__":
    raise SystemExit(main())
