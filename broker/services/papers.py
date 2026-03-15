from __future__ import annotations

import json
import threading
import time
from hashlib import sha1
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from broker.local_broker import BrokerConfig


def _lb():
    from broker import local_broker

    return local_broker


def generate_paper_digest(paper: dict[str, Any], *, backend: str, cancel_check: Any = None) -> str:
    normalized_backend = str(backend or "llama").strip().lower() or "llama"
    if normalized_backend != "llama":
        raise RuntimeError("paper analysis backend must be llama.")
    context = select_paper_analysis_context(paper)
    if not context:
        raise RuntimeError("Paper artifact is empty after extraction.")
    messages = [
        {
            "role": "system",
            "content": (
                "You are a research assistant. Produce a concise paper digest with exactly these headings: "
                "Summary, Key Claims, Method, Results, Limitations, Open Questions. "
                "Use only the supplied paper content and avoid speculation."
            ),
        },
        {
            "role": "user",
            "content": context,
        },
    ]
    answer, _reasoning = _lb().call_llama(messages, cancel_check=cancel_check)
    return answer.strip()


def select_paper_analysis_context(paper: dict[str, Any], *, char_budget: int = 12000) -> str:
    title = str(paper.get("title", "")).strip()
    authors = ", ".join(str(author).strip() for author in paper.get("authors", []) if str(author).strip())
    abstract = str(paper.get("abstract", "")).strip()
    headings = paper.get("headings") if isinstance(paper.get("headings"), list) else []
    sections = paper.get("sections") if isinstance(paper.get("sections"), list) else []

    selected_sections: list[dict[str, Any]] = []
    preferred_patterns = [
        re.compile(r"\babstract\b", re.IGNORECASE),
        re.compile(r"\bintro", re.IGNORECASE),
        re.compile(r"\bmethod|approach|model|architecture\b", re.IGNORECASE),
        re.compile(r"\bresult|evaluation|experiment\b", re.IGNORECASE),
        re.compile(r"\bdiscussion|conclusion|limitation\b", re.IGNORECASE),
    ]
    used_ids: set[str] = set()
    for pattern in preferred_patterns:
        for section in sections:
            section_id = str(section.get("section_id", ""))
            heading = str(section.get("heading", ""))
            if section_id in used_ids:
                continue
            if pattern.search(heading):
                selected_sections.append(section)
                used_ids.add(section_id)
                break
    for section in sections:
        if len(selected_sections) >= 4:
            break
        section_id = str(section.get("section_id", ""))
        if section_id in used_ids:
            continue
        selected_sections.append(section)
        used_ids.add(section_id)

    parts: list[str] = []
    if title:
        parts.append(f"Title: {title}")
    if authors:
        parts.append(f"Authors: {authors}")
    if abstract:
        parts.append(f"Abstract:\n{abstract}")
    if headings:
        heading_lines = [str(item.get("heading", "")).strip() for item in headings[:12] if str(item.get("heading", "")).strip()]
        if heading_lines:
            parts.append("Headings:\n- " + "\n- ".join(heading_lines))
    for section in selected_sections:
        heading = str(section.get("heading", "")).strip() or "Section"
        body = str(section.get("text", "")).strip()
        if body:
            parts.append(f"{heading}:\n{_lb().truncate_text(body, 2500)}")
    joined = "\n\n".join(parts)
    return _lb().truncate_text(joined, char_budget)

class PaperManager:
    def __init__(self, config: BrokerConfig) -> None:
        self._config = config
        self._store = _lb().PaperStore(config.data_dir)
        self._jobs = _lb().AsyncJobStore(config.data_dir, "paper")

    def _worker_payload(self, payload: dict[str, Any], *, mode: str) -> dict[str, Any]:
        data = {"mode": mode}
        for key in ("url", "pdf_path", "pdfPath", "html_path", "htmlPath", "text_path", "textPath"):
            if key in payload and payload.get(key):
                normalized = key.replace("Path", "_path")
                data[normalized] = payload.get(key)
        if "allow_html_fallback" in payload or "allowHtmlFallback" in payload:
            data["allow_html_fallback"] = payload.get("allow_html_fallback", payload.get("allowHtmlFallback", False))
        return data

    def _run_worker(self, payload: dict[str, Any], *, mode: str, timeout_sec: int, cancel_check: Any = None) -> dict[str, Any]:
        if not self._config.paper_worker_path.exists():
            raise RuntimeError(f"Paper worker script not found: {self._config.paper_worker_path}")
        command = [
            self._config.paper_worker_python,
            str(self._config.paper_worker_path),
        ]
        completed = _lb().run_subprocess_with_cancel(
            command,
            input_text=json.dumps(self._worker_payload(payload, mode=mode), ensure_ascii=True),
            timeout_sec=float(timeout_sec),
            cancel_check=cancel_check,
        )
        stdout = str(completed.stdout or "").strip()
        stderr = str(completed.stderr or "").strip()
        if completed.returncode != 0 and not stdout:
            raise RuntimeError(stderr or "Paper worker exited unsuccessfully.")
        parsed = json.loads(stdout or "{}")
        if not isinstance(parsed, dict) or not bool(parsed.get("ok")):
            error = parsed.get("error") if isinstance(parsed.get("error"), dict) else {}
            raise RuntimeError(str(error.get("message", stderr or "Paper worker failed.")))
        data = parsed.get("data")
        if not isinstance(data, dict):
            raise RuntimeError("Paper worker returned an invalid payload.")
        return data

    def inspect(self, payload: dict[str, Any]) -> dict[str, Any]:
        inspect_payload = self._run_worker(
            payload,
            mode="inspect",
            timeout_sec=min(45, self._config.paper_job_timeout_sec),
        )
        cached = self._store.find_by_source_key(str(inspect_payload.get("source_key", "")))
        return {
            "ok": True,
            "inspect": inspect_payload,
            "cached_paper": self._store._summary(cached) if cached else None,
        }

    def start_job(self, payload: dict[str, Any]) -> dict[str, Any]:
        analysis_mode = str(payload.get("analysis_mode", payload.get("analysisMode", "digest")) or "digest").strip().lower()
        if analysis_mode not in {"extract", "digest"}:
            raise ValueError("analysis_mode must be extract or digest.")
        backend = str(payload.get("backend", "") or "").strip().lower()
        if analysis_mode == "digest" and backend not in {"", "llama"}:
            raise ValueError("paper analysis backend must be llama.")
        if analysis_mode == "digest" and not backend:
            backend = "llama"
        summary = {
            "url": str(payload.get("url", "") or ""),
            "pdf_path": str(payload.get("pdf_path", payload.get("pdfPath", "")) or ""),
            "html_path": str(payload.get("html_path", payload.get("htmlPath", "")) or ""),
            "text_path": str(payload.get("text_path", payload.get("textPath", "")) or ""),
            "analysis_mode": analysis_mode,
            "backend": backend,
        }
        job = self._jobs.create(
            "paper.digest" if analysis_mode == "digest" else "paper.extract",
            summary,
        )
        request_payload = {**payload, "analysis_mode": analysis_mode, "backend": backend}
        thread = threading.Thread(target=self._run_job, args=(job["job_id"], request_payload), daemon=True)
        thread.start()
        return {"ok": True, "job": job}

    def _run_job(self, job_id: str, payload: dict[str, Any]) -> None:
        try:
            self._jobs.update(job_id, {"status": "running"})
            if self._jobs.is_cancel_requested(job_id):
                self._jobs.update(job_id, {"status": "cancelled", "error": {"code": "cancelled", "message": "Job cancelled."}})
                return
            paper = self._run_worker(
                payload,
                mode="extract",
                timeout_sec=self._config.paper_job_timeout_sec,
                cancel_check=lambda: self._jobs.is_cancel_requested(job_id),
            )
            stored = self._store.upsert_extracted(paper)
            digest_text = ""
            backend = str(payload.get("backend", "") or "").strip().lower()
            if str(payload.get("analysis_mode", "")) == "digest":
                if self._jobs.is_cancel_requested(job_id):
                    self._jobs.update(job_id, {"status": "cancelled", "error": {"code": "cancelled", "message": "Job cancelled."}})
                    return
                digest_text = generate_paper_digest(
                    stored,
                    backend=backend,
                    cancel_check=lambda: self._jobs.is_cancel_requested(job_id),
                )
                digest_entry = {
                    "digest_id": f"digest_{sha1(f'{job_id}:{backend}:{time.time()}'.encode('utf-8')).hexdigest()[:10]}",
                    "backend": backend,
                    "mode": "research_digest",
                    "created_at": now_iso(),
                    "text": digest_text,
                }
                stored = self._store.append_digest(str(stored.get("paper_id", "")), digest_entry)
            self._jobs.update(
                job_id,
                {
                    "status": "completed",
                    "result": {
                        "paper_id": str(stored.get("paper_id", "")),
                        "title": str(stored.get("title", "")),
                        "section_count": int(stored.get("section_count", 0) or 0),
                        "char_count": int(stored.get("char_count", 0) or 0),
                        "latest_digest_excerpt": _lb().truncate_text(digest_text, 320),
                    },
                    "error": None,
                },
            )
        except Exception as error:
            status = "cancelled" if self._jobs.is_cancel_requested(job_id) else "failed"
            code = "cancelled" if status == "cancelled" else "job_failed"
            self._jobs.update(job_id, {"status": status, "error": {"code": code, "message": str(error)}})

    def list_jobs(self, *, status_filter: str = "") -> list[dict[str, Any]]:
        return self._jobs.list_metadata(status_filter=status_filter)

    def get_job(self, job_id: str) -> dict[str, Any]:
        return self._jobs.get(job_id)

    def cancel_job(self, job_id: str) -> dict[str, Any]:
        payload = self._jobs.cancel(job_id)
        return {"ok": True, "job": payload}

    def list_papers(self) -> dict[str, Any]:
        return {"papers": self._store.list_metadata()}

    def get_paper(self, paper_id: str) -> dict[str, Any]:
        return {"paper": self._store.get(paper_id)}

    def get_section(self, paper_id: str, section_id: str) -> dict[str, Any]:
        return {"paper_id": paper_id, "section": self._store.get_section(paper_id, section_id)}

    def health(self) -> dict[str, Any]:
        return {
            "jobs": self._jobs.health(),
            "papers": len(self._store.list_metadata(limit=500)),
        }

def handle_paper_inspect(manager: PaperManager, data: dict[str, Any]) -> dict[str, Any]:
    return manager.inspect(data)


def handle_paper_job_start(manager: PaperManager, data: dict[str, Any]) -> dict[str, Any]:
    return manager.start_job(data)


def handle_paper_job_get(manager: PaperManager, job_id: str) -> dict[str, Any]:
    return {"job": manager.get_job(job_id)}


def handle_papers_list(manager: PaperManager) -> dict[str, Any]:
    return manager.list_papers()


def handle_paper_get(manager: PaperManager, paper_id: str) -> dict[str, Any]:
    return manager.get_paper(paper_id)


def handle_paper_section_get(manager: PaperManager, paper_id: str, section_id: str) -> dict[str, Any]:
    return manager.get_section(paper_id, section_id)


__all__ = [
    "PaperManager",
    "generate_paper_digest",
    "handle_paper_get",
    "handle_paper_inspect",
    "handle_paper_job_get",
    "handle_paper_job_start",
    "handle_paper_section_get",
    "handle_papers_list",
    "select_paper_analysis_context",
]
