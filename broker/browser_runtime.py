from __future__ import annotations

import json
import re
import threading
import time
import uuid
from collections import deque
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from typing import Callable

from broker.browser_tools import BROWSER_COMMAND_METHODS
from broker.common import compact_whitespace, now_iso
from broker.conversations import CONVERSATION_ID_RE


CLIENT_ID_RE = re.compile(r"^[A-Za-z0-9._-]{1,128}$")


@dataclass
class PendingCommand:
    event: threading.Event
    result: Any = None
    error: str | None = None


@dataclass
class BrowserSession:
    session_id: str
    capability_token: str
    policy: dict[str, Any]
    created_at: str


@dataclass
class BrowserRun:
    session_id: str
    run_id: str
    status: str
    created_at: str
    cancelled_at: str | None = None


def create_tool_envelope(
    *,
    success: bool,
    tool: str,
    tool_call_id: str,
    session_id: str,
    run_id: str,
    data: Any = None,
    error_code: str | None = None,
    error_message: str | None = None,
    policy: dict[str, Any] | None = None,
    duration_ms: int = 0,
    now_iso_func: Callable[[], str] = now_iso,
) -> dict[str, Any]:
    started_at = now_iso_func()
    envelope = {
        "success": success,
        "tool": tool,
        "tool_call_id": tool_call_id,
        "session_id": session_id,
        "run_id": run_id,
        "data": data,
        "error": None,
        "policy": policy,
        "timing": {"duration_ms": max(0, duration_ms)},
        "started_at": started_at,
        "finished_at": now_iso_func(),
    }
    if not success:
        envelope["data"] = None
        envelope["error"] = {
            "code": error_code or "tool_error",
            "message": error_message or "Tool execution failed.",
        }
    return envelope


def summarize_tool_result_text(envelope: Any) -> str:
    if envelope is None:
        return "ok"
    if not isinstance(envelope, dict):
        return str(envelope)
    if "success" in envelope and "tool" in envelope:
        status = "ok" if envelope.get("success") else "error"
        parts = [f"{envelope.get('tool')} {status}"]
        error = envelope.get("error") or {}
        if not envelope.get("success") and isinstance(error, dict) and error.get("message"):
            parts.append(str(error["message"]))
        return " | ".join(parts)
    if isinstance(envelope.get("approvals"), list):
        return f"approvals={len(envelope['approvals'])}"
    if isinstance(envelope.get("events"), list):
        return f"events={len(envelope['events'])}"
    keys = list(envelope.keys())
    if not keys:
        return "ok"
    return f"ok ({','.join(keys[:4])}{',...' if len(keys) > 4 else ''})"


def browser_tool_result(envelope: Any) -> dict[str, Any]:
    is_tool_envelope = (
        isinstance(envelope, dict)
        and "success" in envelope
        and "tool" in envelope
    )
    is_error = bool(
        is_tool_envelope
        and envelope.get("success") is False
        and not bool((envelope.get("policy") or {}).get("requires_approval"))
    )
    return {
        "content": [{"type": "text", "text": summarize_tool_result_text(envelope)}],
        "structured_content": envelope,
        "structuredContent": envelope,
        "is_error": is_error,
        "isError": is_error,
    }


class BrowserConfigManager:
    def __init__(
        self,
        data_dir: Path,
        *,
        unlimited_agent_steps: int = 0,
        min_agent_steps: int = 1,
    ) -> None:
        self._lock = threading.Lock()
        self._config_path = data_dir / "browser_config.json"
        self._unlimited_agent_steps = unlimited_agent_steps
        self._min_agent_steps = min_agent_steps
        self._agent_max_steps = self._unlimited_agent_steps
        self._load_persisted_config()

    def _write_json(self, path: Path, payload: dict[str, Any]) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_suffix(path.suffix + ".tmp")
        tmp.write_text(json.dumps(payload, ensure_ascii=True, indent=2), encoding="utf-8")
        tmp.replace(path)

    def _load_json(self, path: Path) -> dict[str, Any]:
        if not path.exists():
            return {}
        try:
            parsed = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            return {}
        return parsed if isinstance(parsed, dict) else {}

    def _normalize_agent_max_steps(self, value: Any) -> int:
        try:
            parsed = int(value)
        except (TypeError, ValueError) as error:
            raise ValueError("agent_max_steps must be an integer.") from error
        if parsed < self._unlimited_agent_steps:
            raise ValueError(
                "agent_max_steps must be 0 (unlimited) or a positive integer."
            )
        return parsed

    def _config_payload_locked(self) -> dict[str, Any]:
        return {
            "agent_max_steps": self._agent_max_steps,
            "limits": {
                "agent_max_steps": {
                    "min": self._min_agent_steps,
                    "max": None,
                }
            },
        }

    def _load_persisted_config(self) -> None:
        payload = self._load_json(self._config_path)
        raw_steps = payload.get("agent_max_steps", payload.get("agentMaxSteps"))
        if raw_steps is None:
            return
        try:
            self._agent_max_steps = self._normalize_agent_max_steps(raw_steps)
        except ValueError:
            self._agent_max_steps = self._unlimited_agent_steps

    def _save_persisted_config_locked(self) -> None:
        self._write_json(
            self._config_path,
            {
                "agent_max_steps": self._agent_max_steps,
            },
        )

    def config(self) -> dict[str, Any]:
        with self._lock:
            return self._config_payload_locked()

    def agent_max_steps(self) -> int:
        with self._lock:
            return self._agent_max_steps

    def update_config(self, updates: dict[str, Any]) -> dict[str, Any]:
        raw_steps = updates.get("agent_max_steps", updates.get("agentMaxSteps"))
        with self._lock:
            if raw_steps is not None:
                self._agent_max_steps = self._normalize_agent_max_steps(raw_steps)
                self._save_persisted_config_locked()
            return self._config_payload_locked()


class BrowserProfileStore:
    def __init__(
        self,
        data_dir: Path,
        *,
        id_limits: dict[str, int],
        now_iso_func: Callable[[], str] = now_iso,
    ) -> None:
        self._lock = threading.Lock()
        self._state_path = data_dir / "browser_profiles" / "state.json"
        self._limits = id_limits
        self._now_iso = now_iso_func

    def _write_json(self, path: Path, payload: dict[str, Any]) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_suffix(path.suffix + ".tmp")
        tmp.write_text(json.dumps(payload, ensure_ascii=True, indent=2), encoding="utf-8")
        tmp.replace(path)

    def _load_json(self, path: Path) -> dict[str, Any]:
        if not path.exists():
            return {}
        try:
            parsed = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            return {}
        return parsed if isinstance(parsed, dict) else {}

    def _normalize_id(self, value: Any) -> str:
        candidate = compact_whitespace(value, self._limits["id"])
        if not candidate or not CONVERSATION_ID_RE.match(candidate):
            return ""
        return candidate

    def _normalize_timestamp(self, value: Any) -> str:
        candidate = compact_whitespace(value, self._limits["timestamp"])
        return candidate or self._now_iso()

    def _normalize_step(self, value: Any, profile_id: str) -> dict[str, Any] | None:
        if not isinstance(value, dict):
            return None
        raw_url = value.get("url", value.get("page_url", value.get("pageUrl", "")))
        url = str(raw_url or "").strip()[: self._limits["url"]]
        if not url:
            return None
        step_id = self._normalize_id(value.get("id", value.get("step_id", "")))
        if not step_id:
            return None
        return {
            "id": step_id,
            "profile_id": profile_id,
            "title": compact_whitespace(
                value.get("title", value.get("page_title", value.get("pageTitle", ""))),
                self._limits["title"],
            ),
            "url": url,
            "host": compact_whitespace(value.get("host", ""), self._limits["host"]),
            "attached_element": compact_whitespace(
                value.get(
                    "attached_element",
                    value.get("attachedElement", value.get("element", value.get("selector", ""))),
                ),
                self._limits["attached_element"],
            ),
            "summary": compact_whitespace(
                value.get("summary", ""),
                self._limits["summary"],
            ),
            "created_at": self._normalize_timestamp(
                value.get("created_at", value.get("createdAt", "")),
            ),
        }

    def _normalize_profile(self, value: Any) -> dict[str, Any] | None:
        if not isinstance(value, dict):
            return None
        profile_id = self._normalize_id(value.get("id", value.get("profile_id", value.get("profileId", ""))))
        name = compact_whitespace(
            value.get("name", value.get("label", value.get("title", ""))),
            self._limits["name"],
        )
        if not profile_id or not name:
            return None
        steps: list[dict[str, Any]] = []
        seen_step_ids: set[str] = set()
        raw_steps = value.get("steps")
        if isinstance(raw_steps, list):
            for raw_step in raw_steps[-self._limits["steps_per_profile"] :]:
                normalized_step = self._normalize_step(raw_step, profile_id)
                if not normalized_step:
                    continue
                step_id = str(normalized_step.get("id", ""))
                if step_id in seen_step_ids:
                    continue
                seen_step_ids.add(step_id)
                steps.append(normalized_step)
        return {
            "id": profile_id,
            "name": name,
            "created_at": self._normalize_timestamp(
                value.get("created_at", value.get("createdAt", "")),
            ),
            "updated_at": self._normalize_timestamp(
                value.get("updated_at", value.get("updatedAt", "")),
            ),
            "steps": steps,
        }

    def _normalize_attachment(
        self,
        value: Any,
        profiles: list[dict[str, Any]],
    ) -> dict[str, Any] | None:
        if not isinstance(value, dict):
            return None
        profile_id = self._normalize_id(value.get("profile_id", value.get("profileId", "")))
        if not profile_id:
            return None
        profile = next((entry for entry in profiles if entry.get("id") == profile_id), None)
        if not profile:
            return None
        step_id = self._normalize_id(value.get("step_id", value.get("stepId", "")))
        if step_id and not any(str(step.get("id")) == step_id for step in profile.get("steps", [])):
            step_id = ""
        return {
            "profile_id": profile_id,
            "step_id": step_id,
        }

    def _normalize_state(self, value: Any) -> dict[str, Any]:
        raw = value if isinstance(value, dict) else {}
        raw_profiles = raw.get("profiles")
        if not isinstance(raw_profiles, list):
            raw_profiles = raw if isinstance(raw, list) else []

        profiles: list[dict[str, Any]] = []
        seen_profile_ids: set[str] = set()
        for raw_profile in raw_profiles[: self._limits["profiles"]]:
            normalized_profile = self._normalize_profile(raw_profile)
            if not normalized_profile:
                continue
            profile_id = str(normalized_profile.get("id", ""))
            if profile_id in seen_profile_ids:
                continue
            seen_profile_ids.add(profile_id)
            profiles.append(normalized_profile)

        selected_profile_id = self._normalize_id(
            raw.get("selected_profile_id", raw.get("selectedProfileId", "")),
        )
        if selected_profile_id and not any(
            str(profile.get("id")) == selected_profile_id for profile in profiles
        ):
            selected_profile_id = ""
        if not selected_profile_id and profiles:
            selected_profile_id = str(profiles[0].get("id", ""))

        attached_profile = self._normalize_attachment(
            raw.get("attached_profile", raw.get("attachedProfile")),
            profiles,
        )

        return {
            "profiles": profiles,
            "selected_profile_id": selected_profile_id,
            "attached_profile": attached_profile,
        }

    def state(self) -> dict[str, Any]:
        with self._lock:
            return self._normalize_state(self._load_json(self._state_path))

    def replace_state(self, updates: dict[str, Any]) -> dict[str, Any]:
        payload = self._normalize_state(updates)
        with self._lock:
            self._write_json(self._state_path, payload)
            return payload


class ExtensionCommandRelay:
    def __init__(
        self,
        stale_sec: int,
        *,
        now_iso_func: Callable[[], str] = now_iso,
    ) -> None:
        self._stale_sec = max(10, stale_sec)
        self._condition = threading.Condition()
        self._clients: dict[str, float] = {}
        self._queue: deque[dict[str, Any]] = deque()
        self._pending: dict[str, PendingCommand] = {}
        self._now_iso = now_iso_func

    def _normalize_client_id(self, value: Any) -> str:
        cid = str(value or "").strip()
        if not CLIENT_ID_RE.match(cid):
            raise ValueError("Invalid extension client id.")
        return cid

    def _prune_clients_locked(self) -> None:
        cutoff = time.monotonic() - self._stale_sec
        stale = [cid for cid, seen in self._clients.items() if seen < cutoff]
        for cid in stale:
            del self._clients[cid]

    def register(self, client_id: Any) -> dict[str, Any]:
        cid = self._normalize_client_id(client_id)
        with self._condition:
            self._clients[cid] = time.monotonic()
            self._condition.notify_all()
        return {"client_id": cid, "poll_timeout_ms": 25000}

    def poll_next(self, client_id: Any, timeout_ms: int) -> dict[str, Any]:
        cid = self._normalize_client_id(client_id)
        timeout_sec = min(60.0, max(0.0, timeout_ms / 1000.0))
        end_at = time.monotonic() + timeout_sec

        with self._condition:
            self._clients[cid] = time.monotonic()
            while True:
                self._prune_clients_locked()
                self._clients[cid] = time.monotonic()
                if self._queue:
                    return {"command": self._queue.popleft()}
                remaining = end_at - time.monotonic()
                if remaining <= 0:
                    return {"command": None}
                self._condition.wait(remaining)

    def send_command(self, method: str, args: dict[str, Any], timeout_sec: int) -> Any:
        if not method:
            raise ValueError("Extension command method is required.")
        command_id = f"cmd_{uuid.uuid4().hex[:12]}"
        pending = PendingCommand(event=threading.Event())

        with self._condition:
            self._pending[command_id] = pending
            self._queue.append(
                {
                    "command_id": command_id,
                    "method": method,
                    "args": args,
                    "created_at": self._now_iso(),
                }
            )
            self._condition.notify_all()

        if not pending.event.wait(max(1, timeout_sec)):
            with self._condition:
                self._pending.pop(command_id, None)
            raise TimeoutError(f"Extension command timed out: {method}")
        if pending.error:
            raise RuntimeError(pending.error)
        return pending.result

    def submit_result(
        self,
        client_id: Any,
        command_id: str,
        success: bool,
        data: Any,
        error: str | None,
    ) -> bool:
        cid = self._normalize_client_id(client_id)
        with self._condition:
            self._clients[cid] = time.monotonic()
            pending = self._pending.pop(command_id, None)
        if pending is None:
            return False

        if success:
            pending.result = data
        else:
            pending.error = error or "Extension command execution failed."
        pending.event.set()
        return True

    def health(self) -> dict[str, Any]:
        with self._condition:
            self._prune_clients_locked()
            return {
                "connected_clients": len(self._clients),
                "queued_commands": len(self._queue),
                "inflight_commands": len(self._pending),
            }


class BrowserAutomationManager:
    def __init__(
        self,
        default_domain_allowlist: list[str],
        *,
        approval_modes: set[str],
        normalize_domain_allowlist_func: Callable[[Any], list[str]],
        url_host_is_allowed_func: Callable[[str, list[str]], bool],
        now_iso_func: Callable[[], str] = now_iso,
    ) -> None:
        self._default_domain_allowlist = default_domain_allowlist
        self._approval_modes = approval_modes
        self._normalize_domain_allowlist = normalize_domain_allowlist_func
        self._url_host_is_allowed = url_host_is_allowed_func
        self._now_iso = now_iso_func
        self._sessions: dict[str, BrowserSession] = {}
        self._runs: dict[str, BrowserRun] = {}
        self._lock = threading.Lock()

    def _normalize_policy(self, value: Any) -> dict[str, Any]:
        raw = value if isinstance(value, dict) else {}
        allowlist = self._normalize_domain_allowlist(
            raw.get("domain_allowlist", raw.get("domainAllowlist", []))
        )
        if not allowlist:
            allowlist = list(self._default_domain_allowlist)
        approval_mode = str(raw.get("approval_mode", raw.get("approvalMode", "auto-approve"))).strip().lower()
        if approval_mode not in self._approval_modes:
            approval_mode = "auto-approve"
        return {
            "domain_allowlist": allowlist,
            "approval_mode": approval_mode,
        }

    def _run_key(self, session_id: str, run_id: str) -> str:
        return f"{session_id}:{run_id}"

    def _get_session_locked(self, session_id: str) -> BrowserSession:
        session = self._sessions.get(session_id)
        if not session:
            raise ValueError(f"Unknown session: {session_id}")
        return session

    def _assert_capability(self, session: BrowserSession, token: str) -> None:
        if token != session.capability_token:
            raise ValueError(f"Invalid capability token for session {session.session_id}.")

    def _get_run_locked(self, session_id: str, run_id: str) -> BrowserRun:
        run = self._runs.get(self._run_key(session_id, run_id))
        if not run:
            raise ValueError(f"Unknown run {run_id} for session {session_id}.")
        return run

    def _normalize_session_id(self, session_id: Any, *, allow_generate: bool) -> str:
        requested = str(session_id or "").strip()
        if not requested and allow_generate:
            requested = f"session_{uuid.uuid4().hex[:8]}"
        if not CONVERSATION_ID_RE.match(requested):
            raise ValueError("Invalid session id.")
        return requested

    def session_create(self, args: dict[str, Any]) -> dict[str, Any]:
        session_id = self._normalize_session_id(
            args.get("session_id", args.get("sessionId")), allow_generate=True
        )
        policy = self._normalize_policy(args.get("policy"))
        capability_token = f"cap_{uuid.uuid4().hex}"

        with self._lock:
            if session_id in self._sessions:
                raise ValueError(f"Session {session_id} already exists.")
            self._sessions[session_id] = BrowserSession(
                session_id=session_id,
                capability_token=capability_token,
                policy=policy,
                created_at=self._now_iso(),
            )

        return {
            "session_id": session_id,
            "sessionId": session_id,
            "policy": policy,
            "capability_token": capability_token,
            "capabilityToken": capability_token,
        }

    def run_start(self, args: dict[str, Any]) -> dict[str, Any]:
        session_id = self._normalize_session_id(
            args.get("session_id", args.get("sessionId")), allow_generate=False
        )
        run_id = str(args.get("run_id", args.get("runId")) or "").strip()
        if not run_id:
            run_id = f"run_{uuid.uuid4().hex[:8]}"
        if not CONVERSATION_ID_RE.match(run_id):
            raise ValueError("Invalid run id.")
        token = str(args.get("capability_token", args.get("capabilityToken")) or "")

        with self._lock:
            session = self._get_session_locked(session_id)
            self._assert_capability(session, token)
            key = self._run_key(session_id, run_id)
            if key in self._runs:
                raise ValueError(f'Run "{run_id}" already exists for session "{session_id}".')
            self._runs[key] = BrowserRun(
                session_id=session_id,
                run_id=run_id,
                status="running",
                created_at=self._now_iso(),
            )

        return {
            "session_id": session_id,
            "sessionId": session_id,
            "run_id": run_id,
            "runId": run_id,
            "status": "running",
        }

    def run_cancel(self, args: dict[str, Any]) -> dict[str, Any]:
        session_id = self._normalize_session_id(
            args.get("session_id", args.get("sessionId")), allow_generate=False
        )
        run_id = str(args.get("run_id", args.get("runId")) or "").strip()
        token = str(args.get("capability_token", args.get("capabilityToken")) or "")
        if not run_id:
            raise ValueError("run_id is required.")

        with self._lock:
            session = self._get_session_locked(session_id)
            self._assert_capability(session, token)
            run = self._get_run_locked(session_id, run_id)
            run.status = "cancelled"
            run.cancelled_at = self._now_iso()

        return {
            "session_id": session_id,
            "sessionId": session_id,
            "run_id": run_id,
            "runId": run_id,
            "status": "cancelled",
        }

    def approvals_list(self, _args: dict[str, Any]) -> dict[str, Any]:
        return {"approvals": []}

    def events_replay(self, _args: dict[str, Any]) -> dict[str, Any]:
        return {"events": []}

    def approve(self, args: dict[str, Any]) -> dict[str, Any]:
        session_id = self._normalize_session_id(
            args.get("session_id", args.get("sessionId")), allow_generate=False
        )
        token = str(args.get("capability_token", args.get("capabilityToken")) or "")
        with self._lock:
            session = self._get_session_locked(session_id)
            self._assert_capability(session, token)
        return {
            "approved": False,
            "reason": "manual approvals are disabled; policy is auto-approve.",
        }

    def execute_tool(
        self,
        tool_name: str,
        args: dict[str, Any],
        relay: ExtensionCommandRelay,
        timeout_sec: int,
    ) -> dict[str, Any]:
        session_id = self._normalize_session_id(
            args.get("session_id", args.get("sessionId")), allow_generate=False
        )
        run_id = str(args.get("run_id", args.get("runId")) or "").strip()
        token = str(args.get("capability_token", args.get("capabilityToken")) or "")
        tool_call_id = str(args.get("tool_call_id", args.get("toolCallId")) or "").strip()
        if not tool_call_id:
            tool_call_id = f"tool_{uuid.uuid4().hex[:8]}"
        if not run_id:
            raise ValueError("run_id is required for browser tool calls.")

        command_method = BROWSER_COMMAND_METHODS.get(tool_name)
        if not command_method:
            raise ValueError(f"Unsupported browser tool: {tool_name}")

        with self._lock:
            session = self._get_session_locked(session_id)
            self._assert_capability(session, token)
            run = self._get_run_locked(session_id, run_id)
            if run.status != "running":
                raise ValueError(f'Run "{run_id}" is not active.')
            policy = session.policy

        if policy.get("approval_mode") == "auto-deny":
            return create_tool_envelope(
                success=False,
                tool=tool_name,
                tool_call_id=tool_call_id,
                session_id=session_id,
                run_id=run_id,
                error_code="policy_denied",
                error_message="Action denied by policy (auto-deny).",
                policy={"denied": True, "reason": "auto_deny"},
                duration_ms=0,
                now_iso_func=self._now_iso,
            )

        tool_args = args.get("args", {})
        if not isinstance(tool_args, dict):
            raise ValueError("tool args must be an object.")

        if tool_name in {"browser.navigate", "browser.open_tab"}:
            url = str(tool_args.get("url", ""))
            if not self._url_host_is_allowed(url, list(policy["domain_allowlist"])):
                return create_tool_envelope(
                    success=False,
                    tool=tool_name,
                    tool_call_id=tool_call_id,
                    session_id=session_id,
                    run_id=run_id,
                    error_code="domain_not_allowlisted",
                    error_message="Action denied: domain not in allowlist.",
                    policy={"denied": True, "reason": "domain_not_allowlisted"},
                    duration_ms=0,
                    now_iso_func=self._now_iso,
                )

        command_args = dict(tool_args)
        command_args["allowedHosts"] = list(policy["domain_allowlist"])
        command_args["sessionId"] = session_id
        command_args["runId"] = run_id

        started = time.monotonic()
        try:
            data = relay.send_command(command_method, command_args, timeout_sec)
        except TimeoutError:
            return create_tool_envelope(
                success=False,
                tool=tool_name,
                tool_call_id=tool_call_id,
                session_id=session_id,
                run_id=run_id,
                error_code="extension_timeout",
                error_message=f"Extension command timed out: {command_method}",
                policy={"denied": False, "reason": "extension_timeout"},
                duration_ms=int((time.monotonic() - started) * 1000),
                now_iso_func=self._now_iso,
            )
        except Exception as error:
            return create_tool_envelope(
                success=False,
                tool=tool_name,
                tool_call_id=tool_call_id,
                session_id=session_id,
                run_id=run_id,
                error_code="extension_error",
                error_message=str(error),
                policy={"denied": False, "reason": "extension_error"},
                duration_ms=int((time.monotonic() - started) * 1000),
                now_iso_func=self._now_iso,
            )

        return create_tool_envelope(
            success=True,
            tool=tool_name,
            tool_call_id=tool_call_id,
            session_id=session_id,
            run_id=run_id,
            data=data,
            duration_ms=int((time.monotonic() - started) * 1000),
            now_iso_func=self._now_iso,
        )

    def health(self) -> dict[str, Any]:
        with self._lock:
            running_runs = sum(1 for run in self._runs.values() if run.status == "running")
            return {
                "sessions": len(self._sessions),
                "runs": len(self._runs),
                "running_runs": running_runs,
            }

    def close_session(self, session_id: str, run_id: str | None = None) -> None:
        normalized_session_id = self._normalize_session_id(session_id, allow_generate=False)
        normalized_run_id = str(run_id or "").strip()
        with self._lock:
            if normalized_run_id:
                self._runs.pop(self._run_key(normalized_session_id, normalized_run_id), None)
            else:
                run_prefix = f"{normalized_session_id}:"
                for key in list(self._runs.keys()):
                    if key.startswith(run_prefix):
                        self._runs.pop(key, None)
            self._sessions.pop(normalized_session_id, None)
