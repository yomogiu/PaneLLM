#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import statistics
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid
from dataclasses import dataclass, field


BROWSER_FLOW_PROMPT = (
    'open a tab\n'
    'go to google.ca\n'
    'click Create account\n'
    'select "For my personal use"\n'
    'type "John Doe"\n'
    'complete'
)

PROFILE_STEPS = [
    {"summary": "Open google.ca", "url": "https://google.ca", "selector": "#open_tab_btn"},
    {"summary": 'Click Create account', "url": "https://google.ca", "selector": "a[href*=\"accounts\"]"},
    {"summary": 'Select “For my personal use”', "url": "https://google.ca", "selector": "a[role=\"button\"][aria-label*=\"Create account\"]"},
    {"summary": 'Type “John Doe”', "url": "https://google.ca", "selector": "input[name=\"f\"]"},
    {"summary": "Complete", "url": "https://google.ca", "selector": "button[type=\"submit\"]"},
]

TERMINAL_STATUSES = {"completed", "failed", "cancelled", "blocked_for_review"}
EVENT_POLL_TIMEOUT_MS = 20_000
EXTENSION_POLL_TIMEOUT_MS = 25_000
EXTENSION_ERROR_BACKOFF_SEC = 0.2


def _base_url(host: str, port: int) -> str:
    return f"http://{host}:{port}"


def _request(
    method: str,
    base_url: str,
    path: str,
    payload: dict | None = None,
    timeout_sec: float = 15.0,
) -> dict:
    data = None
    if payload is not None:
        encoded = json.dumps(payload).encode("utf-8")
        data = encoded
    request = urllib.request.Request(
        f"{base_url}{path}",
        data=data,
        method=method,
    )
    request.add_header("Content-Type", "application/json")
    request.add_header("Accept", "application/json")
    request.add_header("X-Assistant-Client", "chrome-sidepanel-v1")
    try:
        with urllib.request.urlopen(request, timeout=timeout_sec) as response:
            raw = response.read().decode("utf-8")
            return json.loads(raw or "{}")
    except urllib.error.HTTPError as error:
        body = error.read().decode("utf-8", errors="replace") if error.fp else ""
        raise RuntimeError(f"{method} {path} failed ({error.code}): {body or error.reason}") from error


def _request_get(
    base_url: str,
    path: str,
    params: dict[str, str] | None = None,
    timeout_sec: float = 30.0,
) -> dict:
    query = ""
    if params:
        query = "?" + urllib.parse.urlencode(params)
    return _request("GET", base_url, f"{path}{query}", timeout_sec=timeout_sec)


def _request_post(base_url: str, path: str, payload: dict, timeout_sec: float = 30.0) -> dict:
    return _request("POST", base_url, path, payload=payload, timeout_sec=timeout_sec)


def _is_successful_extension_call(payload: dict) -> bool:
    if not isinstance(payload, dict):
        return False
    return bool(payload.get("ok", False))


def _build_profile_suffix(profile_name: str, profile_id: str, include_element_bindings: bool) -> str:
    lines = [
        f"Browser workflow profile: {profile_name} [{profile_id}]",
        "Steps:",
    ]
    step_lines = []
    if PROFILE_STEPS:
        for index, step in enumerate(PROFILE_STEPS, start=1):
            label = str(step.get("summary") or "").strip()
            selector = str(step.get("selector") or "").strip()
            url = str(step.get("url") or "").strip()
            if include_element_bindings and selector:
                label = f"{label} · {selector}"
            if url:
                label = f"{label} ({url})"
            step_lines.append(f"{index}. {label}".strip())
    else:
        step_lines.append("- No recorded steps yet.")
    lines.extend(step_lines)
    attached_step = PROFILE_STEPS[-1]["summary"] if PROFILE_STEPS else "No attached step."
    if include_element_bindings and PROFILE_STEPS:
        selector = PROFILE_STEPS[-1].get("selector", "").strip()
        if selector:
            attached_step = f"{attached_step} · {selector}"
    lines.append(f"Attached step 5: {attached_step}")
    return "\n".join(lines)


def _extension_result_payload(method: str, args: dict | None) -> dict:
    selected_args = args or {}
    normalized_method = str(method or "")
    if normalized_method == "browser.get_content":
        return {
            "ok": True,
            "method": normalized_method,
            "title": "Google",
            "url": str(selected_args.get("url", "https://google.ca")),
            "content": "mock page content",
            "text": "Google Accounts create account flow page mock",
        }
    if normalized_method == "browser.get_tabs":
        return {
            "ok": True,
            "method": normalized_method,
            "tabs": [
                {
                    "id": 1,
                    "url": "https://google.ca",
                    "title": "Google",
                    "active": True,
                }
            ],
        }
    if normalized_method in {"browser.find_one", "browser.find_elements", "browser.wait_for", "browser.get_element_state"}:
        return {
            "ok": True,
            "method": normalized_method,
            "elements": [
                {
                    "selector": str(selected_args.get("selector", selected_args.get("locator", ""))),
                    "url": selected_args.get("url", "https://google.ca"),
                }
            ],
        }
    if normalized_method in {"browser.navigate", "browser.open_tab"}:
        return {
            "ok": True,
            "method": normalized_method,
            "url": selected_args.get("url", "https://google.ca"),
        }
    if normalized_method in {"browser.click", "browser.type", "browser.close_tab", "browser.group_tabs"}:
        return {
            "ok": True,
            "method": normalized_method,
            "result": "success",
        }
    if normalized_method == "browser.read":
        return {
            "ok": True,
            "method": normalized_method,
            "result": "read",
            "action": str(selected_args.get("action", "")),
            "content": "mock page content",
        }
    return {"ok": True, "method": normalized_method}


@dataclass
class ExtensionSession:
    base_url: str
    stop_after_run: threading.Event
    client_id: str
    calls: int = 0
    accepted_calls: int = 0
    accepted_lock: threading.Lock = field(default_factory=threading.Lock)

    def start(self) -> None:
        self.stop_after_run.clear()
        reg = _request_post(
            self.base_url,
            "/extension/register",
            {
                "client_id": self.client_id,
                "version": "chrome-sidepanel",
                "platform": "chrome-secure-panel",
            },
        )
        if "client_id" not in reg:
            raise RuntimeError("Extension registration failed: no client id returned.")

    def run(self) -> None:
        while not self.stop_after_run.is_set():
            try:
                next_payload = _request_get(
                    self.base_url,
                    "/extension/next",
                    {
                        "client_id": self.client_id,
                        "timeout_ms": str(EXTENSION_POLL_TIMEOUT_MS),
                    },
                )
            except Exception:
                time.sleep(EXTENSION_ERROR_BACKOFF_SEC)
                continue
            command = next_payload.get("command")
            if not isinstance(command, dict):
                continue
            command_id = str(command.get("command_id", "")).strip()
            if not command_id:
                continue
            method = str(command.get("method", ""))
            args = command.get("args", {})
            if not isinstance(args, dict):
                args = {}
            response_payload = _extension_result_payload(method, args)
            try:
                result = _request_post(
                    self.base_url,
                    "/extension/result",
                    {
                        "client_id": self.client_id,
                        "command_id": command_id,
                        "success": True,
                        "data": response_payload,
                    },
                )
            except Exception:
                result = {}
            with self.accepted_lock:
                self.calls += 1
                if _is_successful_extension_call(result):
                    self.accepted_calls += 1

    def stop(self) -> None:
        self.stop_after_run.set()


def _sample_command_count(base_url: str) -> ExtensionSession:
    return ExtensionSession(base_url=base_url, stop_after_run=threading.Event(), client_id=f"bench-{uuid.uuid4().hex}")


def _run_single_bench_case(
    base_url: str,
    profile_suffix: str,
    session_id: str,
    prompt: str,
    timeout_seconds: float,
) -> tuple[str, float, int]:
    worker = _sample_command_count(base_url)
    worker.start()
    thread = threading.Thread(target=worker.run, daemon=True)
    thread.start()
    run_id = ""
    status = "unknown"
    start_ts = time.perf_counter()
    try:
        run_payload = {
            "session_id": session_id,
            "backend": "llama",
            "prompt": prompt,
            "request_prompt_suffix": profile_suffix,
            "force_browser_action": True,
            "allowed_hosts": ["google.ca"],
            "confirmed": False,
        }
        run = _request_post(base_url, "/runs", run_payload, timeout_sec=60.0)
        run_id = str(run.get("run_id", "")).strip()
        if not run_id:
            raise RuntimeError("Run start did not return run_id.")

        after = 0
        deadline = time.perf_counter() + timeout_seconds
        while time.perf_counter() < deadline:
            events = _request_get(
                base_url,
                f"/runs/{urllib.parse.quote(run_id, safe='')}/events",
                {"after": str(after), "timeout_ms": str(EVENT_POLL_TIMEOUT_MS)},
            )
            status = str(events.get("status", ""))
            raw_events = events.get("events", [])
            for event in raw_events if isinstance(raw_events, list) else []:
                after = max(after, int(event.get("seq", after)))
            if status in TERMINAL_STATUSES:
                break
            if not raw_events:
                time.sleep(0.05)
        else:
            status = "timeout"
            try:
                _request_post(base_url, f"/runs/{urllib.parse.quote(run_id, safe='')}/cancel", {})
            except Exception:
                pass
    finally:
        worker.stop()
        thread.join(timeout=5.0)
        elapsed_ms = (time.perf_counter() - start_ts) * 1000.0
    if status == "timeout":
        try:
            events = _request_get(
                base_url,
                f"/runs/{urllib.parse.quote(run_id, safe='')}/events",
                {"after": str(-1), "timeout_ms": "250"},
            )
            status = str(events.get("status", status))
        except Exception:
            pass
    with worker.accepted_lock:
        extension_calls = worker.accepted_calls
    return status, elapsed_ms, extension_calls


def _run_case(
    base_url: str,
    name: str,
    iterations: int,
    include_element_bindings: bool | None,
    timeout_seconds: float,
) -> dict:
    profile_suffix = ""
    if include_element_bindings is not None and include_element_bindings is not False:
        profile_suffix = _build_profile_suffix("TEST_PROFILE", "test_profile", True)
    elif include_element_bindings is False:
        profile_suffix = _build_profile_suffix("TEST_PROFILE", "test_profile", False)

    prompt = BROWSER_FLOW_PROMPT
    if include_element_bindings is None:
        profile_suffix = ""

    records: list[dict[str, object]] = []
    for iteration in range(1, iterations + 1):
        status, elapsed_ms, extension_calls = _run_single_bench_case(
            base_url=base_url,
            profile_suffix=profile_suffix,
            session_id=f"bench_{name}_{iteration}_{uuid.uuid4().hex}",
            prompt=prompt,
            timeout_seconds=timeout_seconds,
        )
        records.append(
            {
                "iteration": iteration,
                "status": status,
                "elapsed_ms": int(elapsed_ms),
                "extension_calls": extension_calls,
            }
        )
        print(f"  [{name}] iter {iteration}: status={status} elapsed_ms={int(elapsed_ms)} extension_calls={extension_calls}")
        if status not in TERMINAL_STATUSES:
            print(f"    ⚠ run did not finish in terminal status: {status}")
    median_elapsed = statistics.median(
        [record["elapsed_ms"] for record in records if isinstance(record["elapsed_ms"], int)],
    )
    median_calls = statistics.median(
        [record["extension_calls"] for record in records if isinstance(record["extension_calls"], int)],
    )
    return {
        "name": name,
        "records": records,
        "median_elapsed_ms": float(median_elapsed),
        "median_extension_calls": float(median_calls),
    }


def _load_browser_step_default(base_url: str) -> int:
    browser_config = _request_get(base_url, "/browser/config")
    browser_payload = browser_config.get("browser", {})
    return int(browser_payload.get("agent_max_steps", 0))


def _set_browser_step_limit(base_url: str, agent_max_steps: int) -> dict:
    return _request_post(base_url, "/browser/config", {"agent_max_steps": agent_max_steps})


def main() -> None:
    parser = argparse.ArgumentParser(description="Benchmark TEST_PROFILE vs no-profile browser runs.")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=7777)
    parser.add_argument("--iterations", type=int, default=3)
    parser.add_argument("--timeout-seconds", type=float, default=120.0)
    parser.add_argument("--step-limit", type=int, default=20)
    args = parser.parse_args()

    base = _base_url(args.host, args.port)
    print(f"Benchmark target: {base}/runs")

    original_steps = _load_browser_step_default(base)
    try:
        _set_browser_step_limit(base, args.step_limit)
        no_profile = _run_case(
            base_url=base,
            name="no_profile",
            iterations=args.iterations,
            include_element_bindings=None,
            timeout_seconds=args.timeout_seconds,
        )
        with_profile = _run_case(
            base_url=base,
            name="with_element_profile",
            iterations=args.iterations,
            include_element_bindings=True,
            timeout_seconds=args.timeout_seconds,
        )
    finally:
        _set_browser_step_limit(base, original_steps)

    no_elapsed = float(no_profile["median_elapsed_ms"])
    with_elapsed = float(with_profile["median_elapsed_ms"])
    no_calls = float(no_profile["median_extension_calls"])
    with_calls = float(with_profile["median_extension_calls"])
    ratio = with_elapsed / no_elapsed if no_elapsed > 0 else float("inf")
    delta_calls = with_calls - no_calls

    print("\nSummary")
    print("-------")
    print(f"No profile median ms: {no_elapsed:.1f} | median extension calls: {no_calls:.1f}")
    print(f"With TEST_PROFILE median ms: {with_elapsed:.1f} | median extension calls: {with_calls:.1f}")
    print(f"Median ratio: {ratio:.3f}")
    print(f"Median extension-call delta: {delta_calls:.1f}")
    if with_elapsed < 15000:
        print("Target check: with-profile median elapsed < 15000ms = PASS")
    else:
        print("Target check: with-profile median elapsed < 15000ms = FAIL")


if __name__ == "__main__":
    main()
