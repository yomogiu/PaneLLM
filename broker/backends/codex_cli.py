from __future__ import annotations

import json
import subprocess
import tempfile
import time
from pathlib import Path
from typing import Any, Callable


def run_subprocess_with_cancel(
    command: list[str],
    *,
    input_text: str,
    timeout_sec: float,
    terminate_subprocess_func: Callable[[Any, float], None],
    cancel_check: Any = None,
    on_process_start: Any = None,
    on_process_end: Any = None,
    cancelled_error_cls: type[BaseException] = RuntimeError,
) -> subprocess.CompletedProcess[str]:
    if cancel_check and cancel_check():
        raise cancelled_error_cls("Request cancelled by user.")
    process = subprocess.Popen(
        command,
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    if on_process_start:
        on_process_start(process)

    stdout = ""
    stderr = ""
    pending_input: str | None = input_text
    deadline = time.monotonic() + max(1.0, float(timeout_sec))
    poll_timeout_sec = 0.25
    try:
        while True:
            if cancel_check and cancel_check():
                terminate_subprocess_func(process, 1.5)
                raise cancelled_error_cls("Request cancelled by user.")
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                terminate_subprocess_func(process, 1.5)
                raise subprocess.TimeoutExpired(command, timeout_sec)
            try:
                stdout, stderr = process.communicate(
                    input=pending_input,
                    timeout=min(poll_timeout_sec, remaining),
                )
                if cancel_check and cancel_check():
                    raise cancelled_error_cls("Request cancelled by user.")
                break
            except subprocess.TimeoutExpired:
                pending_input = None
                continue
    finally:
        if on_process_end:
            on_process_end()

    return subprocess.CompletedProcess(
        command,
        process.returncode,
        stdout,
        stderr,
    )


def build_codex_cli_prompt(
    messages: list[dict[str, str]],
    prompt: str,
    *,
    force_browser_action: bool = False,
) -> str:
    prior_turns: list[str] = []
    for message in messages[:-1]:
        role = str(message.get("role", "")).strip()
        if role not in {"user", "assistant"}:
            continue
        label = "User" if role == "user" else "Assistant"
        content = str(message.get("content", "")).strip()
        if content:
            prior_turns.append(f"{label}: {content}")

    browser_instruction = ""
    if force_browser_action:
        browser_instruction = (
            "System instruction: Browser action mode is enabled for this request. Use the configured "
            "browser MCP tools to navigate and verify fresh web information. Do not rely on built-in "
            "web search tools or unstated prior knowledge for fresh web facts. If browser tools are "
            "unavailable or blocked, explain that clearly and stop. Once the requested browser action "
            "is complete, immediately return a concise final answer and end your turn."
        )

    if not prior_turns:
        if browser_instruction:
            return f"{browser_instruction}\n\nLatest user request:\n{prompt}"
        return prompt

    rendered = (
        "Continue the conversation below. Use the earlier turns only as context and respond to the "
        "latest user request.\n\n"
        "Earlier turns:\n"
        + "\n\n".join(prior_turns)
        + "\n\nLatest user request:\n"
        + prompt
    )
    if browser_instruction:
        return f"{browser_instruction}\n\n{rendered}"
    return rendered


def toml_basic_string(value: str) -> str:
    escaped = value.replace("\\", "\\\\").replace('"', '\\"')
    return f'"{escaped}"'


def toml_string_array(values: list[str]) -> str:
    return "[" + ",".join(toml_basic_string(value) for value in values) + "]"


def toml_inline_table(values: dict[str, str]) -> str:
    parts = [f"{key}={toml_basic_string(values[key])}" for key in sorted(values.keys())]
    return "{" + ",".join(parts) + "}"


def build_codex_cli_browser_mcp_overrides(
    *,
    allowed_hosts: list[str] | None,
    enable_browser_mcp: bool,
    config: Any,
    normalize_domain_allowlist_func: Callable[[Any], list[str]],
    required_client_value: str,
) -> list[str]:
    if not enable_browser_mcp or not config.codex_cli_enable_browser_mcp:
        return []
    server_path = config.codex_cli_browser_mcp_server_path
    if not server_path.exists():
        return []

    normalized_hosts = normalize_domain_allowlist_func(allowed_hosts or [])
    if not normalized_hosts:
        normalized_hosts = list(config.browser_default_domain_allowlist)

    config_root = f"mcp_servers.{config.codex_cli_browser_mcp_name}"
    env_table = toml_inline_table(
        {
            "MCP_BROWSER_USE_BROKER_URL": config.codex_cli_browser_mcp_broker_url,
            "MCP_BROWSER_USE_ALLOWED_HOSTS": ",".join(normalized_hosts),
            "MCP_BROWSER_USE_CLIENT_HEADER": required_client_value,
            "MCP_BROWSER_USE_APPROVAL_MODE": config.codex_cli_browser_mcp_approval_mode,
        }
    )
    return [
        "-c",
        f"{config_root}.command={toml_basic_string(config.codex_cli_browser_mcp_python)}",
        "-c",
        f"{config_root}.args={toml_string_array([str(server_path.resolve())])}",
        "-c",
        f"{config_root}.env={env_table}",
    ]


def call_codex_cli(
    prompt: str,
    messages: list[dict[str, str]],
    cli_session_id: str = "",
    *,
    config: Any,
    repo_root: Path,
    latest_codex_session_entry_func: Callable[[], dict[str, Any] | None],
    discover_new_codex_session_id_func: Callable[[dict[str, Any] | None], str],
    normalize_domain_allowlist_func: Callable[[Any], list[str]],
    required_client_value: str,
    terminate_subprocess_func: Callable[[Any, float], None],
    allowed_hosts: list[str] | None = None,
    enable_browser_mcp: bool = False,
    force_browser_action: bool = False,
    cancel_check: Any = None,
    on_process_start: Any = None,
    on_process_end: Any = None,
    cancelled_error_cls: type[BaseException] = RuntimeError,
) -> tuple[str, str]:
    if not config.codex_cli_path or not config.codex_cli_logged_in:
        raise RuntimeError("Local Codex CLI is not available or not logged in.")

    prompt_text = build_codex_cli_prompt(
        messages,
        prompt,
        force_browser_action=force_browser_action,
    )
    mcp_overrides = build_codex_cli_browser_mcp_overrides(
        allowed_hosts=allowed_hosts,
        enable_browser_mcp=enable_browser_mcp,
        config=config,
        normalize_domain_allowlist_func=normalize_domain_allowlist_func,
        required_client_value=required_client_value,
    )
    base_command = [config.codex_cli_path, *mcp_overrides, "exec"]
    output_path = ""
    previous_entry = None if cli_session_id else latest_codex_session_entry_func()
    timeout_sec = config.codex_timeout_sec
    if enable_browser_mcp:
        timeout_sec = max(timeout_sec, 180)
    if cli_session_id:
        timeout_sec = max(timeout_sec, 240 if enable_browser_mcp else 120)

    try:
        with tempfile.NamedTemporaryFile(prefix="codex-last-", suffix=".txt", delete=False) as tmp:
            output_path = tmp.name

        if cli_session_id:
            command = [
                *base_command,
                "resume",
                cli_session_id,
                "--skip-git-repo-check",
                "-o",
                output_path,
                "-",
            ]
        else:
            command = [
                *base_command,
                "--sandbox",
                "read-only",
                "--color",
                "never",
                "--skip-git-repo-check",
                "-C",
                str(repo_root),
                "-o",
                output_path,
                "-",
            ]

        try:
            completed = run_subprocess_with_cancel(
                command,
                input_text=prompt_text,
                timeout_sec=timeout_sec,
                terminate_subprocess_func=terminate_subprocess_func,
                cancel_check=cancel_check,
                on_process_start=on_process_start,
                on_process_end=on_process_end,
                cancelled_error_cls=cancelled_error_cls,
            )
        except subprocess.TimeoutExpired as error:
            raise RuntimeError(
                f"Codex CLI timed out after {int(timeout_sec)}s. "
                "Increase CODEX_TIMEOUT_SEC if needed."
            ) from error
        if completed.returncode != 0:
            stderr = completed.stderr.strip() or completed.stdout.strip() or "unknown codex CLI failure"
            raise RuntimeError(f"Codex CLI failed: {stderr}")
        if not output_path:
            return "", cli_session_id
        answer = Path(output_path).read_text(encoding="utf-8").strip()
        if cli_session_id:
            return answer, cli_session_id
        return answer, discover_new_codex_session_id_func(previous_entry)
    finally:
        if output_path:
            try:
                Path(output_path).unlink(missing_ok=True)
            except OSError:
                pass


def read_codex_session_index(index_path: Path, *, limit: int = 200) -> list[dict[str, Any]]:
    if not index_path.exists():
        return []
    try:
        lines = index_path.read_text(encoding='utf-8').splitlines()
    except Exception:
        return []
    entries: list[dict[str, Any]] = []
    for line in lines[-limit:]:
        try:
            parsed = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(parsed, dict) and parsed.get('id'):
            entries.append(parsed)
    return entries


def latest_codex_session_entry(
    *,
    read_codex_session_index_func: Callable[..., list[dict[str, Any]]],
) -> dict[str, Any] | None:
    entries = read_codex_session_index_func(limit=200)
    if not entries:
        return None
    return entries[-1]


def discover_new_codex_session_id(
    previous_entry: dict[str, Any] | None,
    *,
    read_codex_session_index_func: Callable[..., list[dict[str, Any]]],
) -> str:
    previous_id = str((previous_entry or {}).get('id', '') or '')
    entries = read_codex_session_index_func(limit=400)
    for entry in reversed(entries):
        entry_id = str(entry.get('id', '') or '')
        if not entry_id:
            continue
        if entry_id != previous_id:
            return entry_id
    return ''
