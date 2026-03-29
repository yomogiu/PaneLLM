from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path
from typing import Callable
from typing import Mapping
from typing import Any


@dataclass(frozen=True)
class BrokerConfig:
    host: str
    port: int
    llama_url: str
    llama_model: str
    llama_api_key: str | None
    mlx_url: str
    mlx_model: str
    mlx_api_key: str | None
    openai_api_key: str | None
    openai_base_url: str
    openai_codex_model: str
    openai_codex_reasoning_effort: str
    openai_codex_max_output_tokens: int
    codex_home: Path
    codex_session_index_path: Path
    codex_cli_path: str | None
    codex_cli_logged_in: bool
    codex_cli_enable_browser_mcp: bool
    codex_cli_browser_mcp_name: str
    codex_cli_browser_mcp_python: str
    codex_cli_browser_mcp_server_path: Path
    codex_cli_browser_mcp_broker_url: str
    codex_cli_browser_mcp_approval_mode: str
    codex_timeout_sec: int
    codex_run_timeout_sec: int
    codex_event_poll_timeout_ms: int
    codex_enable_background: bool
    data_dir: Path
    max_context_messages: int
    max_context_chars: int
    max_summary_chars: int
    local_backend_timeout_sec: int
    local_backend_browser_timeout_sec: int
    browser_command_timeout_sec: int
    extension_client_stale_sec: int
    browser_default_domain_allowlist: list[str]


def load_config(
    *,
    environ: Mapping[str, str],
    default_llama_model: str,
    approval_modes: set[str],
    normalize_domain_allowlist_func: Callable[[Any], list[str]],
    which_func: Callable[[str], str | None],
    run_func: Callable[..., Any],
    module_root: Path,
    repo_root: Path,
    path_home: Path,
) -> BrokerConfig:
    host = environ.get("BROKER_HOST", "127.0.0.1")
    port = int(environ.get("BROKER_PORT", "7777"))
    llama_url = environ.get("LLAMA_URL", "http://127.0.0.1:18000/v1/chat/completions")
    llama_model = environ.get("LLAMA_MODEL", default_llama_model)
    llama_api_key = environ.get("LLAMA_API_KEY")
    mlx_url = environ.get("MLX_URL", "").strip()
    mlx_model = environ.get("MLX_MODEL", "").strip()
    mlx_api_key = environ.get("MLX_API_KEY")
    openai_api_key = environ.get("OPENAI_API_KEY")
    openai_base_url = environ.get("OPENAI_BASE_URL", "https://api.openai.com/v1").rstrip("/")
    openai_codex_model = environ.get("OPENAI_CODEX_MODEL", "gpt-5.3-codex")
    openai_codex_reasoning_effort = environ.get("OPENAI_CODEX_REASONING_EFFORT", "medium")
    openai_codex_max_output_tokens = int(environ.get("OPENAI_CODEX_MAX_OUTPUT_TOKENS", "1800"))
    codex_home = Path(environ.get("CODEX_HOME", str(path_home / ".codex"))).expanduser()
    codex_session_index_path = codex_home / "session_index.jsonl"
    codex_cli_path = which_func("codex")
    codex_cli_logged_in = False
    default_mcp_server_path = repo_root / "tools" / "mcp-servers" / "browser-use" / "server.py"
    codex_cli_enable_browser_mcp = (
        environ.get("BROKER_CODEX_CLI_ENABLE_BROWSER_MCP", "true").strip().lower()
        in {"1", "true", "yes", "on"}
    )
    raw_mcp_name = environ.get("BROKER_CODEX_CLI_BROWSER_MCP_NAME", "browser_use").strip()
    codex_cli_browser_mcp_name = re.sub(r"[^A-Za-z0-9_]", "_", raw_mcp_name) or "browser_use"
    codex_cli_browser_mcp_python = (
        environ.get("BROKER_CODEX_CLI_BROWSER_MCP_PYTHON", "python3").strip()
        or "python3"
    )
    codex_cli_browser_mcp_server_path = Path(
        environ.get(
            "BROKER_CODEX_CLI_BROWSER_MCP_SERVER_PATH",
            str(default_mcp_server_path),
        )
    ).expanduser()
    default_mcp_broker_host = "127.0.0.1" if host in {"0.0.0.0", "::"} else host
    codex_cli_browser_mcp_broker_url = environ.get(
        "BROKER_CODEX_CLI_BROWSER_MCP_BROKER_URL",
        f"http://{default_mcp_broker_host}:{port}",
    ).strip()
    codex_cli_browser_mcp_approval_mode = environ.get(
        "BROKER_CODEX_CLI_BROWSER_MCP_APPROVAL_MODE",
        "auto-approve",
    ).strip().lower()
    if codex_cli_browser_mcp_approval_mode not in approval_modes:
        codex_cli_browser_mcp_approval_mode = "auto-approve"
    if codex_cli_path:
        try:
            status = run_func(
                [codex_cli_path, "login", "status"],
                text=True,
                capture_output=True,
                timeout=5,
                check=False,
            )
            codex_cli_logged_in = status.returncode == 0 and "logged in" in (
                (status.stdout or "") + " " + (status.stderr or "")
            ).lower()
        except Exception:
            codex_cli_logged_in = False
    codex_timeout_sec = int(environ.get("CODEX_TIMEOUT_SEC", "480"))
    codex_run_timeout_sec = int(environ.get("BROKER_CODEX_RUN_TIMEOUT_SEC", "180"))
    codex_event_poll_timeout_ms = int(
        environ.get("BROKER_CODEX_EVENT_POLL_TIMEOUT_MS", "20000")
    )
    local_backend_timeout_sec = int(
        environ.get("BROKER_LOCAL_BACKEND_TIMEOUT_SEC", "120")
    )
    local_backend_browser_timeout_sec = int(
        environ.get("BROKER_LOCAL_BACKEND_BROWSER_TIMEOUT_SEC", "300")
    )
    codex_enable_background = (
        environ.get("BROKER_CODEX_ENABLE_BACKGROUND", "false").strip().lower() == "true"
    )
    default_data_dir = module_root / ".data"
    data_dir = Path(environ.get("BROKER_DATA_DIR", str(default_data_dir)))
    max_context_messages = int(environ.get("BROKER_MAX_CONTEXT_MESSAGES", "32"))
    max_context_chars = int(environ.get("BROKER_MAX_CONTEXT_CHARS", "24000"))
    max_summary_chars = int(environ.get("BROKER_MAX_SUMMARY_CHARS", "5000"))
    browser_command_timeout_sec = int(environ.get("BROKER_BROWSER_COMMAND_TIMEOUT_SEC", "25"))
    extension_client_stale_sec = int(environ.get("BROKER_EXTENSION_CLIENT_STALE_SEC", "90"))
    default_allowlist_raw = environ.get(
        "BROKER_DEFAULT_DOMAIN_ALLOWLIST",
        "127.0.0.1,localhost",
    )
    browser_default_domain_allowlist = normalize_domain_allowlist_func(default_allowlist_raw)
    return BrokerConfig(
        host=host,
        port=port,
        llama_url=llama_url,
        llama_model=llama_model,
        llama_api_key=llama_api_key,
        mlx_url=mlx_url,
        mlx_model=mlx_model,
        mlx_api_key=mlx_api_key,
        openai_api_key=openai_api_key,
        openai_base_url=openai_base_url,
        openai_codex_model=openai_codex_model,
        openai_codex_reasoning_effort=openai_codex_reasoning_effort,
        openai_codex_max_output_tokens=openai_codex_max_output_tokens,
        codex_home=codex_home,
        codex_session_index_path=codex_session_index_path,
        codex_cli_path=codex_cli_path,
        codex_cli_logged_in=codex_cli_logged_in,
        codex_cli_enable_browser_mcp=codex_cli_enable_browser_mcp,
        codex_cli_browser_mcp_name=codex_cli_browser_mcp_name,
        codex_cli_browser_mcp_python=codex_cli_browser_mcp_python,
        codex_cli_browser_mcp_server_path=codex_cli_browser_mcp_server_path,
        codex_cli_browser_mcp_broker_url=codex_cli_browser_mcp_broker_url,
        codex_cli_browser_mcp_approval_mode=codex_cli_browser_mcp_approval_mode,
        codex_timeout_sec=codex_timeout_sec,
        codex_run_timeout_sec=codex_run_timeout_sec,
        codex_event_poll_timeout_ms=codex_event_poll_timeout_ms,
        codex_enable_background=codex_enable_background,
        data_dir=data_dir,
        max_context_messages=max_context_messages,
        max_context_chars=max_context_chars,
        max_summary_chars=max_summary_chars,
        local_backend_timeout_sec=local_backend_timeout_sec,
        local_backend_browser_timeout_sec=local_backend_browser_timeout_sec,
        browser_command_timeout_sec=browser_command_timeout_sec,
        extension_client_stale_sec=extension_client_stale_sec,
        browser_default_domain_allowlist=browser_default_domain_allowlist,
    )
