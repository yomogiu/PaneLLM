#!/usr/bin/env python3
from __future__ import annotations

import os
import platform
import shutil
import socket
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import urlparse


REPO_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_LLAMA_URL = "http://127.0.0.1:18000/v1/chat/completions"
MIN_PYTHON = (3, 10)
CHROME_APP_CANDIDATES = (
    "/Applications/Google Chrome.app",
    "/Applications/Chromium.app",
    str(Path.home() / "Applications" / "Google Chrome.app"),
    str(Path.home() / "Applications" / "Chromium.app"),
)


@dataclass
class CheckResult:
    status: str
    label: str
    detail: str
    suggestion: str = ""


def ok(label: str, detail: str) -> CheckResult:
    return CheckResult("OK", label, detail)


def warn(label: str, detail: str, suggestion: str = "") -> CheckResult:
    return CheckResult("WARN", label, detail, suggestion)


def print_section(title: str, results: list[CheckResult]) -> None:
    print(f"\n{title}")
    print("-" * len(title))
    for result in results:
        print(f"[{result.status}] {result.label}: {result.detail}")
        if result.suggestion:
            print(f"        suggestion: {result.suggestion}")


def detect_browser_app() -> str:
    for candidate in CHROME_APP_CANDIDATES:
        if Path(candidate).exists():
            return candidate
    return ""


def is_port_open(host: str, port: int, timeout_sec: float = 1.5) -> bool:
    try:
        with socket.create_connection((host, port), timeout=timeout_sec):
            return True
    except OSError:
        return False


def run_command(command: list[str], timeout_sec: float = 5.0) -> subprocess.CompletedProcess[str] | None:
    try:
        return subprocess.run(
            command,
            text=True,
            capture_output=True,
            timeout=timeout_sec,
            check=False,
        )
    except (OSError, subprocess.SubprocessError):
        return None


def check_core() -> list[CheckResult]:
    results: list[CheckResult] = []

    if sys.platform == "darwin":
        results.append(ok("Platform", f"macOS {platform.mac_ver()[0] or 'detected'}"))
    else:
        results.append(
            warn(
                "Platform",
                f"expected macOS, found {platform.system()}",
                "Use this checker as guidance only, or run it on macOS.",
            )
        )

    python_version = sys.version_info[:3]
    if python_version >= MIN_PYTHON:
        results.append(
            ok(
                "Python",
                f"{python_version[0]}.{python_version[1]}.{python_version[2]} via {sys.executable}",
            )
        )
    else:
        results.append(
            warn(
                "Python",
                f"{python_version[0]}.{python_version[1]}.{python_version[2]} via {sys.executable}",
                "Use Python 3.10 or newer.",
            )
        )

    if shutil.which("pip3") or shutil.which("pip"):
        results.append(ok("pip", "available"))
    else:
        results.append(warn("pip", "not found in PATH", "Install pip for the Python you plan to use."))

    browser_path = detect_browser_app()
    if browser_path:
        results.append(ok("Browser", browser_path))
    else:
        results.append(
            warn(
                "Browser",
                "Chrome or Chromium not found in standard macOS application paths",
                "Install Chrome or Chromium, or load the extension from a browser with MV3 side panel support.",
            )
        )

    broker_path = REPO_ROOT / "broker" / "local_broker.py"
    extension_path = REPO_ROOT / "chrome_secure_panel" / "manifest.json"
    if broker_path.exists() and extension_path.exists():
        results.append(ok("Repo layout", "broker and extension entrypoints found"))
    else:
        missing = []
        if not broker_path.exists():
            missing.append(str(broker_path))
        if not extension_path.exists():
            missing.append(str(extension_path))
        results.append(
            warn(
                "Repo layout",
                "missing expected project files",
                "Re-run the checker from a valid assist repository checkout.",
            )
        )
        if missing:
            results.append(warn("Missing files", ", ".join(missing)))

    return results


def check_codex_api() -> CheckResult:
    if os.environ.get("OPENAI_API_KEY", "").strip():
        return ok("Codex Responses", "OPENAI_API_KEY is set")
    return warn(
        "Codex Responses",
        "OPENAI_API_KEY is not set",
        'export OPENAI_API_KEY="<your-api-key>"',
    )


def check_codex_cli() -> CheckResult:
    codex_path = shutil.which("codex")
    if not codex_path:
        return warn(
            "Codex CLI",
            "codex is not in PATH",
            "Install the official Codex CLI, then run `codex login`.",
        )
    status = run_command([codex_path, "login", "status"])
    if status and status.returncode == 0 and "logged in" in (
        f"{status.stdout} {status.stderr}".lower()
    ):
        return ok("Codex CLI", f"installed at {codex_path} and logged in")
    return warn(
        "Codex CLI",
        f"installed at {codex_path} but not logged in",
        "Run `codex login` and retry.",
    )


def check_llama() -> CheckResult:
    llama_url = os.environ.get("LLAMA_URL", DEFAULT_LLAMA_URL).strip() or DEFAULT_LLAMA_URL
    parsed = urlparse(llama_url)
    if parsed.scheme not in {"http", "https"} or not parsed.hostname:
        return warn(
            "llama.cpp",
            f"LLAMA_URL is invalid: {llama_url}",
            f'Export a valid URL, for example `export LLAMA_URL="{DEFAULT_LLAMA_URL}"`.',
        )
    port = parsed.port or (443 if parsed.scheme == "https" else 80)
    if is_port_open(parsed.hostname, port):
        return ok("llama.cpp", f"endpoint reachable at {llama_url}")
    return warn(
        "llama.cpp",
        f"cannot connect to {llama_url}",
        "Start your local OpenAI-compatible server, or set LLAMA_URL to the correct host and port.",
    )


def check_mlx() -> list[CheckResult]:
    results: list[CheckResult] = []
    worker_python = os.environ.get("BROKER_MLX_WORKER_PYTHON", "python3").strip() or "python3"
    worker_path = shutil.which(worker_python) if "/" not in worker_python else worker_python
    if not worker_path:
        results.append(
            warn(
                "MLX worker Python",
                f"{worker_python} is not in PATH",
                "Set BROKER_MLX_WORKER_PYTHON to the interpreter you want the MLX worker to use.",
            )
        )
        return results

    import_check = run_command([worker_path, "-c", "import mlx_lm"], timeout_sec=8.0)
    if import_check and import_check.returncode == 0:
        results.append(ok("MLX runtime", f"mlx_lm imports under {worker_path}"))
    else:
        results.append(
            warn(
                "MLX runtime",
                f"mlx_lm does not import under {worker_path}",
                f"{worker_path} -m pip install mlx-lm",
            )
        )

    model_path_raw = os.environ.get("BROKER_MLX_MODEL_PATH", "").strip()
    if not model_path_raw:
        results.append(
            warn(
                "MLX model path",
                "BROKER_MLX_MODEL_PATH is not set",
                'export BROKER_MLX_MODEL_PATH="$HOME/models/mlx/<your-model-folder>"',
            )
        )
        return results

    model_path = Path(model_path_raw).expanduser()
    if model_path.exists():
        results.append(ok("MLX model path", str(model_path)))
    else:
        results.append(
            warn(
                "MLX model path",
                f"path does not exist: {model_path}",
                "Point BROKER_MLX_MODEL_PATH at a local MLX model directory.",
            )
        )

    return results


def render_next_steps(core_results: list[CheckResult], ready_backends: list[str]) -> None:
    blockers = [result for result in core_results if result.status != "OK"]

    print("\nSummary")
    print("-------")
    if blockers:
        print("Core setup still has blockers. Fix those first.")
    else:
        print("Core setup looks usable on this machine.")

    if ready_backends:
        print("Ready backends: " + ", ".join(ready_backends))
    else:
        print("Ready backends: none detected yet")

    print("\nRecommended next steps")
    print("----------------------")
    print("1. Choose one backend path above and fix the missing items for that path.")
    print(f"2. Start the broker: python3 {REPO_ROOT / 'broker' / 'local_broker.py'}")
    print("3. Verify broker health:")
    print("   curl -i -H 'X-Assistant-Client: chrome-sidepanel-v1' http://127.0.0.1:7777/health")
    print(f"4. Load the extension from: {REPO_ROOT / 'chrome_secure_panel'}")
    print("5. Open the side panel and send a simple prompt.")


def main() -> int:
    print("assist macOS readiness check")
    print("read-only; no changes will be made")

    core_results = check_core()
    codex_api_result = check_codex_api()
    codex_cli_result = check_codex_cli()
    llama_result = check_llama()
    mlx_results = check_mlx()
    backend_results = [codex_api_result, codex_cli_result, llama_result, *mlx_results]
    ready_backends: list[str] = []

    if codex_api_result.status == "OK":
        ready_backends.append("Codex Responses")
    if codex_cli_result.status == "OK":
        ready_backends.append("Codex CLI")
    if llama_result.status == "OK":
        ready_backends.append("llama.cpp")
    if mlx_results and all(result.status == "OK" for result in mlx_results):
        ready_backends.append("MLX")

    print_section("Core requirements", core_results)
    print_section("Backend readiness (choose one path)", backend_results)
    render_next_steps(core_results, ready_backends)

    has_core_blocker = any(result.status != "OK" for result in core_results)
    has_ready_backend = bool(ready_backends)
    return 0 if not has_core_blocker and has_ready_backend else 1


if __name__ == "__main__":
    raise SystemExit(main())
