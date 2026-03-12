#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import socket
import sys
import uuid
from pathlib import Path
from typing import Any
from urllib.parse import urlparse
from urllib.request import Request, urlopen


SCRIPT_ROOT = Path(__file__).resolve().parent
DEFAULT_ENV_PATH = SCRIPT_ROOT / ".env"
DEFAULT_GLM_URL = "http://127.0.0.1:18000/v1/chat/completions"
DEFAULT_MODEL = "glm-4.7-flash-llamacpp"
DEFAULT_BRIDGE_SOCKET = "/tmp/ext-agent-bridge.sock"
DEFAULT_TARGET_URL = "http://127.0.0.1:3000"
DEFAULT_MAX_STEPS = 6
DEFAULT_PRIVATE_SYSTEM_PROMPT = "You are a private local assistant. Answer directly and concisely."
DEFAULT_AGENT_SYSTEM_PROMPT = (
    "You are a browser automation planner. "
    "If a browser action is needed, respond with a tool call only. "
    "If a tool result is provided, use it to decide the next step."
)


TOOLS = [
    {
        "type": "function",
        "function": {
            "name": "browser.navigate",
            "description": "Navigate the browser to a URL",
            "parameters": {
                "type": "object",
                "properties": {
                    "url": {"type": "string"},
                },
                "required": ["url"],
                "additionalProperties": False,
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "browser.get_content",
            "description": (
                "Get a navigation-focused summary from the current page. "
                "Use mode=raw_html only when raw HTML is explicitly required."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "selector": {"type": "string"},
                    "mode": {
                        "type": "string",
                        "enum": ["navigation", "raw_html"],
                    },
                    "maxChars": {"type": "integer"},
                    "maxItems": {"type": "integer"},
                },
                "required": [],
                "additionalProperties": False,
            },
        },
    },
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Local assistant CLI with private Q&A, direct tool calls, and agent tool loops.",
        formatter_class=argparse.RawTextHelpFormatter,
        epilog=(
            "Examples:\n"
            "  python llama_browser_tool_loop.py ask \"Summarize the key risks in this note\"\n"
            "  python llama_browser_tool_loop.py tool --tool browser.navigate --args '{\"url\":\"http://127.0.0.1:3000\"}'\n"
            "  python llama_browser_tool_loop.py agent --prompt \"Open http://127.0.0.1:3000 and read the title.\""
        ),
    )
    commands = parser.add_subparsers(dest="command")

    ask = commands.add_parser("ask", help="Ask a private local question (no tool loop).")
    ask.add_argument("question", nargs="?", help="Question text.")
    ask.add_argument(
        "--question-file",
        default=None,
        help="Read question from a text file.",
    )
    ask.add_argument(
        "--stdin-question",
        action="store_true",
        help="Read question from stdin.",
    )
    add_model_args(ask)
    ask.add_argument(
        "--system-prompt",
        default=DEFAULT_PRIVATE_SYSTEM_PROMPT,
        help="System prompt for private Q&A.",
    )

    tool = commands.add_parser("tool", help="Execute one bridge tool call directly.")
    add_bridge_args(tool)
    tool.add_argument(
        "--tool",
        required=True,
        help="Tool name, e.g. browser.navigate or browser.get_content.",
    )
    tool.add_argument(
        "--args",
        default=None,
        help="JSON object string with tool args.",
    )
    tool.add_argument(
        "--args-file",
        default=None,
        help="Path to a file containing a JSON object for tool args.",
    )
    tool.add_argument(
        "--stdin-args",
        action="store_true",
        help="Read tool args JSON from stdin.",
    )
    tool.add_argument(
        "--verbose",
        action="store_true",
        help="Print detailed bridge responses.",
    )

    agent = commands.add_parser("agent", help="Run the OpenAI-style tool loop against browsertool.")
    add_model_args(agent)
    add_bridge_args(agent)
    agent.add_argument(
        "--prompt",
        default=None,
        help="Override the default prompt. If omitted, a prompt is generated from --target-url.",
    )
    agent.add_argument(
        "--prompt-file",
        default=None,
        help="Read prompt from a text file.",
    )
    agent.add_argument(
        "--stdin-prompt",
        action="store_true",
        help="Read prompt from stdin.",
    )
    agent.add_argument(
        "--system-prompt",
        default=DEFAULT_AGENT_SYSTEM_PROMPT,
        help="System prompt for the agent tool loop.",
    )
    agent.add_argument(
        "--max-steps",
        type=int,
        default=DEFAULT_MAX_STEPS,
        help=f"Maximum model/tool turns before aborting (default: {DEFAULT_MAX_STEPS})",
    )

    # Backward compatibility: if no explicit subcommand is provided, default to `agent`.
    argv = sys.argv[1:]
    if not argv:
        argv = ["agent"]
    elif argv[0] in {"-h", "--help"}:
        pass
    elif argv[0] not in {"ask", "tool", "agent"}:
        argv = ["agent", *argv]

    return parser.parse_args(argv)


def add_model_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--glm-url",
        default=DEFAULT_GLM_URL,
        help=f"OpenAI-compatible chat completions endpoint (default: {DEFAULT_GLM_URL})",
    )
    parser.add_argument(
        "--model",
        default=DEFAULT_MODEL,
        help=f"Model name to send to llama.cpp (default: {DEFAULT_MODEL})",
    )
    parser.add_argument(
        "--env-file",
        default=str(DEFAULT_ENV_PATH),
        help=f"Optional .env path for API key loading (default: {DEFAULT_ENV_PATH})",
    )
    parser.add_argument(
        "--api-key",
        default=None,
        help="Explicit API key. If omitted, uses --api-key-env after loading --env-file.",
    )
    parser.add_argument(
        "--api-key-env",
        default="LLAMA_API_KEY",
        help="Environment variable name for API key lookup (default: LLAMA_API_KEY).",
    )
    parser.add_argument(
        "--temperature",
        type=float,
        default=0.1,
        help="Sampling temperature (default: 0.1).",
    )
    parser.add_argument(
        "--max-tokens",
        type=int,
        default=256,
        help="Max completion tokens per request (default: 256).",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Print full JSON responses.",
    )


def add_bridge_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--bridge-socket",
        default=DEFAULT_BRIDGE_SOCKET,
        help=f"browsertool bridge Unix socket (default: {DEFAULT_BRIDGE_SOCKET})",
    )
    parser.add_argument(
        "--target-url",
        default=DEFAULT_TARGET_URL,
        help=f"Target URL used to derive allowlisted host (default: {DEFAULT_TARGET_URL})",
    )
    parser.add_argument(
        "--allow-host",
        action="append",
        default=[],
        help="Additional host to allow in bridge policy. Repeatable.",
    )
    parser.add_argument(
        "--approval-mode",
        default="auto-approve",
        choices=["auto-approve", "manual"],
        help="Bridge approval mode for the created session (default: auto-approve).",
    )


def load_env_file(path: Path) -> None:
    if not path.exists():
        return

    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip("'").strip('"')
        os.environ.setdefault(key, value)


def resolve_api_key(args: argparse.Namespace) -> str | None:
    if getattr(args, "api_key", None):
        return args.api_key
    key_name = getattr(args, "api_key_env", "LLAMA_API_KEY")
    return os.environ.get(key_name)


def bridge_call(socket_path: str, method: str, params: dict[str, Any], timeout: float = 15.0) -> Any:
    request = {
        "id": str(uuid.uuid4()),
        "method": method,
        "params": params,
    }

    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as client:
        client.settimeout(timeout)
        client.connect(socket_path)
        client.sendall((json.dumps(request) + "\n").encode("utf-8"))

        chunks: list[bytes] = []
        while True:
            chunk = client.recv(65536)
            if not chunk:
                break
            chunks.append(chunk)
            if b"\n" in chunk:
                break

    if not chunks:
        raise RuntimeError(f'Bridge returned no response for method "{method}".')

    line = b"".join(chunks).split(b"\n", 1)[0].decode("utf-8").strip()
    payload = json.loads(line)
    if payload.get("error"):
        error = payload["error"]
        raise RuntimeError(error.get("message", f'Bridge error for method "{method}".'))

    return payload.get("result")


def chat_completion(glm_url: str, api_key: str | None, payload: dict[str, Any]) -> dict[str, Any]:
    headers = {"Content-Type": "application/json"}
    if api_key:
        headers["Authorization"] = f"Bearer {api_key}"

    request = Request(
        glm_url,
        data=json.dumps(payload).encode("utf-8"),
        method="POST",
        headers=headers,
    )
    with urlopen(request, timeout=120) as response:
        return json.loads(response.read().decode("utf-8"))


def build_prompt(target_url: str) -> str:
    return f"Open {target_url} and then read the page title."


def derive_allow_hosts(target_url: str, extra_hosts: list[str]) -> list[str]:
    parsed = urlparse(target_url)
    hosts: list[str] = []
    if parsed.hostname:
        hosts.append(parsed.hostname)
    hosts.extend(extra_hosts)

    deduped: list[str] = []
    for host in hosts:
        host = host.strip()
        if host and host not in deduped:
            deduped.append(host)
    return deduped


def parse_tool_arguments(arguments: Any) -> dict[str, Any]:
    if isinstance(arguments, dict):
        return arguments
    if isinstance(arguments, str):
        parsed = json.loads(arguments)
        if not isinstance(parsed, dict):
            raise RuntimeError("Tool arguments JSON must decode to an object.")
        return parsed
    raise RuntimeError("Unsupported tool arguments shape from llama.cpp.")


def print_summary(label: str, payload: dict[str, Any], verbose: bool) -> None:
    print(label)
    if verbose:
        print(json.dumps(payload, indent=2))


def read_text_input(
    *,
    inline_value: str | None,
    file_path: str | None,
    read_stdin: bool,
    label: str,
    default: str | None = None,
) -> str:
    sources = int(inline_value is not None) + int(file_path is not None) + int(read_stdin)
    if sources > 1:
        raise RuntimeError(f"Use only one source for {label}: inline value, file, or stdin.")
    if inline_value is not None:
        return inline_value
    if file_path is not None:
        return Path(file_path).read_text(encoding="utf-8").strip()
    if read_stdin:
        return sys.stdin.read().strip()
    if default is not None:
        return default
    raise RuntimeError(f"{label} is required.")


def parse_json_object(raw: str, label: str) -> dict[str, Any]:
    parsed = json.loads(raw)
    if not isinstance(parsed, dict):
        raise RuntimeError(f"{label} must decode to a JSON object.")
    return parsed


def ensure_bridge_session(
    *,
    bridge_socket: str,
    target_url: str,
    allow_host: list[str],
    approval_mode: str,
    verbose: bool,
) -> tuple[dict[str, Any], dict[str, Any], list[str]]:
    allow_hosts = derive_allow_hosts(target_url, allow_host)
    if not allow_hosts:
        raise RuntimeError("No allowlisted hosts resolved. Check --target-url or --allow-host.")

    health = bridge_call(bridge_socket, "health.check", {})
    print_summary("bridge health:", health, verbose)
    if not health.get("ok"):
        raise RuntimeError("Bridge health check failed.")

    session_id = f"s_{uuid.uuid4().hex[:8]}"
    run_id = f"r_{uuid.uuid4().hex[:8]}"
    session = bridge_call(
        bridge_socket,
        "session.create",
        {
            "sessionId": session_id,
            "policy": {
                "domainAllowlist": allow_hosts,
                "approvalMode": approval_mode,
            },
        },
    )
    run = bridge_call(
        bridge_socket,
        "run.start",
        {
            "sessionId": session["sessionId"],
            "runId": run_id,
            "capabilityToken": session["capabilityToken"],
        },
    )
    return session, run, allow_hosts


def run_private_ask(args: argparse.Namespace) -> int:
    load_env_file(Path(args.env_file))
    api_key = resolve_api_key(args)
    question = read_text_input(
        inline_value=args.question,
        file_path=args.question_file,
        read_stdin=args.stdin_question,
        label="question",
    )

    payload = {
        "model": args.model,
        "messages": [
            {"role": "system", "content": args.system_prompt},
            {"role": "user", "content": question},
        ],
        "temperature": args.temperature,
        "max_tokens": args.max_tokens,
    }
    response = chat_completion(args.glm_url, api_key, payload)
    print_summary("model response:", response, args.verbose)

    message = response["choices"][0]["message"]
    print(message.get("content", ""))
    return 0


def run_direct_tool_call(args: argparse.Namespace) -> int:
    session, run, allow_hosts = ensure_bridge_session(
        bridge_socket=args.bridge_socket,
        target_url=args.target_url,
        allow_host=args.allow_host,
        approval_mode=args.approval_mode,
        verbose=args.verbose,
    )

    raw_args = read_text_input(
        inline_value=args.args,
        file_path=args.args_file,
        read_stdin=args.stdin_args,
        label="tool args",
        default="{}",
    )
    tool_args = parse_json_object(raw_args, "tool args")
    tool_call_id = f"tc_{uuid.uuid4().hex[:8]}"

    print(f"sessionId: {session['sessionId']}")
    print(f"runId: {run['runId']}")
    print(f"allowlist: {', '.join(allow_hosts)}")
    print(f"tool: {args.tool}({json.dumps(tool_args)})")

    result = bridge_call(
        args.bridge_socket,
        "tool.execute",
        {
            "tool": args.tool,
            "sessionId": session["sessionId"],
            "runId": run["runId"],
            "toolCallId": tool_call_id,
            "capabilityToken": session["capabilityToken"],
            "args": tool_args,
        },
    )
    print("bridge result:")
    print(json.dumps(result, indent=2))
    return 0


def run_agent_loop(args: argparse.Namespace) -> int:
    load_env_file(Path(args.env_file))
    api_key = resolve_api_key(args)

    if args.max_steps <= 0:
        print("--max-steps must be greater than 0.", file=sys.stderr)
        return 1

    prompt = read_text_input(
        inline_value=args.prompt,
        file_path=args.prompt_file,
        read_stdin=args.stdin_prompt,
        label="prompt",
        default=build_prompt(args.target_url),
    )
    system_prompt = args.system_prompt

    session, run, allow_hosts = ensure_bridge_session(
        bridge_socket=args.bridge_socket,
        target_url=args.target_url,
        allow_host=args.allow_host,
        approval_mode=args.approval_mode,
        verbose=args.verbose,
    )

    print(f"sessionId: {session['sessionId']}")
    print(f"runId: {run['runId']}")
    print(f"allowlist: {', '.join(allow_hosts)}")

    messages: list[dict[str, Any]] = [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": prompt},
    ]

    for step in range(1, args.max_steps + 1):
        payload = {
            "model": args.model,
            "messages": messages,
            "tools": TOOLS,
            "tool_choice": "auto",
            "temperature": args.temperature,
            "max_tokens": args.max_tokens,
        }
        response = chat_completion(args.glm_url, api_key, payload)
        print_summary(f"\nllama.cpp response {step}:", response, args.verbose)

        choice = response["choices"][0]
        message = choice["message"]
        tool_calls = message.get("tool_calls") or []

        if not tool_calls:
            print("\nfinal assistant answer:\n")
            print(message.get("content", ""))
            return 0

        print(f"\nstep {step}: executing {len(tool_calls)} tool call(s)")
        assistant_message = {
            "role": "assistant",
            "content": message.get("content", ""),
            "tool_calls": tool_calls,
        }
        messages.append(assistant_message)

        for tool_call in tool_calls:
            tool_name = tool_call["function"]["name"]
            tool_args = parse_tool_arguments(tool_call["function"]["arguments"])
            print(f"- {tool_name}({json.dumps(tool_args)})")

            result = bridge_call(
                args.bridge_socket,
                "tool.execute",
                {
                    "tool": tool_name,
                    "sessionId": session["sessionId"],
                    "runId": run["runId"],
                    "toolCallId": tool_call["id"],
                    "capabilityToken": session["capabilityToken"],
                    "args": tool_args,
                },
            )
            print_summary("  bridge result:", result, args.verbose)

            tool_message = {
                "role": "tool",
                "tool_call_id": tool_call["id"],
                "content": json.dumps(
                    {
                        "success": result.get("success"),
                        "data": result.get("data"),
                        "error": result.get("error"),
                        "policy": result.get("policy"),
                    }
                ),
            }
            messages.append(tool_message)

    print(f"Reached max steps ({args.max_steps}) without a final assistant answer.", file=sys.stderr)
    return 1


def main() -> int:
    args = parse_args()
    try:
        if args.command == "ask":
            return run_private_ask(args)
        if args.command == "tool":
            return run_direct_tool_call(args)
        return run_agent_loop(args)
    except Exception as error:
        print(f"error: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
