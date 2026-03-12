#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import sys
import threading
import uuid
from dataclasses import dataclass
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import parse_qs, urlparse
from urllib.request import Request, urlopen


SERVER_NAME = "browser-use"
SERVER_VERSION = "0.1.0"
DEFAULT_PROTOCOL_VERSION = "2024-11-05"
DEFAULT_BROKER_URL = "http://127.0.0.1:7777"
DEFAULT_CLIENT_HEADER = "chrome-sidepanel-v1"
DEFAULT_TIMEOUT_SEC = 30.0
DEFAULT_ALLOWED_HOSTS = [
    "127.0.0.1",
    "localhost",
    "google.com",
    "www.google.com",
    "arxiv.org",
    "www.arxiv.org",
]
APPROVAL_MODES = {"auto-approve", "manual", "auto-deny"}

BROWSER_LOCATOR_SCHEMA = {
    "type": "object",
    "properties": {
        "selector": {"type": "string"},
        "text": {"type": "string"},
        "label": {"type": "string"},
        "role": {"type": "string"},
        "placeholder": {"type": "string"},
        "name": {"type": "string"},
        "exact": {"type": "boolean"},
        "visible": {"type": "boolean"},
        "index": {"type": "integer"},
    },
    "required": [],
    "additionalProperties": False,
}


PROXIED_TOOL_DEFINITIONS = [
    {
        "name": "browser.navigate",
        "description": "Navigate the active tab (or specified tab) to an absolute URL.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "url": {"type": "string"},
                "tabId": {"type": "integer"},
            },
            "required": ["url"],
            "additionalProperties": False,
        },
    },
    {
        "name": "browser.open_tab",
        "description": "Open a new tab on an allowlisted URL.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "url": {"type": "string"},
            },
            "required": ["url"],
            "additionalProperties": False,
        },
    },
    {
        "name": "browser.get_tabs",
        "description": "List allowlisted tabs in the current window.",
        "inputSchema": {
            "type": "object",
            "properties": {},
            "required": [],
            "additionalProperties": False,
        },
    },
    {
        "name": "browser.describe_session_tabs",
        "description": "Describe allowlisted tabs and tab groups in the current window.",
        "inputSchema": {
            "type": "object",
            "properties": {},
            "required": [],
            "additionalProperties": False,
        },
    },
    {
        "name": "browser.switch_tab",
        "description": "Switch to a tab by id.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "tabId": {"type": "integer"},
            },
            "required": ["tabId"],
            "additionalProperties": False,
        },
    },
    {
        "name": "browser.focus_tab",
        "description": "Focus a tab by id.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "tabId": {"type": "integer"},
            },
            "required": ["tabId"],
            "additionalProperties": False,
        },
    },
    {
        "name": "browser.close_tab",
        "description": "Close a tab by id.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "tabId": {"type": "integer"},
            },
            "required": ["tabId"],
            "additionalProperties": False,
        },
    },
    {
        "name": "browser.group_tabs",
        "description": "Group tabs and optionally provide a title, color, and collapsed flag.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "tabIds": {
                    "type": "array",
                    "items": {"type": "integer"},
                },
                "groupName": {"type": "string"},
                "color": {"type": "string"},
                "collapsed": {"type": "boolean"},
            },
            "required": ["tabIds"],
            "additionalProperties": False,
        },
    },
    {
        "name": "browser.click",
        "description": "Click an element by CSS selector.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "selector": {"type": "string"},
                "tabId": {"type": "integer"},
            },
            "required": ["selector"],
            "additionalProperties": False,
        },
    },
    {
        "name": "browser.type",
        "description": "Type text into an input, textarea, or editable element.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "selector": {"type": "string"},
                "text": {"type": "string"},
                "tabId": {"type": "integer"},
                "clear": {"type": "boolean"},
            },
            "required": ["selector", "text"],
            "additionalProperties": False,
        },
    },
    {
        "name": "browser.press_key",
        "description": "Send a keyboard key press to the active element in a tab.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "key": {"type": "string"},
                "tabId": {"type": "integer"},
                "modifiers": {
                    "type": "array",
                    "items": {"type": "string"},
                },
                "repeat": {"type": "integer"},
                "delayMs": {"type": "integer"},
            },
            "required": ["key"],
            "additionalProperties": False,
        },
    },
    {
        "name": "browser.scroll",
        "description": "Scroll the page or an element in the target tab.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "tabId": {"type": "integer"},
                "selector": {"type": "string"},
                "deltaX": {"type": "number"},
                "deltaY": {"type": "number"},
            },
            "required": [],
            "additionalProperties": False,
        },
    },
    {
        "name": "browser.get_content",
        "description": "Get page content or element content by selector.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "tabId": {"type": "integer"},
                "selector": {"type": "string"},
                "maxChars": {"type": "integer"},
            },
            "required": [],
            "additionalProperties": False,
        },
    },
    {
        "name": "browser.find_one",
        "description": "Find one element using a semantic locator and return element metadata.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "tabId": {"type": "integer"},
                "locator": BROWSER_LOCATOR_SCHEMA,
            },
            "required": ["locator"],
            "additionalProperties": False,
        },
    },
    {
        "name": "browser.find_elements",
        "description": "Find matching elements using a semantic locator and return bounded element metadata.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "tabId": {"type": "integer"},
                "locator": BROWSER_LOCATOR_SCHEMA,
                "limit": {"type": "integer"},
            },
            "required": ["locator"],
            "additionalProperties": False,
        },
    },
    {
        "name": "browser.wait_for",
        "description": "Wait for an element locator to become present, visible, hidden, or gone.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "tabId": {"type": "integer"},
                "locator": BROWSER_LOCATOR_SCHEMA,
                "condition": {
                    "type": "string",
                    "enum": ["present", "visible", "hidden", "gone"],
                },
                "timeoutMs": {"type": "integer"},
                "pollMs": {"type": "integer"},
            },
            "required": ["locator"],
            "additionalProperties": False,
        },
    },
    {
        "name": "browser.get_element_state",
        "description": "Resolve one semantic locator and return rich element state metadata.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "tabId": {"type": "integer"},
                "locator": BROWSER_LOCATOR_SCHEMA,
            },
            "required": ["locator"],
            "additionalProperties": False,
        },
    },
    {
        "name": "browser.select_option",
        "description": "Select an option on a matched select element by value, text, or optionIndex.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "tabId": {"type": "integer"},
                "locator": BROWSER_LOCATOR_SCHEMA,
                "value": {"type": "string"},
                "text": {"type": "string"},
                "optionIndex": {"type": "integer"},
            },
            "required": ["locator"],
            "additionalProperties": False,
        },
    },
]

BRIDGE_TOOL_DEFINITIONS = [
    {
        "name": "browser.session_status",
        "description": "Show bridge session ids, policy, and extension relay health.",
        "inputSchema": {
            "type": "object",
            "properties": {},
            "required": [],
            "additionalProperties": False,
        },
    },
    {
        "name": "browser.session_reset",
        "description": "Reset bridge session state and optionally set policy overrides.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "allowedHosts": {
                    "type": "array",
                    "items": {"type": "string"},
                },
                "approvalMode": {
                    "type": "string",
                    "enum": sorted(APPROVAL_MODES),
                },
            },
            "required": [],
            "additionalProperties": False,
        },
    },
]

TOOL_DEFINITIONS = BRIDGE_TOOL_DEFINITIONS + PROXIED_TOOL_DEFINITIONS
TOOL_INDEX = {tool["name"]: tool for tool in TOOL_DEFINITIONS}
PROXIED_TOOL_NAMES = {tool["name"] for tool in PROXIED_TOOL_DEFINITIONS}

RESOURCE_SESSION_STATUS_URI = "browser-use://session/status"
RESOURCE_SESSION_TABS_URI = "browser-use://session/tabs"
RESOURCE_TAB_CONTENT_TEMPLATE = "browser-use://tab/{tabId}/content{?selector,maxChars}"

RESOURCE_DEFINITIONS = [
    {
        "uri": RESOURCE_SESSION_STATUS_URI,
        "name": "Browser Session Status",
        "description": "Current bridge session, policy, and relay health.",
        "mimeType": "application/json",
    },
    {
        "uri": RESOURCE_SESSION_TABS_URI,
        "name": "Browser Session Tabs",
        "description": "Allowlisted tabs and tab groups visible to the session.",
        "mimeType": "application/json",
    },
]

RESOURCE_TEMPLATE_DEFINITIONS = [
    {
        "uriTemplate": RESOURCE_TAB_CONTENT_TEMPLATE,
        "name": "Tab Content",
        "description": "Read page or element content for a specific tab id.",
        "mimeType": "text/plain",
    }
]


class JsonRpcError(Exception):
    def __init__(self, code: int, message: str, data: Any | None = None) -> None:
        super().__init__(message)
        self.code = code
        self.message = message
        self.data = data


class BrokerError(RuntimeError):
    def __init__(self, message: str, *, status: int | None = None) -> None:
        super().__init__(message)
        self.status = status


def parse_csv_hosts(raw_value: str) -> list[str]:
    if not raw_value:
        return []
    return [part.strip() for part in raw_value.split(",") if part.strip()]


def normalize_hosts(values: list[str]) -> list[str]:
    normalized: list[str] = []
    for raw in values:
        value = str(raw or "").strip().lower().strip(".")
        if not value:
            continue
        if "://" in value:
            try:
                parsed = urlparse(value)
                value = str(parsed.hostname or "").strip().lower().strip(".")
            except Exception:
                value = ""
        if value and value not in normalized:
            normalized.append(value)
    return normalized


def extract_structured_content(payload: Any) -> dict[str, Any]:
    if not isinstance(payload, dict):
        return {}
    structured = payload.get("structuredContent")
    if structured is None:
        structured = payload.get("structured_content")
    if isinstance(structured, dict):
        return structured
    return payload


def summarize_payload(payload: Any) -> str:
    if payload is None:
        return "ok"
    if isinstance(payload, dict):
        if "success" in payload and "tool" in payload:
            status = "ok" if payload.get("success") else "error"
            text = f"{payload.get('tool')} {status}"
            error = payload.get("error")
            if isinstance(error, dict) and error.get("message"):
                text += f": {error.get('message')}"
            return text
        if "session_id" in payload or "sessionId" in payload:
            sid = payload.get("session_id") or payload.get("sessionId")
            rid = payload.get("run_id") or payload.get("runId")
            return f"session={sid} run={rid or '<none>'}"
        keys = [str(key) for key in payload.keys()]
        if not keys:
            return "ok"
        return f"ok ({', '.join(keys[:4])}{', ...' if len(keys) > 4 else ''})"
    return str(payload)


def normalize_tool_result(payload: Any) -> dict[str, Any]:
    if not isinstance(payload, dict):
        text = summarize_payload(payload)
        return {
            "content": [{"type": "text", "text": text}],
            "structuredContent": {"value": payload},
            "isError": False,
        }

    content = payload.get("content")
    if not isinstance(content, list) or not content:
        content = [{"type": "text", "text": summarize_payload(payload)}]

    structured = payload.get("structuredContent")
    if structured is None:
        structured = payload.get("structured_content")
    if structured is None:
        structured = payload

    is_error = bool(payload.get("isError", payload.get("is_error", False)))
    if isinstance(structured, dict) and structured.get("success") is False:
        is_error = True

    return {
        "content": content,
        "structuredContent": structured,
        "isError": is_error,
    }


def tool_error_result(message: str) -> dict[str, Any]:
    return {
        "content": [{"type": "text", "text": message}],
        "structuredContent": {
            "success": False,
            "error": {"code": "bridge_error", "message": message},
        },
        "isError": True,
    }


@dataclass
class BridgeState:
    session_id: str
    run_id: str
    capability_token: str


class BrokerClient:
    def __init__(
        self,
        *,
        base_url: str,
        client_header: str,
        timeout_sec: float,
        verbose: bool = False,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.client_header = client_header
        self.timeout_sec = timeout_sec
        self.verbose = verbose

    def _log(self, message: str) -> None:
        if self.verbose:
            print(f"[browser-use:mcp] {message}", file=sys.stderr)

    def _request(self, method: str, path: str, body: dict[str, Any] | None = None) -> dict[str, Any]:
        if not path.startswith("/"):
            raise BrokerError("Invalid broker path.")
        url = f"{self.base_url}{path}"
        headers = {"X-Assistant-Client": self.client_header}
        data = None
        if body is not None:
            data = json.dumps(body).encode("utf-8")
            headers["Content-Type"] = "application/json"

        request = Request(url, data=data, headers=headers, method=method.upper())
        self._log(f"{method.upper()} {path}")

        try:
            with urlopen(request, timeout=max(1.0, self.timeout_sec)) as response:
                raw = response.read().decode("utf-8")
        except HTTPError as error:
            body_text = error.read().decode("utf-8", errors="replace")
            detail = ""
            try:
                parsed = json.loads(body_text)
                if isinstance(parsed, dict):
                    detail = str(parsed.get("error", "")).strip()
            except json.JSONDecodeError:
                detail = ""
            message = detail or body_text.strip() or f"{error.code} {error.reason}"
            raise BrokerError(f"Broker request failed ({error.code}): {message}", status=error.code) from error
        except URLError as error:
            reason = getattr(error, "reason", error)
            raise BrokerError(
                f"Broker unavailable at {self.base_url}: {reason}"
            ) from error

        try:
            payload = json.loads(raw) if raw else {}
        except json.JSONDecodeError as error:
            raise BrokerError("Broker returned invalid JSON.") from error
        if not isinstance(payload, dict):
            raise BrokerError("Broker returned an invalid response shape.")
        return payload

    def call_browser_tool(self, name: str, arguments: dict[str, Any]) -> dict[str, Any]:
        payload = self._request(
            "POST",
            "/browser/tools/call",
            {
                "name": name,
                "arguments": arguments,
            },
        )
        if not isinstance(payload, dict):
            raise BrokerError("Invalid /browser/tools/call response.")
        return payload

    def browser_health(self) -> dict[str, Any]:
        payload = self._request("GET", "/browser/health", None)
        if not isinstance(payload, dict):
            raise BrokerError("Invalid /browser/health response.")
        return payload


class BrowserBridge:
    def __init__(
        self,
        *,
        client: BrokerClient,
        allowed_hosts: list[str],
        approval_mode: str,
    ) -> None:
        self._client = client
        self._allowed_hosts = normalize_hosts(allowed_hosts) or list(DEFAULT_ALLOWED_HOSTS)
        mode = str(approval_mode).strip().lower()
        self._approval_mode = mode if mode in APPROVAL_MODES else "auto-approve"
        self._state: BridgeState | None = None
        self._lock = threading.Lock()

    def _ensure_extension_connected(self) -> dict[str, Any]:
        health = self._client.browser_health()
        relay = health.get("extension_relay")
        connected_clients = 0
        if isinstance(relay, dict):
            connected_clients = int(relay.get("connected_clients", 0) or 0)
        if connected_clients <= 0:
            raise BrokerError(
                "No extension relay is connected. Load the chrome_secure_panel extension and keep it active."
            )
        return health

    def _ensure_state_locked(self) -> BridgeState:
        if self._state is not None:
            return self._state

        requested_session_id = f"mcp_session_{uuid.uuid4().hex[:10]}"
        session_payload = self._client.call_browser_tool(
            "browser.session_create",
            {
                "session_id": requested_session_id,
                "policy": {
                    "domain_allowlist": list(self._allowed_hosts),
                    "approval_mode": self._approval_mode,
                },
            },
        )
        session_data = extract_structured_content(session_payload)
        session_id = str(
            session_data.get("session_id")
            or session_data.get("sessionId")
            or requested_session_id
        )
        capability_token = str(
            session_data.get("capability_token")
            or session_data.get("capabilityToken")
            or ""
        )
        if not capability_token:
            raise BrokerError("Broker did not return a capability token for browser session.")

        run_id = f"mcp_run_{uuid.uuid4().hex[:10]}"
        self._client.call_browser_tool(
            "browser.run_start",
            {
                "session_id": session_id,
                "run_id": run_id,
                "capability_token": capability_token,
            },
        )

        self._state = BridgeState(
            session_id=session_id,
            run_id=run_id,
            capability_token=capability_token,
        )
        return self._state

    def _cancel_state_locked(self) -> None:
        if self._state is None:
            return
        state = self._state
        try:
            self._client.call_browser_tool(
                "browser.run_cancel",
                {
                    "session_id": state.session_id,
                    "run_id": state.run_id,
                    "capability_token": state.capability_token,
                },
            )
        except Exception:
            pass
        self._state = None

    def _should_rebuild_state(self, error_message: str) -> bool:
        normalized = error_message.lower()
        return any(
            marker in normalized
            for marker in [
                "unknown session",
                "unknown run",
                "invalid capability token",
                "not active",
            ]
        )

    def call_tool(self, tool_name: str, tool_args: dict[str, Any]) -> dict[str, Any]:
        if tool_name not in PROXIED_TOOL_NAMES:
            raise ValueError(f"Unsupported proxied tool: {tool_name}")
        if not isinstance(tool_args, dict):
            raise ValueError("Tool arguments must be an object.")

        self._ensure_extension_connected()

        for attempt in range(2):
            with self._lock:
                state = self._ensure_state_locked()
                envelope_args = {
                    "session_id": state.session_id,
                    "run_id": state.run_id,
                    "capability_token": state.capability_token,
                    "tool_call_id": f"mcp_tool_{uuid.uuid4().hex[:10]}",
                    "args": tool_args,
                }
            try:
                return self._client.call_browser_tool(tool_name, envelope_args)
            except BrokerError as error:
                if attempt == 0 and self._should_rebuild_state(str(error)):
                    with self._lock:
                        self._cancel_state_locked()
                    continue
                raise

        raise BrokerError("Unable to execute browser tool after session retry.")

    def status(self) -> dict[str, Any]:
        with self._lock:
            state = self._state
            policy = {
                "allowed_hosts": list(self._allowed_hosts),
                "approval_mode": self._approval_mode,
            }
            session = {
                "active": state is not None,
                "session_id": state.session_id if state else None,
                "run_id": state.run_id if state else None,
            }

        health: dict[str, Any] | None = None
        health_error: str | None = None
        try:
            health = self._client.browser_health()
        except Exception as error:
            health_error = str(error)

        return {
            "session": session,
            "policy": policy,
            "health": health,
            "health_error": health_error,
        }

    def reset(
        self,
        *,
        allowed_hosts: list[str] | None = None,
        approval_mode: str | None = None,
    ) -> dict[str, Any]:
        with self._lock:
            self._cancel_state_locked()
            if allowed_hosts is not None:
                normalized_hosts = normalize_hosts(allowed_hosts)
                if not normalized_hosts:
                    raise ValueError("allowedHosts must include at least one valid host.")
                self._allowed_hosts = normalized_hosts
            if approval_mode is not None:
                normalized_mode = str(approval_mode).strip().lower()
                if normalized_mode not in APPROVAL_MODES:
                    raise ValueError(
                        "approvalMode must be one of: auto-approve, manual, auto-deny."
                    )
                self._approval_mode = normalized_mode
            self._ensure_state_locked()

        return self.status()


class BrowserUseMcpServer:
    def __init__(self, *, bridge: BrowserBridge, verbose: bool = False) -> None:
        self._bridge = bridge
        self._verbose = verbose
        self._initialized = False
        self._protocol_version = DEFAULT_PROTOCOL_VERSION

    def _log(self, message: str) -> None:
        if self._verbose:
            print(f"[browser-use:mcp] {message}", file=sys.stderr)

    def dispatch(self, message: Any) -> dict[str, Any] | None:
        if not isinstance(message, dict):
            return self._error_response(None, JsonRpcError(-32600, "Invalid request payload."))

        request_id = message.get("id")
        is_request = "id" in message
        if message.get("jsonrpc") != "2.0":
            if is_request:
                return self._error_response(request_id, JsonRpcError(-32600, "jsonrpc must be '2.0'."))
            return None

        method = message.get("method")
        if not isinstance(method, str) or not method:
            if is_request:
                return self._error_response(request_id, JsonRpcError(-32600, "method is required."))
            return None

        params = message.get("params", {})
        if params is None:
            params = {}
        if not isinstance(params, dict):
            if is_request:
                return self._error_response(request_id, JsonRpcError(-32602, "params must be an object."))
            return None

        if not is_request:
            self._handle_notification(method, params)
            return None

        try:
            result = self._handle_request(method, params)
            return {"jsonrpc": "2.0", "id": request_id, "result": result}
        except JsonRpcError as error:
            return self._error_response(request_id, error)
        except Exception as error:
            return self._error_response(
                request_id,
                JsonRpcError(-32603, f"Internal error: {error}"),
            )

    def _error_response(self, request_id: Any, error: JsonRpcError) -> dict[str, Any]:
        payload = {
            "jsonrpc": "2.0",
            "id": request_id,
            "error": {
                "code": error.code,
                "message": error.message,
            },
        }
        if error.data is not None:
            payload["error"]["data"] = error.data
        return payload

    def _handle_notification(self, method: str, _params: dict[str, Any]) -> None:
        if method == "notifications/initialized":
            self._initialized = True
            self._log("Client initialized.")

    def _handle_request(self, method: str, params: dict[str, Any]) -> dict[str, Any]:
        if method == "initialize":
            return self._handle_initialize(params)
        if method == "ping":
            return {}
        if not self._initialized:
            raise JsonRpcError(-32002, "Server not initialized.")
        if method == "tools/list":
            return {"tools": TOOL_DEFINITIONS}
        if method == "tools/call":
            return self._handle_tools_call(params)
        if method == "resources/list":
            return {"resources": RESOURCE_DEFINITIONS}
        if method == "resources/templates/list":
            return {"resourceTemplates": RESOURCE_TEMPLATE_DEFINITIONS}
        if method == "resources/read":
            return self._handle_resources_read(params)
        raise JsonRpcError(-32601, f"Method not found: {method}")

    def _handle_initialize(self, params: dict[str, Any]) -> dict[str, Any]:
        client_protocol = params.get("protocolVersion")
        if isinstance(client_protocol, str) and client_protocol.strip():
            self._protocol_version = client_protocol.strip()
        self._initialized = True
        return {
            "protocolVersion": self._protocol_version,
            "capabilities": {
                "tools": {
                    "listChanged": False,
                },
                "resources": {
                    "listChanged": False,
                },
            },
            "serverInfo": {
                "name": SERVER_NAME,
                "version": SERVER_VERSION,
            },
        }

    def _resource_payload(self, uri: str, value: Any, *, mime_type: str) -> dict[str, Any]:
        text = json.dumps(value, ensure_ascii=True, indent=2) if mime_type == "application/json" else str(value)
        return {
            "contents": [
                {
                    "uri": uri,
                    "mimeType": mime_type,
                    "text": text,
                }
            ]
        }

    def _parse_tab_content_uri(self, uri: str) -> tuple[int, str | None, int | None] | None:
        parsed = urlparse(uri)
        if parsed.scheme != "browser-use" or parsed.netloc != "tab":
            return None
        parts = [part for part in parsed.path.split("/") if part]
        if len(parts) != 2 or parts[1] != "content":
            return None
        try:
            tab_id = int(parts[0])
        except ValueError:
            raise JsonRpcError(-32602, "Tab content resource URI must include an integer tab id.")

        query = parse_qs(parsed.query, keep_blank_values=False)
        selector = query.get("selector", [None])[0]
        max_chars_raw = query.get("maxChars", [None])[0]
        max_chars: int | None = None
        if max_chars_raw is not None:
            try:
                max_chars = int(max_chars_raw)
            except ValueError:
                raise JsonRpcError(-32602, "maxChars query value must be an integer.")
        return tab_id, selector, max_chars

    def _handle_resources_read(self, params: dict[str, Any]) -> dict[str, Any]:
        uri = str(params.get("uri", "")).strip()
        if not uri:
            raise JsonRpcError(-32602, "Resource uri is required.")

        if uri == RESOURCE_SESSION_STATUS_URI:
            status = self._bridge.status()
            return self._resource_payload(uri, status, mime_type="application/json")

        if uri == RESOURCE_SESSION_TABS_URI:
            try:
                payload = self._bridge.call_tool("browser.describe_session_tabs", {})
            except Exception as error:
                raise JsonRpcError(-32000, f"Failed to read session tabs resource: {error}")
            structured = extract_structured_content(payload)
            return self._resource_payload(uri, structured, mime_type="application/json")

        tab_resource = self._parse_tab_content_uri(uri)
        if tab_resource is not None:
            tab_id, selector, max_chars = tab_resource
            args: dict[str, Any] = {"tabId": tab_id}
            if selector:
                args["selector"] = selector
            if max_chars is not None:
                args["maxChars"] = max_chars
            try:
                payload = self._bridge.call_tool("browser.get_content", args)
            except Exception as error:
                raise JsonRpcError(-32000, f"Failed to read tab content resource: {error}")
            structured = extract_structured_content(payload)
            if isinstance(structured, dict):
                if "text" in structured:
                    return self._resource_payload(uri, structured.get("text", ""), mime_type="text/plain")
                if "content" in structured:
                    return self._resource_payload(uri, structured.get("content", ""), mime_type="text/plain")
            return self._resource_payload(uri, structured, mime_type="application/json")

        raise JsonRpcError(-32602, f"Unsupported resource uri: {uri}")

    def _handle_tools_call(self, params: dict[str, Any]) -> dict[str, Any]:
        name = str(params.get("name", "")).strip()
        if not name:
            raise JsonRpcError(-32602, "Tool name is required.")
        if name not in TOOL_INDEX:
            raise JsonRpcError(-32602, f"Unsupported tool: {name}")

        arguments = params.get("arguments", {})
        if arguments is None:
            arguments = {}
        if not isinstance(arguments, dict):
            raise JsonRpcError(-32602, "Tool arguments must be an object.")

        if name == "browser.session_status":
            status = self._bridge.status()
            return {
                "content": [{"type": "text", "text": summarize_payload(status)}],
                "structuredContent": status,
                "isError": False,
            }
        if name == "browser.session_reset":
            allowed_hosts_raw = arguments.get("allowedHosts")
            allowed_hosts: list[str] | None
            if allowed_hosts_raw is None:
                allowed_hosts = None
            elif isinstance(allowed_hosts_raw, list):
                allowed_hosts = [str(item) for item in allowed_hosts_raw]
            else:
                raise JsonRpcError(-32602, "allowedHosts must be an array of host strings.")

            approval_mode_raw = arguments.get("approvalMode")
            approval_mode = str(approval_mode_raw) if approval_mode_raw is not None else None
            try:
                status = self._bridge.reset(
                    allowed_hosts=allowed_hosts,
                    approval_mode=approval_mode,
                )
            except Exception as error:
                return tool_error_result(str(error))
            return {
                "content": [{"type": "text", "text": summarize_payload(status)}],
                "structuredContent": status,
                "isError": False,
            }

        try:
            payload = self._bridge.call_tool(name, arguments)
        except Exception as error:
            return tool_error_result(str(error))
        return normalize_tool_result(payload)


def read_mcp_message(stream: Any) -> tuple[dict[str, Any] | None, str]:
    line = stream.readline()
    if not line:
        return None, "headers"

    # Some MCP clients use newline-delimited JSON-RPC instead of Content-Length framing.
    stripped = line.strip()
    if stripped.startswith(b"{") or stripped.startswith(b"["):
        try:
            payload = json.loads(stripped.decode("utf-8"))
        except json.JSONDecodeError as error:
            raise JsonRpcError(-32700, f"Invalid JSON payload: {error}") from error
        if not isinstance(payload, dict):
            raise JsonRpcError(-32600, "JSON-RPC payload must be an object.")
        return payload, "jsonl"

    headers: dict[str, str] = {}
    while True:
        if line in (b"\r\n", b"\n"):
            break
        try:
            decoded = line.decode("ascii")
        except UnicodeDecodeError as error:
            raise JsonRpcError(-32700, f"Invalid header encoding: {error}") from error
        if ":" in decoded:
            key, value = decoded.split(":", 1)
            headers[key.strip().lower()] = value.strip()
        line = stream.readline()
        if not line:
            return None, "headers"

    content_length_raw = headers.get("content-length")
    if content_length_raw is None:
        raise JsonRpcError(-32700, "Missing Content-Length header.")

    try:
        content_length = int(content_length_raw)
    except ValueError as error:
        raise JsonRpcError(-32700, "Invalid Content-Length header.") from error
    if content_length < 0:
        raise JsonRpcError(-32700, "Content-Length must be non-negative.")

    payload_bytes = stream.read(content_length)
    if payload_bytes is None or len(payload_bytes) != content_length:
        return None, "headers"

    try:
        payload = json.loads(payload_bytes.decode("utf-8"))
    except json.JSONDecodeError as error:
        raise JsonRpcError(-32700, f"Invalid JSON payload: {error}") from error

    if not isinstance(payload, dict):
        raise JsonRpcError(-32600, "JSON-RPC payload must be an object.")
    return payload, "headers"


def write_mcp_message(stream: Any, payload: dict[str, Any], transport: str = "headers") -> None:
    body = json.dumps(payload, ensure_ascii=True, separators=(",", ":")).encode("utf-8")
    if transport == "jsonl":
        stream.write(body + b"\n")
        stream.flush()
        return
    header = f"Content-Length: {len(body)}\r\n\r\n".encode("ascii")
    stream.write(header)
    stream.write(body)
    stream.flush()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="MCP bridge exposing browser tools through the local broker and extension relay."
    )
    parser.add_argument(
        "--broker-url",
        default=os.environ.get("MCP_BROWSER_USE_BROKER_URL", DEFAULT_BROKER_URL),
        help=f"Broker base URL (default: {DEFAULT_BROKER_URL})",
    )
    parser.add_argument(
        "--client-header",
        default=os.environ.get("MCP_BROWSER_USE_CLIENT_HEADER", DEFAULT_CLIENT_HEADER),
        help=f"Value for X-Assistant-Client header (default: {DEFAULT_CLIENT_HEADER})",
    )
    parser.add_argument(
        "--timeout-sec",
        type=float,
        default=float(os.environ.get("MCP_BROWSER_USE_TIMEOUT_SEC", str(DEFAULT_TIMEOUT_SEC))),
        help=f"HTTP timeout in seconds (default: {DEFAULT_TIMEOUT_SEC})",
    )
    parser.add_argument(
        "--allowed-hosts",
        default=os.environ.get(
            "MCP_BROWSER_USE_ALLOWED_HOSTS",
            ",".join(DEFAULT_ALLOWED_HOSTS),
        ),
        help="Comma-separated browser host allowlist for created sessions.",
    )
    parser.add_argument(
        "--allow-host",
        action="append",
        default=[],
        help="Additional allowlisted host. Repeatable.",
    )
    parser.add_argument(
        "--approval-mode",
        default=os.environ.get("MCP_BROWSER_USE_APPROVAL_MODE", "auto-approve"),
        choices=sorted(APPROVAL_MODES),
        help="Browser policy approval mode for created sessions.",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Write bridge logs to stderr.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    hosts = parse_csv_hosts(args.allowed_hosts)
    hosts.extend(args.allow_host)
    allowed_hosts = normalize_hosts(hosts) or list(DEFAULT_ALLOWED_HOSTS)

    client = BrokerClient(
        base_url=str(args.broker_url),
        client_header=str(args.client_header),
        timeout_sec=max(1.0, float(args.timeout_sec)),
        verbose=bool(args.verbose),
    )
    bridge = BrowserBridge(
        client=client,
        allowed_hosts=allowed_hosts,
        approval_mode=str(args.approval_mode),
    )
    server = BrowserUseMcpServer(bridge=bridge, verbose=bool(args.verbose))

    stdin = sys.stdin.buffer
    stdout = sys.stdout.buffer
    transport = "headers"

    while True:
        try:
            message, transport = read_mcp_message(stdin)
        except JsonRpcError as error:
            write_mcp_message(
                stdout,
                {
                    "jsonrpc": "2.0",
                    "id": None,
                    "error": {
                        "code": error.code,
                        "message": error.message,
                    },
                },
                transport=transport,
            )
            continue

        if message is None:
            break

        response = server.dispatch(message)
        if response is not None:
            write_mcp_message(stdout, response, transport=transport)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
