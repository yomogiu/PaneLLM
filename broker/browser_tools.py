from __future__ import annotations

from copy import deepcopy
from typing import Any


BROWSER_GET_CONTENT_MODE_NAVIGATION = "navigation"
BROWSER_GET_CONTENT_MODE_RAW_HTML = "raw_html"

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

INTERNAL_BROWSER_TOOL_SPECS: tuple[dict[str, Any], ...] = (
    {
        "name": "browser.navigate",
        "method": "navigate",
        "approval": "manual",
        "description": "Navigate the active tab (or specified tab) to an absolute URL.",
        "parameters": {
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
        "method": "open_tab",
        "approval": "manual",
        "description": "Open a new tab on an allowlisted URL.",
        "parameters": {
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
        "method": "get_tabs",
        "approval": "auto",
        "description": "List allowlisted tabs in the current window.",
        "parameters": {
            "type": "object",
            "properties": {},
            "required": [],
            "additionalProperties": False,
        },
    },
    {
        "name": "browser.describe_session_tabs",
        "method": "describe_session_tabs",
        "approval": "auto",
        "description": "Describe allowlisted tabs and groups in the current window.",
        "parameters": {
            "type": "object",
            "properties": {},
            "required": [],
            "additionalProperties": False,
        },
    },
    {
        "name": "browser.switch_tab",
        "method": "switch_tab",
        "approval": "auto",
        "description": "Activate an allowlisted tab by id.",
        "parameters": {
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
        "method": "focus_tab",
        "approval": "auto",
        "description": "Focus an allowlisted tab by id.",
        "parameters": {
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
        "method": "group_tabs",
        "approval": "manual",
        "description": "Group allowlisted tabs together and optionally label the group.",
        "parameters": {
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
        "name": "browser.close_tab",
        "method": "close_tab",
        "approval": "manual",
        "description": "Close an allowlisted tab by id.",
        "parameters": {
            "type": "object",
            "properties": {
                "tabId": {"type": "integer"},
            },
            "required": ["tabId"],
            "additionalProperties": False,
        },
    },
    {
        "name": "browser.click",
        "method": "click",
        "approval": "manual",
        "description": "Click an element found by CSS selector in the target tab.",
        "parameters": {
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
        "method": "type",
        "approval": "manual",
        "description": "Type text into an input, textarea, or editable element matched by CSS selector.",
        "parameters": {
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
        "method": "press_key",
        "approval": "manual",
        "description": "Send a keyboard key press to the active element in the target tab.",
        "parameters": {
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
        "method": "scroll",
        "approval": "auto",
        "description": "Scroll the page or a matched element in the target tab.",
        "parameters": {
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
        "name": "browser.highlight",
        "method": "highlight",
        "approval": "auto",
        "description": "Temporarily highlight a relevant section on the page and optionally scroll it into view.",
        "parameters": {
            "type": "object",
            "properties": {
                "tabId": {"type": "integer"},
                "locator": BROWSER_LOCATOR_SCHEMA,
                "text": {"type": "string"},
                "scroll": {"type": "boolean"},
                "durationMs": {"type": "integer"},
            },
            "required": [],
            "additionalProperties": False,
        },
    },
    {
        "name": "browser.get_content",
        "method": "get_content",
        "approval": "auto",
        "description": (
            "Get a navigation-focused page digest from the target tab. "
            "Use mode=raw_html only when raw HTML is explicitly required."
        ),
        "parameters": {
            "type": "object",
            "properties": {
                "tabId": {"type": "integer"},
                "selector": {"type": "string"},
                "mode": {
                    "type": "string",
                    "enum": [
                        BROWSER_GET_CONTENT_MODE_NAVIGATION,
                        BROWSER_GET_CONTENT_MODE_RAW_HTML,
                    ],
                },
                "maxChars": {"type": "integer"},
                "maxItems": {"type": "integer"},
            },
            "required": [],
            "additionalProperties": False,
        },
    },
    {
        "name": "browser.find_one",
        "method": "find_one",
        "approval": "auto",
        "description": "Find one element using a semantic locator and return element metadata.",
        "parameters": {
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
        "method": "find_elements",
        "approval": "auto",
        "description": "Find matching elements using a semantic locator and return bounded element metadata.",
        "parameters": {
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
        "method": "wait_for",
        "approval": "auto",
        "description": "Wait for an element locator to become present, visible, hidden, or gone.",
        "parameters": {
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
        "method": "get_element_state",
        "approval": "auto",
        "description": "Resolve one semantic locator and return rich element state metadata.",
        "parameters": {
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
        "method": "select_option",
        "approval": "manual",
        "description": "Select an option on a matched <select> by value, text, or optionIndex.",
        "parameters": {
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
)

LEGACY_MODEL_BROWSER_TOOL_SPECS: tuple[dict[str, Any], ...] = (
    {
        "name": "browser.navigate",
        "description": "Navigate the current tab to an allowlisted absolute URL. Open a new tab only when the user explicitly asks for one or preserving the current page matters.",
        "parameters": {
            "type": "object",
            "properties": {
                "url": {"type": "string"},
                "newTab": {"type": "boolean"},
                "tabId": {"type": "integer"},
            },
            "required": ["url"],
            "additionalProperties": False,
        },
    },
    {
        "name": "browser.tabs",
        "description": "List, activate, close, or group allowlisted tabs.",
        "parameters": {
            "type": "object",
            "properties": {
                "action": {
                    "type": "string",
                    "enum": ["list", "activate", "close", "group"],
                },
                "tabId": {"type": "integer"},
                "tabIds": {
                    "type": "array",
                    "items": {"type": "integer"},
                },
                "groupName": {"type": "string"},
                "color": {"type": "string"},
                "collapsed": {"type": "boolean"},
            },
            "required": ["action"],
            "additionalProperties": False,
        },
    },
    {
        "name": "browser.read",
        "description": "Read the current page, inspect an element, or find matching elements on the page.",
        "parameters": {
            "type": "object",
            "properties": {
                "action": {
                    "type": "string",
                    "enum": ["page_digest", "raw_html", "find", "state"],
                },
                "tabId": {"type": "integer"},
                "selector": {"type": "string"},
                "locator": BROWSER_LOCATOR_SCHEMA,
                "limit": {"type": "integer"},
                "maxChars": {"type": "integer"},
                "maxItems": {"type": "integer"},
            },
            "required": ["action"],
            "additionalProperties": False,
        },
    },
    {
        "name": "browser.interact",
        "description": "Interact with the page by clicking, typing, pressing keys, scrolling, highlighting, waiting, or selecting an option.",
        "parameters": {
            "type": "object",
            "properties": {
                "action": {
                    "type": "string",
                    "enum": [
                        "click",
                        "type",
                        "press_key",
                        "scroll",
                        "highlight",
                        "wait_for",
                        "select_option",
                    ],
                },
                "tabId": {"type": "integer"},
                "selector": {"type": "string"},
                "locator": BROWSER_LOCATOR_SCHEMA,
                "text": {"type": "string"},
                "clear": {"type": "boolean"},
                "key": {"type": "string"},
                "modifiers": {
                    "type": "array",
                    "items": {"type": "string"},
                },
                "repeat": {"type": "integer"},
                "delayMs": {"type": "integer"},
                "deltaX": {"type": "number"},
                "deltaY": {"type": "number"},
                "condition": {
                    "type": "string",
                    "enum": ["present", "visible", "hidden", "gone"],
                },
                "timeoutMs": {"type": "integer"},
                "pollMs": {"type": "integer"},
                "value": {"type": "string"},
                "optionIndex": {"type": "integer"},
                "scroll": {"type": "boolean"},
                "durationMs": {"type": "integer"},
            },
            "required": ["action"],
            "additionalProperties": False,
        },
    },
)


def _copy(value: Any) -> Any:
    return deepcopy(value)


def _model_spec_from_internal(spec: dict[str, Any]) -> dict[str, Any]:
    return {
        "name": str(spec["name"]),
        "description": str(spec["description"]),
        "parameters": _copy(spec["parameters"]),
    }


MODEL_BROWSER_TOOL_SPECS: tuple[dict[str, Any], ...] = tuple(
    _model_spec_from_internal(spec) for spec in INTERNAL_BROWSER_TOOL_SPECS
)


def proxied_browser_tool_specs() -> list[dict[str, Any]]:
    return [_copy(spec) for spec in INTERNAL_BROWSER_TOOL_SPECS]


PROXIED_BROWSER_TOOL_NAMES = {spec["name"] for spec in INTERNAL_BROWSER_TOOL_SPECS}
BROWSER_COMMAND_METHODS = {spec["name"]: spec["method"] for spec in INTERNAL_BROWSER_TOOL_SPECS}
MODEL_BROWSER_TOOL_NAMES = {spec["name"] for spec in MODEL_BROWSER_TOOL_SPECS}
LEGACY_MODEL_BROWSER_TOOL_NAMES = {spec["name"] for spec in LEGACY_MODEL_BROWSER_TOOL_SPECS}
INTERNAL_AUTO_APPROVE_TOOL_NAMES = {
    spec["name"] for spec in INTERNAL_BROWSER_TOOL_SPECS if spec["approval"] == "auto"
}
INTERNAL_MANUAL_APPROVE_TOOL_NAMES = {
    spec["name"] for spec in INTERNAL_BROWSER_TOOL_SPECS if spec["approval"] == "manual"
}


def build_openai_function_tools(
    specs: list[dict[str, Any]] | tuple[dict[str, Any], ...] | None = None,
) -> list[dict[str, Any]]:
    source = specs or MODEL_BROWSER_TOOL_SPECS
    return [
        {
            "type": "function",
            "function": {
                "name": str(spec["name"]),
                "description": str(spec["description"]),
                "parameters": _copy(spec["parameters"]),
            },
        }
        for spec in source
    ]


def build_responses_function_tools(
    tools: list[dict[str, Any]] | None = None,
) -> list[dict[str, Any]]:
    source = tools or build_openai_function_tools()
    response_tools: list[dict[str, Any]] = []
    for tool in source:
        function = tool.get("function") or {}
        response_tools.append(
            {
                "type": "function",
                "name": str(function.get("name", "")),
                "description": str(function.get("description", "")),
                "parameters": _copy(function.get("parameters") or {}),
                "strict": True,
            }
        )
    return response_tools


def build_mcp_tool_definitions(
    specs: list[dict[str, Any]] | tuple[dict[str, Any], ...] | None = None,
) -> list[dict[str, Any]]:
    source = specs or INTERNAL_BROWSER_TOOL_SPECS
    return [
        {
            "name": str(spec["name"]),
            "description": str(spec["description"]),
            "inputSchema": _copy(spec["parameters"]),
        }
        for spec in source
    ]


LLAMA_BROWSER_TOOLS = build_openai_function_tools(MODEL_BROWSER_TOOL_SPECS)
CODEX_BROWSER_TOOLS = build_responses_function_tools(LLAMA_BROWSER_TOOLS)
PROXIED_TOOL_DEFINITIONS = build_mcp_tool_definitions(INTERNAL_BROWSER_TOOL_SPECS)
LEGACY_LLAMA_BROWSER_TOOLS = build_openai_function_tools(LEGACY_MODEL_BROWSER_TOOL_SPECS)
LEGACY_CODEX_BROWSER_TOOLS = build_responses_function_tools(LEGACY_LLAMA_BROWSER_TOOLS)
