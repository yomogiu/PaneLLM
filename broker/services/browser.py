from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable


@dataclass(frozen=True)
class BrowserServiceHandlers:
    handle_browser_config_get: Callable[[], dict[str, Any]]
    handle_browser_config_post: Callable[[dict[str, Any]], dict[str, Any]]
    handle_browser_profiles_get: Callable[[], dict[str, Any]]
    handle_browser_profiles_post: Callable[[dict[str, Any]], dict[str, Any]]
    handle_browser_tool_call: Callable[[dict[str, Any]], dict[str, Any]]


def build_browser_service_handlers(
    *,
    browser_config: Any,
    browser_profiles: Any,
    browser_automation: Any,
    extension_relay: Any,
    config: Any,
    browser_tool_names: set[str],
    browser_tool_result_func: Callable[[Any], dict[str, Any]],
) -> BrowserServiceHandlers:
    def handle_browser_config_get() -> dict[str, Any]:
        return {"ok": True, "browser": browser_config.config()}

    def handle_browser_config_post(data: dict[str, Any]) -> dict[str, Any]:
        updates = dict(data) if isinstance(data, dict) else {}
        payload = browser_config.update_config(updates)
        return {"ok": True, "browser": payload}

    def handle_browser_profiles_get() -> dict[str, Any]:
        return {"ok": True, "browser_profiles": browser_profiles.state()}

    def handle_browser_profiles_post(data: dict[str, Any]) -> dict[str, Any]:
        updates = dict(data) if isinstance(data, dict) else {}
        payload = browser_profiles.replace_state(updates)
        return {"ok": True, "browser_profiles": payload}

    def handle_browser_tool_call(data: dict[str, Any]) -> dict[str, Any]:
        tool_name = str(data.get("name", "")).strip()
        args = data.get("arguments", {})
        if not tool_name:
            raise ValueError("Tool name is required.")
        if tool_name not in browser_tool_names:
            raise ValueError(f"Unsupported browser tool: {tool_name}")
        if not isinstance(args, dict):
            raise ValueError("Tool arguments must be an object.")

        if tool_name == "browser.session_create":
            return browser_tool_result_func(browser_automation.session_create(args))
        if tool_name == "browser.run_start":
            return browser_tool_result_func(browser_automation.run_start(args))
        if tool_name == "browser.run_cancel":
            return browser_tool_result_func(browser_automation.run_cancel(args))
        if tool_name == "browser.approvals_list":
            return browser_tool_result_func(browser_automation.approvals_list(args))
        if tool_name == "browser.events_replay":
            return browser_tool_result_func(browser_automation.events_replay(args))
        if tool_name == "browser.approve":
            return browser_tool_result_func(browser_automation.approve(args))

        envelope = browser_automation.execute_tool(
            tool_name=tool_name,
            args=args,
            relay=extension_relay,
            timeout_sec=config.browser_command_timeout_sec,
        )
        return browser_tool_result_func(envelope)

    return BrowserServiceHandlers(
        handle_browser_config_get=handle_browser_config_get,
        handle_browser_config_post=handle_browser_config_post,
        handle_browser_profiles_get=handle_browser_profiles_get,
        handle_browser_profiles_post=handle_browser_profiles_post,
        handle_browser_tool_call=handle_browser_tool_call,
    )
