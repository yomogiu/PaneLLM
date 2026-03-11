# browser-use MCP bridge

This MCP server exposes browser tools backed by the existing local broker + extension relay.

It does not control Chrome directly. Tool calls flow as:

`MCP client -> this server (stdio) -> broker /browser/tools/call -> extension command loop -> Chrome APIs`

## Prerequisites

1. Start broker:
   - `python3 broker/local_broker.py`
2. Load and keep running `chrome_secure_panel/` extension (so relay stays connected).

## Run

```bash
python3 tools/mcp-servers/browser-use/server.py
```

## Environment

- `MCP_BROWSER_USE_BROKER_URL` (default: `http://127.0.0.1:7777`)
- `MCP_BROWSER_USE_CLIENT_HEADER` (default: `chrome-sidepanel-v1`)
- `MCP_BROWSER_USE_TIMEOUT_SEC` (default: `30`)
- `MCP_BROWSER_USE_ALLOWED_HOSTS` (default: `127.0.0.1,localhost,google.com,www.google.com,arxiv.org,www.arxiv.org`)
- `MCP_BROWSER_USE_APPROVAL_MODE` (default: `auto-approve`, also supports `manual` and `auto-deny`)

CLI flags are also available (`--broker-url`, `--allowed-hosts`, `--allow-host`, `--approval-mode`).

## Exposed tools

Bridge/session utilities:

- `browser.session_status`
- `browser.session_reset`

Browser action tools:

- `browser.navigate`
- `browser.open_tab`
- `browser.get_tabs`
- `browser.describe_session_tabs`
- `browser.switch_tab`
- `browser.focus_tab`
- `browser.close_tab`
- `browser.group_tabs`
- `browser.click`
- `browser.type`
- `browser.press_key`
- `browser.scroll`
- `browser.get_content`

The bridge auto-creates broker browser sessions/runs and injects capability tokens for browser actions.

## Example MCP config snippet

```json
{
  "mcpServers": {
    "browser-use": {
      "command": "python3",
      "args": ["tools/mcp-servers/browser-use/server.py"]
    }
  }
}
```
