# Secure Assistant Panel

Chrome side panel for the local broker.

## Load in Chrome

1. Open `chrome://extensions`
2. Enable **Developer mode**
3. Click **Load unpacked**
4. Select `chrome_secure_panel/`

## Security defaults

- No extension `localStorage`
- UI state is runtime-only
- Host allowlist policy persists in extension `chrome.storage.local`
- Broker communication is localhost-only and header-gated
- Page context toggle defaults to off
- Page context capture requires an allowlisted host
- High-risk prompts require confirmation before sending
- Saved conversations are persisted by the broker, not the extension

## Browser relay

The background worker registers with the broker and executes browser commands through:

- `POST /extension/register`
- `GET /extension/next`
- `POST /extension/result`

Host allowlists are enforced in the extension before navigation or interaction actions run.

## Codex modes

The panel supports three Codex shapes:

- `Responses` mode: interactive run timeline, polling, approvals, cancel, and reload-safe run replay
- `CLI` mode: multi-turn chat through the locally installed `codex` binary using its ChatGPT login
- `Legacy` mode: falls back to the broker’s deprecated `/route` Codex request path

The panel automatically uses:

- `Responses` mode when broker health reports `codex_backend: responses_ready`
- `CLI` mode when broker health reports `codex_backend: cli_ready`

## Runtime message types

Background worker RPC now supports:

- `assistant.health`
- `assistant.query`
- `assistant.codex.run.start`
- `assistant.codex.run.events`
- `assistant.codex.run.approval`
- `assistant.codex.run.cancel`
- `assistant.history.list`
- `assistant.history.get`
- `assistant.history.delete`
- `assistant.history.rewrite`
- `assistant.models.get`
- `assistant.mlx.status`
- `assistant.mlx.config`
- `assistant.mlx.session.start`
- `assistant.mlx.session.stop`
- `assistant.mlx.session.restart`
- `assistant.mlx.adapters.list`
- `assistant.mlx.adapters.load`
- `assistant.mlx.adapters.unload`
- `assistant.tools.page_hosts.get`
- `assistant.tools.page_hosts.allow`
- `assistant.tools.page_hosts.remove_allow`
- `assistant.tools.page_hosts.allow_active_tab`
- `assistant.tools.page_hosts.active_tab`
- `assistant.browser.tool.call`

## Expanding website allowlists

Use the **Tools** tab in the side panel to manage the runtime allowlist.

If you need a permanent default host in source control, update:

- `manifest.json` -> `host_permissions`
- `background.js` -> `DEFAULT_ALLOWED_PAGE_HOSTS`

After edits, reload the extension in `chrome://extensions`.
