# AGENTS.md

## Purpose
This repository contains a local-first assistant stack:
- A localhost broker (`broker/local_broker.py`) that routes requests to llama.cpp or an optional Codex command.
- A Chrome side panel extension (`chrome_secure_panel/`) that collects user prompts, optional page context, and conversation history.
- A standalone CLI tool loop (`llama_browser_tool_loop.py`) for private ask/tool/agent flows over a Unix socket bridge.

Use this guide when making code changes so behavior, security constraints, and cross-file contracts stay aligned.

## Repository Map
- `broker/local_broker.py`: single-file HTTP API server and on-disk conversation store.
- `broker/README.md`: broker setup, environment variables, and API contract notes.
- `chrome_secure_panel/manifest.json`: MV3 permissions, host permissions, CSP, side panel entrypoint.
- `chrome_secure_panel/background.js`: extension service worker, broker RPC calls, allowlist checks, risk detection, page capture.
- `chrome_secure_panel/sidepanel.js`: UI state, prompt send flow, confirmation flow, conversation history rendering.
- `chrome_secure_panel/sidepanel.html` + `sidepanel.css`: side panel markup and styles.
- `llama_browser_tool_loop.py`: CLI that talks to OpenAI-compatible chat endpoint and a local browser bridge socket.

## Local Runbook
1. Start broker:
   - `python3 broker/local_broker.py`
2. Load extension:
   - Open `chrome://extensions`
   - Enable Developer mode
   - Load unpacked `chrome_secure_panel/`
3. Optional CLI flow:
   - `python3 llama_browser_tool_loop.py --help`
   - `python3 llama_browser_tool_loop.py ask "Your question"`

## Broker Contract (Do Not Drift)
- Broker listens on `BROKER_HOST:BROKER_PORT` (defaults `127.0.0.1:7777`).
- Trust checks in `_ensure_trusted()` are mandatory:
  - Loopback clients only.
  - Required header: `X-Assistant-Client: chrome-sidepanel-v1`.
  - If `Origin` is present, it must be `chrome-extension://...`.
- Main endpoints:
  - `GET /health`
  - `POST /route`
  - `GET /conversations`
  - `GET /conversations/<id>`
  - `DELETE /conversations/<id>`
- Conversation files are persisted under `broker/.data/conversations/*.json` with restrictive permissions (best effort).

## Security Invariants
- Keep extension storage stateless (no persistent extension `localStorage` for chat data).
- Keep broker localhost-only and header-gated.
- Keep page automation and context capture host-allowlisted:
  - `ALLOWED_PAGE_HOSTS` in `chrome_secure_panel/background.js`.
  - Default should stay tight (`127.0.0.1`, `localhost`) unless a change is explicitly required.
- High-risk prompt detection/confirmation is enforced end-to-end:
  - Regex exists in both broker and background worker.
  - If you change one, update both and verify confirmation flow.

## Cross-File Change Rules
- If request/response fields change for `assistant.query` or `/route`, update:
  - `chrome_secure_panel/sidepanel.js`
  - `chrome_secure_panel/background.js`
  - `broker/local_broker.py`
  - `broker/README.md`
- If conversation schema changes, update:
  - `ConversationStore` logic in broker
  - Side panel history/message rendering assumptions
- If CSP/permissions change, verify `manifest.json` stays minimal and still permits broker connectivity.

## Environment Variables
Broker configuration is environment-driven. Keep these names stable unless migration is planned:
- `BROKER_HOST`, `BROKER_PORT`, `BROKER_DATA_DIR`
- `LLAMA_URL`, `LLAMA_MODEL`, `LLAMA_API_KEY`
- `CODEX_COMMAND`, `CODEX_TIMEOUT_SEC`
- `BROKER_MAX_CONTEXT_MESSAGES`, `BROKER_MAX_CONTEXT_CHARS`, `BROKER_MAX_SUMMARY_CHARS`

## Manual Verification Checklist
After meaningful changes, run these checks:
1. Health header gate:
   - `curl -i http://127.0.0.1:7777/health` should return `403` without required header.
   - `curl -i -H 'X-Assistant-Client: chrome-sidepanel-v1' http://127.0.0.1:7777/health` should return `200`.
2. Extension basic flow:
   - Side panel opens and shows broker online/offline status.
   - Sending prompt yields assistant response.
3. Risk confirmation:
   - Prompt containing a high-risk keyword triggers confirmation UI.
   - Confirm continues request; cancel aborts cleanly.
4. Conversation history:
   - New chat appears in history.
   - Existing chat can be loaded and deleted.
5. Page context gating:
   - Allowed host works.
   - Non-allowlisted active tab is blocked with explicit error.

## Coding Style Expectations
- Keep Python and JS dependency-light (current code uses stdlib + Web APIs only).
- Prefer small, explicit helper functions over hidden framework behavior.
- Avoid broad refactors unless requested; preserve existing API shapes.
- Preserve clear runtime errors for invalid input paths.
