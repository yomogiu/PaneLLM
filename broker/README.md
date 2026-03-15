# Local Broker

The broker is the local control plane for chat routing, backend discovery, browser automation, and conversation persistence.

## Start

```bash
python3 broker/local_broker.py
```

## Core environment

- `BROKER_HOST` default `127.0.0.1`
- `BROKER_PORT` default `7777`
- `BROKER_DATA_DIR` default `broker/.data`
- `LLAMA_URL` default `http://127.0.0.1:18000/v1/chat/completions`
- `LLAMA_MODEL` optional override
- `LLAMA_API_KEY` optional
- `OPENAI_API_KEY` enables Codex Responses mode
- `OPENAI_BASE_URL` default `https://api.openai.com/v1`
- `OPENAI_CODEX_MODEL` default `gpt-5.3-codex`
- `OPENAI_CODEX_REASONING_EFFORT` default `medium`
- `OPENAI_CODEX_MAX_OUTPUT_TOKENS` default `1800`
- `CODEX_TIMEOUT_SEC` default `480`
- `BROKER_CODEX_CLI_ENABLE_BROWSER_MCP` default `true`
- `BROKER_CODEX_CLI_BROWSER_MCP_NAME` default `browser_use`
- `BROKER_CODEX_CLI_BROWSER_MCP_PYTHON` default `python3`
- `BROKER_CODEX_CLI_BROWSER_MCP_SERVER_PATH` default `tools/mcp-servers/browser-use/server.py`
- `BROKER_CODEX_CLI_BROWSER_MCP_BROKER_URL` default `http://<broker-host>:<broker-port>` with loopback fallback
- `BROKER_CODEX_CLI_BROWSER_MCP_APPROVAL_MODE` default `auto-approve`
- `BROKER_CODEX_RUN_TIMEOUT_SEC` default `180`
- `BROKER_CODEX_EVENT_POLL_TIMEOUT_MS` default `20000`
- `BROKER_CODEX_ENABLE_BACKGROUND` reserved, default `false`
- `BROKER_MAX_CONTEXT_MESSAGES` default `32`
- `BROKER_MAX_CONTEXT_CHARS` default `24000`
- `BROKER_MAX_SUMMARY_CHARS` default `5000`
- `BROKER_BROWSER_COMMAND_TIMEOUT_SEC` default `25`
- `BROKER_EXTENSION_CLIENT_STALE_SEC` default `90`
- `BROKER_DEFAULT_DOMAIN_ALLOWLIST` default `127.0.0.1,localhost`
- `MLX_URL` optional local OpenAI-compatible MLX endpoint
- `MLX_MODEL` optional preferred MLX model id
- `MLX_API_KEY` optional bearer token for the MLX server
- `BROKER_EXPERIMENT_WORKER_PYTHON` default `python3`
- `BROKER_EXPERIMENT_WORKER_PATH` default `broker/experiment_worker.py`
- `BROKER_EXPERIMENT_JOB_TIMEOUT_SEC` default `900`
- `BROKER_TRAINING_WORKER_PYTHON` default `python3`
- `BROKER_TRAINING_JOB_TIMEOUT_SEC` default `7200`

## Health

`GET /health` reports broker readiness:

```json
{
  "ok": true,
  "codex_configured": true,
  "codex_backend": "responses_ready | cli_ready | disabled",
  "codex_responses_ready": true,
  "codex_cli_ready": false,
  "extension_relay": {},
  "browser_automation": {},
  "codex_runs": {},
  "llama": {},
  "experiments": {},
}
```

`codex_backend` is:

- `responses_ready` when `OPENAI_API_KEY` is set
- `cli_ready` when the local `codex` CLI is installed and logged in
- `disabled` when neither Codex path is available

## Run API

Interactive chat for `codex`, `llama`, and `mlx` uses one run surface:

- `POST /runs`
- `GET /runs/<run_id>/events?after=<seq>&timeout_ms=<n>`
- `POST /runs/<run_id>/approval`
- `POST /runs/<run_id>/cancel`

`POST /runs` request shape:

```json
{
  "session_id": "string",
  "backend": "codex | llama | mlx",
  "prompt": "string",
  "rewrite_message_index": 2,
  "chat_template_kwargs": "{\"enable_thinking\":true,\"clear_thinking\":false}",
  "reasoning_budget": 0,
  "page_context": {
    "title": "string",
    "url": "string",
    "selection": "string",
    "text_excerpt": "string"
  },
  "allowed_hosts": ["localhost"],
  "force_browser_action": false,
  "confirmed": false,
  "risk_signals": ["high_risk_prompt"]
}
```

Notes:

- `rewrite_message_index` rewrites a prior user turn and truncates later turns before the new run starts.
- `chat_template_kwargs` and `reasoning_budget` are forwarded to llama.cpp when `backend` is `llama`.
- `force_browser_action` requires an active extension relay client and at least one allowlisted host.
- High-risk prompts return `requires_confirmation: true` until the caller confirms.

Response shape:

```json
{
  "requires_confirmation": false,
  "run_id": "run_...",
  "status": "thinking",
  "conversation_id": "string",
  "backend": "codex",
  "backend_metadata": {
    "mode": "responses | cli | llama | mlx",
    "model": "gpt-5.3-codex",
    "browser_tools_enabled": true,
    "browser_action_forced": false,
    "llama_request_options": {}
  }
}
```

Event feed notes:

- Run files are persisted under `broker/.data/codex_runs/<run_id>.json`.
- Conversation files keep user and assistant messages only.
- Codex-specific replay state lives under `conversation.codex`.

## Browser automation

The broker owns browser tool schemas and policy. The canonical tool catalog lives in `broker/browser_tools.py` and is shared by broker-native and MCP surfaces.

Broker endpoints:

- `GET /browser/config`
- `POST /browser/config`
- `POST /browser/tools/call`
- `GET /browser/health`

`POST /browser/tools/call` accepts:

```json
{
  "name": "browser.navigate",
  "arguments": {}
}
```

Important tool behavior:

- `browser.get_content` defaults to a compact navigation digest.
- Pass `{"mode":"raw_html"}` only when raw HTML is explicitly required.
- Auto-approve tools and manual-approval tools are derived from the shared catalog, not hand-maintained separately per surface.

## Codex CLI behavior

When `OPENAI_API_KEY` is not set but the local `codex` CLI is installed and logged in, the broker uses CLI-backed runs.

Current behavior:

- Runs `codex exec` non-interactively for the first turn
- Persists the local Codex CLI session id per conversation
- Uses `codex exec resume <session_id>` on later turns
- Uses local ChatGPT-authenticated credentials from the official CLI
- Injects a session-scoped browser MCP override when the extension relay is connected
- Passes broker URL and allowlisted hosts to that MCP server via per-run config overrides


## Backend discovery

- `GET /models`

`GET /models` returns backend availability and per-backend capability metadata for the sidepanel backend selector.

MLX is treated as another OpenAI-compatible local chat backend:

- `MLX_URL` points at a local chat completions endpoint
- `MLX_MODEL` optionally pins the preferred model id
- `MLX_API_KEY` optionally sets a bearer token
- Chat still uses `POST /runs` with `backend: "mlx"`

## Extension relay and conversations

Extension relay endpoints:

- `POST /extension/register`
- `GET /extension/next?client_id=<id>&timeout_ms=25000`
- `POST /extension/result`

Conversation endpoints:

- `GET /conversations`
- `GET /conversations/<id>`
- `DELETE /conversations/<id>`

Conversation rewrite happens through `POST /runs` with `rewrite_message_index`, not through a separate conversation rewrite endpoint.

## Security properties

- Broker accepts loopback clients only.
- Broker requires `X-Assistant-Client: chrome-sidepanel-v1`.
- Broker only accepts `chrome-extension://...` origins when `Origin` is present.
- OpenAI credentials remain broker-side.
- Page context and browser tool output are treated as untrusted input.
- Suspicious instruction-like page or tool content can block a run for review.
