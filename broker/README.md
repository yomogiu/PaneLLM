# Local Broker Setup

This broker keeps model routing local, keeps extension storage stateless, and now supports three Codex modes:

- `Responses` mode: broker-managed interactive runs over the OpenAI Responses API.
- `CLI` mode: one-shot calls through the locally installed `codex` binary authenticated with ChatGPT.
- `Legacy command` mode: deprecated one-shot `CODEX_COMMAND` subprocess execution.

## 1) Start the broker

```bash
python3 broker/local_broker.py
```

Optional environment variables:

- `BROKER_HOST` (default `127.0.0.1`)
- `BROKER_PORT` (default `7777`)
- `BROKER_DATA_DIR` (default `broker/.data`)
- `LLAMA_URL` (default `http://127.0.0.1:18000/v1/chat/completions`)
- `LLAMA_MODEL` (default `glm-4.7-flash-llamacpp`)
- `LLAMA_API_KEY` (optional)
- `OPENAI_API_KEY` (enables Codex Responses mode)
- `OPENAI_BASE_URL` (default `https://api.openai.com/v1`)
- `OPENAI_CODEX_MODEL` (default `gpt-5.3-codex-spark`)
- `OPENAI_CODEX_REASONING_EFFORT` (default `medium`)
- `OPENAI_CODEX_MAX_OUTPUT_TOKENS` (default `1800`)
- `CODEX_COMMAND` (optional legacy fallback command)
- `CODEX_TIMEOUT_SEC` (default `480`)
- `BROKER_CODEX_CLI_ENABLE_BROWSER_MCP` (default `true`)
- `BROKER_CODEX_CLI_BROWSER_MCP_NAME` (default `browser_use`)
- `BROKER_CODEX_CLI_BROWSER_MCP_PYTHON` (default `python3`)
- `BROKER_CODEX_CLI_BROWSER_MCP_SERVER_PATH` (default `tools/mcp-servers/browser-use/server.py`)
- `BROKER_CODEX_CLI_BROWSER_MCP_BROKER_URL` (default `http://<broker-host>:<broker-port>`, with loopback fallback)
- `BROKER_CODEX_CLI_BROWSER_MCP_APPROVAL_MODE` (default `auto-approve`)
- `BROKER_CODEX_RUN_TIMEOUT_SEC` (default `180`)
- `BROKER_CODEX_EVENT_POLL_TIMEOUT_MS` (default `20000`)
- `BROKER_CODEX_ENABLE_BACKGROUND` (reserved, default `false`)
- `BROKER_MAX_CONTEXT_MESSAGES` (default `32`)
- `BROKER_MAX_CONTEXT_CHARS` (default `24000`)
- `BROKER_MAX_SUMMARY_CHARS` (default `5000`)
- `BROKER_BROWSER_COMMAND_TIMEOUT_SEC` (default `25`)
- `BROKER_EXTENSION_CLIENT_STALE_SEC` (default `90`)
- `BROKER_DEFAULT_DOMAIN_ALLOWLIST` (default `127.0.0.1,localhost`)
- `BROKER_MLX_MODEL_PATH` (required to enable MLX backend)
- `BROKER_MLX_WORKER_PYTHON` (default `python3`)
- `BROKER_MLX_WORKER_PATH` (default `broker/mlx_worker.py`)
- `BROKER_MLX_START_TIMEOUT_SEC` (default `60`)
- `BROKER_MLX_STOP_TIMEOUT_SEC` (default `8`)
- `BROKER_MLX_GENERATION_TIMEOUT_SEC` (default `180`)
- `BROKER_MLX_MAX_CONTEXT_CHARS` (default `56000`, capped at `56000`)
- `BROKER_MLX_DEFAULT_TEMPERATURE` (default `0.2`)
- `BROKER_MLX_DEFAULT_TOP_P` (default `0.95`)
- `BROKER_MLX_DEFAULT_TOP_K` (default `50`)
- `BROKER_MLX_DEFAULT_MAX_TOKENS` (default `512`)
- `BROKER_MLX_DEFAULT_REPETITION_PENALTY` (default `1.0`)
- `BROKER_MLX_DEFAULT_SEED` (optional)
- `BROKER_MLX_DEFAULT_ENABLE_THINKING` (default `false`)
- `BROKER_MLX_DEFAULT_SYSTEM_PROMPT` (optional)

## `/health`

`GET /health` returns broker readiness plus Codex mode:

```json
{
  "ok": true,
  "codex_configured": true,
  "codex_backend": "responses_ready | cli_ready | legacy_command | disabled",
  "codex_responses_ready": true,
  "codex_cli_ready": false,
  "codex_legacy_command": false,
  "mlx": {
    "available": true,
    "status": "running",
    "worker_pid": 12345,
    "last_error": ""
  }
}
```

If the official `codex` CLI is installed and `codex login status` reports a ChatGPT login, broker health will report `codex_backend: cli_ready` without requiring `OPENAI_API_KEY`.

## Codex Responses run API

The side panel uses these broker endpoints when `OPENAI_API_KEY` is set:

- `POST /codex/runs`
- `GET /codex/runs/<run_id>/events?after=<seq>&timeout_ms=<n>`
- `POST /codex/runs/<run_id>/approval`
- `POST /codex/runs/<run_id>/cancel`

Unified run API (all backends, same underlying manager):

- `POST /runs`
- `GET /runs/<run_id>/events?after=<seq>&timeout_ms=<n>`
- `POST /runs/<run_id>/approval`
- `POST /runs/<run_id>/cancel`

`POST /codex/runs` request shape:

```json
{
  "session_id": "string",
  "prompt": "string",
  "rewrite_message_index": 2,
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

Response shape:

```json
{
  "requires_confirmation": false,
  "run_id": "run_...",
  "status": "thinking",
  "conversation_id": "string",
  "backend_metadata": {
    "mode": "responses",
    "model": "gpt-5.3-codex",
    "browser_tools_enabled": true
  }
}
```

Event feed notes:

- Events are persisted under `broker/.data/codex_runs/<run_id>.json`.
- Conversation files keep `messages` as user/assistant only.
- Codex metadata is stored separately on the conversation under `conversation.codex`.

Approval policy in v1:

- Auto-approve: `browser.get_tabs`, `browser.describe_session_tabs`, `browser.get_content`, `browser.scroll`, `browser.switch_tab`, `browser.focus_tab`
- Manual approval: `browser.navigate`, `browser.open_tab`, `browser.click`, `browser.type`, `browser.press_key`, `browser.close_tab`, `browser.group_tabs`
- Non-allowlisted hosts are denied before the extension executes anything

## Codex CLI fallback

When `OPENAI_API_KEY` is not set but the local `codex` CLI is installed and logged in, `/route` can use the local CLI automatically.

Current broker behavior in CLI mode:

- Runs `codex exec` non-interactively for the first turn
- Persists the local Codex CLI session id per broker conversation
- Uses `codex exec resume <session_id>` on later turns for multi-turn chat continuity
- Uses local ChatGPT-authenticated credentials from the official CLI
- Uses a read-only sandbox on the initial CLI turn
- Injects a session-scoped MCP server override (`mcp_servers.browser_use`) per invocation when the extension relay is connected
- Passes broker URL and allowlisted hosts to that MCP server via per-run `-c` config overrides (no global `codex mcp add` required)

This mode gives you subscription-backed Codex chat without an API key, but it does not power the interactive `/codex/runs` event/approval flow.

## Legacy Codex command

If `CODEX_COMMAND` is set, `/route` still supports the deprecated one-shot Codex path.

The broker writes JSON to stdin:

```json
{
  "session_id": "string",
  "prompt": "string",
  "messages": [{"role": "user|assistant", "content": "string"}]
}
```

The command may return:

- Plain text on stdout, or
- JSON with an `answer` field, for example `{"answer":"..."}`.

## `/route` browser behavior

- `POST /route` now accepts an optional `request_id` (`[A-Za-z0-9._-]{1,128}`) so the extension can cancel in-flight requests.
- `POST /route` also accepts optional `rewrite_message_index` to rewrite a prior user turn and truncate later turns before re-running.
- `POST /route/cancel` accepts `{ "session_id": "...", "request_id": "..." }` and marks that route request as cancelled.
- Cancel is best-effort for remote HTTP model calls and immediate for local subprocess-backed Codex CLI/legacy calls.
- The extension sends its normalized host allowlist with `/route`.
- `POST /route` accepts optional `force_browser_action` (boolean) for `llama`, `codex`, and `mlx` requests.
- For `llama` and `mlx` requests, this enables a browser tool loop through the extension relay when a client is connected.
- Broker-side browser policy falls back to `BROKER_DEFAULT_DOMAIN_ALLOWLIST` only when the request does not provide `allowed_hosts`.
- For `codex` CLI requests over `/route`, broker can inject the local `browser-use` MCP bridge on that run only, using the same allowlist.
- When `force_browser_action` is true, broker requires extension relay availability and at least one allowlisted host.
- For `codex` requests, broker adds a strict browser-action system instruction.
- `codex` requests through `/route` use, in order: local Codex Responses runs only when the extension selects `/codex/runs`, then local `codex` CLI when available, then `CODEX_COMMAND` if configured.

`POST /route` request shape (new fields emphasized):

```json
{
  "session_id": "string",
  "request_id": "req_abc123",
  "backend": "llama | codex | mlx",
  "prompt": "string",
  "rewrite_message_index": 2,
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

`POST /route` response includes:

```json
{
  "requires_confirmation": false,
  "request_id": "req_abc123",
  "cancelled": false,
  "answer": "...",
  "context_usage": {
    "backend": "mlx",
    "used_chars": 1120,
    "limit_chars": 24000,
    "messages_used": 12,
    "max_messages": 32,
    "truncated": false,
    "summary_included": false,
    "summary_chars": 0,
    "truncated_dropped_messages": 0
  },
  "reasoning_hidden": true,
  "reasoning_hidden_chars": 812,
  "reasoning_blocks": ["reasoning text block 1", "reasoning text block 2"]
}
```

## MLX API and contract

Broker endpoints for MLX runtime control:

- `GET /models`
- `GET /mlx/status`
- `POST /mlx/config`
- `POST /mlx/session/start`
- `POST /mlx/session/stop`
- `POST /mlx/session/restart`
- `GET /mlx/adapters`
- `POST /mlx/adapters/load`
- `POST /mlx/adapters/unload`

`POST /mlx/config` updates MLX generation parameters and optional persistent system prompt:

```json
{
  "generation": {
    "temperature": 0.2,
    "top_p": 0.95,
    "top_k": 50,
    "max_tokens": 512,
    "repetition_penalty": 1.0,
    "seed": null,
    "enable_thinking": false
  },
  "system_prompt": "You are a helpful local assistant."
}
```

`GET /models` response includes backend availability and MLX status:

```json
{
  "backends": [
    { "id": "codex", "label": "Codex", "available": true },
    { "id": "llama", "label": "llama.cpp", "available": true },
    { "id": "mlx", "label": "MLX Local", "available": true }
  ],
  "mlx": {
    "status": "running",
    "model_path": "/models/my-mlx-model",
    "generation_config": {
      "temperature": 0.2,
      "top_p": 0.95,
      "top_k": 50,
      "max_tokens": 512,
      "repetition_penalty": 1.0,
      "seed": null,
      "enable_thinking": false
    },
    "system_prompt": "You are a helpful local assistant.",
    "contract": {
      "schema_version": "mlx_chat_v1",
      "message_format": "openai_chat_messages_v1",
      "tool_call_format": "none_v1",
      "chat_template_assumption": "qwen_jinja_default_or_plaintext_fallback_v1",
      "tokenizer_template_mode": "apply_chat_template_default_v1",
      "max_context_behavior": "tail_truncate_chars_v1",
      "max_context_chars": 56000
    }
  }
}
```

MLX worker contract is explicit and versioned so future training pipelines can target it safely:

- Message format: OpenAI-style chat messages with `role` in `{system,user,assistant}` and string `content`.
- Tool-call format: none in v1 (`tool_call_format: none_v1`).
- Chat template assumption: broker/worker prefers tokenizer-default Qwen-style Jinja templating first, with a plaintext role-header fallback when unavailable:
  - `ROLE:\n<content>` joined by blank lines.
- Thinking output is controlled by `generation.enable_thinking` (runtime + persisted config). Default is disabled unless explicitly enabled.
- When MLX system prompt is configured, broker prepends it as a leading `system` message before worker generation.
- Tokenizer template assumption: enabled by default in v1 (`tokenizer_template_mode: apply_chat_template_default_v1`) with fallback below.
- Max-context behavior: char-tail truncation (`max_context_behavior: tail_truncate_chars_v1`).
- Llama/Codex use `BROKER_MAX_CONTEXT_CHARS` with a minimum effective value of `2000`.
- MLX uses `BROKER_MLX_MAX_CONTEXT_CHARS`, capped at `56000`, with a minimum effective value of `2000`.

`generate` broker->worker payload (internal contract):

```json
{
  "request_id": "mlx_...",
  "op": "generate",
  "schema_version": "mlx_chat_v1",
  "contract": {
    "schema_version": "mlx_chat_v1",
    "message_format": "openai_chat_messages_v1",
    "tool_call_format": "none_v1",
      "chat_template_assumption": "qwen_jinja_default_or_plaintext_fallback_v1",
      "tokenizer_template_mode": "apply_chat_template_default_v1",
    "max_context_behavior": "tail_truncate_chars_v1",
    "max_context_chars": 56000
  },
  "messages": [
    { "role": "system", "content": "You are a helpful local assistant." },
    { "role": "user", "content": "Hi" }
  ],
  "params": {
    "temperature": 0.2,
    "top_p": 0.95,
    "top_k": 50,
    "max_tokens": 512,
    "repetition_penalty": 1.0,
    "seed": null,
    "enable_thinking": false
  }
}
```

## Browser automation API

The broker keeps the manual test endpoint:

- `POST /browser/tools/call`

Request shape:

```json
{
  "name": "browser.session_create | browser.run_start | browser.run_cancel | browser.approvals_list | browser.events_replay | browser.approve | browser.navigate | browser.get_content | browser.get_tabs | browser.open_tab | browser.switch_tab | browser.close_tab | browser.focus_tab | browser.group_tabs | browser.describe_session_tabs | browser.click | browser.type | browser.press_key | browser.scroll",
  "arguments": {}
}
```

Response shape:

```json
{
  "content": [{"type": "text", "text": "..."}],
  "structured_content": {},
  "is_error": false
}
```

## Extension command relay API

The extension background worker long-polls broker commands and posts results:

- `POST /extension/register`
- `GET /extension/next?client_id=<id>&timeout_ms=25000`
- `POST /extension/result`

## Conversation history API

The broker persists conversations on disk and exposes:

- `GET /conversations`
- `GET /conversations/<id>`
- `DELETE /conversations/<id>`
- `POST /conversations/<id>/rewrite`

Conversations are saved whenever user/assistant turns are added.

Assistant messages may include optional `reasoning_blocks` for debug visibility when the model emitted
`<think>` or `<thinking>` content.

`POST /conversations/<id>/rewrite` request shape:

```json
{
  "backend": "llama | codex | mlx",
  "prompt": "string",
  "request_id": "req_abc123",
  "rewrite_message_index": 2,
  "page_context": {
    "title": "string",
    "url": "string",
    "selection": "string",
    "text_excerpt": "string"
  },
  "allowed_hosts": ["localhost"],
  "confirmed": false,
  "risk_signals": ["high_risk_prompt"]
}
```

Behavior:

- `rewrite_message_index` must target an existing user message.
- The broker truncates from that message onward, replaces that user prompt, and regenerates from that point.

## 2) Load extension in Chrome

1. Open `chrome://extensions`
2. Enable **Developer mode**
3. Click **Load unpacked**
4. Select `chrome_secure_panel/`

## 3) Security properties

- Side panel data stays in extension runtime memory.
- No extension `localStorage` usage.
- Broker accepts loopback clients only.
- Broker requires `X-Assistant-Client: chrome-sidepanel-v1`.
- Broker only allows `chrome-extension://...` origins when `Origin` is set.
- OpenAI credentials remain broker-side.
- Codex page context and browser tool output are treated as untrusted input.
- Suspicious instruction-like page/tool content blocks the run for review.

## 4) Domain allowlist

Use the side panel **Tools** tab to manage runtime allow/disallow hosts.

For source-controlled defaults, edit `DEFAULT_ALLOWED_PAGE_HOSTS` in `chrome_secure_panel/background.js`.
The default list remains intentionally tight to local domains plus the small demo allowlist in the extension.
