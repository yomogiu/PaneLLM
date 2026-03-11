# Assist

Local-first assistant stack with a localhost broker, a Chrome side panel UI, and broker-managed browser automation.

## Architecture

```
Chrome Side Panel (UI + user actions)
  -> Extension background worker (policy checks, broker RPC)
    -> Local broker (routing, persistence, run state, security gates)
      -> Model backends (llama / MLX worker / Codex Responses / Codex CLI / legacy command)
      -> Extension relay loop for browser actions (Chrome tabs/scripting APIs)
```

### Core components

- `broker/local_broker.py`
  - Single-process HTTP broker and conversation store.
  - Routes chat requests, manages Codex run lifecycle, MLX runtime lifecycle, and browser session/run state.
- `chrome_secure_panel/`
  - MV3 side panel extension (`sidepanel.js`, `background.js`, `manifest.json`).
  - Handles UI, prompt submission, history, and extension-side browser execution.
- `llama_browser_tool_loop.py`
  - Standalone CLI loop for local ask/tool/agent workflows.
- `tools/mcp-servers/browser-use/server.py`
  - MCP bridge that proxies browser tool calls through broker APIs.

## Runtime flows

### 1) Chat flow

1. Side panel sends prompt to extension background worker.
2. Background worker captures optional page context and calls broker (`/route` or `/codex/runs`).
3. Broker builds model context from persisted conversation and routes to selected backend (`llama`, `mlx`, `codex`).
4. Broker persists user/assistant turns and returns response/events.

### 2) Browser automation flow

1. Model emits browser tool intent (llama tool loop or Codex run tools).
2. Broker validates policy and pushes commands to extension relay.
3. Extension executes via Chrome APIs on allowlisted hosts only.
4. Results return to broker, then back into the model/run stream.

### 3) Conversation/history flow

- Broker persists conversations in `broker/.data/conversations/*.json`.
- Extension uses history APIs for list/get/delete/rewrite.
- Prompt rewrite is linear today: editing an older user turn truncates later turns and regenerates.

## MLX backend

### Runtime architecture

- MLX runs as a broker-managed worker process in [broker/mlx_worker.py](broker/mlx_worker.py).
- Broker owns MLX lifecycle (`start`/`stop`/`restart`), generation settings, adapter registry, and telemetry.
- Side panel `Models` tab is the operator UI for MLX runtime control.

### Models tab capabilities

- Backend selection includes `MLX Local`.
- Runtime controls: start, stop, restart, refresh status.
- Generation controls: `temperature`, `top_p`, `top_k`, `max_tokens`, `repetition_penalty`, `seed`, `enable_thinking`.
- Adapter controls: list/load/unload LoRA checkpoints (checkpoint reload only, no merge in v1).
- Runtime trends: latency, tokens/sec, restart success/failure counters.

### Stable MLX contract (v1)

- Versioned schema: `schema_version: mlx_chat_v1`.
- Message shape: OpenAI-style chat messages (`role`, `content`).
- Tool calls: disabled in v1 (`tool_call_format: none_v1`).
- Template assumption: tokenizer `apply_chat_template` (Qwen-style Jinja, with plaintext role-header fallback).
- Context behavior: tail truncation by char budget (`max_context_behavior: tail_truncate_chars_v1`).
- Llama/Codex use the shared `BROKER_MAX_CONTEXT_CHARS` budget (default `24000` chars).
- MLX uses `BROKER_MLX_MAX_CONTEXT_CHARS` (default `56000` chars, capped at `56000`).
- Contract metadata is exposed by broker status payloads and shown in the Models tab.
- `/route` responses now include `context_usage` on successful assistant replies, with backend-aware char and message window usage.
- The side panel now surfaces `context_usage` as `Context: used/limit` in the chat header.

### MLX local data

- Conversations (all backends): `broker/.data/conversations/*.json`
- MLX generation settings: `broker/.data/mlx_config.json`
- MLX adapter registry: `broker/.data/mlx_adapters.json`
- MLX reasoning mode: runtime toggle via Models tab (`generation.enable_thinking`)

## Security model

- Broker accepts loopback clients only.
- Required header: `X-Assistant-Client: chrome-sidepanel-v1`.
- If `Origin` exists, it must be `chrome-extension://...`.
- Browser/page-context actions are host-allowlisted in extension runtime.
- High-risk prompts require explicit confirmation.
- OpenAI/Codex credentials stay broker-side; extension never stores chat in persistent local storage.

## Install

You need three things before the side panel will answer:

- Python 3 for `broker/local_broker.py`
- Chrome or Chromium with Developer mode enabled
- At least one configured backend: Codex Responses, Codex CLI, llama.cpp, or MLX

The broker itself is stdlib-only. Backend setup depends on which model path you want to use.

### 1) Run the macOS checker

For macOS, start here:

```bash
python3 scripts/check_macos.py
```

The checker is read-only. It verifies core requirements, probes each backend path, and prints exact follow-up commands for anything missing. It exits non-zero until core requirements are satisfied and at least one backend is ready.

### 2) Choose a backend

Pick one working backend before you start the broker.

Codex Responses via API key:

```bash
export OPENAI_API_KEY="<your-api-key>"
python3 broker/local_broker.py
```

Codex CLI via local ChatGPT login:

```bash
codex login
python3 broker/local_broker.py
```

llama.cpp via an OpenAI-compatible local endpoint:

```bash
export LLAMA_URL="http://127.0.0.1:18000/v1/chat/completions"
python3 broker/local_broker.py
```

MLX via a local model directory:

```bash
python3 -m pip install mlx-lm
export BROKER_MLX_MODEL_PATH="$HOME/models/mlx/<your-model-folder>"
export BROKER_MLX_MAX_CONTEXT_CHARS=56000
python3 broker/local_broker.py
```

If you use MLX, install `mlx-lm` in the Python environment used by the broker worker. If that is not the same interpreter as `python3`, set `BROKER_MLX_WORKER_PYTHON` as well.

### 3) Verify the broker

The broker rejects requests without the required client header. This check should return `200` and a JSON body with `"ok": true`:

```bash
curl -i \
  -H 'X-Assistant-Client: chrome-sidepanel-v1' \
  http://127.0.0.1:7777/health
```

Expected signals in the response:

- `codex_backend: responses_ready` if `OPENAI_API_KEY` is set
- `codex_backend: cli_ready` if the local `codex` CLI is installed and logged in
- `codex_backend: legacy_command` if `CODEX_COMMAND` is configured
- `codex_backend: disabled` if no Codex path is configured yet

If you call `/health` without `X-Assistant-Client`, `403` is expected.

### 4) Load the Chrome extension

1. Open `chrome://extensions`
2. Enable **Developer mode**
3. Click **Load unpacked**
4. Select `chrome_secure_panel/`

### 5) Confirm the first run

Open the side panel and check that broker status shows online. Then send a simple prompt.

If you plan to let the assistant read or act on live pages, use the side panel **Tools** tab to allowlist the active host first. By default, browser actions stay limited to local hosts.

### 6) Optional CLI

```bash
python3 llama_browser_tool_loop.py --help
```

## Repo guide

- [broker/README.md](broker/README.md): broker endpoints, contracts, env vars
- [chrome_secure_panel/README.md](chrome_secure_panel/README.md): extension behavior and RPC surface
- [tools/README.md](tools/README.md): tool and MCP layout
- [WEB_SEARCH.md](WEB_SEARCH.md): longer-horizon task architecture notes
