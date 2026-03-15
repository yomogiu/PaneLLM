# Assist

Local-first assistant stack with a localhost broker, a Chrome side panel UI, and broker-managed browser automation.

Current release target: `v0.1.0`.

## Architecture

```
Chrome Side Panel (UI + user actions)
  -> Extension background worker (policy checks, broker RPC, host list enforcement)
    -> Local broker (routing, persistence, run/job state, security gates)
      -> Model backends (llama / MLX / Codex Responses / Codex CLI)
      -> Extension relay loop for browser actions (Chrome tabs/scripting APIs)
```

### Core components

- `broker/local_broker.py`
  - Single-process HTTP control plane and API entrypoint.
  - Owns run lifecycle, browser tool policy, extension relay, async jobs, and persistence orchestration.
- `broker/browser_tools.py`
  - Canonical browser tool catalog used by broker-native and MCP-exposed tool calls.
- `broker/services/mlx_runtime.py`
  - MLX runtime control and status primitives.
- `broker/mlx_worker.py`
  - MLX backend process entrypoint.
- `broker/experiment_worker.py`
  - Async experiment executor.
- `broker/training_worker.py`
  - Async LoRA training/checkpoint executor.
- `chrome_secure_panel/`
  - MV3 side panel extension (`sidepanel.js`, `background.js`, `manifest.json`).
  - Polls broker state, submits runs, manages the relay loop, and renders Models/Tools/History UI.
- `tools/mcp-servers/browser-use/server.py`
  - MCP wrapper over broker browser tools (`/browser/tools/call`).

## Runtime flows

### 1) Chat flow

1. Side panel submits a prompt through `assistant.run.start`.
2. Background worker optionally captures page context and calls broker `POST /runs`.
3. Broker resolves model backend (`codex`, `llama`, `mlx`) and writes conversation state to `broker/.data/conversations/*.json`.
4. Assistant stream events are returned from `GET /runs/<run_id>/events`.
5. Risks can block a run until confirmed via `POST /runs/<run_id>/approval`.
6. Runs can be canceled with `POST /runs/<run_id>/cancel`.

### 2) Read-assistant context flow

1. Side panel requests context capture with `assistant.read.context.capture`.
2. Background extracts the active-tab summary if the active host is allowlisted.
3. Captured context is attached to the next run as `page_context` and is bounded by broker prompt-size limits.

### 3) Browser automation flow

1. Broker receives a browser tool request (`POST /browser/tools/call`) from model or MCP.
2. Tool call is validated against the allowlist and tool catalog.
3. Broker enqueues extension relay command.
4. Extension executes Chrome APIs on allowlisted hosts only.
5. Results return through broker and into the run stream.

### 4) Async jobs flow

- Experiments and MLX training jobs are async: `POST /experiments/jobs`, `POST /mlx/training/jobs`, etc.
- Jobs appear under `/jobs` and can be cancelled by `POST /jobs/<job_id>/cancel`.
- Side panel tools surfaces job/run state across experiments and training flows.

## API surface (implemented now)

- `GET /health`
- `GET /models`
- `POST /runs`
- `GET /runs/<run_id>/events`
- `POST /runs/<run_id>/approval`
- `POST /runs/<run_id>/cancel`
- `GET /browser/health`
- `GET /browser/config`
- `POST /browser/config`
- `POST /browser/tools/call`
- `POST /extension/register`
- `GET /extension/next`
- `POST /extension/result`
- `GET /jobs?kind=<experiment|training>&status=<queued|running|completed|failed|cancelled>`
- `POST /jobs/<job_id>/cancel`
- `GET /conversations`
- `GET /conversations/<conversation_id>`
- `DELETE /conversations/<conversation_id>`
- `POST /experiments/jobs`
- `GET /experiments/jobs/<job_id>`
- `GET /experiments`
- `GET /experiments/<experiment_id>`
- `GET /experiments/<experiment_id>/compare/<other_experiment_id>`
- `POST /mlx/training/datasets/import`
- `GET /mlx/training/datasets`
- `GET /mlx/training/datasets/<dataset_id>`
- `DELETE /mlx/training/datasets/<dataset_id>`
- `POST /mlx/training/jobs`
- `GET /mlx/training/jobs/<job_id>`
- `GET /mlx/training/runs`
- `GET /mlx/training/runs/<run_id>`
- `POST /mlx/training/checkpoints/promote`
- `POST /mlx/config`
- `GET /mlx/status`
- `POST /mlx/session/start`
- `POST /mlx/session/stop`
- `POST /mlx/session/restart`
- `GET /mlx/adapters`
- `POST /mlx/adapters/load`
- `POST /mlx/adapters/unload`

## MLX backend

### Runtime architecture

- MLX runs as a broker-managed worker process in [broker/mlx_worker.py](broker/mlx_worker.py).
- Broker owns runtime lifecycle (`start`/`stop`/`restart`), generation settings, adapter registry, and telemetry.
- Side panel `Models` tab controls MLX runtime.

### Stable MLX contract (v1)

- Versioned schema: `schema_version: mlx_chat_v1`.
- Message shape: OpenAI-style chat messages (`role`, `content`).
- Tool calls: disabled in v1 (`tool_call_format: none_v1`).
- Context behavior: tail truncation by char budget (`max_context_behavior: tail_truncate_chars_v1`).
- Llama/Codex use `BROKER_MAX_CONTEXT_CHARS` (default `24000` chars).
- MLX uses `BROKER_MLX_MAX_CONTEXT_CHARS` (default `56000`, capped at `56000`).
- Contract metadata is exposed from `/health` and `/mlx/status` payloads.

### MLX local data

- Conversations (all backends): `broker/.data/conversations/*.json`
- Run state: `broker/.data/codex_runs/*.json`
- MLX generation settings: `broker/.data/mlx_config.json`
- MLX adapter registry: `broker/.data/mlx_adapters.json`
- Browser policy config: `broker/.data/browser_config.json`
- Jobs:
  - `broker/.data/jobs/experiment/*.json`
  - `broker/.data/jobs/training/*.json`
- Experiments/training metadata:
  - `broker/.data/experiments/`
  - `broker/.data/mlx_training/datasets/<dataset_id>/`
  - `broker/.data/mlx_training/runs/<run_id>/`
- MLX reasoning mode: runtime toggle via Models tab (`generation.enable_thinking`)

## Legacy workflow status

- `IMPROVEMENTS.md` notes reflected retired paper endpoints and are now folded in here.
- The legacy paper route family (`/papers/inspect`, `/papers/jobs`) is not present in the broker now.
- The active first-party content-reading path is read-assistant capture (`assistant.read.context.capture`) + prompt context on `/runs`.

## Security model

- Broker accepts loopback clients only.
- Required header: `X-Assistant-Client: chrome-sidepanel-v1`.
- If `Origin` exists, it must be `chrome-extension://...`.
- Browser/page-context actions are host-allowlisted in extension runtime.
- Default extension allowlist: `127.0.0.1`, `localhost`, `google.com`, `www.google.com`, `arxiv.org`, `www.arxiv.org`.
- Runtime allowlist can be edited in the side panel **Tools** tab.
- High-risk prompts can require explicit confirmation.
- OpenAI/Codex credentials stay broker-side; extension has no persistent chat store.

## Install

You need three things before the side panel will answer:

- Python 3 for `broker/local_broker.py`
- Chrome or Chromium with Developer mode enabled
- At least one configured backend: Codex Responses, Codex CLI, llama.cpp, or MLX

### 1) Run the macOS checker

```bash
python3 scripts/check_macos.py
```

### 2) Configure and start a backend

Codex Responses:

```bash
export OPENAI_API_KEY="<your-api-key>"
python3 broker/local_broker.py
```

Codex CLI:

```bash
codex login
python3 broker/local_broker.py
```

llama.cpp endpoint:

```bash
export LLAMA_URL="http://127.0.0.1:18000/v1/chat/completions"
python3 broker/local_broker.py
```

MLX:

```bash
python3 -m pip install mlx-lm
export BROKER_MLX_MODEL_PATH="$HOME/models/mlx/<your-model-folder>"
python3 broker/local_broker.py
```

If you use MLX, install `mlx-lm` in the same interpreter as `BROKER_MLX_WORKER_PYTHON`/worker if needed.

### 3) Verify broker health

```bash
curl -i \
  -H 'X-Assistant-Client: chrome-sidepanel-v1' \
  http://127.0.0.1:7777/health
```

Expected response clues:

- `codex_backend: responses_ready` when `OPENAI_API_KEY` is set
- `codex_backend: cli_ready` when logged-in local `codex` CLI is available
- `codex_backend: disabled` if no Codex path is configured

### 4) Load Chrome extension

1. Open `chrome://extensions`
2. Enable **Developer mode**
3. Click **Load unpacked**
4. Select `chrome_secure_panel/`

### 5) First run

Open side panel, confirm broker status is online, and send a prompt. If browser actions will be used, confirm host allowlist in **Tools** first.

## Repo guide

- [broker/README.md](broker/README.md): broker endpoints, contracts, env vars
- [chrome_secure_panel/README.md](chrome_secure_panel/README.md): extension behavior and RPC surface
- [tools/README.md](tools/README.md): tool and MCP layout
- [WEB_SEARCH.md](WEB_SEARCH.md): architecture notes and planning context
