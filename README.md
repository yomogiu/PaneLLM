# Assist

Local-first assistant stack with a localhost broker, a Chrome side panel UI, and broker-managed browser automation.

Current release target: `v0.1.0`.

## Demo

- [Watch the demo video](assets/demo.mp4)

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
  - Owns run lifecycle, browser tool policy, extension relay, and persistence orchestration.
- `broker/browser_tools.py`
  - Canonical browser tool catalog used by broker-native and MCP-exposed tool calls.
- `chrome_secure_panel/`
  - MV3 side panel extension (`sidepanel.js`, `background.js`, `manifest.json`).
  - Polls broker state, submits runs, manages the relay loop, and renders Chat/Tools/History UI.
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

### 4) Paper workspace flow

1. Opening an arXiv page or restoring a paper-linked chat resolves a paper context in the side panel.
2. arXiv paper state is grouped by versionless paper id, while optional version metadata (`v1`, `v2`, etc.) is preserved for badges, history labels, highlights, and summaries.
3. The side panel loads per-paper state through `assistant.paper.get` and broker paper routes. The `This Paper` history section is filtered by base paper id and auto-restore prefers an exact version match before falling back to the newest same-paper session.
4. The Summary tab uses a dedicated hidden run to generate and persist a paper-summary artifact in `broker/.data/papers/*.json`, including summary provenance metadata such as the paper version used for the last saved summary.
5. `Explain Selection` chats stay in the normal chat transcript, but completed explain-selection runs are auto-saved immediately into the paper Highlights artifact. The Highlights tab briefly glows to indicate a successful save.
6. `POST /papers/highlights_capture` remains as an idempotent backfill path for older conversations, but normal explain-selection saving no longer depends on starting a new chat.
7. Paper summary and paper highlights are separate artifacts from the chat transcript, but they are loaded together when the same paper comes back into focus.

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
- `GET /conversations`
- `GET /conversations/<conversation_id>`
- `DELETE /conversations/<conversation_id>`
- `GET /papers?source=<source>&paper_id=<paper_id>`
- `POST /papers/summary_request`
- `POST /papers/highlights_capture`
- `POST /papers/summary_generate`

## MLX backend

MLX is now treated the same way as `llama.cpp`: an OpenAI-compatible endpoint selected through the normal run path.

- Configure `MLX_URL` to point at a local chat completions endpoint.
- Optionally set `MLX_MODEL` to pin the preferred model id.
- Optionally set `MLX_API_KEY` for bearer-token auth.
- Use `backend: "mlx"` on `POST /runs` for chat completions and broker-mediated browser tool calls.

### MLX local data

- Conversations (all backends): `broker/.data/conversations/*.json`
  - Conversation `codex` state may include paper-session metadata such as `paper_version`, `paper_version_url`, `paper_chat_kind`, `paper_history_label`, and `paper_focus_text`.
- Run state: `broker/.data/codex_runs/*.json`
- Paper workspace state: `broker/.data/papers/*.json`
  - Paper records may include `observed_versions`, `last_summary_version`, `summary`, and saved highlight artifacts.
- Browser policy config: `broker/.data/browser_config.json`

## Legacy workflow status

- `IMPROVEMENTS.md` notes reflected retired paper endpoints and are now folded in here.
- The legacy paper route family (`/papers/inspect`, `/papers/jobs`) is not present in the broker now.
- The active first-party content-reading path is read-assistant capture (`assistant.read.context.capture`) plus paper workspace state and prompt context on `/runs`.

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
export MLX_URL="http://127.0.0.1:8080/v1/chat/completions"
export MLX_MODEL="<your-mlx-model-id>"
python3 broker/local_broker.py
```

If your MLX server requires auth, also set `MLX_API_KEY`.

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
