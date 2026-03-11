# TODO: Local MLX Backend + Models Tab Architecture

## Goal

Add a local MLX backend that appears in the extension backend dropdown, with broker-managed runtime controls, a top-level Models tab for configuration, checkpoint-based LoRA adapter reloads (no weight merge), and simple runtime graphs.

## Delivery Plan

### 1) Runtime + routing

- [x] Extend backend routing from `{llama,codex}` to `{llama,codex,mlx}` across extension + broker contracts.
- [x] Add broker-managed MLX worker lifecycle (`start`, `stop`, `restart`, `status`) with health/readiness checks.
- [x] Keep `/route` as the primary inference path; support `backend=mlx`.
- [x] Add model discovery/state endpoint so extension can populate backend/model selectors.

### 2) Extension Models tab

- [x] Add top-level tabs in the side panel: `Chat` and `Models` (history panel behavior unchanged).
- [x] Add backend/model selector in Models tab including `MLX Local`.
- [x] Add editable hyperparameters for MLX (temperature, top_p, top_k, max_tokens, repetition penalty, seed).
- [x] Add apply/save controls and clear runtime status messages.
- [x] Add explicit `Restart MLX Session` action from the Models tab.

### 3) LoRA checkpoints (reload only, no merge)

- [x] Add broker-side adapter checkpoint registry persisted under broker data dir.
- [x] Support external training artifact import (metadata + filesystem path), not broker-run training.
- [x] Support adapter load/unload/reload for the active MLX runtime.
- [x] Enforce checkpoint-based updates only; do not merge adapter weights into base model in v1.

### 4) Broker API surface

- [x] Add `GET /models`.
- [x] Add `GET /mlx/status`.
- [x] Add `POST /mlx/config`.
- [x] Add `POST /mlx/session/restart`.
- [x] Add adapter endpoints:
  - `GET /mlx/adapters`
  - `POST /mlx/adapters/load`
  - `POST /mlx/adapters/unload`
- [x] Add matching extension background message types and sidepanel call sites.

### 5) Simple graphs

- [x] Add lightweight graphs in Models tab (no heavy chart dependency):
  - latency trend (rolling window)
  - tokens/sec trend (rolling window)
  - restart success/failure counts
- [x] Source graph data from broker MLX telemetry/status payloads.

### 6) Future loop hooks (deferred)

- [ ] Add TODO placeholders + data hooks for task/feedback/golden-example storage.
- [ ] Defer full task -> teacher -> golden-example loop automation to a later phase.
- [ ] Defer worker-subagent auto-detection/retry orchestration to a later phase.

## Verification Checklist

1. `MLX Local` appears in extension backend/model controls and requests route with `backend=mlx`.
2. Hyperparameter updates affect subsequent MLX responses without extension reload.
3. `Restart MLX Session` succeeds/fails with clear UI status and broker diagnostics.
4. Adapter checkpoint load/unload works and reload behavior is deterministic across restarts.
5. Graphs render empty state first, then update from broker runtime metrics.
6. Existing llama/codex flows, risk confirmation, allowlist gating, and history remain functional.

## Assumptions

- MLX runs as a broker-managed local worker process.
- LoRA training is external; broker handles checkpoint registry + reload only.
- Models tab is a top-level tab in the sidepanel UI.

# TODO: Codex Session-Scoped MCP Bridge Plan

## Goal

Expose browser tools to Codex when the extension uses broker `/route` + Codex CLI fallback, without global `codex mcp add` registration.

## Architecture Design

### 1) Runtime topology (session-scoped MCP only)

Request flow:

1. Sidepanel sends `assistant.query` (`backend=codex`) to extension background.
2. Background sends `/route` to broker with `allowed_hosts`.
3. Broker enters Codex CLI path and launches:
   - `codex exec ...` (new conversation), or
   - `codex exec resume <session_id> ...` (continuation)
4. Broker injects per-run Codex config overrides via `-c`:
   - `mcp_servers.browser_use.command`
   - `mcp_servers.browser_use.args`
   - `mcp_servers.browser_use.env`
5. Codex loads MCP server for that run only.
6. MCP server proxies browser calls back to broker `/browser/tools/call`.
7. Broker routes tool command to extension relay.
8. Extension executes Chrome API call and returns result.
9. Result returns through broker -> MCP server -> Codex -> broker response.

Key property:

- No persistent MCP registration in Codex global config.

### 2) Trust and policy boundaries

- Extension remains the only component touching Chrome APIs.
- Broker remains the policy authority:
  - loopback + required header gating
  - domain allowlist enforcement
  - command timeout handling
- MCP server is transport glue, not policy owner.

### 3) Configuration contract

Broker-owned toggles:

- `BROKER_CODEX_CLI_ENABLE_BROWSER_MCP`
- `BROKER_CODEX_CLI_BROWSER_MCP_NAME`
- `BROKER_CODEX_CLI_BROWSER_MCP_PYTHON`
- `BROKER_CODEX_CLI_BROWSER_MCP_SERVER_PATH`
- `BROKER_CODEX_CLI_BROWSER_MCP_BROKER_URL`
- `BROKER_CODEX_CLI_BROWSER_MCP_APPROVAL_MODE`

Behavior requirements:

- If relay is disconnected, broker can skip MCP injection for that run.
- If MCP server path is missing, broker falls back to normal Codex CLI behavior.

## Delivery Plan

### Phase A (implemented)

- Add broker config fields for Codex CLI MCP injection.
- Add helper builders for TOML-safe `-c` values.
- Inject per-run MCP overrides in both `codex exec` and `codex exec resume`.
- Pass extension allowlist into MCP env (`MCP_BROWSER_USE_ALLOWED_HOSTS`).
- Keep change local to broker CLI path (`/route` codex fallback).

### Phase B (next)

- Add structured broker logs for:
  - `codex_cli_mcp_injected=true/false`
  - skip reasons (`relay_disconnected`, `missing_server_path`, `disabled_by_env`)
- Surface this status in `/health` for easier operator debugging.

### Phase C (hardening)

- Add strict allowlist intersection checks between:
  - extension runtime allowlist
  - broker policy allowlist
  - MCP session policy
- Add integration test script for end-to-end Codex CLI + MCP browser action.

## Coding Guidance

- Keep broker request/response contract unchanged unless required.
- Prefer additive configuration over branching sidepanel protocols.
- Centralize MCP override construction in broker helper functions to avoid drift.
- Use absolute MCP server path in overrides; avoid cwd-sensitive command args.
- Never persist MCP registration through `codex mcp add/remove`.
- Maintain explicit failure behavior:
  - missing server path => skip injection
  - broker/tool failure => clear user-facing error message

## Verification Checklist

1. `codex mcp list` remains empty before and after extension requests.
2. Extension Codex CLI request with browser intent triggers browser action successfully.
3. MCP-injected run respects `allowed_hosts` from extension.
4. `BROKER_CODEX_CLI_ENABLE_BROWSER_MCP=false` disables injection cleanly.
5. Broker still works in:
   - Responses mode
   - CLI mode without MCP server path
   - Legacy command mode
