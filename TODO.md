# TODO: Backend Endpoint Parity

## Goal

Keep `mlx` as a peer of `llama.cpp`: a selectable OpenAI-compatible endpoint used through the unified `/runs` surface for chat and broker-mediated browser tool calls.

## Current guardrails

- [x] Extension backend selector exposes `codex`, `llama`, and `mlx`.
- [x] Extension does not manage MLX runtime lifecycle, adapters, training, or experiments.
- [x] Broker treats MLX as another local backend discovered from `MLX_URL` / `MLX_MODEL` / `MLX_API_KEY`.
- [x] Browser tools continue to route through the shared broker relay regardless of selected backend.

## Follow-up ideas

- [ ] Keep backend capability metadata (`GET /models`) focused on selector/runtime compatibility only.
- [ ] Preserve parity between `llama` and `mlx` request options where the shared local-backend path supports it.
- [ ] Continue removing stale docs or notes if older runtime-management language resurfaces.

# TODO: Codex Session-Scoped MCP Bridge Plan

## Goal

Expose browser tools to Codex when the extension uses broker `/runs` + Codex CLI fallback, without global `codex mcp add` registration.

## Architecture Design

### 1) Runtime topology (session-scoped MCP only)

Request flow:

1. Sidepanel sends `assistant.run.start` (`backend=codex`) to extension background.
2. Background sends `POST /runs` to broker with `allowed_hosts`.
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
- Keep change local to broker CLI-backed run path.

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
   - CLI mode without browser MCP injection
