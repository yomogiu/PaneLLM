# Codex Interactive Session Integration Plan

## Summary
Replace the current one-shot `CODEX_COMMAND` path with a broker-managed OpenAI Responses API session engine for Codex. The broker remains the only component that holds OpenAI credentials and owns the model/tool loop; the Chrome extension continues to be the execution surface for browser capabilities. Ship this as a full session UI: persistent runs, live status/events, per-action approval, cancel/retry, and conversation continuity across reloads.

Do not wrap Codex as a plain subprocess for v1. That shape cannot safely support multi-turn tool loops, approvals, or durable run state. Keep the existing subprocess contract only as a deprecated fallback mode.

## Public Interface Changes
Add broker HTTP endpoints for Codex session orchestration:
- `POST /codex/runs`
  - Starts a Codex run for a conversation with prompt, optional page context, and per-run policy.
  - Returns `run_id`, `status`, `conversation_id`, `backend_metadata`.
- `GET /codex/runs/<run_id>/events?after=<seq>&timeout_ms=<n>`
  - Long-poll event feed for side panel live updates.
- `POST /codex/runs/<run_id>/approval`
  - Body: `{ "approval_id": "...", "decision": "approve|deny" }`
- `POST /codex/runs/<run_id>/cancel`
- Keep `POST /browser/tools/call` unchanged for manual testing and broker-internal tool execution.

Add extension runtime message types:
- `assistant.codex.run.start`
- `assistant.codex.run.events`
- `assistant.codex.run.approval`
- `assistant.codex.run.cancel`

Extend conversation persistence without breaking existing history rendering:
- Keep `messages` as user/assistant only.
- Add a `codex` metadata block on the conversation:
  - `mode`, `model`, `last_response_id`, `active_run_id`, `last_run_status`
- Persist run/event logs separately under broker data, keyed by `run_id`.

Add new environment variables without renaming existing ones:
- `OPENAI_API_KEY`
- `OPENAI_BASE_URL` optional
- `OPENAI_CODEX_MODEL` default `gpt-5.3-codex`
- `OPENAI_CODEX_REASONING_EFFORT` default `medium`
- `OPENAI_CODEX_MAX_OUTPUT_TOKENS`
- `BROKER_CODEX_RUN_TIMEOUT_SEC`
- `BROKER_CODEX_EVENT_POLL_TIMEOUT_MS`
- `BROKER_CODEX_ENABLE_BACKGROUND` default `false`

## Implementation Changes

### Broker orchestration
- Implement a Codex run worker in the broker that calls `POST /v1/responses` directly with stdlib HTTP, not a shell command.
- Use `previous_response_id` to continue the same interactive Codex chain per conversation.
- Stream or incrementally process Responses output inside the broker and translate it into local run events.
- Set `parallel_tool_calls=false` so browser actions remain deterministic and sequential.
- Define browser tools as strict function schemas with concise descriptions and `additionalProperties: false`.
- Use `tool_choice.allowed_tools` per run so only the extension capabilities intended for that run are exposed.
- Persist the last successful `response_id` only after a model step is durably stored.

### Tool loop
- Expose current extension-backed browser capabilities to Codex as custom functions:
  - `browser.navigate`
  - `browser.get_content`
  - `browser.get_tabs`
  - `browser.open_tab`
  - `browser.switch_tab`
  - `browser.close_tab`
  - `browser.focus_tab`
  - `browser.group_tabs`
  - `browser.describe_session_tabs`
  - `browser.click`
  - `browser.type`
  - `browser.press_key`
  - `browser.scroll`
- Do not expose lifecycle tools like `browser.session_create` or `browser.run_start` to Codex; the broker owns session/run creation.
- When Codex requests a tool call, broker validates arguments, checks allowlist and risk policy, then either:
  - executes immediately through the extension relay, or
  - emits a `waiting_approval` event and pauses the run.
- After tool completion, broker submits `function_call_output` back to Responses and continues until final assistant output or cancellation.

### Security policy
- Keep OpenAI credentials broker-side only; the extension never talks to OpenAI directly.
- Treat webpage content, selected text, tool output, and on-screen instructions as untrusted input.
- Approval policy for v1:
  - Auto-approve: `get_tabs`, `describe_session_tabs`, `get_content`, `scroll`, `switch_tab`, `focus_tab`
  - Manual approval: `navigate`, `open_tab`, `click`, `type`, `press_key`, `close_tab`, `group_tabs`
  - Deny if host is not in the broker session allowlist
- Keep the current high-risk regex, but move to a shared broker-owned action classifier so prompt risk and tool risk use one policy source.
- Show approval at action time, not only at prompt time, with action summary, host, selector/text preview when available, and explicit approve/deny controls.
- Add hard stops for suspicious content:
  - If a page or tool output appears to instruct the model to ignore prior policy, broker marks the run `blocked_for_review`.
  - Codex system instructions explicitly say only direct user messages grant permission.
- Do not enable Responses background mode by default because this stack is local-first/privacy-sensitive and background mode requires stored server-side response state. Keep it as an opt-in for later.

### UI and UX
- Side panel gets a Codex run timeline with statuses such as `thinking`, `calling_tool`, `waiting_approval`, `tool_result`, `completed`, `failed`, `cancelled`.
- Display partial assistant text and tool events live from the event feed.
- Approval cards are inline in the conversation, with approve/deny buttons and a cancel-run control.
- Preserve current simple llama flow; only the Codex backend uses the new run UI.
- Health status should distinguish:
  - `codex disabled`
  - `codex legacy command`
  - `codex responses ready`

### Documentation and migration
- Update broker and extension README/API docs to describe the new Codex run protocol, env vars, approval semantics, and event model.
- Mark `CODEX_COMMAND` as legacy/deprecated for non-interactive mode only.
- Document that Codex backend requires an OpenAI API key and uses the Responses API.

## Test Plan
Manual and automated scenarios:
- Broker health reports Codex Responses readiness when `OPENAI_API_KEY` is set.
- A Codex conversation survives page reload and resumes using the stored `last_response_id`.
- Codex can execute a read-only browser task end-to-end without manual approval.
- Codex pauses on a risky tool call, the side panel shows the approval card, and approve resumes the same run.
- Deny leaves the run in a safe terminal state and produces a clear assistant-visible explanation.
- Navigation or tab open to a non-allowlisted host is rejected before the extension executes anything.
- Extension disconnect or relay timeout surfaces as a run failure without losing the conversation.
- Cancel stops an in-flight run and prevents further tool execution.
- Old conversation history still loads correctly because `messages` remain user/assistant shaped.
- Legacy llama backend behavior remains unchanged.

## Assumptions And Defaults
- Target implementation is browser-tool integration through custom function calling, not OpenAI's built-in `computer` tool, because this repo already has an extension-backed browser harness and OpenAI recommends keeping an existing harness as a normal tool interface.
- Default Codex model is `gpt-5.3-codex` as of March 8, 2026; make it configurable.
- Default reasoning effort is `medium`.
- Default tool execution is sequential.
- Default privacy posture is synchronous broker-managed runs with local persistence; Responses background mode is opt-in, not default.
- Full session UI is in scope for the first implementation, including live event feed and per-action approval.

## References
- OpenAI Responses migration guide: https://developers.openai.com/api/docs/guides/migrate-to-responses
- OpenAI function calling guide: https://developers.openai.com/api/docs/guides/function-calling
- OpenAI tools guide: https://developers.openai.com/api/docs/guides/tools
- OpenAI computer use guide: https://developers.openai.com/api/docs/guides/tools-computer-use
- OpenAI background mode guide: https://developers.openai.com/api/docs/guides/background
- OpenAI GPT-5 model/tooling guidance: https://developers.openai.com/api/docs/guides/latest-model
- OpenAI Codex model docs: https://developers.openai.com/api/docs/models/gpt-5.3-codex
