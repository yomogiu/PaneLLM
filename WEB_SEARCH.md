# Web Search And Browser Research Architecture

## Status

Nice to have.

This document describes an optional future architecture, not a required implementation gap. The current extension and broker already cover the common short-form browsing workflow well enough for home and single-user use.

## Summary

Adopt a hybrid architecture where model backends plan and reason, but the broker remains the only component that executes browser actions, enforces policy, and owns task state. Extend the current single-turn `/route` flow into a broker-managed task loop that supports multi-step, multi-page work such as literature research, product and price collection, and cross-tab synthesis. Keep the current simple chat path for ordinary prompts; only task-shaped requests enter the richer orchestration path.

Default design choices:
- Broker remains the execution and security boundary.
- Both `llama` and future `codex` integrations use the same broker-owned browser session and run model.
- Tasks are explicit broker objects with progress, collected findings, and stop conditions.
- Browser actions return structured observations, not only success text.

## Current Assessment

The current architecture is already in a good place for the main use case:

- The extension remains intentionally thin and works well as a browser relay, page-context capture layer, and approval surface.
- Most additional complexity proposed here belongs in the broker, not in the extension.
- The broker is efficient enough for home and single-user use in its current local-first form.
- The current request/run model is already sufficient for ordinary chat, short browser actions, and interactive browsing flows.

The main tradeoff is complexity, not missing basic capability. Adding tasks, routing, and task APIs would expand long-running orchestration features, but would also increase maintenance cost, persistence surface, and UI state handling.

## When This Becomes Worth Doing

Revisit this architecture when there is a concrete need for one or more of the following:

- Long-running multi-step research or shopping workflows that outgrow a single request/run loop.
- Persistent collected findings, artifacts, or deduplicated items across many browsing steps.
- Partial-result progress UI for work that may take multiple pages or many tool turns.
- A shared planner contract so `llama` and `codex` use the same broker-managed task model.

## Why Not Now

Do not treat this file as the default next step while the current flow is working well.

- The current extension is not obviously bloated and does not need a task system to stay useful.
- Task routing and task APIs add real broker complexity without much payoff for short interactive browsing.
- The largest benefit here is for persistent research orchestration, not ordinary assistant use.
- The repo should only absorb this design once the simpler request/run model proves insufficient in practice.

## Key Changes

### 1. Add a broker-owned task abstraction

Add a broker task layer above the current conversation turn model.

Task responsibilities:
- Represent a long-running user objective across many tool and model steps.
- Track status: `queued | running | waiting_confirmation | completed | failed | cancelled`.
- Track backend: `llama | codex`.
- Track browser session and run ids plus the policy snapshot.
- Track accumulated findings and intermediate artifacts.
- Track step budget, timeout budget, and completion criteria.
- Persist enough state to resume or inspect a task after interruption.

Suggested task shape:
- `task_id`
- `conversation_id`
- `backend`
- `goal`
- `task_type`
- `status`
- `policy`
- `session_id`
- `run_id`
- `planner_messages`
- `artifacts`
- `collected_items`
- `last_observation`
- `step_count`
- `max_steps`
- `created_at`
- `updated_at`

Default task types:
- `search`
- `research`
- `shopping_compare`
- `browse_extract`
- `generic_agent`

Task creation rule:
- Plain question and answer stays on `/route`.
- Requests that imply multi-step browsing, comparison, extraction, or repeated search create a task.

### 2. Separate planning from browser execution

Create a backend planner interface inside the broker so `llama` and `codex` can both drive the same broker loop.

Planner contract:
- Input:
  - task goal
  - prior planner messages
  - browser tool schema
  - current broker observations
  - collected findings so far
  - completion criteria
- Output:
  - final answer, or
  - one or more tool calls, or
  - a structured broker action such as `complete`, `ask_user`, `refine_plan`, or `record_item`

Keep the broker in charge of:
- tool execution
- retries and timeouts
- browser sessions
- allowlist enforcement
- high-risk confirmation
- state persistence
- step limits and cancellation

For Codex:
- Extend the `CODEX_COMMAND` protocol from plain `{session_id, prompt, messages}` to a versioned planner payload.
- Support both old and new protocol versions during migration.
- If the Codex wrapper only supports the old format, keep existing plain-answer behavior.

Recommended versioned planner I/O:
- Request includes `protocol_version`, `task`, `messages`, `tools`, `observations`, and `artifacts`.
- Response includes either `answer` or `tool_calls` plus optional `task_updates`.

### 3. Upgrade browser and tool outputs into structured observations

Current click, type, and navigate responses are too thin for long tasks. Expand broker-fed observations so the planner can decide reliably when to stop.

Required observation envelope after browser actions:
- current URL
- page title
- tab id
- action result metadata
- lightweight page summary or visible text excerpt
- optional extracted structured entities where available

Add broker-level post-action observation policies:
- After `navigate` and successful search submission, automatically capture a page summary.
- After `click`, detect whether URL, title, or content changed and capture a fresh observation.
- After opening or searching tabs, include tab inventory so the planner can revisit sources.

New high-value tool and observation capabilities:
- `browser.get_visible_text`
- `browser.extract_links`
- `browser.extract_search_results`
- `browser.extract_product_cards`
- `browser.wait_for_navigation_or_dom_change`
- `browser.get_page_snapshot`

Prefer broker-owned extraction helpers for repeated patterns:
- search result cards
- paper metadata
- product title, price, rating, and seller cards

This reduces backend prompt burden and improves consistency across `llama` and `codex`.

### 4. Add task memory and artifact accumulation

Long tasks need working memory separate from raw message history.

Broker should maintain:
- `collected_items`
- `notes`
- `visited_urls`
- `open_tabs`
- `dedupe_keys`

Examples:
- Research task item:
  - `title`, `authors`, `source`, `url`, `abstract_excerpt`, `year`, `confidence`
- Shopping comparison item:
  - `brand`, `product_name`, `size`, `price`, `currency`, `seller`, `url`, `availability`, `confidence`

Broker behaviors:
- Deduplicate repeated findings.
- Allow planner to append or update items rather than re-scrape everything.
- Feed compact summaries of collected items back into the planner each step.
- Persist artifacts so the final answer can be regenerated or inspected.

### 5. Define completion rules and stop conditions

Do not rely on the model to infer "done" from raw browsing alone. Each task should have explicit broker-side completion criteria.

Default rules:
- `search`: complete when the target results page is loaded unless the user asked for synthesis.
- `research`: complete when enough candidate sources are collected and summarized, or step budget is reached with partial results.
- `shopping_compare`: complete when each requested item or brand has at least one normalized price result, or unavailable status is established.
- `browse_extract`: complete when the requested fields are extracted from the page or pages.

Task policies should include:
- `max_steps`
- `max_tabs`
- `per_host_limits`
- `required_fields`
- `minimum_items`
- `finish_mode`: `open_results | summarize | compare | exhaustive_with_budget`

If the task ends on budget exhaustion:
- return partial results
- include what was completed
- state what remains unresolved
- do not return a generic failure if useful data was already collected

### 6. Extend the extension and broker APIs minimally

Keep the UI simple, but add task-aware response support.

Broker API additions:
- `POST /tasks/start`
- `GET /tasks/<id>`
- `POST /tasks/<id>/cancel`
- optional `GET /tasks/<id>/events`

`/route` behavior:
- remains for ordinary chat
- may return a `task_started` response for long-running work
- may stream or poll task progress later if desired

Side panel additions:
- show task progress for active tasks
- show states such as `researching`, `collecting_prices`, or `waiting_confirmation`
- allow cancellation
- render partial results before final completion

Do not expose browser tool execution directly to the UI beyond current broker-mediated patterns.

## Test Plan

### Core orchestration
- Start a research task and verify the broker creates task state, browser session, and planner loop.
- Start a shopping comparison task and verify collected items persist across multiple tool steps.
- Cancel a running task and verify the browser run and session are cleaned up.

### Completion behavior
- "Search Google for Iran news" completes when the results page is open under `search` finish mode.
- "Research papers written by Google DeepMind" gathers multiple sources and returns a synthesized summary with a source list.
- "Go to Amazon and look up prices of these cookie brands" returns a normalized comparison table with one row per requested brand.

### Failure and recovery
- Search page loads but extraction fails: the task returns a partial result with a clear error.
- A host is not on the allowlist: the task fails safely with an explicit policy reason.
- The planner exceeds the step budget: the task returns partial findings, not only a generic timeout or failure.
- The extension disconnects mid-task: the task moves to failed or interrupted state and can be retried or resumed according to policy.

### Backend compatibility
- `llama` works with the new planner loop.
- Old `codex` wrappers still work through legacy plain-answer mode.
- New `codex` planner protocol can issue tool calls through the same broker execution path.

### Safety
- High-risk actions still require confirmation during long tasks.
- Non-allowlisted domains are blocked consistently for both backends.
- Purchase and checkout flows are never auto-completed without explicit confirmation.

## Assumptions And Defaults

- Recommended architecture is hybrid, with broker-owned execution and backend-owned planning.
- Security boundary stays at the broker; backends do not directly execute browser tools.
- Legacy Codex wrappers remain supported until a versioned planner protocol is introduced.
- Long-running tasks should return partial useful output on budget exhaustion.
- Broker-managed extraction helpers are preferred over asking models to parse raw HTML repeatedly.
- Initial task routing can be heuristic-based, then replaced later by an explicit classifier if needed.
