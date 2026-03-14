# P1/P2 Fix Plan

## Scope

This document replaces the earlier broad track note with the two active correctness fixes from review:

- P1: URL-based paper extraction should prefer the cited paper PDF when a landing page exposes one.
- P2: `prompt_eval` experiment jobs must not be labeled or stored as adapter runs.

## P1: Extract From Cited PDFs Instead Of Landing Pages

### Problem

For URL inputs such as `https://arxiv.org/abs/...`, paper inspect already discovers a `pdf_url`, but extraction still processes the landing-page HTML. That produces artifacts and digests polluted with site chrome, metadata panels, submission history, and download links instead of the paper body.

### Root Cause

- `inspect_source()` resolves metadata from the landing page and records `pdf_url`.
- `extract_source()` continues with the original HTML source instead of promoting the discovered PDF to the extraction source.
- The resulting stored artifact reflects the wrapper page, not the paper.

### Required Fix

When extraction starts from a URL-backed HTML landing page:

1. Inspect the landing page first.
2. If inspection yields a usable `pdf_url`, fetch that PDF and extract from it instead of the HTML page.
3. Preserve useful landing-page metadata from inspect:
   - title
   - authors
   - abstract
   - arXiv ID
   - discovered `pdf_url`
4. Mark the artifact so the final stored record reflects the actual extracted source format and URL.
5. Fall back to HTML extraction only when:
   - no `pdf_url` is present, or
   - the PDF fetch/extraction attempt fails and a controlled fallback is explicitly allowed.

### Implementation Notes

- The promotion should happen inside `broker/paper_worker.py`, close to `extract_source()`, because that worker already owns source resolution and extraction-format decisions.
- The worker should avoid mutating inspect behavior; `POST /papers/inspect` remains metadata-oriented.
- If a PDF promotion attempt fails, the error handling should make the decision obvious:
  - either fail fast with the PDF error, or
  - fall back to HTML only with an explicit reason recorded in the artifact/result path.
- The common case for arXiv and similar landing pages should become PDF-first.

### Validation

- `POST /papers/inspect` on an arXiv `abs` URL still returns landing-page metadata plus `pdf_url`.
- `POST /papers/jobs` on the same URL stores a paper artifact whose sections come from the paper body, not the landing page.
- The artifact should report PDF extraction as the effective extraction path.
- Regression coverage should include:
  - landing page with discovered `pdf_url`
  - landing page without `pdf_url`
  - PDF promotion failure behavior

## P2: Drop `adapter_path` From `prompt_eval` Jobs

### Problem

If the UI has an adapter path loaded or typed, `prompt_eval` jobs inherit that path into job summaries and persisted artifacts even though the experiment worker only loads adapters for `adapter_eval`. The run executes against the base model but is tagged like an adapter run, which corrupts the experiment library and comparison workflow.

### Root Cause

- The side panel includes `adapterPath` whenever one is present.
- `ExperimentManager.start_job()` resolves and persists that adapter path before kind-specific filtering.
- `prompt_eval` job summaries and saved artifacts therefore carry adapter metadata they did not use.

### Required Fix

For `prompt_eval`:

1. Ignore `adapter_path` and `adapter_id` during job setup.
2. Persist an empty adapter path in:
   - async job summary
   - worker payload
   - saved experiment artifact
3. Keep adapter resolution mandatory only for `adapter_eval`.
4. Ensure UI/job labels use base-model semantics for `prompt_eval` even if an adapter is loaded in the interactive MLX runtime.

### Implementation Notes

- The normalization should happen in `broker/local_broker.py` inside `ExperimentManager.start_job()`.
- The kind should be resolved before adapter metadata is carried forward.
- `experiment_worker.py` should continue to receive `adapter_path` only for `adapter_eval`.

### Validation

- Starting `prompt_eval` with an adapter loaded still runs successfully but produces:
  - no adapter tag in the job list
  - empty `adapter_path` in the stored artifact
  - base-model semantics in compare views
- Starting `adapter_eval` without an adapter still fails validation.
- Starting `adapter_eval` with an adapter still produces paired base/adapter results.

## Acceptance Criteria

- P1: URL-based paper jobs for arXiv-style landing pages persist paper-body sections and digests derived from the cited PDF.
- P2: `prompt_eval` records are never mislabeled as adapter runs.
- Both fixes are covered by focused automated tests or deterministic smoke checks.
