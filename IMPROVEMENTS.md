# Retired Paper Pipeline

The legacy broker paper workflow is no longer active:

- `/papers/inspect` and `/papers/jobs` broker routes are retired.
- `broker/paper_worker.py` and paper-side persistence paths are no longer in use.
- The chat-first read assistant path (`assistant.read.context.capture`) is now the supported page-reading flow.

For active improvements, use the existing read-assistant + chat workflows and experiment/training broker jobs.
