# Tools

This directory is the integration point for local tool adapters.

Current layout:

- `mcp-servers/`: Model Context Protocol servers that expose tool surfaces to MCP-capable clients.

Conventions:

- Keep each integration in its own subdirectory with a focused `README.md`.
- Prefer dependency-light implementations (stdlib where practical).
- Keep localhost trust and broker header requirements intact when calling the broker.
