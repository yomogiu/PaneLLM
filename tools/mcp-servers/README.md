# MCP Servers

MCP servers in this folder expose local capabilities to models through the MCP tool API.

## Structure

Each server should live in its own subdirectory:

- `<server-name>/README.md`: setup, environment, and tool list.
- `<server-name>/server.py` (or equivalent): stdio MCP entrypoint.

## Included

- `browser-use/`: MCP bridge for browser tools executed via the extension relay and broker `/browser/tools/call` endpoint.
