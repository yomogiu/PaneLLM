#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

if ! command -v python3 >/dev/null 2>&1; then
  echo "Python 3 is required to run broker/local_broker.py."
  exit 1
fi

BACKEND_HINT="No model backend detected in env."
if [[ -n "${OPENAI_API_KEY:-}" ]]; then
  BACKEND_HINT="OPENAI_API_KEY"
elif [[ -n "${LLAMA_URL:-}" || -n "${LLAMA_MODEL:-}" ]]; then
  BACKEND_HINT="LLAMA endpoint (LLAMA_URL)"
elif [[ -n "${MLX_URL:-}" || -n "${MLX_MODEL:-}" || -n "${MLX_API_KEY:-}" ]]; then
  BACKEND_HINT="MLX endpoint (MLX_URL)"
elif command -v codex >/dev/null 2>&1; then
  BACKEND_HINT="Codex CLI (run 'codex login' if needed)"
fi

echo "PaneLLM launch helper"
echo "Backend: $BACKEND_HINT"

if [[ "$BACKEND_HINT" == "No model backend detected in env." ]]; then
  echo "No backend is configured yet."
  echo "Set one now, then run this script again:"
  echo "  export OPENAI_API_KEY=\"...\""
  echo "  # or"
  echo "  export LLAMA_URL=\"http://127.0.0.1:18000/v1/chat/completions\""
  echo "  # or"
  echo "  export MLX_URL=\"http://127.0.0.1:8080/v1/chat/completions\""
  echo
fi

echo "Starting broker from ${ROOT_DIR}"
echo "Keep this terminal open while you use the side panel."
echo
echo "After startup:"
echo "- Open chrome://extensions"
echo "- Enable Developer mode"
echo "- Load unpacked extension from chrome_secure_panel/"
echo "- Open the side panel and start chatting"
echo

exec python3 broker/local_broker.py
