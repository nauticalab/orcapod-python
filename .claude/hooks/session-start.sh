#!/bin/bash
set -euo pipefail

# Only run in remote (Claude Code on the web) environments
if [ "${CLAUDE_CODE_REMOTE:-}" != "true" ]; then
  exit 0
fi

# Install system dependencies required by pygraphviz
if ! dpkg -s libgraphviz-dev >/dev/null 2>&1; then
  sudo apt-get update -qq
  sudo apt-get install -y -qq graphviz libgraphviz-dev >/dev/null 2>&1
fi

# Install Python dependencies using uv
cd "$CLAUDE_PROJECT_DIR"
uv sync --group dev
