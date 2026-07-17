#!/usr/bin/env bash
set -euo pipefail
REPO_DIR="$(cd "$(dirname "$0")/.." && pwd)"
source "${REPO_DIR}/.venv/bin/activate"
exec gunicorn "qc.dashboard.app:app" \
    --bind 0.0.0.0:8080 \
    --workers 2 \
    --timeout 60 \
    --access-logfile - \
    --error-logfile -
