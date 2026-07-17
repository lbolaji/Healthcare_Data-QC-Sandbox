#!/usr/bin/env bash
set -euo pipefail

BUCKET="${QC_SOURCE_BUCKET:?QC_SOURCE_BUCKET not set}"
PREFIX="${QC_SOURCE_PREFIX:-landing}"
SENTINEL_FILE="${QC_HISTORY_PATH:-/data/history}/.last_run_date"
TODAY=$(date -u +%Y-%m-%d)
REPO_DIR="$(cd "$(dirname "$0")/.." && pwd)"

# Check if already ran today
if [[ -f "$SENTINEL_FILE" ]] && [[ "$(cat "$SENTINEL_FILE")" == "$TODAY" ]]; then
    echo "[$TODAY] already ran today — skipping"
    exit 0
fi

# Check if today's data has landed in S3
KEY_COUNT=$(aws s3 ls "s3://${BUCKET}/${PREFIX}/date=${TODAY}/" 2>/dev/null | wc -l || echo 0)
if [[ "$KEY_COUNT" -eq 0 ]]; then
    echo "[$TODAY] no data found at s3://${BUCKET}/${PREFIX}/date=${TODAY}/ — waiting"
    exit 0
fi

echo "[$TODAY] data found — starting QC run"
source "${REPO_DIR}/.venv/bin/activate"
python -m qc.main --date "$TODAY"
STATUS=$?

if [[ $STATUS -eq 0 ]]; then
    echo "$TODAY" > "$SENTINEL_FILE"
    echo "[$TODAY] QC run completed successfully"
else
    echo "[$TODAY] QC run failed with exit code $STATUS — will retry next poll"
fi

exit $STATUS
