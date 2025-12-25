#!/bin/bash
#
# Airflow Integration Test Script
# Simulates Airflow operator behavior with retry logic based on exit codes
#
# Prerequisites:
#   - Running MSSQL and PostgreSQL instances
#   - Valid config file at the path specified
#
# Usage:
#   ./scripts/test-airflow.sh [config-file] [state-file]
#
# Exit codes:
#   0 - Success
#   1 - Config error (no retry)
#   2 - Connection error (retry)
#   3 - Transfer error (retry with backoff)
#   4 - Validation error (no retry)
#   5 - Cancelled (manual intervention)
#   6 - State error (no retry)
#   7 - IO error (retry)

set -e

CONFIG_FILE="${1:-test-config.yaml}"
STATE_FILE="${2:-state.json}"
BINARY="./target/release/mssql-pg-migrate"

MAX_RETRIES=3
RETRY=0
BACKOFF=30

echo "=== Airflow Integration Test ==="
echo "Config: $CONFIG_FILE"
echo "State: $STATE_FILE"
echo ""

# Build release binary if needed
if [ ! -f "$BINARY" ]; then
    echo "Building release binary..."
    cargo build --release
fi

# Clean up old state file
rm -f "$STATE_FILE"

while [ $RETRY -lt $MAX_RETRIES ]; do
    echo "--- Attempt $((RETRY + 1)) of $MAX_RETRIES ---"

    set +e
    $BINARY -c "$CONFIG_FILE" --state-file "$STATE_FILE" --output-json run
    EXIT_CODE=$?
    set -e

    echo "Exit code: $EXIT_CODE"

    case $EXIT_CODE in
        0)
            echo "SUCCESS: Migration completed"
            exit 0
            ;;
        1)
            echo "FATAL: Config error - no retry"
            exit 1
            ;;
        2)
            echo "RECOVERABLE: Connection error - retrying in ${BACKOFF}s..."
            ((RETRY++))
            sleep $BACKOFF
            ;;
        3)
            echo "RECOVERABLE: Transfer error - retrying with backoff..."
            ((RETRY++))
            BACKOFF=$((BACKOFF * 2))
            sleep $BACKOFF
            ;;
        4)
            echo "FATAL: Validation error - investigate"
            exit 1
            ;;
        5)
            echo "CANCELLED: Task was cancelled"
            exit 0
            ;;
        6)
            echo "FATAL: State error - check state file"
            exit 1
            ;;
        7)
            echo "RECOVERABLE: IO error - retrying in ${BACKOFF}s..."
            ((RETRY++))
            sleep $BACKOFF
            ;;
        *)
            echo "UNKNOWN: Exit code $EXIT_CODE"
            exit 1
            ;;
    esac
done

echo "MAX RETRIES EXCEEDED"
exit 1
