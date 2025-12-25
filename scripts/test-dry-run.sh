#!/bin/bash
#
# Dry-Run Test Script
# Tests the --dry-run flag to ensure no data is transferred
#
# Prerequisites:
#   - Running MSSQL and PostgreSQL instances
#   - Valid config file at the path specified
#   - Target database should have existing tables to verify no changes
#
# Usage:
#   ./scripts/test-dry-run.sh [config-file]

set -e

CONFIG_FILE="${1:-test-config.yaml}"
BINARY="./target/release/mssql-pg-migrate"

echo "=== Dry-Run Tests ==="
echo "Config: $CONFIG_FILE"
echo ""

# Build release binary if needed
if [ ! -f "$BINARY" ]; then
    echo "Building release binary..."
    cargo build --release
fi

# Test 1: Basic dry-run (text output)
echo "--- Test 1: Basic dry-run (text output) ---"
set +e
$BINARY -c "$CONFIG_FILE" run --dry-run
EXIT_CODE=$?
set -e
echo ""
echo "Exit code: $EXIT_CODE"
if [ $EXIT_CODE -eq 0 ]; then
    echo "PASS: Dry-run completed successfully"
else
    echo "FAIL: Dry-run failed with exit code $EXIT_CODE"
fi
echo ""

# Test 2: JSON output
echo "--- Test 2: Dry-run with JSON output ---"
set +e
OUTPUT=$($BINARY -c "$CONFIG_FILE" --output-json run --dry-run)
EXIT_CODE=$?
set -e
echo "Exit code: $EXIT_CODE"
echo "Output:"
echo "$OUTPUT" | head -30
echo ""

# Validate JSON structure and dry_run status
if echo "$OUTPUT" | jq -e '.status == "dry_run"' > /dev/null 2>&1; then
    echo "PASS: Status is 'dry_run'"
else
    echo "FAIL: Status should be 'dry_run'"
fi

if echo "$OUTPUT" | jq -e '.rows_transferred == 0' > /dev/null 2>&1; then
    echo "PASS: rows_transferred is 0"
else
    echo "FAIL: rows_transferred should be 0 in dry-run"
fi

if echo "$OUTPUT" | jq -e '.tables_total > 0' > /dev/null 2>&1; then
    TABLES=$(echo "$OUTPUT" | jq -r '.tables_total')
    echo "PASS: Found $TABLES tables to process"
else
    echo "FAIL: Should have found tables to process"
fi

echo ""
echo "=== Dry-Run Tests Complete ==="
