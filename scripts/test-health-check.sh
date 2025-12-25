#!/bin/bash
#
# Health Check Test Script
# Tests the health-check command with various scenarios
#
# Prerequisites:
#   - Running MSSQL and PostgreSQL instances
#   - Valid config file at the path specified
#
# Usage:
#   ./scripts/test-health-check.sh [config-file]

set -e

CONFIG_FILE="${1:-test-config.yaml}"
BINARY="./target/release/mssql-pg-migrate"

echo "=== Health Check Tests ==="
echo "Config: $CONFIG_FILE"
echo ""

# Build release binary if needed
if [ ! -f "$BINARY" ]; then
    echo "Building release binary..."
    cargo build --release
fi

# Test 1: Basic health check (text output)
echo "--- Test 1: Basic health check (text output) ---"
set +e
$BINARY -c "$CONFIG_FILE" health-check
EXIT_CODE=$?
set -e
echo "Exit code: $EXIT_CODE"
if [ $EXIT_CODE -eq 0 ]; then
    echo "PASS: Health check succeeded"
else
    echo "FAIL: Health check failed with exit code $EXIT_CODE"
fi
echo ""

# Test 2: JSON output
echo "--- Test 2: Health check with JSON output ---"
set +e
OUTPUT=$($BINARY -c "$CONFIG_FILE" --output-json health-check)
EXIT_CODE=$?
set -e
echo "Exit code: $EXIT_CODE"
echo "Output:"
echo "$OUTPUT" | head -20
echo ""

# Validate JSON structure
if echo "$OUTPUT" | jq -e '.healthy' > /dev/null 2>&1; then
    echo "PASS: Output is valid JSON with 'healthy' field"
else
    echo "FAIL: Output is not valid JSON or missing 'healthy' field"
fi

if echo "$OUTPUT" | jq -e '.source_connected' > /dev/null 2>&1; then
    echo "PASS: Output has 'source_connected' field"
else
    echo "FAIL: Output missing 'source_connected' field"
fi

if echo "$OUTPUT" | jq -e '.target_connected' > /dev/null 2>&1; then
    echo "PASS: Output has 'target_connected' field"
else
    echo "FAIL: Output missing 'target_connected' field"
fi

if echo "$OUTPUT" | jq -e '.source_latency_ms >= 0' > /dev/null 2>&1; then
    echo "PASS: Output has 'source_latency_ms' >= 0"
else
    echo "FAIL: Output missing or invalid 'source_latency_ms'"
fi

echo ""
echo "=== Health Check Tests Complete ==="
