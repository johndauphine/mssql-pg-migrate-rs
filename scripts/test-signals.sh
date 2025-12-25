#!/bin/bash
#
# Signal Handling Test Script
# Tests SIGINT (Ctrl-C) and SIGTERM handling for graceful shutdown
#
# Prerequisites:
#   - Running MSSQL and PostgreSQL instances with data
#   - Valid config file at the path specified
#
# Usage:
#   ./scripts/test-signals.sh [config-file] [signal] [delay]
#
# Arguments:
#   config-file - Path to config YAML (default: test-config.yaml)
#   signal      - Signal to send: INT or TERM (default: TERM)
#   delay       - Seconds to wait before sending signal (default: 5)

set -e

CONFIG_FILE="${1:-test-config.yaml}"
SIGNAL="${2:-TERM}"
DELAY="${3:-5}"
STATE_FILE="signal-test-state.json"
BINARY="./target/release/mssql-pg-migrate"

echo "=== Signal Handling Test ==="
echo "Config: $CONFIG_FILE"
echo "Signal: SIG$SIGNAL"
echo "Delay: ${DELAY}s"
echo ""

# Build release binary if needed
if [ ! -f "$BINARY" ]; then
    echo "Building release binary..."
    cargo build --release
fi

# Clean up old state file
rm -f "$STATE_FILE"

# Start migration in background
echo "Starting migration..."
$BINARY -c "$CONFIG_FILE" --state-file "$STATE_FILE" run &
PID=$!

echo "Migration PID: $PID"
echo "Waiting ${DELAY}s before sending signal..."
sleep "$DELAY"

# Check if process is still running
if ! kill -0 $PID 2>/dev/null; then
    echo "ERROR: Process already exited before signal could be sent"
    exit 1
fi

# Send signal
echo "Sending SIG$SIGNAL to PID $PID..."
kill -"$SIGNAL" $PID

# Wait for process to exit
echo "Waiting for graceful shutdown..."
set +e
wait $PID
EXIT_CODE=$?
set -e

echo ""
echo "=== Results ==="
echo "Exit code: $EXIT_CODE"

# Check expected exit code
if [ $EXIT_CODE -eq 5 ]; then
    echo "PASS: Exit code is 5 (cancelled)"
else
    echo "FAIL: Expected exit code 5, got $EXIT_CODE"
fi

# Check state file was saved
if [ -f "$STATE_FILE" ]; then
    echo "PASS: State file was created"
    echo ""
    echo "State file contents:"
    cat "$STATE_FILE" | head -20
else
    echo "FAIL: State file was not created"
fi

echo ""
echo "=== Cleanup ==="
rm -f "$STATE_FILE"
echo "Done"
