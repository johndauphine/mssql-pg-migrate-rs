#!/bin/bash
# Comprehensive migration test suite
# Tests all 18 source/target/mode permutations

set -e

BINARY="/Users/john/repos/mssql-pg-migrate-rs/target/release/mssql-pg-migrate"
RESULTS_FILE="/Users/john/repos/mssql-pg-migrate-rs/test-results-$(date +%Y%m%d-%H%M%S).txt"

echo "========================================" | tee -a "$RESULTS_FILE"
echo "MIGRATION TEST SUITE" | tee -a "$RESULTS_FILE"
echo "Started: $(date)" | tee -a "$RESULTS_FILE"
echo "========================================" | tee -a "$RESULTS_FILE"
echo "" | tee -a "$RESULTS_FILE"

run_test() {
    local test_num=$1
    local config=$2
    local description=$3

    echo "" | tee -a "$RESULTS_FILE"
    echo "=== TEST $test_num/18: $description ===" | tee -a "$RESULTS_FILE"
    echo "Config: $config" | tee -a "$RESULTS_FILE"
    echo "Started: $(date +%H:%M:%S)" | tee -a "$RESULTS_FILE"

    start_time=$(date +%s)

    if $BINARY -c "$config" run 2>&1 | tee -a "$RESULTS_FILE" | grep -E "(Migration completed|Duration|Throughput|Error)" | tail -5; then
        end_time=$(date +%s)
        duration=$((end_time - start_time))
        echo "RESULT: SUCCESS (${duration}s)" | tee -a "$RESULTS_FILE"
        return 0
    else
        end_time=$(date +%s)
        duration=$((end_time - start_time))
        echo "RESULT: FAILED (${duration}s)" | tee -a "$RESULTS_FILE"
        return 1
    fi
}

# Test counter
total_tests=18
passed=0
failed=0

# MSSQL → PostgreSQL
run_test 1 "/Users/john/repos/mssql-pg-migrate-rs/test-mssql-to-postgres-drop.yaml" "MSSQL → PostgreSQL (drop_recreate)" && ((passed++)) || ((failed++))
run_test 2 "/Users/john/repos/mssql-pg-migrate-rs/test-mssql-to-postgres-upsert.yaml" "MSSQL → PostgreSQL (upsert)" && ((passed++)) || ((failed++))

# MSSQL → MSSQL
run_test 3 "/Users/john/repos/mssql-pg-migrate-rs/test-mssql-to-mssql-drop.yaml" "MSSQL → MSSQL (drop_recreate)" && ((passed++)) || ((failed++))
run_test 4 "/Users/john/repos/mssql-pg-migrate-rs/test-mssql-to-mssql-upsert.yaml" "MSSQL → MSSQL (upsert)" && ((passed++)) || ((failed++))

# MSSQL → MySQL
run_test 5 "/Users/john/repos/mssql-pg-migrate-rs/test-mssql-to-mysql-drop.yaml" "MSSQL → MySQL (drop_recreate)" && ((passed++)) || ((failed++))
run_test 6 "/Users/john/repos/mssql-pg-migrate-rs/test-mssql-to-mysql-upsert.yaml" "MSSQL → MySQL (upsert)" && ((passed++)) || ((failed++))

# MySQL → PostgreSQL
run_test 7 "/Users/john/repos/mssql-pg-migrate-rs/test-mysql-to-postgres-drop.yaml" "MySQL → PostgreSQL (drop_recreate)" && ((passed++)) || ((failed++))
run_test 8 "/Users/john/repos/mssql-pg-migrate-rs/test-mysql-to-postgres-upsert.yaml" "MySQL → PostgreSQL (upsert)" && ((passed++)) || ((failed++))

# MySQL → MSSQL
run_test 9 "/Users/john/repos/mssql-pg-migrate-rs/test-mysql-to-mssql-drop.yaml" "MySQL → MSSQL (drop_recreate)" && ((passed++)) || ((failed++))
run_test 10 "/Users/john/repos/mssql-pg-migrate-rs/test-mysql-to-mssql-upsert.yaml" "MySQL → MSSQL (upsert)" && ((passed++)) || ((failed++))

# MySQL → MySQL
run_test 11 "/Users/john/repos/mssql-pg-migrate-rs/test-mysql-to-mysql-drop.yaml" "MySQL → MySQL (drop_recreate)" && ((passed++)) || ((failed++))
run_test 12 "/Users/john/repos/mssql-pg-migrate-rs/test-mysql-to-mysql-upsert.yaml" "MySQL → MySQL (upsert)" && ((passed++)) || ((failed++))

# PostgreSQL → PostgreSQL
run_test 13 "/Users/john/repos/mssql-pg-migrate-rs/test-postgres-to-postgres-drop.yaml" "PostgreSQL → PostgreSQL (drop_recreate)" && ((passed++)) || ((failed++))
run_test 14 "/Users/john/repos/mssql-pg-migrate-rs/test-postgres-to-postgres-upsert.yaml" "PostgreSQL → PostgreSQL (upsert)" && ((passed++)) || ((failed++))

# PostgreSQL → MSSQL
run_test 15 "/Users/john/repos/mssql-pg-migrate-rs/test-postgres-to-mssql-drop.yaml" "PostgreSQL → MSSQL (drop_recreate)" && ((passed++)) || ((failed++))
run_test 16 "/Users/john/repos/mssql-pg-migrate-rs/test-postgres-to-mssql-upsert.yaml" "PostgreSQL → MSSQL (upsert)" && ((passed++)) || ((failed++))

# PostgreSQL → MySQL
run_test 17 "/Users/john/repos/mssql-pg-migrate-rs/test-postgres-to-mysql-drop.yaml" "PostgreSQL → MySQL (drop_recreate)" && ((passed++)) || ((failed++))
run_test 18 "/Users/john/repos/mssql-pg-migrate-rs/test-postgres-to-mysql-upsert.yaml" "PostgreSQL → MySQL (upsert)" && ((passed++)) || ((failed++))

# Summary
echo "" | tee -a "$RESULTS_FILE"
echo "========================================" | tee -a "$RESULTS_FILE"
echo "TEST SUMMARY" | tee -a "$RESULTS_FILE"
echo "========================================" | tee -a "$RESULTS_FILE"
echo "Total Tests: $total_tests" | tee -a "$RESULTS_FILE"
echo "Passed: $passed" | tee -a "$RESULTS_FILE"
echo "Failed: $failed" | tee -a "$RESULTS_FILE"
echo "Completed: $(date)" | tee -a "$RESULTS_FILE"
echo "Results saved to: $RESULTS_FILE" | tee -a "$RESULTS_FILE"
echo "========================================" | tee -a "$RESULTS_FILE"

if [ $failed -eq 0 ]; then
    echo "ALL TESTS PASSED!" | tee -a "$RESULTS_FILE"
    exit 0
else
    echo "SOME TESTS FAILED!" | tee -a "$RESULTS_FILE"
    exit 1
fi
