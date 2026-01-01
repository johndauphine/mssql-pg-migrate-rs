#!/bin/bash
# Benchmark: Rust vs Airflow/Python migration performance
# Dataset: StackOverflow2010 (~19.3M rows)

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
AIRFLOW_DIR="/Users/john/repos/mssql-to-postgres-pipeline"
RESULTS_FILE="$SCRIPT_DIR/benchmark-results.md"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}  Migration Benchmark: Rust vs Airflow ${NC}"
echo -e "${BLUE}========================================${NC}"
echo ""

# Function to get PostgreSQL row counts
get_pg_row_count() {
    docker exec postgres-target psql -U postgres -d stackoverflow -t -c "
        SELECT COALESCE(SUM(n_live_tup), 0)::bigint
        FROM pg_stat_user_tables
        WHERE schemaname = 'public'
    " 2>/dev/null | tr -d ' '
}

# Function to truncate all tables in PostgreSQL
reset_postgres() {
    echo -e "${YELLOW}Resetting PostgreSQL target database...${NC}"
    docker exec postgres-target psql -U postgres -d stackoverflow -c "
        DO \$\$
        DECLARE
            r RECORD;
        BEGIN
            FOR r IN (SELECT tablename FROM pg_tables WHERE schemaname = 'public') LOOP
                EXECUTE 'DROP TABLE IF EXISTS public.' || quote_ident(r.tablename) || ' CASCADE';
            END LOOP;
        END \$\$;
    " 2>/dev/null
    echo -e "${GREEN}PostgreSQL reset complete${NC}"
}

# Function to run Rust migration
run_rust_benchmark() {
    echo -e "\n${BLUE}--- Running Rust Migration ---${NC}"

    cd "$SCRIPT_DIR"

    # Ensure binary exists
    if [ ! -f "./mssql-pg-migrate" ]; then
        echo -e "${RED}Error: mssql-pg-migrate binary not found. Run: cargo build --release --features tui && cp target/release/mssql-pg-migrate .${NC}"
        exit 1
    fi

    START_TIME=$(date +%s.%N)

    ./mssql-pg-migrate -c benchmark-config.yaml run 2>&1 | tee /tmp/rust_benchmark.log

    END_TIME=$(date +%s.%N)
    RUST_DURATION=$(echo "$END_TIME - $START_TIME" | bc)
    RUST_ROWS=$(get_pg_row_count)
    RUST_THROUGHPUT=$(echo "scale=0; $RUST_ROWS / $RUST_DURATION" | bc)

    echo -e "${GREEN}Rust Migration Complete${NC}"
    echo -e "  Duration: ${RUST_DURATION}s"
    echo -e "  Rows: ${RUST_ROWS}"
    echo -e "  Throughput: ${RUST_THROUGHPUT} rows/sec"

    # Export for report
    export RUST_DURATION RUST_ROWS RUST_THROUGHPUT
}

# Function to run Airflow migration
run_airflow_benchmark() {
    echo -e "\n${BLUE}--- Running Airflow/Python Migration ---${NC}"

    # Check if Airflow is running
    if ! curl -s http://localhost:8080/health > /dev/null 2>&1; then
        echo -e "${RED}Error: Airflow is not running. Start it with: cd $AIRFLOW_DIR && astro dev start${NC}"
        exit 1
    fi

    # Trigger the DAG via API
    echo "Triggering Airflow DAG..."

    # Get auth token (basic auth for local Astro)
    AUTH="admin:admin"

    START_TIME=$(date +%s.%N)

    # Trigger DAG run
    DAG_RUN=$(curl -s -X POST "http://localhost:8080/api/v1/dags/mssql_to_postgres_migration/dagRuns" \
        -H "Content-Type: application/json" \
        -u "$AUTH" \
        -d '{"conf": {}}')

    DAG_RUN_ID=$(echo "$DAG_RUN" | python3 -c "import sys, json; print(json.load(sys.stdin).get('dag_run_id', ''))" 2>/dev/null || echo "")

    if [ -z "$DAG_RUN_ID" ]; then
        echo -e "${RED}Failed to trigger DAG. Response: $DAG_RUN${NC}"
        echo -e "${YELLOW}Skipping Airflow benchmark...${NC}"
        export AIRFLOW_DURATION="N/A"
        export AIRFLOW_ROWS="N/A"
        export AIRFLOW_THROUGHPUT="N/A"
        return
    fi

    echo "DAG Run ID: $DAG_RUN_ID"
    echo "Waiting for DAG to complete..."

    # Poll for completion
    while true; do
        STATUS=$(curl -s "http://localhost:8080/api/v1/dags/mssql_to_postgres_migration/dagRuns/$DAG_RUN_ID" \
            -u "$AUTH" | python3 -c "import sys, json; print(json.load(sys.stdin).get('state', 'unknown'))" 2>/dev/null || echo "unknown")

        if [ "$STATUS" = "success" ] || [ "$STATUS" = "failed" ]; then
            break
        fi

        echo -n "."
        sleep 5
    done
    echo ""

    END_TIME=$(date +%s.%N)
    AIRFLOW_DURATION=$(echo "$END_TIME - $START_TIME" | bc)

    if [ "$STATUS" = "success" ]; then
        AIRFLOW_ROWS=$(get_pg_row_count)
        AIRFLOW_THROUGHPUT=$(echo "scale=0; $AIRFLOW_ROWS / $AIRFLOW_DURATION" | bc)
        echo -e "${GREEN}Airflow Migration Complete${NC}"
    else
        AIRFLOW_ROWS="FAILED"
        AIRFLOW_THROUGHPUT="N/A"
        echo -e "${RED}Airflow Migration Failed${NC}"
    fi

    echo -e "  Duration: ${AIRFLOW_DURATION}s"
    echo -e "  Rows: ${AIRFLOW_ROWS}"
    echo -e "  Throughput: ${AIRFLOW_THROUGHPUT} rows/sec"

    export AIRFLOW_DURATION AIRFLOW_ROWS AIRFLOW_THROUGHPUT
}

# Function to generate results report
generate_report() {
    echo -e "\n${BLUE}--- Generating Report ---${NC}"

    cat > "$RESULTS_FILE" << EOF
# Benchmark Results: Rust vs Airflow

**Date:** $(date -u +"%Y-%m-%d %H:%M:%S UTC")
**Dataset:** StackOverflow2010 (~19.3M rows)

## Environment

| Component | Details |
|-----------|---------|
| Host | $(uname -m) macOS |
| MSSQL | SQL Server 2022 (Docker) |
| PostgreSQL | PostgreSQL 16 (Docker, port 5433) |
| Rust Config | 6 workers, 12 readers, 8 writers, 100K chunks, UNLOGGED |
| Airflow Config | 9 parallel tasks, 10K chunks |

## Results

| Metric | Rust | Airflow/Python | Improvement |
|--------|------|----------------|-------------|
| Duration | ${RUST_DURATION}s | ${AIRFLOW_DURATION}s | $(echo "scale=2; $AIRFLOW_DURATION / $RUST_DURATION" | bc 2>/dev/null || echo "N/A")x faster |
| Rows Migrated | ${RUST_ROWS} | ${AIRFLOW_ROWS} | - |
| Throughput | ${RUST_THROUGHPUT} rows/sec | ${AIRFLOW_THROUGHPUT} rows/sec | $(echo "scale=2; $RUST_THROUGHPUT / $AIRFLOW_THROUGHPUT" | bc 2>/dev/null || echo "N/A")x |

## Notes

- Rust uses binary COPY format and UNLOGGED tables
- Airflow uses CSV COPY format with standard tables
- Both use parallel table transfers
EOF

    echo -e "${GREEN}Report saved to: $RESULTS_FILE${NC}"
}

# Main execution
echo -e "${YELLOW}Step 1: Reset PostgreSQL${NC}"
reset_postgres

echo -e "\n${YELLOW}Step 2: Run Rust Benchmark${NC}"
run_rust_benchmark

echo -e "\n${YELLOW}Step 3: Reset PostgreSQL${NC}"
reset_postgres

echo -e "\n${YELLOW}Step 4: Run Airflow Benchmark${NC}"
run_airflow_benchmark

echo -e "\n${YELLOW}Step 5: Generate Report${NC}"
generate_report

echo -e "\n${BLUE}========================================${NC}"
echo -e "${GREEN}  Benchmark Complete!${NC}"
echo -e "${BLUE}========================================${NC}"
echo ""
cat "$RESULTS_FILE"
