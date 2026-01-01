#!/bin/bash
# Quick Rust-only benchmark
# Dataset: StackOverflow2010 (~19.3M rows)

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

# Colors
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}  Rust Migration Benchmark${NC}"
echo -e "${BLUE}========================================${NC}"

cd "$SCRIPT_DIR"

# Ensure binary exists
if [ ! -f "./mssql-pg-migrate" ]; then
    echo "Building binary..."
    cargo build --release --features tui && cp target/release/mssql-pg-migrate .
fi

# Reset PostgreSQL
echo -e "\n${YELLOW}Resetting PostgreSQL...${NC}"
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
echo -e "${GREEN}Done${NC}"

# Run migration
echo -e "\n${YELLOW}Running Rust migration...${NC}"
echo ""

time ./mssql-pg-migrate -c benchmark-config.yaml run

echo ""
echo -e "${BLUE}========================================${NC}"
echo -e "${GREEN}  Benchmark Complete!${NC}"
echo -e "${BLUE}========================================${NC}"

# Show row counts
echo -e "\n${YELLOW}Final row counts:${NC}"
docker exec postgres-target psql -U postgres -d stackoverflow -c "
    SELECT schemaname, relname as table, n_live_tup as rows
    FROM pg_stat_user_tables
    WHERE schemaname = 'public'
    ORDER BY n_live_tup DESC;
"
