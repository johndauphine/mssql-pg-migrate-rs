//! Benchmark for comparing COPY TO BINARY vs SELECT performance.
//!
//! Run with: cargo test --release --package mssql-pg-migrate -- bench_copy --nocapture --ignored

#[cfg(test)]
mod bench {
    use crate::config::SourceConfig;
    use crate::source::{PgSourcePool, SourcePool, Table};
    use crate::target::SqlValue;
    use std::time::Instant;
    use tokio::sync::mpsc;

    /// Benchmark configuration - update these to match your environment.
    fn get_test_config() -> SourceConfig {
        SourceConfig {
            r#type: "postgres".to_string(),
            host: "localhost".to_string(),
            port: 5433,
            database: "stackoverflow".to_string(),
            user: "postgres".to_string(),
            password: "PostgresPassword123".to_string(),
            schema: "public".to_string(),
            encrypt: false,
            trust_server_cert: true,
            auth: crate::config::AuthMethod::Native,
        }
    }

    /// Benchmark SELECT-based reads.
    #[tokio::test]
    #[ignore] // Run with --ignored flag
    async fn bench_select_reads() {
        let config = get_test_config();
        let pool = PgSourcePool::new(&config, 8).await.expect("Failed to connect");

        // Get table schema
        let tables: Vec<Table> = pool.extract_schema("public").await.expect("Failed to extract schema");
        let votes_table = tables.iter().find(|t| t.name.to_lowercase() == "votes").expect("Votes table not found");

        println!("\n=== SELECT Benchmark ===");
        println!("Table: public.{} ({} rows)", votes_table.name, votes_table.row_count);

        let columns: Vec<String> = votes_table.columns.iter().map(|c| c.name.clone()).collect();
        let col_types: Vec<String> = votes_table.columns.iter().map(|c| c.data_type.clone()).collect();

        // Build SELECT query
        let col_list = columns.iter()
            .map(|c: &String| format!("\"{}\"", c.replace('"', "\"\"")))
            .collect::<Vec<_>>()
            .join(", ");
        let sql = format!("SELECT {} FROM public.\"{}\" ORDER BY \"Id\" LIMIT 1000000", col_list, votes_table.name);

        // Warm up
        let _ = pool.query_rows_fast(&sql, &columns, &col_types).await;

        // Benchmark
        let iterations = 3;
        let mut total_rows = 0i64;
        let mut total_time = std::time::Duration::ZERO;

        for i in 0..iterations {
            let start = Instant::now();
            let rows = pool.query_rows_fast(&sql, &columns, &col_types).await.expect("Query failed");
            let elapsed = start.elapsed();

            total_rows += rows.len() as i64;
            total_time += elapsed;

            let rows_per_sec = rows.len() as f64 / elapsed.as_secs_f64();
            println!("  Run {}: {} rows in {:?} ({:.0} rows/s)", i + 1, rows.len(), elapsed, rows_per_sec);
        }

        let avg_rows_per_sec = total_rows as f64 / total_time.as_secs_f64();
        println!("\nSELECT Average: {:.0} rows/s", avg_rows_per_sec);
    }

    /// Benchmark COPY TO BINARY reads.
    #[tokio::test]
    #[ignore] // Run with --ignored flag
    async fn bench_copy_reads() {
        let config = get_test_config();
        let pool = PgSourcePool::new(&config, 8).await.expect("Failed to connect");

        // Get table schema
        let tables: Vec<Table> = pool.extract_schema("public").await.expect("Failed to extract schema");
        let votes_table = tables.iter().find(|t| t.name.to_lowercase() == "votes").expect("Votes table not found");

        println!("\n=== COPY TO BINARY Benchmark ===");
        println!("Table: public.{} ({} rows)", votes_table.name, votes_table.row_count);

        let columns: Vec<String> = votes_table.columns.iter().map(|c| c.name.clone()).collect();
        let col_types: Vec<String> = votes_table.columns.iter().map(|c| c.data_type.clone()).collect();

        // Warm up
        let (tx, mut rx) = mpsc::channel::<Vec<Vec<SqlValue>>>(100);
        let warmup_handle = tokio::spawn(async move {
            let mut count = 0i64;
            while let Some(batch) = rx.recv().await {
                count += batch.len() as i64;
            }
            count
        });

        let _ = pool.copy_rows_binary(
            "public",
            &votes_table.name,
            &columns,
            &col_types,
            Some("Id"),
            Some(1),
            Some(100000),
            tx,
            10000,
        ).await;

        let _ = warmup_handle.await;

        // Benchmark - read 1M rows
        let iterations = 3;
        let mut total_rows = 0i64;
        let mut total_time = std::time::Duration::ZERO;

        for i in 0..iterations {
            let (tx, mut rx) = mpsc::channel::<Vec<Vec<SqlValue>>>(100);

            let recv_handle = tokio::spawn(async move {
                let mut count = 0i64;
                while let Some(batch) = rx.recv().await {
                    count += batch.len() as i64;
                }
                count
            });

            let start = Instant::now();
            let min_pk = 1i64;
            let max_pk = 1_000_000i64;

            let rows_read = pool.copy_rows_binary(
                "public",
                &votes_table.name,
                &columns,
                &col_types,
                Some("Id"),
                Some(min_pk),
                Some(max_pk),
                tx,
                10000,
            ).await.expect("COPY failed");

            let recv_rows = recv_handle.await.expect("Receiver task failed");
            let elapsed = start.elapsed();

            total_rows += rows_read;
            total_time += elapsed;

            let rows_per_sec = rows_read as f64 / elapsed.as_secs_f64();
            println!("  Run {}: {} rows in {:?} ({:.0} rows/s) [recv: {}]",
                     i + 1, rows_read, elapsed, rows_per_sec, recv_rows);
        }

        let avg_rows_per_sec = total_rows as f64 / total_time.as_secs_f64();
        println!("\nCOPY Average: {:.0} rows/s", avg_rows_per_sec);
    }

    /// Compare both methods side by side.
    #[tokio::test]
    #[ignore] // Run with --ignored flag
    async fn bench_compare_copy_vs_select() {
        println!("\n========================================");
        println!("COPY TO BINARY vs SELECT Benchmark");
        println!("========================================\n");

        let config = get_test_config();
        let pool = PgSourcePool::new(&config, 8).await.expect("Failed to connect");

        // Get table schema
        let tables: Vec<Table> = pool.extract_schema("public").await.expect("Failed to extract schema");
        let votes_table = tables.iter().find(|t| t.name.to_lowercase() == "votes").expect("Votes table not found");

        println!("Table: public.{}", votes_table.name);
        println!("Total rows: {}", votes_table.row_count);
        println!("Columns: {}", votes_table.columns.len());
        println!();

        let columns: Vec<String> = votes_table.columns.iter().map(|c| c.name.clone()).collect();
        let col_types: Vec<String> = votes_table.columns.iter().map(|c| c.data_type.clone()).collect();

        let test_sizes = [100_000, 500_000, 1_000_000];

        for &size in &test_sizes {
            println!("\n--- {} rows ---", size);

            // SELECT benchmark
            let col_list = columns.iter()
                .map(|c: &String| format!("\"{}\"", c.replace('"', "\"\"")))
                .collect::<Vec<_>>()
                .join(", ");
            let sql = format!("SELECT {} FROM public.\"{}\" WHERE \"Id\" >= 1 AND \"Id\" <= {} ORDER BY \"Id\"", col_list, votes_table.name, size);

            let start = Instant::now();
            let select_rows = pool.query_rows_fast(&sql, &columns, &col_types).await.expect("SELECT failed");
            let select_time = start.elapsed();
            let select_rate = select_rows.len() as f64 / select_time.as_secs_f64();

            // COPY benchmark
            let (tx, mut rx) = mpsc::channel::<Vec<Vec<SqlValue>>>(100);
            let recv_handle = tokio::spawn(async move {
                let mut count = 0i64;
                while let Some(batch) = rx.recv().await {
                    count += batch.len() as i64;
                }
                count
            });

            let start = Instant::now();
            let copy_rows = pool.copy_rows_binary(
                "public",
                &votes_table.name,
                &columns,
                &col_types,
                Some("Id"),
                Some(1),
                Some(size as i64),
                tx,
                10000,
            ).await.expect("COPY failed");
            let _ = recv_handle.await;
            let copy_time = start.elapsed();
            let copy_rate = copy_rows as f64 / copy_time.as_secs_f64();

            let speedup = copy_rate / select_rate;

            println!("SELECT: {} rows in {:?} ({:.0} rows/s)", select_rows.len(), select_time, select_rate);
            println!("COPY:   {} rows in {:?} ({:.0} rows/s)", copy_rows, copy_time, copy_rate);
            println!("Speedup: {:.2}x", speedup);
        }

        println!("\n========================================");
    }
}
