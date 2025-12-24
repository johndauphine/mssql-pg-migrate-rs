# Repository Guidelines

## Project Structure & Module Organization
- Root workspace uses Rust with two crates: `crates/mssql-pg-migrate` (core library) and `crates/mssql-pg-migrate-cli` (CLI).
- Config examples live in `examples/` and `config.yaml` at the root; state/config samples like `test-config.yaml` may exist locally.
- Rust sources: `crates/mssql-pg-migrate/src/` (config, orchestrator, transfer, source, target, typemap, state, error) and `crates/mssql-pg-migrate-cli/src/main.rs` for the CLI entrypoint.

## Build, Test, and Development Commands
- Build release binaries: `cargo build --release`.
- Run all tests: `cargo test`.
- Run CLI with a config: `target/release/mssql-pg-migrate -c config.yaml run`.
- Lint/format (if installed): `cargo fmt -- --check` and `cargo clippy --all-targets --all-features`.

## Coding Style & Naming Conventions
- Rust 2021 edition; prefer idiomatic Rust naming (snake_case for functions/vars, UpperCamelCase for types).
- Use `cargo fmt` for formatting; avoid manual alignment that fights rustfmt.
- Keep modules cohesive (e.g., transfer logic in `transfer/`, DB ops in `source/` or `target/`).
- Log with `tracing`; avoid println! in library code.

## Testing Guidelines
- Place Rust tests in `tests/` or inline `#[cfg(test)]` modules next to the code.
- Prefer deterministic tests that mock DB interactions where possible; integration tests that hit real DBs should be clearly marked or config-gated.
- Run `cargo test` before submitting changes; for performance-sensitive paths, consider microbenchmarks under `benches/` (criterion) if needed.

## Commit & Pull Request Guidelines
- Commit messages: concise, imperative mood (e.g., “Add CODEX findings document”); keep related changes together.
- Include brief PR descriptions: what changed, why, and how to validate (commands or configs used).
- Link related issues when applicable; include screenshots only if UI/log output is relevant.

## Security & Configuration Tips
- Do not commit real credentials; use example configs and environment variables for secrets.
- Prefer encrypted connections when available; align MSSQL `encrypt/trust_server_cert` and Postgres `ssl_mode` with deployment policies.
- Avoid logging sensitive row data; keep logs at info/debug focused on table/row counts and timings.
