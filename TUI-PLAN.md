# Claude Code-style TUI Plan

## Goals and Non-goals
- Deliver a conversational, command-palette-first TUI (Claude Code CLI vibe) that wraps the existing `run`, `resume`, `validate`, and `health-check` flows with live progress and logs.
- Keep default CLI behavior unchanged; TUI runs via an opt-in subcommand (`mssql-pg-migrate tui`).
- Make the TUI safe for long migrations: resumable, interruptible, and able to surface actionable errors.
- Non-goals: building a full config editor or schema explorer; network calls remain limited to the existing orchestrator paths.

## Experience Blueprint (Claude Code-like)
- Launch: `mssql-pg-migrate tui -c config.yaml` opens a full-screen view with a slim status bar, main panel for streaming output, and a right rail for context (config summary, active table, throughput).
- Command palette (`Ctrl+P` / `:`): fuzzy search actions such as “Run migration”, “Dry run”, “Resume from state”, “Validate rows”, “Health check”, “Tail logs”, “Open config path”.
- Conversation-style transcript in the main pane: system messages, action summaries, and streaming progress updates rendered as steps.
- Live panels: throughput chart (sparkline), table progress list, and a compact log tail; collapsible to stay readable on 80x24 terminals.
- Interruptions: `Ctrl+C` triggers a confirmation modal; cancellation routes through the existing `CancellationToken`.

## Key User Flows
- **Pick config**: auto-load `-c` path, show parsed summary (source/target hosts, mode, workers, state file). Allow quick switching by typing a new path in the palette.
- **Health check**: run `health_check()` in background; render per-endpoint status/latency; keep last N results in history.
- **Run / Dry run**: start migration with optional overrides (workers, schemas, state file). Stream phases and per-table progress; surface failures with retry hints.
- **Resume**: require state file selection; show what the state contains (run id, pending tables) before continuing.
- **Validation**: run row-count validation and surface mismatches with copyable remediation commands.
- **Logs**: embed a scrollback pane backed by `tracing` channel; palette action to save logs to file.

## Architecture and Technical Shape
- **New TUI module**: `crates/mssql-pg-migrate-cli/src/tui/` with `app.rs` (state), `ui.rs` (render), `events.rs` (input/tick), `actions.rs` (commands), and `logging.rs` (subscriber layer that feeds an mpsc channel).
- **Runtime**: `ratatui` + `crossterm` for drawing; reuse the existing Tokio runtime (single async main) with a select loop over input, ticks, and worker tasks.
- **Command palette**: lightweight fuzzy matcher (e.g., `fuzzy-matcher` or `skim` crate) on a static list of actions and recent commands.
- **Progress plumbing**: extend `Orchestrator` with an optional progress callback or channel sink (parallel to the current JSON-to-stderr) so the TUI can consume structured `ProgressUpdate` events without parsing stderr.
- **Cancellation**: share a `CancellationToken` between the TUI loop and orchestrator; palette action for “Cancel run” sets the token.
- **Error surface**: normalize `MigrateError` into user-facing messages with retry suggestions (e.g., re-run with `--resume` when state exists).

## Implementation Steps
1) **Refactor for reuse**: extract CLI command handlers into reusable functions (run, resume, validate, health-check) that accept a `RunContext` (config path, state file, overrides, progress sink, cancellation token).
2) **Add dependencies**: include `ratatui`, `crossterm`, `futures`, `tokio-stream`, and a fuzzy matcher in the CLI crate; gate with a `tui` feature if footprint is a concern.
3) **Scaffold TUI**: build the app state, event loop, and base layout (status bar, main transcript, side panels, command palette modal). Add safe terminal teardown on panic.
4) **Wire commands**: implement palette actions that spawn tasks for `health_check`, `run`/`dry_run`, `resume`, and `validate`, piping progress and final results back to the UI state.
5) **Progress/log integration**: add a progress callback path in `Orchestrator`; add a `tracing_subscriber::Layer` that writes to both stderr (optional) and an in-memory ring buffer for the TUI log pane.
6) **UX polish**: add keybindings, minimal color theme, and responsive layout (hide graphs on small widths). Ensure copy-friendly output for errors and table failures.
7) **Docs and usage**: update `README.md` with `tui` command, key bindings, and limits; add a short “how to cancel/resume” section. Include a gif/screencast if allowed later.
8) **Validation**: manual smoke test against `config.example.yaml` for each action; add unit tests for palette search and progress handler; keep `cargo fmt`/`cargo test` green.

## Open Questions / Decisions to Lock
- Acceptable dependency footprint? (If minimal required, gate under `--features tui`).
- Should the TUI allow editing config fields inline or remain read-only with path switching only?
- Do we keep stderr JSON progress when TUI is active (for piping) or silence it to avoid double output?
- Maximum log buffer to hold in-memory without impacting long migrations?
