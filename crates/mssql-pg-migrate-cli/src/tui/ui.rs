//! UI rendering for the TUI.
//!
//! Implements the View function of the Elm architecture - pure rendering
//! from application state to terminal frames.

use crate::tui::app::{App, InputMode, MigrationPhase};
use crate::tui::wizard::WizardStep;
use ratatui::{
    layout::{Constraint, Direction, Layout, Rect},
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::{Block, Borders, Clear, List, ListItem, Paragraph, Sparkline, Wrap},
    Frame,
};
use std::sync::atomic::Ordering;

/// Render the entire application UI.
pub fn render(frame: &mut Frame, app: &App) {
    // Log panel height: 3 lines collapsed, 40% of screen expanded
    let log_height = if app.log_expanded {
        Constraint::Percentage(40)
    } else {
        Constraint::Length(3)
    };

    // Command input height: 1 line when in command mode, 0 when not
    let command_input_height = if app.input_mode != InputMode::Normal {
        Constraint::Length(1)
    } else {
        Constraint::Length(0)
    };

    // Progress bar height: show during active phases
    let show_progress = matches!(
        app.phase,
        MigrationPhase::Transferring | MigrationPhase::PreparingTarget | MigrationPhase::Finalizing
    );
    let progress_height = if show_progress {
        Constraint::Length(3)
    } else {
        Constraint::Length(0)
    };

    // Main layout: status bar (top), progress, content (middle), logs, command input, footer (bottom)
    let main_chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(1),  // Status bar
            progress_height,        // Progress bars (only during transfer)
            Constraint::Min(10),    // Content
            log_height,             // Log panel (collapsed or expanded)
            command_input_height,   // Command input line
            Constraint::Length(1),  // Footer
        ])
        .split(frame.area());

    render_status_bar(frame, app, main_chunks[0]);

    // Render progress bars if in transfer phase
    if show_progress {
        render_progress_bars(frame, app, main_chunks[1]);
    }

    render_content(frame, app, main_chunks[2]);
    render_logs(frame, app, main_chunks[3]);

    // Render command input if in command mode
    if app.input_mode != InputMode::Normal {
        render_command_input(frame, app, main_chunks[4]);
    }

    render_footer(frame, app, main_chunks[5]);

    // Render suggestions dropdown above command input
    if app.input_mode != InputMode::Normal && !app.suggestions.is_empty() {
        render_suggestions(frame, app, main_chunks[4]);
    }

    // Render overlays on top
    if app.palette_open.load(Ordering::Relaxed) {
        render_palette(frame, app);
    }

    if app.show_help {
        render_help(frame);
    }

    // Render wizard overlay if active
    if app.wizard.is_some() {
        render_wizard(frame, app);
    }
}

/// Render the status bar at the top.
fn render_status_bar(frame: &mut Frame, app: &App, area: Rect) {
    let phase_color = match app.phase {
        MigrationPhase::Idle => Color::Gray,
        MigrationPhase::Connecting | MigrationPhase::ExtractingSchema => Color::Yellow,
        MigrationPhase::PreparingTarget | MigrationPhase::Transferring => Color::Cyan,
        MigrationPhase::Finalizing => Color::Blue,
        MigrationPhase::Completed => Color::Green,
        MigrationPhase::Failed => Color::Red,
        MigrationPhase::Cancelled => Color::Yellow,
    };

    let status = Line::from(vec![
        Span::styled(
            format!(" [{}] ", app.phase),
            Style::default().fg(phase_color).add_modifier(Modifier::BOLD),
        ),
        Span::raw(format!(
            "Tables {}/{} | ",
            app.tables_completed, app.tables_total
        )),
        Span::styled(
            format!("{} rows/sec", format_number(app.rows_per_second)),
            Style::default().fg(Color::Cyan),
        ),
        Span::raw(" | "),
        Span::raw(app.elapsed_formatted()),
        Span::raw(" | "),
        Span::styled(
            format!("{} rows", format_number(app.rows_transferred)),
            Style::default().fg(Color::Green),
        ),
    ]);

    let status_bar = Paragraph::new(status)
        .style(Style::default().bg(Color::DarkGray));

    frame.render_widget(status_bar, area);
}

/// Render progress bars during migration.
fn render_progress_bars(frame: &mut Frame, app: &App, area: Rect) {
    // Calculate row-based progress percentage (more responsive than table-based)
    let rows_percent = if app.total_rows > 0 {
        (app.rows_transferred as f64 / app.total_rows as f64) * 100.0
    } else {
        0.0
    };

    // Build the progress bar
    let bar_width = area.width.saturating_sub(60) as usize;
    let filled = ((rows_percent / 100.0) * bar_width as f64) as usize;
    let empty = bar_width.saturating_sub(filled);

    let bar = format!(
        "[{}{}]",
        "█".repeat(filled),
        "░".repeat(empty),
    );

    // Phase-specific message
    let phase_msg = match app.phase {
        MigrationPhase::PreparingTarget => "Preparing target database...",
        MigrationPhase::Transferring => "Transferring data...",
        MigrationPhase::Finalizing => "Finalizing...",
        _ => "",
    };

    let progress_line = Line::from(vec![
        Span::styled(" ", Style::default()),
        Span::styled(&bar, Style::default().fg(Color::Cyan)),
        Span::raw(" "),
        Span::styled(
            format!("{:5.1}%", rows_percent),
            Style::default().fg(Color::White).add_modifier(Modifier::BOLD),
        ),
        Span::raw(" │ "),
        Span::styled(
            format!("{}/{} tables", app.tables_completed, app.tables_total),
            Style::default().fg(Color::Yellow),
        ),
        Span::raw(" │ "),
        Span::styled(
            format!("{} rows/sec", format_number(app.rows_per_second)),
            Style::default().fg(Color::Green),
        ),
        Span::raw(" │ "),
        Span::styled(phase_msg, Style::default().fg(Color::DarkGray)),
    ]);

    let progress = Paragraph::new(vec![
        Line::from(""),
        progress_line,
        Line::from(""),
    ])
    .block(Block::default().borders(Borders::BOTTOM));

    frame.render_widget(progress, area);
}

/// Render the main content area (transcript + side panel).
fn render_content(frame: &mut Frame, app: &App, area: Rect) {
    // Split content: transcript (left) and side panel (right)
    let content_chunks = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([
            Constraint::Min(40),      // Transcript
            Constraint::Length(30),   // Side panel
        ])
        .split(area);

    render_transcript(frame, app, content_chunks[0]);
    render_side_panel(frame, app, content_chunks[1]);
}

/// Render the conversation-style transcript.
fn render_transcript(frame: &mut Frame, app: &App, area: Rect) {
    // Calculate visible height (excluding borders)
    let visible_height = area.height.saturating_sub(2) as usize;

    // Clamp scroll to valid range
    let max_scroll = app.transcript.len().saturating_sub(visible_height);
    let actual_scroll = app.transcript_scroll.min(max_scroll);

    let items: Vec<ListItem> = app
        .transcript
        .iter()
        .skip(actual_scroll)
        .take(visible_height)
        .map(|entry| {
            let icon_style = match entry.icon {
                '\u{2713}' => Style::default().fg(Color::Green),  // Check
                '\u{2717}' => Style::default().fg(Color::Red),    // X
                '\u{23F3}' => Style::default().fg(Color::Yellow), // Hourglass
                _ => Style::default().fg(Color::Cyan),            // Arrow
            };

            let mut spans = vec![
                Span::styled(format!(" {} ", entry.icon), icon_style),
                Span::raw(&entry.message),
            ];

            if let Some(ref detail) = entry.detail {
                spans.push(Span::raw(" "));
                spans.push(Span::styled(
                    format!("({})", detail),
                    Style::default().fg(Color::DarkGray),
                ));
            }

            ListItem::new(Line::from(spans))
        })
        .collect();

    let title = format!(" Transcript ({}) ", app.transcript.len());
    let transcript = List::new(items)
        .block(
            Block::default()
                .borders(Borders::ALL)
                .title(title),
        );

    frame.render_widget(transcript, area);
}

/// Render the side panel with config summary and throughput.
fn render_side_panel(frame: &mut Frame, app: &App, area: Rect) {
    let side_chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(9),  // Config summary
            Constraint::Min(5),     // Throughput sparkline
        ])
        .split(area);

    // Config summary
    let config_text = vec![
        Line::from(vec![
            Span::styled("Source: ", Style::default().fg(Color::DarkGray)),
            Span::raw(&app.config_summary.source_host),
        ]),
        Line::from(vec![
            Span::styled("  DB: ", Style::default().fg(Color::DarkGray)),
            Span::raw(&app.config_summary.source_database),
        ]),
        Line::from(""),
        Line::from(vec![
            Span::styled("Target: ", Style::default().fg(Color::DarkGray)),
            Span::raw(&app.config_summary.target_host),
        ]),
        Line::from(vec![
            Span::styled("  DB: ", Style::default().fg(Color::DarkGray)),
            Span::raw(&app.config_summary.target_database),
        ]),
        Line::from(vec![
            Span::styled("  Mode: ", Style::default().fg(Color::DarkGray)),
            Span::raw(&app.config_summary.target_mode),
        ]),
    ];

    let config_para = Paragraph::new(config_text)
        .block(
            Block::default()
                .borders(Borders::ALL)
                .title(" Config "),
        );

    frame.render_widget(config_para, side_chunks[0]);

    // Throughput sparkline
    let sparkline = Sparkline::default()
        .block(
            Block::default()
                .borders(Borders::ALL)
                .title(" Throughput "),
        )
        .data(&app.throughput_history)
        .style(Style::default().fg(Color::Cyan));

    frame.render_widget(sparkline, side_chunks[1]);
}

/// Render the log panel.
fn render_logs(frame: &mut Frame, app: &App, area: Rect) {
    // Calculate visible height (excluding borders)
    let visible_height = area.height.saturating_sub(2) as usize;

    // Clamp scroll to ensure we show as many logs as possible
    // If log_scroll is past the point where we can fill the screen, clamp it
    let max_scroll = app.logs.len().saturating_sub(visible_height);
    let actual_scroll = app.log_scroll.min(max_scroll);

    let log_lines: Vec<Line> = app
        .logs
        .iter()
        .skip(actual_scroll)
        .take(visible_height)
        .map(|line| {
            let style = if line.contains("ERROR") || line.contains("error") {
                Style::default().fg(Color::Red)
            } else if line.contains("WARN") || line.contains("warn") {
                Style::default().fg(Color::Yellow)
            } else if line.contains("DEBUG") || line.contains("debug") {
                Style::default().fg(Color::DarkGray)
            } else {
                Style::default()
            };
            Line::styled(line.as_str(), style)
        })
        .collect();

    let title = if app.log_expanded {
        format!(" Logs ({}) - Press 'l' to collapse ", app.logs.len())
    } else {
        format!(" Logs ({}) - Press 'l' to expand ", app.logs.len())
    };

    let logs = Paragraph::new(log_lines)
        .block(
            Block::default()
                .borders(Borders::ALL)
                .title(title),
        )
        .wrap(Wrap { trim: true });

    frame.render_widget(logs, area);
}

/// Render the footer with key hints.
fn render_footer(frame: &mut Frame, app: &App, area: Rect) {
    let spans = if app.input_mode != InputMode::Normal {
        // Command mode footer
        vec![
            Span::styled(" Tab ", Style::default().bg(Color::DarkGray)),
            Span::raw(" Accept "),
            Span::styled(" Enter ", Style::default().bg(Color::DarkGray)),
            Span::raw(" Execute "),
            Span::styled(" Esc ", Style::default().bg(Color::DarkGray)),
            Span::raw(" Cancel "),
            Span::styled(" ↑↓ ", Style::default().bg(Color::DarkGray)),
            Span::raw(" Navigate "),
        ]
    } else {
        // Normal mode footer
        vec![
            Span::styled(" / ", Style::default().bg(Color::DarkGray)),
            Span::raw(" Commands "),
            Span::styled(" Ctrl+P ", Style::default().bg(Color::DarkGray)),
            Span::raw(" Palette "),
            Span::styled(" q ", Style::default().bg(Color::DarkGray)),
            Span::raw(" Quit "),
            Span::styled(" ? ", Style::default().bg(Color::DarkGray)),
            Span::raw(" Help "),
            Span::styled(" l ", Style::default().bg(Color::DarkGray)),
            Span::raw(" Logs "),
        ]
    };

    let footer = Paragraph::new(Line::from(spans));
    frame.render_widget(footer, area);
}

/// Render the command input line.
fn render_command_input(frame: &mut Frame, app: &App, area: Rect) {
    let mode_indicator = match app.input_mode {
        InputMode::Command => ":",
        InputMode::FileInput => "@",
        InputMode::Normal => ">",
        InputMode::Wizard => "wizard>",
    };

    let input_style = Style::default().fg(Color::Cyan);

    let input = Paragraph::new(Line::from(vec![
        Span::styled(mode_indicator, input_style),
        Span::raw(" "),
        Span::raw(&app.command_input),
        Span::styled("█", Style::default().fg(Color::Gray)), // Cursor
    ]))
    .style(Style::default().bg(Color::Black));

    frame.render_widget(input, area);
}

/// Render the suggestions dropdown above the command input.
fn render_suggestions(frame: &mut Frame, app: &App, input_area: Rect) {
    let suggestion_count = app.suggestions.len().min(8);
    if suggestion_count == 0 {
        return;
    }

    // Position dropdown above the input line
    let dropdown_height = suggestion_count as u16;
    let dropdown_area = Rect {
        x: input_area.x + 2,
        y: input_area.y.saturating_sub(dropdown_height + 1),
        width: input_area.width.saturating_sub(4).min(60),
        height: dropdown_height,
    };

    // Clear the area first
    frame.render_widget(Clear, dropdown_area);

    let items: Vec<ListItem> = app
        .suggestions
        .iter()
        .take(8)
        .enumerate()
        .map(|(i, suggestion)| {
            let style = if i == app.selected_suggestion {
                Style::default()
                    .bg(Color::Blue)
                    .fg(Color::White)
                    .add_modifier(Modifier::BOLD)
            } else {
                Style::default().bg(Color::DarkGray)
            };

            let display = if suggestion.is_dir {
                format!(" {}/ ", suggestion.display.trim_end_matches('/'))
            } else {
                format!(" {} ", suggestion.display)
            };

            ListItem::new(Line::styled(display, style))
        })
        .collect();

    let list = List::new(items)
        .block(Block::default().style(Style::default().bg(Color::DarkGray)));

    frame.render_widget(list, dropdown_area);
}

/// Render the command palette overlay.
fn render_palette(frame: &mut Frame, app: &App) {
    let area = centered_rect(60, 50, frame.area());

    // Clear the area first
    frame.render_widget(Clear, area);

    // Palette content
    let inner_chunks = Layout::default()
        .direction(Direction::Vertical)
        .margin(1)
        .constraints([
            Constraint::Length(3),  // Search input
            Constraint::Min(1),     // Results
        ])
        .split(area);

    // Search input
    let input = Paragraph::new(format!("> {}", app.palette_query))
        .block(
            Block::default()
                .borders(Borders::ALL)
                .title(" Command Palette "),
        );
    frame.render_widget(input, area);

    // Action list
    let items: Vec<ListItem> = app
        .filtered_actions
        .iter()
        .enumerate()
        .map(|(i, action)| {
            let style = if i == app.selected_action {
                Style::default()
                    .bg(Color::Blue)
                    .fg(Color::White)
                    .add_modifier(Modifier::BOLD)
            } else {
                Style::default()
            };

            let mut spans = vec![
                Span::raw("  "),
                Span::styled(action.label(), style),
            ];

            if let Some(shortcut) = action.shortcut() {
                spans.push(Span::styled(
                    format!(" ({})", shortcut),
                    Style::default().fg(Color::DarkGray),
                ));
            }

            ListItem::new(Line::from(spans))
        })
        .collect();

    let list = List::new(items).block(Block::default().borders(Borders::NONE));

    // Position list below input
    let list_area = Rect {
        x: inner_chunks[1].x,
        y: area.y + 3,
        width: inner_chunks[1].width,
        height: inner_chunks[1].height.saturating_sub(1),
    };

    frame.render_widget(list, list_area);
}

/// Render the help overlay.
fn render_help(frame: &mut Frame) {
    let area = centered_rect(60, 80, frame.area());
    frame.render_widget(Clear, area);

    let help_text = vec![
        Line::from(""),
        Line::from(Span::styled(" Keyboard Shortcuts ", Style::default().add_modifier(Modifier::BOLD))),
        Line::from(""),
        Line::from(vec![
            Span::styled("  /         ", Style::default().fg(Color::Cyan)),
            Span::raw("Start typing a command"),
        ]),
        Line::from(vec![
            Span::styled("  Ctrl+P    ", Style::default().fg(Color::Cyan)),
            Span::raw("Open command palette"),
        ]),
        Line::from(vec![
            Span::styled("  Ctrl+C    ", Style::default().fg(Color::Cyan)),
            Span::raw("Cancel current operation"),
        ]),
        Line::from(vec![
            Span::styled("  q         ", Style::default().fg(Color::Cyan)),
            Span::raw("Quit"),
        ]),
        Line::from(vec![
            Span::styled("  ?         ", Style::default().fg(Color::Cyan)),
            Span::raw("Toggle this help"),
        ]),
        Line::from(vec![
            Span::styled("  l         ", Style::default().fg(Color::Cyan)),
            Span::raw("Toggle log panel"),
        ]),
        Line::from(vec![
            Span::styled("  j/k       ", Style::default().fg(Color::Cyan)),
            Span::raw("Scroll logs"),
        ]),
        Line::from(""),
        Line::from(Span::styled(" Slash Commands ", Style::default().add_modifier(Modifier::BOLD))),
        Line::from(""),
        Line::from(vec![
            Span::styled("  /run      ", Style::default().fg(Color::Green)),
            Span::raw("Start migration"),
        ]),
        Line::from(vec![
            Span::styled("  /resume   ", Style::default().fg(Color::Green)),
            Span::raw("Resume interrupted migration"),
        ]),
        Line::from(vec![
            Span::styled("  /dry-run  ", Style::default().fg(Color::Green)),
            Span::raw("Run without making changes"),
        ]),
        Line::from(vec![
            Span::styled("  /validate ", Style::default().fg(Color::Green)),
            Span::raw("Validate row counts"),
        ]),
        Line::from(vec![
            Span::styled("  /health   ", Style::default().fg(Color::Green)),
            Span::raw("Run health check"),
        ]),
        Line::from(vec![
            Span::styled("  /logs     ", Style::default().fg(Color::Green)),
            Span::raw("Save logs to file"),
        ]),
        Line::from(vec![
            Span::styled("  /clear    ", Style::default().fg(Color::Green)),
            Span::raw("Clear transcript"),
        ]),
        Line::from(""),
        Line::from(vec![
            Span::styled("  @path     ", Style::default().fg(Color::Yellow)),
            Span::raw("File path completion (Tab to accept)"),
        ]),
        Line::from(""),
    ];

    let help = Paragraph::new(help_text)
        .block(
            Block::default()
                .borders(Borders::ALL)
                .title(" Help ")
                .style(Style::default().bg(Color::DarkGray)),
        );

    frame.render_widget(help, area);
}

/// Render the configuration wizard overlay.
fn render_wizard(frame: &mut Frame, app: &App) {
    let wizard = match &app.wizard {
        Some(w) => w,
        None => return,
    };

    let area = centered_rect(70, 70, frame.area());
    frame.render_widget(Clear, area);

    // Build wizard content
    let mut lines: Vec<Line> = vec![
        Line::from(""),
        Line::from(Span::styled(
            " Configuration Wizard ",
            Style::default().add_modifier(Modifier::BOLD).fg(Color::Cyan),
        )),
        Line::from(format!(
            " Step {} of {} ",
            wizard.step.step_number(),
            WizardStep::total_steps()
        )),
        Line::from(""),
    ];

    // Add transcript of previous answers
    for line in &wizard.transcript {
        lines.push(Line::from(vec![
            Span::styled(" \u{2713} ", Style::default().fg(Color::Green)),
            Span::raw(line),
        ]));
    }

    if !wizard.transcript.is_empty() {
        lines.push(Line::from(""));
    }

    // Current prompt
    let prompt = wizard.get_prompt();
    let input_display = if wizard.step.is_password() {
        "*".repeat(wizard.input.len())
    } else {
        wizard.input.clone()
    };

    lines.push(Line::from(vec![
        Span::styled(&prompt, Style::default().fg(Color::Yellow)),
    ]));

    lines.push(Line::from(vec![
        Span::styled(" > ", Style::default().fg(Color::Cyan)),
        Span::raw(&input_display),
        Span::styled("█", Style::default().fg(Color::Gray)),
    ]));

    // Error message if any
    if let Some(ref error) = wizard.error {
        lines.push(Line::from(""));
        lines.push(Line::from(vec![
            Span::styled(" Error: ", Style::default().fg(Color::Red)),
            Span::styled(error, Style::default().fg(Color::Red)),
        ]));
    }

    // Footer hints
    lines.push(Line::from(""));
    lines.push(Line::from(vec![
        Span::styled(" Enter ", Style::default().bg(Color::DarkGray)),
        Span::raw(" Submit "),
        Span::styled(" ↑ ", Style::default().bg(Color::DarkGray)),
        Span::raw(" Back "),
        Span::styled(" Esc ", Style::default().bg(Color::DarkGray)),
        Span::raw(" Cancel "),
    ]));

    let wizard_widget = Paragraph::new(lines)
        .block(
            Block::default()
                .borders(Borders::ALL)
                .title(" Wizard ")
                .style(Style::default().bg(Color::Black)),
        );

    frame.render_widget(wizard_widget, area);
}

/// Create a centered rectangle.
fn centered_rect(percent_x: u16, percent_y: u16, area: Rect) -> Rect {
    let popup_layout = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Percentage((100 - percent_y) / 2),
            Constraint::Percentage(percent_y),
            Constraint::Percentage((100 - percent_y) / 2),
        ])
        .split(area);

    Layout::default()
        .direction(Direction::Horizontal)
        .constraints([
            Constraint::Percentage((100 - percent_x) / 2),
            Constraint::Percentage(percent_x),
            Constraint::Percentage((100 - percent_x) / 2),
        ])
        .split(popup_layout[1])[1]
}

/// Format a large number with commas.
fn format_number(n: i64) -> String {
    let s = n.to_string();
    let mut result = String::new();
    for (i, c) in s.chars().rev().enumerate() {
        if i > 0 && i % 3 == 0 {
            result.push(',');
        }
        result.push(c);
    }
    result.chars().rev().collect()
}
