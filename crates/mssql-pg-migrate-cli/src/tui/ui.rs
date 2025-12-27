//! UI rendering for the TUI.
//!
//! Implements the View function of the Elm architecture - pure rendering
//! from application state to terminal frames.

use crate::tui::app::{App, MigrationPhase};
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
    // Main layout: status bar (top), content (middle), footer (bottom)
    let main_chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(1),  // Status bar
            Constraint::Min(10),    // Content
            Constraint::Length(3),  // Log panel (collapsed)
            Constraint::Length(1),  // Footer
        ])
        .split(frame.area());

    render_status_bar(frame, app, main_chunks[0]);
    render_content(frame, app, main_chunks[1]);
    render_logs(frame, app, main_chunks[2]);
    render_footer(frame, app, main_chunks[3]);

    // Render overlays on top
    if app.palette_open.load(Ordering::Relaxed) {
        render_palette(frame, app);
    }

    if app.show_help {
        render_help(frame);
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
    let items: Vec<ListItem> = app
        .transcript
        .iter()
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

    let transcript = List::new(items)
        .block(
            Block::default()
                .borders(Borders::ALL)
                .title(" Transcript "),
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
    let log_lines: Vec<Line> = app
        .logs
        .iter()
        .skip(app.log_scroll)
        .take(area.height as usize)
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

    let logs = Paragraph::new(log_lines)
        .block(
            Block::default()
                .borders(Borders::ALL)
                .title(format!(" Logs ({}) ", app.logs.len())),
        )
        .wrap(Wrap { trim: true });

    frame.render_widget(logs, area);
}

/// Render the footer with key hints.
fn render_footer(frame: &mut Frame, _app: &App, area: Rect) {
    let footer = Paragraph::new(Line::from(vec![
        Span::styled(" Ctrl+P ", Style::default().bg(Color::DarkGray)),
        Span::raw(" Command palette "),
        Span::styled(" q ", Style::default().bg(Color::DarkGray)),
        Span::raw(" Quit "),
        Span::styled(" ? ", Style::default().bg(Color::DarkGray)),
        Span::raw(" Help "),
        Span::styled(" l ", Style::default().bg(Color::DarkGray)),
        Span::raw(" Logs "),
    ]));

    frame.render_widget(footer, area);
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
    let area = centered_rect(50, 60, frame.area());
    frame.render_widget(Clear, area);

    let help_text = vec![
        Line::from(""),
        Line::from(vec![
            Span::styled("  Ctrl+P  ", Style::default().fg(Color::Cyan)),
            Span::raw("Open command palette"),
        ]),
        Line::from(vec![
            Span::styled("  :       ", Style::default().fg(Color::Cyan)),
            Span::raw("Open command palette"),
        ]),
        Line::from(vec![
            Span::styled("  Ctrl+C  ", Style::default().fg(Color::Cyan)),
            Span::raw("Cancel current operation"),
        ]),
        Line::from(vec![
            Span::styled("  q       ", Style::default().fg(Color::Cyan)),
            Span::raw("Quit"),
        ]),
        Line::from(vec![
            Span::styled("  ?       ", Style::default().fg(Color::Cyan)),
            Span::raw("Toggle this help"),
        ]),
        Line::from(vec![
            Span::styled("  l       ", Style::default().fg(Color::Cyan)),
            Span::raw("Toggle log panel"),
        ]),
        Line::from(vec![
            Span::styled("  j/k     ", Style::default().fg(Color::Cyan)),
            Span::raw("Scroll logs"),
        ]),
        Line::from(""),
        Line::from(vec![
            Span::styled("  Esc     ", Style::default().fg(Color::Cyan)),
            Span::raw("Close palette/help"),
        ]),
        Line::from(vec![
            Span::styled("  Enter   ", Style::default().fg(Color::Cyan)),
            Span::raw("Select action"),
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
