//! Tracing integration for the TUI.
//!
//! Provides a custom tracing layer that captures log events and sends them
//! to the TUI's log buffer via a channel.

use std::fmt::Write as FmtWrite;
use tokio::sync::mpsc;
use tracing::{Event, Subscriber};
use tracing_subscriber::layer::Context;
use tracing_subscriber::Layer;

/// A tracing layer that sends formatted log lines to a channel.
pub struct TuiLogLayer {
    tx: mpsc::Sender<String>,
}

impl TuiLogLayer {
    /// Create a new TUI log layer.
    pub fn new(tx: mpsc::Sender<String>) -> Self {
        Self { tx }
    }
}

impl<S> Layer<S> for TuiLogLayer
where
    S: Subscriber,
{
    fn on_event(&self, event: &Event<'_>, _ctx: Context<'_, S>) {
        let mut message = String::new();

        // Format timestamp
        let now = chrono::Local::now();
        let _ = write!(message, "{} ", now.format("%H:%M:%S"));

        // Format level
        let level = event.metadata().level();
        let _ = write!(message, "[{:5}] ", level);

        // Format target (module path)
        let target = event.metadata().target();
        if !target.is_empty() {
            let _ = write!(message, "{}: ", target);
        }

        // Format fields (the actual message)
        let mut visitor = MessageVisitor::new();
        event.record(&mut visitor);
        message.push_str(&visitor.message);

        // Send to channel (non-blocking)
        let _ = self.tx.try_send(message);
    }
}

/// Visitor for extracting the message from a tracing event.
struct MessageVisitor {
    message: String,
}

impl MessageVisitor {
    fn new() -> Self {
        Self {
            message: String::new(),
        }
    }
}

impl tracing::field::Visit for MessageVisitor {
    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        if field.name() == "message" {
            let _ = write!(self.message, "{:?}", value);
        } else {
            if !self.message.is_empty() {
                self.message.push(' ');
            }
            let _ = write!(self.message, "{}={:?}", field.name(), value);
        }
    }

    fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
        if field.name() == "message" {
            self.message.push_str(value);
        } else {
            if !self.message.is_empty() {
                self.message.push(' ');
            }
            let _ = write!(self.message, "{}={}", field.name(), value);
        }
    }

    fn record_i64(&mut self, field: &tracing::field::Field, value: i64) {
        if !self.message.is_empty() {
            self.message.push(' ');
        }
        let _ = write!(self.message, "{}={}", field.name(), value);
    }

    fn record_u64(&mut self, field: &tracing::field::Field, value: u64) {
        if !self.message.is_empty() {
            self.message.push(' ');
        }
        let _ = write!(self.message, "{}={}", field.name(), value);
    }

    fn record_bool(&mut self, field: &tracing::field::Field, value: bool) {
        if !self.message.is_empty() {
            self.message.push(' ');
        }
        let _ = write!(self.message, "{}={}", field.name(), value);
    }
}
