//! Transfer pipeline abstractions using Template Method pattern.
//!
//! This module provides abstractions for the data transfer process:
//!
//! - [`TransferPipeline`]: Trait defining the transfer algorithm skeleton
//! - [`TransferJob`]: Command pattern encapsulating a transfer work unit
//! - [`PipelineConfig`]: Configuration for pipeline behavior
//!
//! # Design Patterns
//!
//! - **Template Method**: `TransferPipeline` defines the algorithm skeleton with
//!   customizable steps for setup, read, write, and finalize operations.
//! - **Command**: `TransferJob` encapsulates all information needed to execute
//!   a transfer operation, enabling queuing and deferred execution.
//!
//! # Architecture
//!
//! The pipeline separates concerns:
//! - **What** to transfer (TransferJob)
//! - **How** to transfer (TransferPipeline implementation)
//! - **Configuration** of the transfer (PipelineConfig)
//!
//! This allows different pipeline implementations (parallel, serial, streaming)
//! to be used interchangeably.

mod job;
mod template;

pub use job::{JobResult, TransferJob};
pub use template::{PipelineConfig, PipelineStats, TransferPipeline};
