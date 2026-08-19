//! `BridgeClient` trait: the single seam between the Nautilus execution client and the Go bridge.

use anyhow::Result;
use async_trait::async_trait;

use super::protocol::{CommandEnvelope, ReportEnvelope};

/// An asynchronous report stream produced by a bridge (fills, order-state updates, rejections).
pub type BridgeReportStream = tokio::sync::mpsc::UnboundedReceiver<ReportEnvelope>;

/// A client of the Go bridge.
///
/// T4 scope is the in-process `FakeBridge`; the production WebSocket/HTTP adapter is added in a
/// later phase. Every implementation must be usable from a single-threaded (non-`Send`)
/// Nautilus runtime context, so report consumption is exposed as an owned receiver that a
/// caller-owned task processes.
#[async_trait]
pub trait BridgeClient {
    /// Whether the client currently holds a live bridge connection.
    fn is_connected(&self) -> bool;

    /// Establishes the bridge connection. Idempotent.
    async fn connect(&mut self) -> Result<()>;

    /// Tears down the bridge connection. Idempotent.
    async fn disconnect(&mut self) -> Result<()>;

    /// Sends a command envelope and returns the synchronously produced report envelope.
    async fn send_command(&mut self, envelope: CommandEnvelope) -> Result<ReportEnvelope>;

    /// Hands over the receiver for asynchronous bridge reports (fills, rejects, order updates).
    ///
    /// Returns `None` if the stream has already been taken. The caller owns the returned receiver.
    fn take_reports(&mut self) -> Option<BridgeReportStream>;
}
