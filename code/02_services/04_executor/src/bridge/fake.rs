//! In-process fake bridge implementing the Go bridge protocol.
//!
//! Deterministic and scripted so the Nautilus execution service can be exercised offline with
//! zero network egress (the network-isolation guarantee). Mirrors the order lifecycle of
//! `code/02_services/06_execution_bridge/go-bridge/fake_broker.go`.

use std::collections::{HashMap, VecDeque};

use anyhow::{anyhow, Context as _};
use async_trait::async_trait;
use tracing::debug;

use super::client::{BridgeClient, BridgeReportStream};
use super::protocol::{
    Command, CommandEnvelope, ReportEnvelope, RECORD_REPORT,
};

/// Scripted synchronous reply for the next command.
#[derive(Debug, Clone)]
pub enum CommandScript {
    /// Successful acknowledgement of the command.
    Accept,
    /// Successful acknowledgement followed by an asynchronous full-fill on the report stream.
    AcceptThenFill,
    /// Resource-side rejection with a reason.
    Reject(String),
    /// A response the service cannot classify as success or rejection.
    Unknown(String),
    /// No reply within the command window (bridge stall).
    Timeout,
}

/// The order-status value used in reports (mirrors the bridge's `order_status` strings).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FakeOrderStatus {
    Open,
    PartialFill,
    Filled,
    Canceled,
}

impl FakeOrderStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Open => "OPEN",
            Self::PartialFill => "PARTIAL_FILL",
            Self::Filled => "FILLED",
            Self::Canceled => "CANCELED",
        }
    }
}

/// A recorded order held by the fake bridge.
#[derive(Debug, Clone)]
pub struct OrderRecord {
    pub broker_order_id: String,
    pub client_order_ref: String,
    pub symbol: String,
    pub quantity: String,
    pub status: FakeOrderStatus,
    pub filled_qty: String,
    pub price: String,
}

/// The in-process fake bridge.
pub struct FakeBridge {
    connected: bool,
    reports_tx: Option<tokio::sync::mpsc::UnboundedSender<ReportEnvelope>>,
    reports_rx: Option<BridgeReportStream>,
    orders: HashMap<String, OrderRecord>,
    counter: u64,
    scripts: VecDeque<CommandScript>,
    emit_fill_after_place: bool,
}

impl FakeBridge {
    pub fn new() -> Self {
        Self {
            connected: false,
            reports_tx: None,
            reports_rx: None,
            orders: HashMap::new(),
            counter: 0,
            scripts: VecDeque::new(),
            emit_fill_after_place: false,
        }
    }

    /// Queues a scripted reply for the next `send_command` call (consumed in FIFO order).
    pub fn script(&mut self, script: CommandScript) -> &mut Self {
        self.scripts.push_back(script);
        self
    }

    /// Enables emission of an asynchronous full-fill report after every successful place
    /// (used to reproduce fill-driven position updates end-to-end).
    pub fn emit_fill_after_place(&mut self, enabled: bool) -> &mut Self {
        self.emit_fill_after_place = enabled;
        self
    }

    fn next_broker_id(&mut self) -> String {
        self.counter += 1;
        format!("BRK-{:04}", self.counter)
    }

    fn make_success(&self, cmd: Command, request_id: &str, broker_order_id: &str) -> ReportEnvelope {
        ReportEnvelope {
            record_type: RECORD_REPORT.to_string(),
            contract_version: 1,
            request_id: request_id.to_string(),
            command: cmd.as_str().to_string(),
            outcome: "SUCCESS".to_string(),
            broker_order_id: broker_order_id.to_string(),
            ..ReportEnvelope::default()
        }
    }

    fn push_report(&self, report: ReportEnvelope) {
        if let Some(tx) = &self.reports_tx {
            let _ = tx.send(report);
        }
    }
}

impl Default for FakeBridge {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl BridgeClient for FakeBridge {
    fn is_connected(&self) -> bool {
        self.connected
    }

    async fn connect(&mut self) -> anyhow::Result<()> {
        self.connected = true;
        if self.reports_tx.is_none() {
            let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
            self.reports_tx = Some(tx);
            self.reports_rx = Some(rx);
        }
        Ok(())
    }

    async fn disconnect(&mut self) -> anyhow::Result<()> {
        self.connected = false;
        Ok(())
    }

    async fn send_command(&mut self, envelope: CommandEnvelope) -> anyhow::Result<ReportEnvelope> {
        envelope
            .validate()
            .map_err(|e| anyhow!("invalid command: {e}"))?;

        let script = self.scripts.pop_front();
        let Some(script) = script else {
            return Err(anyhow!("fake bridge: unexpected command {}", envelope.command));
        };

        let cmd = envelope
            .command()
            .ok_or_else(|| anyhow!("unsupported command {}", envelope.command))?;

        match script {
            CommandScript::Timeout => Err(anyhow!("fake bridge: command {} timed out", cmd)),
            CommandScript::Reject(reason) => Ok(ReportEnvelope {
                record_type: RECORD_REPORT.to_string(),
                contract_version: 1,
                request_id: envelope.request_id.clone(),
                command: cmd.as_str().to_string(),
                outcome: "REJECTED".to_string(),
                reason,
                broker_order_id: envelope.broker_order_id.clone(),
                ..ReportEnvelope::default()
            }),
            CommandScript::Unknown(reason) => Ok(ReportEnvelope {
                record_type: RECORD_REPORT.to_string(),
                contract_version: 1,
                request_id: envelope.request_id.clone(),
                command: cmd.as_str().to_string(),
                outcome: "UNKNOWN".to_string(),
                reason,
                broker_order_id: envelope.broker_order_id.clone(),
                ..ReportEnvelope::default()
            }),
            CommandScript::Accept => self.handle_accept(cmd, envelope).await,
            CommandScript::AcceptThenFill => {
                let report = self.handle_accept(cmd, envelope.clone()).await?;
                self.emit_fill(&envelope);
                Ok(report)
            }
        }
    }

    fn take_reports(&mut self) -> Option<BridgeReportStream> {
        self.reports_rx.take()
    }
}

impl FakeBridge {
    async fn handle_accept(
        &mut self,
        cmd: Command,
        envelope: CommandEnvelope,
    ) -> anyhow::Result<ReportEnvelope> {
        match cmd {
            Command::Place => {
                let order = envelope
                    .order
                    .as_ref()
                    .context("place requires an order")?;
                let broker_order_id = self.next_broker_id();
                self.orders.insert(
                    broker_order_id.clone(),
                    OrderRecord {
                        broker_order_id: broker_order_id.clone(),
                        client_order_ref: envelope.client_order_ref.clone(),
                        symbol: order.symbol.clone(),
                        quantity: order.quantity.clone(),
                        status: FakeOrderStatus::Open,
                        filled_qty: "0".to_string(),
                        price: order.price.clone(),
                    },
                );
                Ok(self.make_success(Command::Place, &envelope.request_id, &broker_order_id))
            }
            Command::Modify => {
                // Re-record the referenced order; report success with the same broker id.
                let record = self
                    .orders
                    .get_mut(&envelope.broker_order_id)
                    .context("modify references unknown order")?;
                if let Some(order) = &envelope.order {
                    record.price = order.price.clone();
                }
                Ok(self.make_success(
                    Command::Modify,
                    &envelope.request_id,
                    &envelope.broker_order_id,
                ))
            }
            Command::Cancel => {
                let record = self
                    .orders
                    .get_mut(&envelope.broker_order_id)
                    .context("cancel references unknown order")?;
                record.status = FakeOrderStatus::Canceled;
                debug!(broker_order_id = %envelope.broker_order_id, "fake bridge canceled order");
                self.push_report(ReportEnvelope {
                    record_type: RECORD_REPORT.to_string(),
                    contract_version: 1,
                    request_id: envelope.request_id.clone(),
                    command: Command::Cancel.as_str().to_string(),
                    outcome: "SUCCESS".to_string(),
                    broker_order_id: envelope.broker_order_id.clone(),
                    order_status: Some(FakeOrderStatus::Canceled.as_str().to_string()),
                    report_type: Some("order_canceled".to_string()),
                    ..ReportEnvelope::default()
                });
                Ok(self.make_success(
                    Command::Cancel,
                    &envelope.request_id,
                    &envelope.broker_order_id,
                ))
            }
            Command::QueryOrder => {
                let record = self
                    .orders
                    .get(&envelope.broker_order_id)
                    .context("query references unknown order")?;
                Ok(ReportEnvelope {
                    record_type: RECORD_REPORT.to_string(),
                    contract_version: 1,
                    request_id: envelope.request_id.clone(),
                    command: Command::QueryOrder.as_str().to_string(),
                    outcome: "SUCCESS".to_string(),
                    broker_order_id: record.broker_order_id.clone(),
                    order_status: Some(record.status.as_str().to_string()),
                    report_type: Some("order_status".to_string()),
                    ..ReportEnvelope::default()
                })
            }
            _ => Err(anyhow!("fake bridge: scripted Accept for non-order command")),
        }
    }

    fn emit_fill(&self, envelope: &CommandEnvelope) {
        let Some(order) = envelope.order.as_ref() else { return };
        let broker_order_id = self
            .orders
            .get(&envelope.client_order_ref)
            .map(|r| r.broker_order_id.clone())
            .or_else(|| {
                self.orders
                    .iter()
                    .find(|(_, r)| r.client_order_ref == envelope.client_order_ref)
                    .map(|(id, _)| id.clone())
            })
            .unwrap_or_default();
        if broker_order_id.is_empty() {
            return;
        }
        self.push_report(ReportEnvelope {
            record_type: RECORD_REPORT.to_string(),
            contract_version: 1,
            request_id: envelope.request_id.clone(),
            command: Command::Place.as_str().to_string(),
            outcome: "SUCCESS".to_string(),
            broker_order_id,
            order_status: Some(FakeOrderStatus::Filled.as_str().to_string()),
            report_type: Some("order_filled".to_string()),
            fill_quantity: Some(order.quantity.clone()),
            fill_price: Some(if order.price.is_empty() {
                "100".to_string()
            } else {
                order.price.clone()
            }),
            ..ReportEnvelope::default()
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bridge::protocol::{Command, OrderCommand, OrderType, Product, TransactionType, Validity};

    fn place_env() -> CommandEnvelope {
        CommandEnvelope {
            instruction_id: "inst-1".into(),
            execution_attempt_id: "att-1".into(),
            client_order_ref: "CLIENT-1".into(),
            order: Some(
                OrderCommand::new("NFO", "NIFTY")
                    .with_quantity("10")
                    .with_side(TransactionType::Buy)
                    .with_order_type(OrderType::Lmt)
                    .with_product(Product::Cash)
                    .with_validity(Validity::Day)
                    .with_price("100"),
            ),
            ..CommandEnvelope::new(Command::Place, "req-1")
        }
    }

    #[tokio::test]
    async fn place_returns_success_with_broker_id() {
        let mut b = FakeBridge::new();
        b.connect().await.unwrap();
        b.script(CommandScript::Accept);
        let mut env = place_env();
        env.command = Command::Place.as_str().to_string();
        let rep = b.send_command(env).await.unwrap();
        assert!(rep.is_success());
        assert!(!rep.broker_order_id.is_empty());
        b.disconnect().await.unwrap();
        assert!(!b.is_connected());
    }

    #[tokio::test]
    async fn rejection_and_unknown_classify() {
        let mut b = FakeBridge::new();
        b.connect().await.unwrap();
        b.script(CommandScript::Reject("insufficient_margin".into()));
        b.script(CommandScript::Unknown("ambigous_state".into()));
        let mut env = place_env();
        env.command = Command::Place.as_str().to_string();
        let rej = b.send_command(env.clone()).await.unwrap();
        assert_eq!(rej.outcome(), Some(crate::bridge::protocol::ReportOutcome::Rejected));
        let unk = b.send_command(env).await.unwrap();
        assert_eq!(unk.outcome(), Some(crate::bridge::protocol::ReportOutcome::Unknown));
    }

    #[tokio::test]
    async fn timeout_returns_error() {
        let mut b = FakeBridge::new();
        b.connect().await.unwrap();
        b.script(CommandScript::Timeout);
        let mut env = place_env();
        env.command = Command::Place.as_str().to_string();
        assert!(b.send_command(env).await.is_err());
    }
}
