//! Custom Nautilus [`ExecutionClient`] backed by an upstream bridge.
//!
//! `BridgeExecutionClient` is the long-lived execution/position authority of the service. It is
//! built on the audited Nautilus Rust execution APIs: an [`ExecutionClientCore`] (cache, connected
//! and started state) and an [`ExecutionEventEmitter`] (order/report/account event generation).
//!
//! Command flow is *enqueue-then-pump*: the synchronous `ExecutionClient` trait methods
//! ([`Self::submit_order`], [`Self::modify_order`], [`Self::cancel_order`]) validate the safety
//! gate and enqueue bridge jobs; an asynchronous pump ([`Self::process_pending`]) performs the
//! bridge round-trip and emits the resulting Nautilus events. This keeps the client usable from a
//! single-threaded (non-`Send`) Nautilus runtime context while the bridge I/O stays `async`.
//!
//! The service always boots into the safety gate `HALTED` state; while `HALTED` no broker command
//! is emitted and orders are denied. The only enablement path is
//! `HALTED -> RECONCILING -> APPROVAL_PENDING -> ENABLED`.

use std::{
    cell::RefCell,
    collections::{HashMap, VecDeque},
    rc::Rc,
};

use anyhow::{bail, Context, Result};
use async_trait::async_trait;
use nautilus_common::{
    clients::ExecutionClient,
    live::runner::get_exec_event_sender,
    messages::execution::{
        BatchCancelOrders, BatchModifyOrders, CancelAllOrders, CancelOrder, GenerateFillReports,
        GenerateOrderStatusReport, GenerateOrderStatusReports, GeneratePositionStatusReports,
        ModifyOrder, QueryAccount, QueryOrder, SubmitOrder, SubmitOrderList,
    },
};
use nautilus_core::{time::get_atomic_clock_realtime, Params, UnixNanos, UUID4};
use nautilus_live::{ExecutionClientCore, ExecutionEventEmitter};
use nautilus_model::{
    accounts::AccountAny,
    enums::{LiquiditySide, OmsType, OrderSide, OrderType as NautilusOrderType, TimeInForce},
    events::AccountState,
    identifiers::{AccountId, ClientId, ClientOrderId, InstrumentId, TradeId, Venue, VenueOrderId},
    instruments::Instrument,
    orders::{Order, OrderAny},
    reports::{FillReport, OrderStatusReport, PositionStatusReport},
    types::{AccountBalance, Currency, MarginBalance, Price, Quantity},
};

use crate::{
    bridge::{
        protocol::{
            Command, CommandEnvelope, OrderCommand, OrderType as BridgeOrderType, Product,
            ReportEnvelope, ReportOutcome, TransactionType, Validity,
        },
        BridgeClient, BridgeReportStream,
    },
    gate::{ExecState, Gate},
};

/// A bridge command awaiting its round-trip in the pump.
struct PendingJob {
    command: Command,
    client_order_id: ClientOrderId,
    envelope: CommandEnvelope,
}

/// The custom Nautilus execution client.
///
/// Deliberately non-`Send`: it owns `Rc`/`RefCell` state (cache view, gate, bridge, report stream,
/// positions) and is intended to run on a single-threaded Nautilus runtime context.
pub struct BridgeExecutionClient {
    core: ExecutionClientCore,
    clock: &'static nautilus_core::time::AtomicTime,
    emitter: ExecutionEventEmitter,
    bridge: Rc<RefCell<Box<dyn BridgeClient>>>,
    gate: Rc<RefCell<Gate>>,
    /// Queued bridge jobs awaiting [`Self::process_pending`].
    pending: RefCell<VecDeque<PendingJob>>,
    /// Asynchronous bridge report stream (fills, order-state updates, rejections).
    reports: RefCell<Option<BridgeReportStream>>,
    /// Venue order id recorded per client order id after a successful place.
    orders: Rc<RefCell<HashMap<ClientOrderId, VenueOrderId>>>,
    /// Net position (signed quantity) tracked per instrument from fills.
    positions: Rc<RefCell<HashMap<InstrumentId, rust_decimal::Decimal>>>,
}

impl BridgeExecutionClient {
    /// Creates a new client, always booting the safety gate into `HALTED`.
    #[must_use]
    pub fn new(core: ExecutionClientCore, bridge: Box<dyn BridgeClient>) -> Self {
        Self {
            clock: get_atomic_clock_realtime(),
            emitter: ExecutionEventEmitter::new(
                get_atomic_clock_realtime(),
                core.trader_id,
                core.account_id,
                core.account_type,
                core.base_currency,
            ),
            core,
            bridge: Rc::new(RefCell::new(bridge)),
            gate: Rc::new(RefCell::new(Gate::new())),
            pending: RefCell::new(VecDeque::new()),
            reports: RefCell::new(None),
            orders: Rc::new(RefCell::new(HashMap::new())),
            positions: Rc::new(RefCell::new(HashMap::new())),
        }
    }

    /// Returns a reference to the shared safety gate.
    #[must_use]
    pub fn gate(&self) -> &Rc<RefCell<Gate>> {
        &self.gate
    }

    /// Current gate state.
    #[must_use]
    pub fn gate_state(&self) -> ExecState {
        self.gate.borrow().state()
    }

    /// Net position (signed quantity) currently held for an instrument.
    #[must_use]
    pub fn position(&self, instrument_id: &InstrumentId) -> rust_decimal::Decimal {
        self.positions
            .borrow()
            .get(instrument_id)
            .copied()
            .unwrap_or_default()
    }

    /// Advances the enablement path one sanctioned step (`HALTED -> ... -> ENABLED`).
    ///
    /// Returns `InvalidTransition` when the requested step is not sanctioned.
    pub fn advance_gate(&self, to: ExecState) -> Result<(), crate::gate::InvalidTransition> {
        self.gate.borrow_mut().transition(to)
    }

    /// Applies a safety halt, returning the gate to `HALTED` from any state (fail-closed).
    pub fn safety_halt(&self) {
        self.gate.borrow_mut().safety_halt();
    }

    /// Drains queue bridge jobs, performing the bridge round-trip and emitting events, then
    /// drains any asynchronous bridge reports (fills, cancellations).
    pub async fn process_pending(&mut self) -> Result<()> {
        let jobs = std::mem::take(&mut *self.pending.get_mut());
        for job in jobs {
            self.execute_job(job).await?;
        }
        self.drain_reports();
        Ok(())
    }

    /// Single-threaded invariant: only the pump touches the bridge, so holding the `RefCell`
    /// guard across the deterministic bridge round-trip cannot alias another borrow.
    #[allow(clippy::await_holding_refcell_ref)]
    async fn execute_job(&mut self, job: PendingJob) -> Result<()> {
        let order = self
            .core
            .get_order(&job.client_order_id)
            .with_context(|| format!("order {} not in cache", job.client_order_id))?;
        let ts_event = self.clock.get_time_ns();

        let reply = self
            .bridge
            .borrow_mut()
            .send_command(job.envelope)
            .await
            .with_context(|| format!("bridge {} failed", job.command))?;

        match reply.outcome() {
            Some(ReportOutcome::Success) => match job.command {
                Command::Place => {
                    let venue_order_id = VenueOrderId::new(&reply.broker_order_id);
                    self.emitter.emit_order_submitted(&order);
                    self.emitter
                        .emit_order_accepted(&order, venue_order_id, ts_event);
                    self.orders
                        .borrow_mut()
                        .insert(job.client_order_id, venue_order_id);
                }
                Command::Modify => {
                    let venue_order_id = VenueOrderId::new(&reply.broker_order_id);
                    let new_qty = order.quantity();
                    let new_px = order.price();
                    self.emitter.emit_order_updated(
                        &order,
                        venue_order_id,
                        new_qty,
                        new_px,
                        None,
                        None,
                        ts_event,
                    );
                }
                Command::Cancel => {
                    // The fake bridge additionally pushes an asynchronous `order_canceled` report;
                    // the terminal canceled event is emitted when that report is drained.
                }
                _ => {}
            },
            Some(ReportOutcome::Rejected) => {
                self.emitter
                    .emit_order_rejected(&order, &reply.reason, ts_event, false);
            }
            Some(ReportOutcome::Unknown) | None => {
                // Ambiguous outcome: fail closed and halt.
                self.gate.borrow_mut().safety_halt();
                self.emitter.emit_order_rejected(
                    &order,
                    &format!("{}: UNKNOWN bridge outcome; safety-halted", reply.outcome),
                    ts_event,
                    false,
                );
            }
        }
        Ok(())
    }

    fn drain_reports(&self) {
        let mut guard = self.reports.borrow_mut();
        if let Some(rx) = guard.as_mut() {
            while let Ok(report) = rx.try_recv() {
                self.handle_report(report);
            }
        }
    }

    fn handle_report(&self, report: ReportEnvelope) {
        let Ok(order) = self
            .core
            .get_order(&ClientOrderId::new(&report.client_order_ref))
        else {
            return;
        };
        let ts_event = self.clock.get_time_ns();

        match report.report_type.as_deref() {
            Some("order_filled") => self.handle_fill(&order, &report, ts_event),
            Some("order_canceled") => {
                let venue_order_id = VenueOrderId::new(&report.broker_order_id);
                self.emitter
                    .emit_order_canceled(&order, Some(venue_order_id), ts_event);
            }
            _ => {}
        }
    }

    fn handle_fill(&self, order: &OrderAny, report: &ReportEnvelope, ts_event: UnixNanos) {
        let venue_order_id = VenueOrderId::new(&report.broker_order_id);
        let trade_id = if report.postback_event_id.is_empty() {
            TradeId::new(format!("T{}", ts_event))
        } else {
            TradeId::new(&report.postback_event_id)
        };
        let last_qty = Quantity::from(report.fill_quantity.as_deref().unwrap_or("0"));
        let last_px = Price::from(report.fill_price.as_deref().unwrap_or("0"));
        let quote_currency = self
            .core
            .cache()
            .instrument(&order.instrument_id())
            .map(|i| i.quote_currency())
            .unwrap_or_else(|| Currency::from("USDT"));

        // Update net position from the fill.
        let qty = last_qty.as_decimal();
        let signed = match order.order_side() {
            OrderSide::Buy => qty,
            OrderSide::Sell => -qty,
            _ => qty,
        };
        *self
            .positions
            .borrow_mut()
            .entry(order.instrument_id())
            .or_default() += signed;

        self.emitter.emit_order_filled(
            order,
            venue_order_id,
            None,
            trade_id,
            last_qty,
            last_px,
            quote_currency,
            None,
            LiquiditySide::Taker,
            ts_event,
        );
    }

    /// Builds a bridge [`CommandEnvelope`] for an order.
    fn build_order_envelope(&self, command: Command, order: &OrderAny) -> CommandEnvelope {
        let side = match order.order_side() {
            OrderSide::Buy => TransactionType::Buy,
            OrderSide::Sell => TransactionType::Sell,
            _ => TransactionType::Buy,
        };
        let bridge_type = match order.order_type() {
            NautilusOrderType::Market => BridgeOrderType::Mkt,
            NautilusOrderType::Limit => BridgeOrderType::Lmt,
            NautilusOrderType::StopMarket => BridgeOrderType::SlMkt,
            NautilusOrderType::StopLimit => BridgeOrderType::SlLmt,
            _ => BridgeOrderType::Lmt,
        };
        let validity = match order.time_in_force() {
            TimeInForce::Ioc => Validity::Ioc,
            _ => Validity::Day,
        };
        let symbol = order.instrument_id().symbol.to_string();
        let quantity = order.quantity().to_string();
        let price = order.price().map(|p| p.to_string()).unwrap_or_default();

        let mut envelope = CommandEnvelope::new(command, &UUID4::new().to_string());
        envelope.client_order_ref = order.client_order_id().to_string();
        envelope.order = Some(
            OrderCommand::new(self.core.venue.as_str(), &symbol)
                .with_side(side)
                .with_quantity(&quantity)
                .with_order_type(bridge_type)
                .with_product(Product::Intraday)
                .with_validity(validity)
                .with_price(&price),
        );
        envelope
    }

    fn order_status_report(&self, order: &OrderAny) -> OrderStatusReport {
        let venue_order_id = self
            .orders
            .borrow()
            .get(&order.client_order_id())
            .copied()
            .unwrap_or_else(|| VenueOrderId::new(""));
        let ts = self.clock.get_time_ns();
        OrderStatusReport::new(
            self.core.account_id,
            order.instrument_id(),
            Some(order.client_order_id()),
            venue_order_id,
            order.order_side(),
            order.order_type(),
            order.time_in_force(),
            order.status(),
            order.quantity(),
            order.filled_qty(),
            ts,
            ts,
            ts,
            None,
        )
    }
}

#[async_trait(?Send)]
impl ExecutionClient for BridgeExecutionClient {
    fn is_connected(&self) -> bool {
        self.core.is_connected()
    }

    fn client_id(&self) -> ClientId {
        self.core.client_id
    }

    fn account_id(&self) -> AccountId {
        self.core.account_id
    }

    fn venue(&self) -> Venue {
        self.core.venue
    }

    fn oms_type(&self) -> OmsType {
        self.core.oms_type
    }

    fn get_account(&self) -> Option<AccountAny> {
        self.core
            .cache()
            .account(&self.core.account_id)
            .map(|account_ref| (*account_ref).clone())
    }

    fn generate_account_state(
        &self,
        balances: Vec<AccountBalance>,
        margins: Vec<MarginBalance>,
        reported: bool,
        ts_event: UnixNanos,
        info: Option<Params>,
    ) -> Result<()> {
        let _ = info;
        let state = AccountState::new(
            self.core.account_id,
            self.core.account_type,
            balances,
            margins,
            reported,
            UUID4::new(),
            ts_event,
            self.clock.get_time_ns(),
            self.core.base_currency,
        );
        self.emitter.send_account_state(state);
        Ok(())
    }

    fn start(&mut self) -> Result<()> {
        if self.core.is_started() {
            return Ok(());
        }
        let sender = get_exec_event_sender();
        self.emitter.set_sender(sender);
        self.core.set_started();
        Ok(())
    }

    fn stop(&mut self) -> Result<()> {
        if self.core.is_stopped() {
            return Ok(());
        }
        self.core.set_stopped();
        Ok(())
    }

    #[allow(clippy::await_holding_refcell_ref)]
    async fn connect(&mut self) -> Result<()> {
        self.bridge.borrow_mut().connect().await?;
        if let Some(rx) = self.bridge.borrow_mut().take_reports() {
            *self.reports.get_mut() = Some(rx);
        }
        self.core.set_connected();
        Ok(())
    }

    #[allow(clippy::await_holding_refcell_ref)]
    async fn disconnect(&mut self) -> Result<()> {
        self.bridge.borrow_mut().disconnect().await?;
        self.core.set_disconnected();
        Ok(())
    }

    fn submit_order(&self, cmd: SubmitOrder) -> Result<()> {
        if !self.gate.borrow().can_execute() {
            if let Ok(order) = self.core.get_order(&cmd.client_order_id) {
                self.emitter.emit_order_denied(
                    &order,
                    "execution gate HALTED: enable path before submitting",
                );
            }
            return Ok(());
        }
        let Some(order) = self.core.get_order(&cmd.client_order_id).ok() else {
            bail!("order {} not in cache", cmd.client_order_id);
        };
        let envelope = self.build_order_envelope(Command::Place, &order);
        self.pending.borrow_mut().push_back(PendingJob {
            command: Command::Place,
            client_order_id: cmd.client_order_id,
            envelope,
        });
        Ok(())
    }

    fn submit_order_list(&self, cmd: SubmitOrderList) -> Result<()> {
        // Batch submission (`OrderList` over `OrderInitialized`) is not part of T4's fake-bridge
        // scope; individual orders are driven through `Self::submit_order`.
        let _ = cmd;
        Ok(())
    }

    fn modify_order(&self, cmd: ModifyOrder) -> Result<()> {
        if !self.gate.borrow().can_execute() {
            if let Ok(order) = self.core.get_order(&cmd.client_order_id) {
                self.emitter.emit_order_modify_rejected(
                    &order,
                    None,
                    "execution gate HALTED: enable path before modifying",
                    self.clock.get_time_ns(),
                );
            }
            return Ok(());
        }
        let Some(order) = self.core.get_order(&cmd.client_order_id).ok() else {
            bail!("order {} not in cache", cmd.client_order_id);
        };
        let mut envelope = self.build_order_envelope(Command::Modify, &order);
        envelope.broker_order_id = cmd
            .venue_order_id
            .map(|v| v.to_string())
            .unwrap_or_default();
        self.pending.borrow_mut().push_back(PendingJob {
            command: Command::Modify,
            client_order_id: cmd.client_order_id,
            envelope,
        });
        Ok(())
    }

    fn batch_modify_orders(&self, cmd: BatchModifyOrders) -> Result<()> {
        for modify in cmd.modifies {
            self.modify_order(modify)?;
        }
        Ok(())
    }

    fn cancel_order(&self, cmd: CancelOrder) -> Result<()> {
        if !self.gate.borrow().can_execute() {
            if let Ok(order) = self.core.get_order(&cmd.client_order_id) {
                self.emitter.emit_order_cancel_rejected(
                    &order,
                    None,
                    "execution gate HALTED: enable path before cancelling",
                    self.clock.get_time_ns(),
                );
            }
            return Ok(());
        }
        let Some(order) = self.core.get_order(&cmd.client_order_id).ok() else {
            bail!("order {} not in cache", cmd.client_order_id);
        };
        let mut envelope = self.build_order_envelope(Command::Cancel, &order);
        envelope.broker_order_id = cmd
            .venue_order_id
            .map(|v| v.to_string())
            .unwrap_or_default();
        self.pending.borrow_mut().push_back(PendingJob {
            command: Command::Cancel,
            client_order_id: cmd.client_order_id,
            envelope,
        });
        Ok(())
    }

    fn cancel_all_orders(&self, cmd: CancelAllOrders) -> Result<()> {
        // T4 scope: cancelling a specific order. All-orders cancellation for a venue is a
        // later-phase bridge feature.
        let _ = cmd;
        Ok(())
    }

    fn batch_cancel_orders(&self, cmd: BatchCancelOrders) -> Result<()> {
        for cancel in cmd.cancels {
            self.cancel_order(cancel)?;
        }
        Ok(())
    }

    fn query_account(&self, cmd: QueryAccount) -> Result<()> {
        let _ = cmd;
        Ok(())
    }

    fn query_order(&self, cmd: QueryOrder) -> Result<()> {
        if let Ok(order) = self.core.get_order(&cmd.client_order_id) {
            let report = self.order_status_report(&order);
            self.emitter.send_order_status_report(report);
        }
        Ok(())
    }

    async fn generate_order_status_report(
        &self,
        cmd: &GenerateOrderStatusReport,
    ) -> Result<Option<OrderStatusReport>> {
        let Some(client_order_id) = &cmd.client_order_id else {
            return Ok(None);
        };
        if let Ok(order) = self.core.get_order(client_order_id) {
            return Ok(Some(self.order_status_report(&order)));
        }
        Ok(None)
    }

    async fn generate_order_status_reports(
        &self,
        cmd: &GenerateOrderStatusReports,
    ) -> Result<Vec<OrderStatusReport>> {
        let _ = cmd;
        let known: Vec<ClientOrderId> = self.orders.borrow().keys().cloned().collect();
        let mut reports = Vec::new();
        for client_order_id in known {
            if let Ok(order) = self.core.get_order(&client_order_id) {
                reports.push(self.order_status_report(&order));
            }
        }
        Ok(reports)
    }

    async fn generate_fill_reports(&self, cmd: GenerateFillReports) -> Result<Vec<FillReport>> {
        let _ = cmd;
        Ok(Vec::new())
    }

    async fn generate_position_status_reports(
        &self,
        cmd: &GeneratePositionStatusReports,
    ) -> Result<Vec<PositionStatusReport>> {
        let _ = cmd;
        Ok(Vec::new())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use nautilus_common::cache::Cache;
    use nautilus_model::enums::AccountType;
    use nautilus_model::identifiers::TraderId;
    use std::{cell::RefCell, rc::Rc};

    #[test]
    fn client_boots_into_halted() {
        let core = ExecutionClientCore::new(
            TraderId::from("TRADER-001"),
            ClientId::from("EXEC-001"),
            Venue::from("NFO"),
            OmsType::Netting,
            AccountId::from("EXEC-001"),
            AccountType::Cash,
            None,
            Rc::new(RefCell::new(Cache::default())),
        );
        let client = BridgeExecutionClient::new(core, Box::new(crate::bridge::FakeBridge::new()));
        assert_eq!(client.gate_state(), ExecState::Halted);
        assert!(!client.is_connected());
    }
}
