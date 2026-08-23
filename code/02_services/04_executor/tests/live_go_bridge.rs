//! Cross-language interop test against a running Go execution bridge.
//!
//! Exercises the production [`HttpBridgeClient`] transport against the *real* Go bridge
//! (`code/02_services/06_execution_bridge/go-bridge`) running in its `fake` profile — no real
//! Arrow credentials involved. This is the wire-conformance proof that the Rust command path and
//! the Go `server.go` contract interoperate byte-for-byte.
//!
//! Run:  start the Go bridge in fake mode, then point this test at it:
//!
//! ```text
//! EXECUTION_BRIDGE_MODE=fake EXECUTION_BRIDGE_AUTH_TOKEN=devtest \
//!   EXECUTION_BRIDGE_LISTEN_ADDR=127.0.0.1:8788 go run .
//! EXECUTION_BRIDGE_URL=http://127.0.0.1:8788 EXECUTION_BRIDGE_AUTH_TOKEN=devtest \
//!   cargo test --test live_go_bridge
//! ```
//!
//! The test skips (not fails) when the env vars are unset, so the rest of the suite still runs
//! in fully-offline environments.

use nautilus_execution_service::bridge::protocol::{
    Command, CommandEnvelope, OrderCommand, OrderType, Product, ReportOutcome, TransactionType,
    Validity,
};
use nautilus_execution_service::bridge::{BridgeClient, HttpBridgeClient};

fn bridge_from_env() -> Option<(String, String)> {
    let url = std::env::var("EXECUTION_BRIDGE_URL").ok()?;
    let token = std::env::var("EXECUTION_BRIDGE_AUTH_TOKEN").ok()?;
    Some((url, token))
}

#[tokio::test]
async fn live_place_command_round_trips_against_go_fake_bridge() {
    let Some((url, token)) = bridge_from_env() else {
        eprintln!("skipping: EXECUTION_BRIDGE_URL/EXECUTION_BRIDGE_AUTH_TOKEN not set");
        return;
    };

    let mut client = HttpBridgeClient::new(url, token);
    client
        .connect()
        .await
        .expect("bridge reachable over /healthz");
    assert!(client.is_connected());

    let envelope = CommandEnvelope {
        record_type: "execution_command".into(),
        contract_version: 1,
        request_id: "live-go-001".into(),
        command: Command::Place.as_str().into(),
        instruction_id: "ins-live".into(),
        execution_attempt_id: "ea-live".into(),
        client_order_ref: "COREF00000001".into(),
        broker_order_id: String::new(),
        order: Some(
            OrderCommand::new("NSE", "RELIANCE")
                .with_quantity("5")
                .with_side(TransactionType::Buy)
                .with_order_type(OrderType::Mkt)
                .with_product(Product::Cash)
                .with_validity(Validity::Day),
        ),
    };

    let report = client
        .send_command(envelope)
        .await
        .expect("bridge accepted the place command");
    assert_eq!(report.outcome(), Some(ReportOutcome::Success));
    assert_eq!(report.client_order_ref, "COREF00000001".to_string());
    assert!(
        report.broker_order_id.starts_with("fake-broker-order"),
        "expected a fake broker_order_id, got {:?}",
        report.broker_order_id
    );
}
