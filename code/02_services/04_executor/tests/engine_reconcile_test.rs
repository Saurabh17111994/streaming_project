//! Offline engine_reconcile test (Todo #29 — UNKNOWN→HALT never retry)
//!
//! Proves the offline reconcile path never re-issues Place:
//!   - `classify_bridge_report` mapping (pure, no I/O)
//!   - `unknown_halts_never_retries` gate
//!   - `reconcile_execution_mass_status` with FakeBroker: 2 UNKNOWN refs → [Accepted, StillUnknownHalted], 0 Place calls
//!
//! Offline contract: no FLUSS_BOOTSTRAP, no Arrow, gate HALTED (default). All I/O is the
//! in-process `FakeBridge`.

use nautilus_execution_service::bridge::protocol::ReportEnvelope;
use nautilus_execution_service::bridge::{CommandScript, FakeBridge};
use nautilus_execution_service::engine::reconcile::{reconcile_execution_mass_status, ReconcileDecision};
use nautilus_execution_service::execution::client::{classify_bridge_report, unknown_halts_never_retries, BridgeOutcome};

// ---------- classify_bridge_report ----------

fn report(outcome: &str, broker_order_id: &str) -> ReportEnvelope {
    ReportEnvelope {
        outcome: outcome.to_string(),
        broker_order_id: broker_order_id.to_string(),
        ..ReportEnvelope::default()
    }
}

#[test]
fn classify_success_with_broker_id_is_accepted() {
    let r = report("SUCCESS", "BRK-0001");
    assert_eq!(classify_bridge_report(&r), BridgeOutcome::Accepted);
    assert!(!unknown_halts_never_retries(BridgeOutcome::Accepted));
}

#[test]
fn classify_rejected_is_rejected() {
    let r = report("REJECTED", "BRK-0001");
    assert_eq!(classify_bridge_report(&r), BridgeOutcome::Rejected);
    assert!(!unknown_halts_never_retries(BridgeOutcome::Rejected));

    // Rejected with empty broker id still Rejected (broker decision is terminal)
    let r2 = report("REJECTED", "");
    assert_eq!(classify_bridge_report(&r2), BridgeOutcome::Rejected);
}

#[test]
fn classify_unknown_is_unknown() {
    let r = report("UNKNOWN", "BRK-0001");
    assert_eq!(classify_bridge_report(&r), BridgeOutcome::Unknown);
    assert!(unknown_halts_never_retries(BridgeOutcome::Unknown));

    let r2 = report("UNKNOWN", "");
    assert_eq!(classify_bridge_report(&r2), BridgeOutcome::Unknown);
    assert!(unknown_halts_never_retries(classify_bridge_report(&r2)));
}

#[test]
fn classify_success_with_empty_broker_id_is_unknown() {
    // Success but empty broker_order_id → Unknown (dossier §Reconciliation: never treat as accepted)
    let r = report("SUCCESS", "");
    assert_eq!(classify_bridge_report(&r), BridgeOutcome::Unknown);
    assert!(unknown_halts_never_retries(classify_bridge_report(&r)));
}

#[test]
fn classify_unknown_halts_never_retries_only_for_unknown() {
    assert!(unknown_halts_never_retries(BridgeOutcome::Unknown));
    assert!(!unknown_halts_never_retries(BridgeOutcome::Accepted));
    assert!(!unknown_halts_never_retries(BridgeOutcome::Rejected));
}

#[test]
fn classify_unrecognized_outcome_is_unknown() {
    let r = report("", "BRK-0001");
    assert_eq!(classify_bridge_report(&r), BridgeOutcome::Unknown);
    let r2 = report("BOGUS", "BRK-0001");
    assert_eq!(classify_bridge_report(&r2), BridgeOutcome::Unknown);
}

// ---------- reconcile_execution_mass_status ----------

#[tokio::test(flavor = "current_thread")]
async fn engine_reconcile_two_unknowns_yields_accepted_and_still_unknown_without_place() {
    // Offline: no FLUSS, no Arrow. Bridge is FakeBridge only.
    let mut fake = FakeBridge::new();

    // Script the two QueryOrder calls and the trailing ReconcileOrders mass snapshot:
    //   1st QueryOrder -> SUCCESS+orderNo (synthetic, via handle_accept Accept)
    //   2nd QueryOrder -> UNKNOWN (scripted Unknown)
    //   ReconcileOrders mass -> SUCCESS snapshot (Accept)
    fake.script(CommandScript::Accept);
    fake.script(CommandScript::Unknown("still-unknown".into()));
    fake.script(CommandScript::Accept);

    let refs = vec!["REF-ACCEPT-01".to_string(), "REF-UNKNOWN-02".to_string()];

    let decisions = reconcile_execution_mass_status(&mut fake, &refs)
        .await
        .expect("reconcile should succeed offline");

    assert_eq!(decisions.len(), 2, "one decision per UNKNOWN ref");
    assert_eq!(decisions[0].0, "REF-ACCEPT-01");
    assert_eq!(decisions[0].1, ReconcileDecision::Accepted, "Success+orderNo → Accepted");
    assert_eq!(decisions[1].0, "REF-UNKNOWN-02");
    assert_eq!(
        decisions[1].1,
        ReconcileDecision::StillUnknownHalted,
        "Unknown → StillUnknownHalted (HALT, never retry Place)"
    );

    // Never retry Place: only QueryOrder + ReconcileOrders were issued
    assert_eq!(fake.place_call_count(), 0, "UNKNOWN→HALT must never retry Place");
    assert_eq!(fake.query_call_count(), 2, "one QueryOrder per UNKNOWN ref");
    assert_eq!(fake.reconcile_call_count(), 1, "trailing ReconcileOrders mass snapshot");
    assert_eq!(fake.command_count(), 3);
    let log = fake.command_log();
    assert_eq!(log, vec!["query-order", "query-order", "reconcile-orders"]);

    // Unknowns halt and never become a retried Place on a second reconcile pass
    fake.script(CommandScript::Unknown("still-unknown".into()));
    fake.script(CommandScript::Unknown("still-unknown".into()));
    fake.script(CommandScript::Accept);
    let second = reconcile_execution_mass_status(&mut fake, &refs).await.unwrap();
    assert_eq!(second[1].1, ReconcileDecision::StillUnknownHalted);
    // Still zero Places after second pass
    assert_eq!(fake.place_call_count(), 0, "second pass must also never retry Place");
}

#[tokio::test(flavor = "current_thread")]
async fn engine_reconcile_empty_unknowns_only_does_mass_snapshot_no_place() {
    let mut fake = FakeBridge::new();
    fake.script(CommandScript::Accept); // mass ReconcileOrders only

    let decisions = reconcile_execution_mass_status(&mut fake, &[]).await.unwrap();
    assert!(decisions.is_empty());
    assert_eq!(fake.place_call_count(), 0);
    assert_eq!(fake.query_call_count(), 0);
    assert_eq!(fake.reconcile_call_count(), 1);
    assert_eq!(fake.command_count(), 1);
    assert_eq!(fake.command_log(), vec!["reconcile-orders"]);
}

#[tokio::test(flavor = "current_thread")]
async fn engine_reconcile_rejected_maps_to_rejected_without_place() {
    // Exercise the Rejected branch as well (not required by the ticket but probes the
    // non-Unknown guard: only Unknown halts).
    let mut fake = FakeBridge::new();
    fake.script(CommandScript::Reject("insufficient_margin".into()));
    fake.script(CommandScript::Accept); // mass

    let refs = vec!["REF-REJECT-01".to_string()];
    let decisions = reconcile_execution_mass_status(&mut fake, &refs).await.unwrap();
    assert_eq!(decisions[0].1, ReconcileDecision::Rejected);
    assert_eq!(fake.place_call_count(), 0);
    assert_eq!(fake.query_call_count(), 1);
    assert_eq!(fake.reconcile_call_count(), 1);
}
