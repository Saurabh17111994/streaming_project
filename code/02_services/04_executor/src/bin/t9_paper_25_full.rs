//! T9 full-25 evidence — 25 orders plus UNKNOWN reconciliation templates + audit offload/restore.
//!
//! Extends `t9_paper_25` with: reconciliation snapshots for the 3 UNKNOWN slots (templated,
//! `captured: false`, `expected_delay_ms: 220`), encrypted audit offload with a real SHA-256
//! integrity root, restore verification, and deletion-governance (legal hold / 1y policy).
//!
//! Offline only: `harness.engine_exercised` is false; snapshots are templates and the audit
//! chain hashes the scenario vector content, not live broker transitions.

use anyhow::Result;
use nautilus_execution_service::t9paper::{
    assert_no_secrets, audit_offload_and_restore, count_outcome, finalize_evidence, order_json,
    reconciliation_snapshot, scenario_for_idx, shadow_position, write_json, Run, Scenario,
    PAPER_INSTRUMENTS, RETENTION_POLICY,
};
use nautilus_sandbox::config::SandboxExecutionClientConfig;
use serde_json::{json, Value};

fn main() -> Result<()> {
    let _sandbox_cfg = SandboxExecutionClientConfig::default();

    let run = Run::start("t9-full-25")?;

    let orders: Vec<Value> = (0..PAPER_INSTRUMENTS.len())
        .map(|idx| order_json(&run.run_id, idx))
        .collect();
    let unknowing: Vec<usize> = (0..PAPER_INSTRUMENTS.len())
        .filter(|&idx| scenario_for_idx(idx) == Scenario::Unknown)
        .collect();
    let reconciliation_snapshots: Vec<Value> = unknowing
        .iter()
        .map(|&idx| reconciliation_snapshot(idx))
        .collect();
    let shadow_positions: Vec<Value> = (0..PAPER_INSTRUMENTS.len()).map(shadow_position).collect();
    let audit = audit_offload_and_restore(&run.output_dir, &run.run_id, &orders)?;

    let filled = count_outcome(&orders, Scenario::Filled);
    let partial = count_outcome(&orders, Scenario::PartialFill);
    let rejected = count_outcome(&orders, Scenario::Rejected);
    let unknown = count_outcome(&orders, Scenario::Unknown);
    let disconnect = count_outcome(&orders, Scenario::Disconnect);

    let deletion_governance = json!({
        "legal_hold": false,
        "deletion_blocked_until": "2027-08-19 (1y)",
        "policy": RETENTION_POLICY,
    });

    let evidence = finalize_evidence(run.evidence(
        "full-25",
        json!({
            "instruments": PAPER_INSTRUMENTS.len(),
            "orders": orders,
            "summary": {
                "total": PAPER_INSTRUMENTS.len(),
                "filled": filled,
                "partial_fill": partial,
                "rejected": rejected,
                "unknown_timeout": unknown,
                "disconnect": disconnect,
                "filled_or_partial": filled + partial,
                "shadow_new_broker_commands": 0,
            },
            "shadow_positions": shadow_positions,
            "reconciliation_snapshots": reconciliation_snapshots,
            "reconciliation_summary": {
                "unknowns": unknown,
                "snapshots_templated": reconciliation_snapshots.len(),
                "captured": false,
                "all_mismatch_blocks_release": true,
                "no_auto_retry": true,
            },
            "audit_offload": audit["audit_offload"],
            "audit_restore": audit["audit_restore"],
            "deletion_governance": deletion_governance,
            "checks": [
                "scenario vectors: 10 FILLED / 5 PARTIAL / 5 REJECT / 3 UNKNOWN / 2 DISCONNECT (expectations, not observed)",
                "UNKNOWN reconciliation snapshots are templates (captured: false, expected_delay_ms 220)",
                "audit offload encrypted with real SHA-256 integrity root; restore verified",
                "legal hold / 1y deletion governance applied",
                "shadow compare broker/Nautilus/Fluss all expected_match, 0 new broker commands",
                "engine_exercised: false - real sandbox round-trip awaits LiveNode wiring",
            ],
        }),
    ));

    let evidence_path = write_json(&run.output_dir, "evidence.json", &evidence)?;
    assert_no_secrets(&evidence);
    assert_eq!(unknown, 3, "UNKNOWN rows must be exactly 3");
    assert_eq!(
        reconciliation_snapshots.len(),
        3,
        "reconciliation templates must cover the 3 UNKNOWN slots"
    );
    assert_eq!(
        audit["verified"], true,
        "audit restore must verify integrity_root"
    );
    assert!(
        evidence["shadow_positions"]
            .as_array()
            .unwrap()
            .iter()
            .all(|p| p["expected_match"] == true),
        "all shadow positions must expect a match"
    );

    println!("T9 full-25 evidence written to {}", evidence_path.display());
    println!("{}", serde_json::to_string_pretty(&evidence)?);
    println!(
        "T9 full-25 OK: reconciliation templates 3 (UNKNOWN), audit offload/restore verified ({}), shadow 0, all expected_match, evidence {}",
        audit["audit_restore"]["integrity_root"].as_str().unwrap_or_default(),
        evidence["evidence_hash"].as_str().unwrap(),
    );
    Ok(())
}
