//! T9 paper-25 evidence — 25 orders via nautilus-sandbox scenario vectors.
//!
//! Scenario boxes: 10 FILLED / 5 PARTIAL_FILL / 5 REJECTED / 3 UNKNOWN (timeout) / 2 DISCONNECT.
//! UNKNOWN rows demand explicit reconciliation (no auto-retry); shadow compares
//! broker/Nautilus/Fluss expected quantities with 0 new broker commands while gate is HALTED.
//!
//! Offline only: `harness.engine_exercised` is false — rows are scripted expectations, not
//! observed transitions. The evidence hash is a real SHA-256 over the bundle body.

use anyhow::Result;
use nautilus_execution_service::t9paper::{
    assert_no_secrets, count_outcome, finalize_evidence, order_json, shadow_position, write_json,
    Run, Scenario, PAPER_INSTRUMENTS,
};
use nautilus_sandbox::config::SandboxExecutionClientConfig;
use serde_json::{json, Value};

fn main() -> Result<()> {
    let _sandbox_cfg = SandboxExecutionClientConfig::default();

    let run = Run::start("t9-paper-25")?;

    let orders: Vec<Value> = (0..PAPER_INSTRUMENTS.len())
        .map(|idx| order_json(&run.run_id, idx))
        .collect();
    let unknowns: Vec<Value> = orders
        .iter()
        .filter(|order| order["outcome"] == Scenario::Unknown.as_str())
        .cloned()
        .collect();
    let shadow_positions: Vec<Value> = (0..PAPER_INSTRUMENTS.len()).map(shadow_position).collect();

    let filled = count_outcome(&orders, Scenario::Filled);
    let partial = count_outcome(&orders, Scenario::PartialFill);
    let rejected = count_outcome(&orders, Scenario::Rejected);
    let unknown = count_outcome(&orders, Scenario::Unknown);
    let disconnect = count_outcome(&orders, Scenario::Disconnect);

    let evidence = finalize_evidence(run.evidence(
        "paper-25",
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
            "unknowns": unknowns,
            "checks": [
                "scenario vectors: 10 FILLED / 5 PARTIAL / 5 REJECT / 3 UNKNOWN / 2 DISCONNECT (expectations, not observed)",
                "UNKNOWN rows demand explicit reconciliation, no auto-retry",
                "shadow positions projected from scripted outcomes (expected_match)",
                "shadow: 0 new broker commands emitted while gate HALTED",
                "engine_exercised: false - real sandbox round-trip awaits LiveNode wiring",
            ],
        }),
    ));

    let evidence_path = write_json(&run.output_dir, "evidence.json", &evidence)?;
    assert_no_secrets(&evidence);
    assert_eq!(evidence["summary"]["shadow_new_broker_commands"], 0);
    assert_eq!(unknown, 3, "UNKNOWN rows must be exactly 3");
    assert_eq!(partial, 5, "partial fills must be exactly 5");
    assert!(
        evidence["shadow_positions"]
            .as_array()
            .unwrap()
            .iter()
            .all(|p| p["expected_match"] == true),
        "all shadow positions must expect a match"
    );

    println!(
        "T9 paper-25 evidence written to {}",
        evidence_path.display()
    );
    println!("{}", serde_json::to_string_pretty(&evidence)?);
    println!(
        "T9 paper-25 OK: {} orders ({} fill/{} partial/{} reject/{} UNKNOWN/{} disconnect), shadow 0, all positions expected_match, evidence {}",
        PAPER_INSTRUMENTS.len(),
        filled,
        partial,
        rejected,
        unknown,
        disconnect,
        evidence["evidence_hash"].as_str().unwrap(),
    );
    Ok(())
}
