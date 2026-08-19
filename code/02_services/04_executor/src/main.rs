use nautilus_execution_service::{bridge, config::ServiceConfig, engine::EngineFactory, gate::Gate, health::HealthStatus};

fn main() -> anyhow::Result<()> {
    println!("nautilus-execution-service bootstrap ok");
    let gate = Gate::new();
    let health = HealthStatus::new(gate.state());
    assert!(health.health_does_not_imply_enabled(), "health must not imply ENABLED");
    let _ = EngineFactory::verify_construction_path()?;
    let _ = bridge::PROTOCOL_VERSION;
    // Service boots HALTED even when process health is good
    assert_eq!(gate.state().to_string(), "HALTED");
    Ok(())
}
