//! Service bootstrap and runtime handle (WP-1).
//!
//! [`Runtime::init`] constructs the fail-closed boot surface: a gate that always starts `HALTED`,
//! a health snapshot that never implies `ENABLED`, and the shared [`ServerState`] the health
//! server reads. `main` calls `init` and then serves health until a shutdown signal arrives.

use anyhow::{ensure, Result};

use crate::clockwatch::{DriftMonitor, DriftStatus};
use crate::config::ServiceConfig;
use crate::engine::EngineFactory;
use crate::gate::{ExecState, Gate};
use crate::health::HealthStatus;
use crate::http::{self, ServerState};

/// The alive, booted service handle.
#[derive(Debug)]
pub struct Runtime {
    pub config: ServiceConfig,
    gate: Gate,
    state: ServerState,
}

impl Runtime {
    /// Builds the boot surface and asserts the fail-closed invariants:
    /// gate `HALTED` and health does not imply `ENABLED`.
    pub fn init(config: ServiceConfig) -> Result<Self> {
        // Offline `LiveNode` construction probe (compile + path verification only).
        EngineFactory::verify_construction_path()?;

        let gate = Gate::new();
        ensure!(
            gate.state() == ExecState::Halted,
            "service must boot HALTED"
        );
        let health = HealthStatus::new(gate.state());
        ensure!(
            health.health_does_not_imply_enabled(),
            "health must not imply ENABLED at boot"
        );
        let state = if config.gateway_shared_secret.is_empty() {
            ServerState::new(gate.state())
        } else {
            ServerState::with_gateway_auth(
                gate.state(),
                config.gateway_shared_secret.clone(),
                config.protocol_version.clone(),
            )
        };
        Ok(Self {
            config,
            gate,
            state,
        })
    }

    /// Current gate state (always `HALTED` at boot; monotonic from there).
    pub fn gate_state(&self) -> ExecState {
        self.gate.state()
    }

    /// Shared health state for the HTTP server.
    pub fn server_state(&self) -> ServerState {
        self.state.clone()
    }

    /// Health document for `/healthz`.
    pub fn health_json(&self) -> serde_json::Value {
        http::health_json(&self.state)
    }

    /// Samples clock drift and enforces it on the gate (B8): `Beyond`/`Unmeasurable`
    /// trigger `safety_halt()`; recovery is only ever via the sanctioned human path.
    /// The live NTP source is wired in Workstream D behind `OffsetSource`.
    pub fn enforce_clock_drift(&mut self, monitor: &mut DriftMonitor) -> DriftStatus {
        monitor.enforce(&mut self.gate)
    }

    /// Starts graceful shutdown: `/readyz` returns 503 while draining.
    pub fn begin_shutdown(&self) {
        self.state.set_draining(true);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::clockwatch::{FailingSource, FixedOffsetSource};
    use crate::config::ServiceConfig;

    fn halted_config() -> ServiceConfig {
        ServiceConfig {
            gateway_endpoint: String::new(),
            bridge_endpoint: String::new(),
            bridge_auth_token: String::new(),
            log_level: "info".into(),
            execution_enabled: false,
            listen_addr: "127.0.0.1:8787".into(),
            gateway_shared_secret: String::new(),
            protocol_version: "execution-gateway.v1".into(),
            clock_offset_limit_ms: 200,
            durable_gate_enabled: false,
            durable_attempts_enabled: false,
            durable_journal_enabled: false,
            durable_audit_enabled: false,
        }
    }

    #[test]
    fn runtime_boots_halted_and_not_trading() {
        let rt = Runtime::init(halted_config()).unwrap();
        assert_eq!(rt.gate_state(), ExecState::Halted);
        let h = rt.health_json();
        assert_eq!(h["gate_state"], "HALTED");
        assert_eq!(h["trading_ready"], false);
        assert_eq!(h["enabled"], false);
        // Health being alive must not imply ENABLED trading.
        assert_ne!(h["gate_state"], "ENABLED");
    }

    #[test]
    fn begin_shutdown_marks_draining() {
        let rt = Runtime::init(halted_config()).unwrap();
        assert!(!rt.health_json()["draining"].as_bool().unwrap());
        rt.begin_shutdown();
        assert!(rt.health_json()["draining"].as_bool().unwrap());
    }

    #[test]
    fn clock_drift_enforcement_uses_configured_limit_and_stays_fail_closed() {
        // Ties the config value (CLOCK_OFFSET_LIMIT_MS=200) through the runtime's
        // enforce_clock_drift to the DriftMonitor classification: at the limit it is
        // WITHIN (no halt — the gate was already HALTED at boot and stays so), and an
        // unmeasurable probe fails closed to HALTED rather than ever opening the gate.
        let mut rt = Runtime::init(halted_config()).unwrap();
        let mut within = DriftMonitor::new(200, Box::new(FixedOffsetSource(200)));
        assert_eq!(
            rt.enforce_clock_drift(&mut within),
            DriftStatus::Within(200)
        );
        assert_eq!(rt.gate_state(), ExecState::Halted);

        // The offline slice boots HALTED, so a beyond-limit sample cannot "halt" further —
        // the fail-closed contract is that it must NEVER leave HALTED. An unmeasurable
        // probe exercises the same enforcement path and must keep the gate HALTED.
        let mut beyond = DriftMonitor::new(200, Box::new(FixedOffsetSource(500)));
        assert_eq!(
            rt.enforce_clock_drift(&mut beyond),
            DriftStatus::Beyond(500)
        );
        let mut failing = DriftMonitor::new(200, Box::new(FailingSource));
        assert!(matches!(
            rt.enforce_clock_drift(&mut failing),
            DriftStatus::Unmeasurable(_)
        ));
        assert_eq!(
            rt.gate_state(),
            ExecState::Halted,
            "fail-closed: never leaves HALTED"
        );
    }
}
