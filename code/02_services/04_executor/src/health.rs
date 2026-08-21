use crate::gate::ExecState;

/// Health dimensions — process health never implies ENABLED trading.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HealthStatus {
    pub process_alive: bool,
    pub readiness: bool,
    pub gate_state: ExecState,
    pub trading_ready: bool,
}

impl HealthStatus {
    pub fn new(gate_state: ExecState) -> Self {
        Self {
            process_alive: true,
            readiness: true,
            gate_state,
            trading_ready: gate_state == ExecState::Enabled,
        }
    }
    pub fn trading_implies_enabled(&self) -> bool {
        !self.trading_ready || self.gate_state == ExecState::Enabled
    }
    pub fn health_does_not_imply_enabled(&self) -> bool {
        self.process_alive && self.gate_state != ExecState::Enabled && !self.trading_ready
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::gate::ExecState;
    #[test]
    fn halted_not_trading() {
        let h = HealthStatus::new(ExecState::Halted);
        assert!(!h.trading_ready);
        assert!(h.health_does_not_imply_enabled());
    }
    #[test]
    fn enabled_trading() {
        let h = HealthStatus::new(ExecState::Enabled);
        assert!(h.trading_ready);
        assert!(h.trading_implies_enabled());
    }
}
