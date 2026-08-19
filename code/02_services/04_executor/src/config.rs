use anyhow::{bail, Context, Result};

/// Strict service configuration — HALTED default, fail-closed.
#[derive(Debug, Clone)]
pub struct ServiceConfig {
    pub gateway_endpoint: String,
    pub bridge_endpoint: String,
    pub log_level: String,
    pub execution_enabled: bool,
}

impl ServiceConfig {
    /// Parses from environment; fails closed on missing/invalid values.
    pub fn from_env() -> Result<Self> {
        let gateway = std::env::var("GATEWAY_ENDPOINT").context("GATEWAY_ENDPOINT must be set")?;
        let bridge = std::env::var("BRIDGE_ENDPOINT").context("BRIDGE_ENDPOINT must be set")?;
        if gateway.is_empty() || bridge.is_empty() { bail!("gateway/bridge endpoints must be non-empty"); }
        let enabled = std::env::var("EXECUTION_ENABLED").map(|v| v=="true" || v=="1").unwrap_or(false);
        if enabled { bail!("EXECUTION_ENABLED must not be true at boot — service always starts HALTED"); }
        Ok(Self{ gateway_endpoint: gateway, bridge_endpoint: bridge, log_level: std::env::var("LOG_LEVEL").unwrap_or_else(|_| "info".into()), execution_enabled: false })
    }
    pub fn is_halted_default(&self) -> bool { !self.execution_enabled }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test] fn halted_default(){ let c = ServiceConfig{ gateway_endpoint:"http://gw:8080".into(), bridge_endpoint:"http://bridge:8787".into(), log_level:"info".into(), execution_enabled:false }; assert!(c.is_halted_default()); }
}
