//! Execution safety gate state machine.
//!
//! The only enablement path is `HALTED -> RECONCILING -> APPROVAL_PENDING -> ENABLED`. A service
//! boots into `HALTED`; while `HALTED` no broker command is emitted. A safety halt may return to
//! `HALTED` from any state. This enforces the plan's invariant that the default and uncertain
//! state is `HALTED` ("live money: prohibited").

use std::fmt;

/// The execution gate state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExecState {
    /// No execution is permitted; the service boot state.
    Halted,
    /// Reconciling local state against the broker before any new submission is allowed.
    Reconciling,
    /// Waiting for explicit human approval to enable execution.
    ApprovalPending,
    /// Execution is permitted.
    Enabled,
}

impl ExecState {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Halted => "HALTED",
            Self::Reconciling => "RECONCILING",
            Self::ApprovalPending => "APPROVAL_PENDING",
            Self::Enabled => "ENABLED",
        }
    }
}

impl fmt::Display for ExecState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// A gate transition that is not part of the sanctioned enablement path.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InvalidTransition {
    pub from: ExecState,
    pub to: ExecState,
}

impl fmt::Display for InvalidTransition {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "invalid gate transition {} -> {}", self.from, self.to)
    }
}

impl std::error::Error for InvalidTransition {}

/// The execution safety gate.
#[derive(Debug, Clone)]
pub struct Gate {
    state: ExecState,
    safety_halt_count: u64,
}

impl Default for Gate {
    fn default() -> Self {
        Self::new()
    }
}

impl Gate {
    /// A fresh gate always boots into `HALTED`.
    pub fn new() -> Self {
        Self {
            state: ExecState::Halted,
            safety_halt_count: 0,
        }
    }

    pub fn state(&self) -> ExecState {
        self.state
    }

    /// Whether broker commands may be emitted.
    pub fn can_execute(&self) -> bool {
        self.state == ExecState::Enabled
    }

    /// The number of times the gate has been safety-halted.
    pub fn safety_halt_count(&self) -> u64 {
        self.safety_halt_count
    }

    /// Attempts a forward transition along the sanctioned enablement path.
    ///
    /// Returns `InvalidTransition` if `from -> to` is not sanctioned.
    pub fn transition(&mut self, to: ExecState) -> Result<(), InvalidTransition> {
        let from = self.state;
        let sanctioned = match (from, to) {
            (ExecState::Halted, ExecState::Reconciling)
            | (ExecState::Reconciling, ExecState::ApprovalPending)
            | (ExecState::ApprovalPending, ExecState::Enabled) => true,
            _ => false,
        };
        if !sanctioned {
            return Err(InvalidTransition { from, to });
        }
        self.state = to;
        Ok(())
    }

    /// Raises a safety halt, returning the gate to `HALTED` from any state.
    pub fn safety_halt(&mut self) {
        if self.state != ExecState::Halted {
            self.safety_halt_count += 1;
        }
        self.state = ExecState::Halted;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn boots_into_halted_and_cannot_execute() {
        let g = Gate::new();
        assert_eq!(g.state(), ExecState::Halted);
        assert!(!g.can_execute());
    }

    #[test]
    fn only_enablement_path_is_sanctioned() {
        let mut g = Gate::new();
        // HALTED -> ENABLED directly is not sanctioned
        assert!(g.transition(ExecState::Enabled).is_err());
        // sanctioned path
        g.transition(ExecState::Reconciling).unwrap();
        g.transition(ExecState::ApprovalPending).unwrap();
        g.transition(ExecState::Enabled).unwrap();
        assert!(g.can_execute());
    }

    #[test]
    fn safety_halt_returns_to_halted_from_any_state() {
        let mut g = Gate::new();
        g.transition(ExecState::Reconciling).unwrap();
        g.transition(ExecState::ApprovalPending).unwrap();
        g.transition(ExecState::Enabled).unwrap();
        g.safety_halt();
        assert_eq!(g.state(), ExecState::Halted);
        assert!(!g.can_execute());
        assert_eq!(g.safety_halt_count(), 1);
    }
}
