//! Execution safety gate state machine.
//!
//! The only enablement path is `HALTED -> RECONCILING -> APPROVAL_PENDING -> ENABLED`. A service
//! boots into `HALTED`; while `HALTED` no broker command is emitted. A safety halt may return to
//! `HALTED` from any state. This enforces the plan's invariant that the default and uncertain
//! state is `HALTED` ("live money: prohibited").
//!
//! # Two-approval enablement (CONTROL-\*, INVARIANT-003)
//!
//! The final `APPROVAL_PENDING -> ENABLED` step is **not** reachable through [`Gate::transition`]:
//! a bare transition to `Enabled` is always rejected. The only route to `ENABLED` is
//! [`Gate::enable`], which requires:
//!
//! - a **current gate epoch** declared via [`Gate::set_epoch`] — and the epoch passed to
//!   `enable` must match it (a stale control-plane thread cannot enable on a mismatched/old term);
//! - **two approvals** ([`Gate::record_approval`]) from **two distinct operators**, each **authorized**
//!   (provisioned via [`Gate::add_authorized`] / [`Gate::new_with_authorized`]), both bound to the
//!   **same evidence hash**; and
//! - the evidence hash approvals were granted for is still bound (approval after the evidence
//!   changed is rejected).
//!
//! This makes INVARIANT-003 ("no ENABLED gate without two approvals") structurally enforced rather
//! than conventional. A safety halt (fail-closed) clears both approvals and the bound evidence, so
//! re-enabling always requires a fresh two-approval round (CONTROL-006).

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

/// Why [`Gate::enable`] rejected moving to `Enabled`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EnableError {
    /// `enable` was attempted outside `ApprovalPending`.
    NotApprovalPending(ExecState),
    /// No current epoch was declared (`Gate::set_epoch` never called).
    EpochUnset,
    /// The epoch supplied to `enable` does not match the gate's current epoch.
    EpochMismatch { expected: u64, got: u64 },
    /// INVARIANT-003: fewer than two approvals are recorded.
    RequiresTwoApprovals,
    /// Approvals were recorded but no evidence hash was bound.
    EvidenceMissing,
}

impl fmt::Display for EnableError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NotApprovalPending(s) => write!(f, "enable not allowed in {s}"),
            Self::EpochUnset => write!(f, "enable requires a declared gate epoch"),
            Self::EpochMismatch { expected, got } => {
                write!(f, "enable epoch mismatch: gate epoch {expected}, got {got}")
            }
            Self::RequiresTwoApprovals => {
                write!(f, "enable requires two distinct authorized approvals")
            }
            Self::EvidenceMissing => write!(f, "enable requires a bound evidence hash"),
        }
    }
}

impl std::error::Error for EnableError {}

/// Why [`Gate::record_approval`] rejected the approval.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ApprovalError {
    /// Approval was recorded outside `ApprovalPending`.
    NotApprovalPending(ExecState),
    /// The operator is not in the authorized set.
    Unauthorized,
    /// The same operator is trying to supply both the first and the second approval.
    SingleApprover,
    /// The approval binds a different evidence hash than a previously recorded approval.
    EvidenceChanged,
}

impl fmt::Display for ApprovalError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NotApprovalPending(s) => write!(f, "approval not allowed in {s}"),
            Self::Unauthorized => write!(f, "approver is not authorized"),
            Self::SingleApprover => {
                write!(f, "one operator cannot supply both approvals")
            }
            Self::EvidenceChanged => {
                write!(f, "approval evidence hash does not match the already-bound evidence")
            }
        }
    }
}

impl std::error::Error for ApprovalError {}

/// The execution safety gate.
#[derive(Debug, Clone)]
pub struct Gate {
    state: ExecState,
    safety_halt_count: u64,
    /// Current control-plane epoch (0 = not yet declared).
    epoch: u64,
    /// Evidence hash the approvals are bound to; `None` until first approval.
    enabled_evidence: Option<String>,
    /// First approver identity.
    approval_a: Option<String>,
    /// Second (distinct) approver identity.
    approval_b: Option<String>,
    /// Authorized operator identities.
    authorized: Vec<String>,
}

impl Default for Gate {
    fn default() -> Self {
        Self::new()
    }
}

impl Gate {
    /// A fresh gate always boots into `HALTED` with no authorized operators.
    pub fn new() -> Self {
        Self {
            state: ExecState::Halted,
            safety_halt_count: 0,
            epoch: 0,
            enabled_evidence: None,
            approval_a: None,
            approval_b: None,
            authorized: Vec::new(),
        }
    }

    /// A gate booting into `HALTED` with a pre-provisioned authorized operator set.
    pub fn new_with_authorized(operators: &[&str]) -> Self {
        let mut g = Self::new();
        g.authorized = operators.iter().map(|s| s.to_string()).collect();
        g
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

    /// The current control-plane epoch.
    pub fn epoch(&self) -> u64 {
        self.epoch
    }

    /// Declares the current control-plane epoch. `enable` requires `epoch > 0`.
    pub fn set_epoch(&mut self, epoch: u64) {
        self.epoch = epoch;
    }

    /// Provisions an authorized operator identity. Only authorized operators may approve.
    pub fn add_authorized(&mut self, operator: &str) {
        if !self.authorized.iter().any(|o| o == operator) {
            self.authorized.push(operator.to_string());
        }
    }

    /// Advances the enablement path one sanctioned step. The `APPROVAL_PENDING -> ENABLED` hop is
    /// intentionally absent: `Enabled` is reachable only via [`Gate::enable`] after two approvals.
    ///
    /// Returns `InvalidTransition` if `from -> to` is not sanctioned.
    pub fn transition(&mut self, to: ExecState) -> Result<(), InvalidTransition> {
        let from = self.state;
        let sanctioned = matches!(
            (from, to),
            (ExecState::Halted, ExecState::Reconciling)
                | (ExecState::Reconciling, ExecState::ApprovalPending)
        );
        if !sanctioned {
            return Err(InvalidTransition { from, to });
        }
        self.state = to;
        Ok(())
    }

    /// Records one operator approval binding an evidence hash. INVARIANT-003 requires two
    /// **distinct authorized** operators before `enable` becomes possible.
    pub fn record_approval(
        &mut self,
        approver: &str,
        evidence_hash: &str,
    ) -> Result<(), ApprovalError> {
        if self.state != ExecState::ApprovalPending {
            return Err(ApprovalError::NotApprovalPending(self.state));
        }
        if !self.authorized.iter().any(|o| o == approver) {
            return Err(ApprovalError::Unauthorized);
        }
        if self.approval_a.as_deref() == Some(approver) {
            return Err(ApprovalError::SingleApprover);
        }
        if let Some(bound) = &self.enabled_evidence {
            if bound != evidence_hash {
                return Err(ApprovalError::EvidenceChanged);
            }
        }
        if self.approval_a.is_none() {
            self.approval_a = Some(approver.to_string());
        } else {
            self.approval_b = Some(approver.to_string());
        }
        if self.enabled_evidence.is_none() {
            self.enabled_evidence = Some(evidence_hash.to_string());
        }
        Ok(())
    }

    /// The authoritative, and only, route to `ENABLED`.
    ///
    /// Requires the current epoch match the declared gate epoch, two distinct authorized
    /// approvals bound to a present evidence hash. Otherwise returns an [`EnableError`].
    pub fn enable(&mut self, epoch: u64) -> Result<(), EnableError> {
        if self.state != ExecState::ApprovalPending {
            return Err(EnableError::NotApprovalPending(self.state));
        }
        if self.epoch == 0 {
            return Err(EnableError::EpochUnset);
        }
        if epoch != self.epoch {
            return Err(EnableError::EpochMismatch {
                expected: self.epoch,
                got: epoch,
            });
        }
        // INVARIANT-003: no ENABLED gate without two approvals.
        if self.approval_a.is_none() || self.approval_b.is_none() {
            return Err(EnableError::RequiresTwoApprovals);
        }
        if self.enabled_evidence.is_none() {
            return Err(EnableError::EvidenceMissing);
        }
        self.state = ExecState::Enabled;
        Ok(())
    }

    /// Raises a safety halt, returning the gate to `HALTED` from any state and invalidating both
    /// approvals and the bound evidence (fail-closed), so re-enabling always re-approves.
    pub fn safety_halt(&mut self) {
        if self.state != ExecState::Halted {
            self.safety_halt_count += 1;
        }
        self.approval_a = None;
        self.approval_b = None;
        self.enabled_evidence = None;
        self.state = ExecState::Halted;
    }
}
#[cfg(test)]
mod tests {
    use super::*;

    fn authorized() -> Gate {
        let mut g = Gate::new();
        g.add_authorized("OPS-A");
        g.add_authorized("OPS-B");
        g
    }

    /// Drives the gate to APPROVAL_PENDING (no approvals yet).
    fn to_approval_pending(g: &mut Gate) {
        g.transition(ExecState::Reconciling).unwrap();
        g.transition(ExecState::ApprovalPending).unwrap();
    }

    #[test]
    fn boots_into_halted_and_cannot_execute() {
        let g = Gate::new();
        assert_eq!(g.state(), ExecState::Halted);
        assert!(!g.can_execute());
    }

    #[test]
    fn only_enablement_path_is_sanctioned() {
        let mut g = authorized();
        // HALTED -> ENABLED directly is not sanctioned.
        assert!(g.transition(ExecState::Enabled).is_err());
        // HALTED -> APPROVAL_PENDING skips RECONCILING.
        assert!(g.transition(ExecState::ApprovalPending).is_err());
        // Sanctioned path into APPROVAL_PENDING.
        to_approval_pending(&mut g);
        // Bare transition to ENABLED is now rejected even from APPROVAL_PENDING
        // (INVARIANT-003 structural enforcement).
        assert!(g.transition(ExecState::Enabled).is_err());
        g.set_epoch(1);
        g.record_approval("OPS-A", "h1").unwrap();
        g.record_approval("OPS-B", "h1").unwrap();
        g.enable(1).unwrap();
        assert!(g.can_execute());
    }

    #[test]
    fn safety_halt_returns_to_halted_from_any_state() {
        let mut g = authorized();
        to_approval_pending(&mut g);
        g.set_epoch(1);
        g.record_approval("OPS-A", "h1").unwrap();
        g.record_approval("OPS-B", "h1").unwrap();
        g.enable(1).unwrap();
        assert!(g.can_execute());
        g.safety_halt();
        assert_eq!(g.state(), ExecState::Halted);
        assert!(!g.can_execute());
        assert_eq!(g.safety_halt_count(), 1);
    }

    #[test]
    fn gate_cannot_skip_steps_or_silently_regress() {
        // T5 reconciliation (CHG-044): no jump may skip a step and no sanctioned
        // backward step exists other than the safety halt — matching the Java
        // canonical GateState.legalTargets().
        let mut g = authorized();
        // HALTED -> APPROVAL_PENDING skips RECONCILING.
        assert!(g.transition(ExecState::ApprovalPending).is_err());
        // HALTED -> ENABLED skips the whole enablement path.
        assert!(g.transition(ExecState::Enabled).is_err());
        g.transition(ExecState::Reconciling).unwrap();
        // APPROVAL_PENDING -> RECONCILING silently regresses instead of halting.
        assert!(g.transition(ExecState::Reconciling).is_err());
        g.transition(ExecState::ApprovalPending).unwrap();
        assert!(g.transition(ExecState::Reconciling).is_err());
        // Bare ApprovalPending -> Enabled is rejected; enable requires two approvals.
        assert!(g.transition(ExecState::Enabled).is_err());
        g.set_epoch(1);
        g.record_approval("OPS-A", "h1").unwrap();
        g.record_approval("OPS-B", "h1").unwrap();
        g.enable(1).unwrap();
        assert!(g.can_execute());
        // ENABLED can only go to HALTED (safety halt).
        assert!(g.transition(ExecState::Reconciling).is_err());
        g.safety_halt();
        assert_eq!(g.state(), ExecState::Halted);
    }

    #[test]
    fn invariant003_no_enabled_without_two_approvals() {
        let mut g = authorized();
        to_approval_pending(&mut g);
        g.set_epoch(1);
        // Zero approvals: enable rejected.
        assert_eq!(g.enable(1), Err(EnableError::RequiresTwoApprovals));
        // Single approval is still not enough (single approval must never enable).
        g.record_approval("OPS-A", "h1").unwrap();
        assert_eq!(g.enable(1), Err(EnableError::RequiresTwoApprovals));
        // Two distinct approvals enable.
        g.record_approval("OPS-B", "h1").unwrap();
        g.enable(1).unwrap();
        assert!(g.can_execute());
    }

    #[test]
    fn control001_unauthorized_operator_cannot_approve_or_enable() {
        let mut g = authorized();
        to_approval_pending(&mut g);
        g.set_epoch(1);
        // "MALLORY" is not authorized -> rejected, and never enables.
        assert_eq!(
            g.record_approval("MALLORY", "h1"),
            Err(ApprovalError::Unauthorized)
        );
        assert_eq!(g.enable(1), Err(EnableError::RequiresTwoApprovals));
    }

    #[test]
    fn control001_single_operator_cannot_supply_both_approvals() {
        let mut g = Gate::new();
        g.add_authorized("OPS-A"); // only one operator authorized
        to_approval_pending(&mut g);
        g.set_epoch(1);
        g.record_approval("OPS-A", "h1").unwrap();
        // Same actor tries to be the second approval -> SingleApprover.
        assert_eq!(
            g.record_approval("OPS-A", "h1"),
            Err(ApprovalError::SingleApprover)
        );
        assert_eq!(g.enable(1), Err(EnableError::RequiresTwoApprovals));
    }

    #[test]
    fn control002_approval_after_evidence_change_is_rejected() {
        let mut g = authorized();
        to_approval_pending(&mut g);
        g.set_epoch(1);
        g.record_approval("OPS-A", "h1").unwrap();
        // B approves a DIFFERENT evidence hash -> evidence changed, rejected.
        assert_eq!(
            g.record_approval("OPS-B", "h2"),
            Err(ApprovalError::EvidenceChanged)
        );
        assert_eq!(g.enable(1), Err(EnableError::RequiresTwoApprovals));
        // B re-approving the ORIGINAL hash is valid; enable succeeds.
        g.record_approval("OPS-B", "h1").unwrap();
        g.enable(1).unwrap();
        assert!(g.can_execute());
    }

    #[test]
    fn control002_enable_requires_declared_epoch() {
        let mut g = authorized();
        to_approval_pending(&mut g);
        // Epoch never declared (0).
        g.record_approval("OPS-A", "h1").unwrap();
        g.record_approval("OPS-B", "h1").unwrap();
        assert_eq!(g.enable(1), Err(EnableError::EpochUnset));
        g.set_epoch(1);
        g.enable(1).unwrap();
        assert!(g.can_execute());
    }

    #[test]
    fn control002_mismatched_epoch_cannot_enable() {
        let mut g = authorized();
        to_approval_pending(&mut g);
        g.set_epoch(5); // current control-plane epoch 5
        g.record_approval("OPS-A", "h1").unwrap();
        g.record_approval("OPS-B", "h1").unwrap();
        // A stale control-plane thread carrying epoch 4 cannot enable.
        assert_eq!(
            g.enable(4),
            Err(EnableError::EpochMismatch { expected: 5, got: 4 })
        );
        assert!(!g.can_execute());
        // Correct epoch enables.
        g.enable(5).unwrap();
        assert!(g.can_execute());
    }

    #[test]
    fn control006_safety_halt_invalidates_approvals_require_reapproval() {
        let mut g = authorized();
        to_approval_pending(&mut g);
        g.set_epoch(1);
        g.record_approval("OPS-A", "h1").unwrap();
        g.record_approval("OPS-B", "h1").unwrap();
        g.enable(1).unwrap();
        assert!(g.can_execute());
        // Rollback/during-active-gate -> safety halt clears approvals (fail-closed).
        g.safety_halt();
        assert!(!g.can_execute());
        // Old approvals are gone; re-enabling requires a fresh two-approval round.
        to_approval_pending(&mut g);
        assert_eq!(g.enable(1), Err(EnableError::RequiresTwoApprovals));
        g.record_approval("OPS-A", "h1").unwrap();
        g.record_approval("OPS-B", "h1").unwrap();
        g.enable(1).unwrap();
        assert!(g.can_execute());
    }

    #[test]
    fn control002_operator_session_restart_requires_reapproval() {
        // A fresh gate (operator session restarted) holds no approvals: even with a
        // declared epoch it cannot enable until both operators approve again.
        let mut g = Gate::new_with_authorized(&["OPS-A", "OPS-B"]);
        to_approval_pending(&mut g);
        g.set_epoch(1);
        assert_eq!(g.enable(1), Err(EnableError::RequiresTwoApprovals));
        g.record_approval("OPS-A", "h1").unwrap();
        g.record_approval("OPS-B", "h1").unwrap();
        g.enable(1).unwrap();
        assert!(g.can_execute());
    }

    #[test]
    fn approval_requires_approval_pending_state() {
        let mut g = authorized();
        // Approval is only honored while APPROVAL_PENDING.
        assert_eq!(
            g.record_approval("OPS-A", "h1"),
            Err(ApprovalError::NotApprovalPending(ExecState::Halted))
        );
        to_approval_pending(&mut g);
        g.set_epoch(1);
        g.record_approval("OPS-A", "h1").unwrap();
        // Enable is only honored while APPROVAL_PENDING.
        g.safety_halt();
        assert_eq!(
            g.enable(1),
            Err(EnableError::NotApprovalPending(ExecState::Halted))
        );
    }
}
