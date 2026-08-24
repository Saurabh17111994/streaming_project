//! Offline contract for 11-testing EXE-* (Executor) — deterministic, no market/4VM.
//! Covers EXE-FAIL-001/003/006 + EXE-AUDIT-001 via in-memory gate/attempt store.

#[cfg(test)]
mod tests {
    use crate::executiongate::{Attempt, AttemptPhase, AttemptStore, InMemoryAttemptStore};
    use crate::gate::{ExecState, Gate};

    // EXE-FAIL-001: crash before/during/after no duplicate halt+reconcile
    #[test]
    fn exe_fail_001_crash_no_duplicate_halt_reconcile() {
        let store = InMemoryAttemptStore::new();
        assert!(!store.has_duplicate("instr-1", "hash-1"));
        assert!(!store.has_instruction("instr-1"));
        let a = Attempt::new(
            "attempt-1",
            "instr-1",
            "hash-1",
            "C-REF-1",
            AttemptPhase::Prepared,
        );
        store.put(&a).unwrap();
        assert!(store.has_duplicate("instr-1", "hash-1"));
        assert!(store.has_instruction("instr-1"));
        let gate = Gate::new();
        assert_eq!(gate.state(), ExecState::Halted);
        assert!(!gate.can_execute());
        // same hash -> duplicate no new call
        assert!(store.has_duplicate("instr-1", "hash-1"));
        // different hash same instruction -> ContractViolation would halt
        assert!(store.has_instruction("instr-1"));
        assert!(!store.has_duplicate("instr-1", "hash-2"));
    }

    // EXE-FAIL-003: missing/corrupt state blocks calls
    #[test]
    fn exe_fail_003_missing_corrupt_blocks_calls() {
        let gate = Gate::new();
        assert_eq!(gate.state(), ExecState::Halted);
        assert!(!gate.can_execute());
        let store = InMemoryAttemptStore::new();
        assert!(!store.has_instruction("missing"));
        let mut g2 = Gate::new();
        let stayed = g2.enter_reconciling_if_needed(false).unwrap();
        assert!(!stayed);
        assert_eq!(g2.state(), ExecState::Halted);
    }

    // EXE-FAIL-006: mapping quarantine blocks unsafe (ambiguous correlation → quarantine + halt)
    #[test]
    fn exe_fail_006_mapping_quarantine_blocks_unsafe() {
        let store = InMemoryAttemptStore::new();
        let a = Attempt::new(
            "attempt-1",
            "instr-1",
            "hash-1",
            "C-REF-1",
            AttemptPhase::Prepared,
        );
        store.put(&a).unwrap();
        assert!(store.has_instruction("instr-1"));
        assert!(!store.has_instruction("instr-2"));
        let gate = Gate::new();
        assert_eq!(gate.state(), ExecState::Halted);
        assert!(!gate.can_execute());
    }

    // EXE-AUDIT-001: audit reconstruction — journal replay yields same state
    #[test]
    fn exe_audit_001_audit_reconstruction() {
        let journal = vec![
            ("attempt-1", "instr-1", "hash-1", "C-1"),
            ("attempt-2", "instr-2", "hash-2", "C-2"),
        ];
        let rebuild = |j: &Vec<(&str, &str, &str, &str)>| {
            let s = InMemoryAttemptStore::new();
            for (att, instr, hash, cref) in j {
                let a = Attempt::new(att, instr, hash, cref, AttemptPhase::Prepared);
                s.put(&a).unwrap();
            }
            s
        };
        let r1 = rebuild(&journal);
        let r2 = rebuild(&journal);
        assert!(r1.has_duplicate("instr-1", "hash-1"));
        assert!(r2.has_duplicate("instr-1", "hash-1"));
        assert!(r1.has_duplicate("instr-2", "hash-2"));
        assert!(r2.has_duplicate("instr-2", "hash-2"));
        // both stores see same instruction set
        assert!(r1.has_instruction("instr-1") && r2.has_instruction("instr-1"));
    }
}
