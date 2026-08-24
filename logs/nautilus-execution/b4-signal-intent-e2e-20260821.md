# B4 — Signal→Intent flow evidence (2026-08-21)

Master-plan Task B4.4 — evidence file for the signal→intent half (intent→fill deferred).

**What was verified (2026-08-21)**

- Compute side: enabled pipeline produced Signal_Candidates rows and immutable `Execution_Intent` rows; disabled-by-default produced zero intents (fail-closed, 18 `ExecutionIntent*` tests green on the same path).
- Gateway side: HALTED gate defers intents (`DEFERRED`, fail-closed) with zero `Execution_Attempts`/`Order_Lifecycle` side effects.

**Disposition**

Signal→intent verified; intent→fill pending broker sandbox order (A2/A3/A5). DoD of B4 (full round-trip) NOT yet met — recorded honestly.

**Evidence**

- Change record: CHG-071.
- Companion: `b4-halted-e2e-20260821.md` (CHG-087, live compose details + runner).
