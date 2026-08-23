# P10 rehearsal Design-B — 2026-08-17 (all 10 re-scoped P10.1 boxes PASS)

Evidence for Item D (18-signal-job-remaining-work-plan.md) on Design-B.

Isolation: overlay compose project `p10` (remapped ports 9124/6123/8081), byte-copy live data (tablet 29G + remote 17G + ZK), archived chk-179 stable prefix, deviations DB1–DB6 recorded.

Boxes (09-production-swarm.md §P10 re-scoped):
- Run 1 cutover: archived chk-179 strict restore (allowNonRestoredState=false) → RUNNING 13.9s, first checkpoint 6s ≤30s, KV `Signal_Candidates_current` frozen 1,025 keys, LOG `Signal_Candidates` +42 (258,995→259,121), exposure 126 LOG / 0 KV new keys
- Runs 2-3 bounded replay ×2 from same offsets: identical +42 re-appends retained, KV byte-identical 1,025 both times
- Run 4 re-cutover from era's own chk-198: counter 198→199, LOG +0, KV frozen 1,025
- Rollback half VACUOUS on Design-B (DB2: single LOG artifact, consumer repoint VACUOUS) — recorded as such
- Checkpoints: counter 179→217 across runs, 0 failed, strict restore verified
- Deviations: DB1 overlay ports, DB2 VACUOUS rollback, DB3 byte-copy, DB4 MiniCluster submission, DB5 archived chk prefix, DB6 exposure sampling

Deliverable: P10.2 (blue-green cutover) + P10.3 (rollback) runbook in 09-production-swarm.md §13 (command sequences, deferred execution — no prod).

Evidence per-run counts + timeline + archived chk paths + O2 queries herein.
