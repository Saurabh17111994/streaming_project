# C2 — Config parity + pin update (2026-08-21)

Master-plan Task C2 — DoD: parity tests green; divergent config fails in both languages.

**What was done**

- `C2.1` — parity table lifted: Go `hft_policy_test.go` 1,1,1,1 → -1,1,1,3 and Java `ConfigParityTest.java` 1,1,1,1 → -1,1,1,3; rejection updated to 4/abc (range 1..3). `IngestionConfig.java` exactInt → intRange(1..3).
- `C2.2` — both sides now flow through `hftRange`/`intRange` (CONNECTIONS 1..3, LATENCY 50..60_000); reject 0/4/abc symmetrically.
- `C2.3` — Java BUILD SUCCESS + Go PASS (18.6 s); Rust 153/0 unaffected.

**Disposition**

Parity enforced in both languages; divergent config fails identically.

**Evidence**

- Change record: CHG-067.
- Files: `code/02_services/01_ingestion/go-bridge/hft_policy_test.go`, `code/02_services/01_ingestion/.../ConfigParityTest.java`, `IngestionConfig.java`.
