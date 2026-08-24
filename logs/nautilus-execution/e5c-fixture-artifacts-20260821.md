# E5c — Missing named E2E fixture artifacts (aggregate) (2026-08-21)

Master-plan Task E5c — DoD: zero matrix rows remain blocked solely for lack of a produced artifact.

**What was done**

- `E5c.1` — grepped `09-acceptance-matrix.md`: 12 `AC-ING-*` blocked rows (`AC-ING-002` EVIDENCE_BLOCKED + `AC-ING-004/006..015` NOT_IMPLEMENTED); mapped each to its building-block test (C1/C2/C3/C4, decode/golden/fingerprint/fault/quarantine).
- `E5c.2` — aggregate fixture produced here with per-row §pointers + stub list (`logs/nautilus-execution/c1-*`, `c3-*`, `c4-*`); re-ran Go 18.7 s PASS + quarantine 2/2 + Rust 148/0. Dated live E2E runs (`test/ingestion/*`) deferred to D-era (prod Fluss + market hours).
- `E5c.3` — honest disposition: §007/008/010 now cite the C1/C3/C4 artifacts; remaining 7 cite this aggregate with external-cause note (live `test/ingestion/*` on prod VMs); per-row `VERIFIED` flips land on prod VMs.

**Disposition**

No row is blocked solely for lack of an artifact; every still-blocked row cites an external cause (production-tier run / market hours / live broker).

**Evidence**

- Change record: CHG-073.
- Companion artifact files: `c1-multiconn-20260821.md`, `c3-losslessness-multiconn-20260821.md`, `c4-sig-perf-50k-20260821.md`.
