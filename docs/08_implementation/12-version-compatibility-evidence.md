<!-- markdownlint-disable MD013 -->

# Version compatibility evidence plan

This document is the **capability-evidence plan** for the pinned version matrix.
Its job is to turn each matrix row from `UNKNOWN` (pinned, not proven) into
`COMPATIBLE` (proven by a capability test). It maps every boundary in
`code/01_platform/04_scripts/version_matrix.yaml` to the concrete test IDs already
defined in `11-testing-and-release.md`, states the pass condition, says where the
result is recorded, and defines the gate that lets `make ddl` actually create the
tables.

## Authority and scope

- Authority order: executable tests > this plan > the matrix file > prose.
- This is a **plan only** (documentation). It executes nothing and needs a
  runnable environment (Docker) to run.
- Pinned versions live in `code/01_platform/04_scripts/versions.pin`; the matrix
  and verification script live next to it (`version_matrix.yaml`,
  `version_matrix_verify.py`).
- The DDL application contract (`ddl_apply.py`) refuses to apply tables until the
  gate below is met.

## Prerequisites

1. `versions.pin` contains the pinned platform versions (done: Flink 2.2.1,
   Fluss 0.9.1-incubating, ZooKeeper 3.9.2, Java 17.0.19, Python 3.11.9,
   pinned images).
2. A runnable local stack (Docker + Compose) per `08-local-compose.md` using the
   pinned images.
3. `make ddl` already emits `schema_manifest.json` (21 tables) — confirmed; it
   still refuses application by design.
4. For external rows, a sandbox broker and Arrow REST stub (no real credentials).

## Boundary → evidence mapping

For each boundary: run the listed test(s) against the running stack, record the
output into the matrix columns (`result`, `observed_behavior`, `limitations`,
`date`, `fixture`, `scenario`), then flip `compatibility_class` to `COMPATIBLE`
on pass. External boundaries that need real broker/Arrow contracts stay
`TO_BE_VERIFIED` (blocked by DATA-GAP-001 / DATA-GAP-005).

| # | Boundary | Matrix id | Evidence test IDs | Pass condition |
| --- | --- | --- | --- | --- |
| 1 | Java 17.0.19 | VM-JAVA-001 | `LOCAL-INT-002`; build smoke on Java 17 | `common` + services compile/run on the pinned JVM; effective config reports `17.0.19`; no module fails to start |
| 2 | Python 3.11.9 | VM-PYTHON-002 | run `ddl_apply.py` + `version_matrix_verify.py` on 3.11 | Both scripts execute; matrix parses; verifier passes |
| 3 | Flink 2.2.1 | VM-FLINK-SRV-003, VM-FLINK-API-004 | `COMPAT-FLINK-001`; `SIG-HARNESS-003`, `SIG-HARNESS-005`; `STATE-COMPAT-001` | Source/sink checkpoint, restore, rescale correct on 2.2.1; savepoint restores through the approved compatibility path |
| 4 | Fluss 0.9.1-incubating | VM-FLUSS-SRV-005 | `COMPAT-FLUSS-001`..`004`; `SCHEMA-UNIT-001`/`002`/`003` | All 21 DDLs parse/apply; effective schema == manifest; LOG/KV/changelog behavior matches; stale/conflict KV rejected and audited |
| 5 | Fluss connector (fluss-flink-2.2:0.9.1) | VM-FLUSS-CONN-007 | `COMPAT-FLINK-001`; `SIG-INT-001` | Pinned connector checkpoint/restore on the 2.2.1 boundary works with the Fluss source/sink |
| 5a | ZooKeeper ensemble (3.9.2) | VM-ZK-013 | `SWARM-INT-002`; `SWARM-FAIL-001`; `PERF-NODELOSS-001` | 3-node ensemble starts; quorum 2-of-3 survives one node loss; Fluss coordinator/tablet register via `zookeeper.address`; Flink JobManager HA leader election + failover works |
| 5b | Fluss state-table schema + rehydration (DEC-038, 2026-08-14) | VM-FLUSS-SRV-005 | `SIG-STATE-001` to `SIG-STATE-003`; `STATE-COMPAT-001` (Fluss-state-table half); rehydration integration test | Dedup state-table schema/serialization reads and writes match on the pinned connector; restart rehydrates the Flink working cache from the Fluss table; schema change is additive or blocks before unsafe use; Fluss unavailability/incompatibility fails closed |

**Partial evidence recorded (2026-08-09, SAFETY-INT-001):** `fluss-flink-2.2:0.9.1-incubating` resolves from Maven Central; `FlussSource.build()` performs a live `Admin.getTableInfo` (fail-fast); live KV upsert → primary-key lookup → `RowDataDeserializationSchema` read worked against Fluss 0.9.1-incubating. This covers the connector boundary partially (VM-FLUSS-CONN-007 remains `UNKNOWN` — full checkpoint/restore `COMPAT-FLINK-001` / `SIG-INT-001` still pending). Evidence: `docs/08_implementation/04-signal-job.md` §Connector and compile evidence, `logs/safety-int-001/`.
| 6 | Broker market feed | VM-BROKER-MKT-008 | `BROKER-MD-001` | Live corpus (2026-08-13) decodes to typed fields on both feeds; full-mode 249 B confirmed; unknown protocol version quarantined; behavior recorded as evidence — `COMPATIBLE` (`EVIDENCE_RECORDED_LIVE`) |
| 7 | Broker postback | VM-BROKER-PBK-009 | `BROKER-PB-001` | Postback status/identity/timestamp/optional-field behavior recorded; unsupported behavior quarantined — `TO_BE_VERIFIED` |
| 8 | Arrow REST API | VM-ARROW-010 | `ARROW-REST-001`, `ARROW-REST-002` | Request/response/auth/timeout captured; client-reference correlates one attempt to one broker order — `TO_BE_VERIFIED` |
| 9 | OpenObserve | VM-OPENOBS-011 | `OPS-INT-001`, `OPS-FAIL-001` | Telemetry envelope/redaction correct; OpenObserve outage leaves durable audit available — `TO_BE_VERIFIED` |
| 10 | Base images | VM-IMAGES-012 | `LOCAL-INT-002`; `SWARM-INT-001`; `SEC-IMAGE-001` | Images referenced by digest; no mutable tag; SBOM/vulnerability policy passes |

**Live evidence recorded (2026-08-13, BROKER-MD-001):** real wire frames from the HFT
Arrow feed (socket.arrow.trade HFT 40/196 B zstd LE) captured raw and decoded to
typed fields; paise scaling verified; AutoLogin (non-interactive, no device token)
verified after the vendored-SDK fix (validate-2fa host api.arrow.trade + `appID` body
field). VM-BROKER-MKT-008 flipped `COMPATIBLE` (`EVIDENCE_RECORDED_LIVE`).
Live reconnect/replay/echo is not an acceptance item (DEC-037, 2026-08-13);
bridge-level ING-RES-001 soak PASS (2026-08-13) is the reconnect evidence.
Evidence: `logs/broker-md-001/`. (The Standard feed `ds.arrow.trade` evidence
13/17/93/249 B was retired with the Standard feed removal 2026-08-14.)

## Environment tiers

Run the evidence in this order (matches the mandatory build order):

1. **Clean local Flink/Fluss stack** — for `COMPAT-FLUSS-*` and `COMPAT-FLINK-*`
   (boundaries 1, 3, 4, 5). This is what unblocks DDL application.
2. **Sandbox broker + Arrow stub** — for `BROKER-MD-001`, `BROKER-PB-001`,
   `ARROW-REST-*` (boundaries 6, 7, 8). These are live-money gates, not DDL gates.
3. **Observability stack** — for `OPS-*` / OpenObserve (boundary 9).

## Execution order

1. Stand up the local stack with pinned images (`08-local-compose.md`).
2. `make ddl` — confirms the version gate passes and emits the manifest.
3. Run `COMPAT-FLUSS-001`..`004` and `COMPAT-FLINK-001` against the running stack.
4. Record each result into `version_matrix.yaml` (flip `compatibility_class` to
   `COMPATIBLE` on pass).
5. Confirm `00_RECONCILIATION_BLOCKER.md` exit criteria are met.
6. Run `make ddl` again — it now applies the tables (or reports the remaining
   blocker explicitly).

## The gate (when DDL application unblocks)

`make ddl` applies the tables only when **all** hold:

- `VM-JAVA-001`, `VM-PYTHON-002`, `VM-FLINK-SRV-003`, `VM-FLINK-API-004`,
  `VM-FLUSS-SRV-005`, `VM-FLUSS-CONN-007` are `COMPATIBLE`.
- `COMPAT-FLUSS-001`..`004` and `COMPAT-FLINK-001` pass.
- The reconciliation blocker exit criteria (`00_RECONCILIATION_BLOCKER.md`) are met.

External rows (`VM-BROKER-MKT-008`, `VM-BROKER-PBK-009`, `VM-ARROW-010`,
`VM-OPENOBS-011`) may remain `TO_BE_VERIFIED`. They gate the **release**
`REL-PROTO` (live-money) gate, not DDL application — the schema does not depend on
the broker contract.

## Status tracking

- Each run fills the matrix columns `result`, `observed_behavior`, `limitations`,
  `date`, `fixture`, `scenario`.
- `compatibility_class` flips `UNKNOWN → COMPATIBLE` on pass (or stays
  `TO_BE_VERIFIED` for external rows).
- The `01-foundation.md` "Version matrix" checklist item already records
  `Implementation: Implemented`; flip its `Evidence` axis to `Tested` once this
  plan's boundaries 1–5 are `COMPATIBLE`.
- Every run records exact versions/digests, fixture checksum, and the
  `schema_manifest` ID, per `11-testing-and-release.md` test-record format.

## Honest limitation

This plan **documents** how to prove the versions. It does not run the tests.
Until the Docker environment exists and boundaries 1–5 are executed and pass, the
matrix stays `UNKNOWN` and `make ddl` continues to refuse table application. That
is the intended fail-closed posture.
