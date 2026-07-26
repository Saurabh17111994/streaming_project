# High Findings

These findings may not immediately permit duplicate orders, but they prevent deterministic implementation, reliable recovery, or credible SLO evidence.

## AUD-H01 — Watermark and final-candle semantics are ambiguous

**Evidence:** `02-functional/03-compute.md:30-38,59-63` uses bounded out-of-orderness, allowed lateness, and final emission without defining the intended wall-clock delay.

**Issue:** The requirements do not clearly distinguish watermark delay from finalization delay and do not describe a standard Flink trigger/update model.

**Cause/risk:** Candles may finalize too early, too late, or behave differently across implementations. Late-event metrics and strategy timing become incomparable.

**Recommended solution:** Define a custom final-only contract: no provisional row; emit once when `watermark >= window_end + finalization_delay`; discard later events in MVP. State whether `finalization_delay` includes watermark out-of-orderness and allowed-lateness values.

**Acceptance:** Deterministic event-time fixtures prove emission time, accepted-late behavior, post-final discard, and no correction rows.

**Status:** `OPEN`

## AUD-H02 — Source partitions, connection slots, and idleness are not mapped

**Evidence:** `01-ingestion.md:25-45`, `03-compute.md:30-36`.

**Issue:** Broker connection slots, Fluss source splits, Flink source subtasks, and watermark partitions are not related.

**Cause/risk:** An idle partition may stall all watermarks, or aggressive idleness may finalize windows while delayed data is still expected.

**Recommended solution:** Define the source split identity, instrument assignment, reconnect/reassignment behavior, idle threshold, market-session inactivity behavior, and watermark state recovery.

**Acceptance:** Tests cover one silent instrument, one stalled connection, reconnect, reassignment, skew, and delayed packet arrival.

**Status:** `OPEN`

## AUD-H03 — Deduplication horizon is not quantitatively defined

**Evidence:** `03-compute.md:22-28` requires TTL to cover retry/replay horizon but no bound is defined.

**Issue:** Deployment cannot validate the TTL, and state size cannot be estimated.

**Cause/risk:** Duplicates may escape after recovery, or dedup state may grow beyond checkpoint/memory capacity.

**Recommended solution:** Define a named horizon as the maximum supported append retry, connector rewind, broker replay, checkpoint restore, and operational replay interval plus safety margin. Require a state-size model based on accepted events/s, horizon, serialized entry size, and backend overhead.

**Acceptance:** Config rejection tests and 150k/s state/checkpoint growth tests pass.

**Status:** `OPEN`

## AUD-H04 — Signal-job lifecycle/reservation state ingress is undefined

**Evidence:** `04-business-logic.md:7-17`, `05-interfaces.md:56-60`.

**Issue:** The Signal job needs external lifecycle/position/reservation state but no source, changelog, key, freshness, or checkpoint contract is defined.

**Cause/risk:** A synchronous lookup, stale cache, or inconsistent changelog can create decisions using obsolete exposure or reservation information.

**Recommended solution:** Specify the source/event, key, changelog mode, freshness/version field, stale threshold, checkpoint inclusion, partial visibility behavior, and safe degradation. Prohibit blocking hot-path lookups unless bounded async behavior is explicitly approved and benchmarked.

**Status:** `OPEN`

## AUD-H05 — Instruction supersession is not atomically coordinated with Executor

**Evidence:** `04-business-logic.md:39-43`, `10-ranking.md:31-40`.

**Issue:** Signal requires the previous instruction to be disposed before replacement, but Executor owns execution disposition and cross-table ordering is not atomic.

**Cause/risk:** Old and replacement instructions may both be submitted during reordering or partial visibility.

**Recommended solution:** Define immutable supersession/cancellation events. Executor is authoritative for submission eligibility and requires predecessor disposition before accepting a replacement. Unresolved predecessor state suppresses the replacement.

**Status:** `OPEN`

## AUD-H06 — Position identity and fill arithmetic are incomplete

**Evidence:** `06-action-capture.md:66-72`, `05-babysitter.md:20-26`.

**Issue:** Re-entry, scale-in, scale-out, side changes, correction/busts, fees, precision, and quantity underflow are unspecified.

**Cause/risk:** Different implementations can derive different positions from the same fills, making Babysitter and recovery unsafe.

**Recommended solution:** Define uniqueness scope, position minting/closure/re-entry rules, side model, quantity/price decimal rules, fee treatment, correction policy, and impossible-fill behavior.

**Status:** `OPEN`

## AUD-H07 — Lifecycle ordering uses source versions that may not exist

**Evidence:** `06-action-capture.md:46-52` requires source version/state version while `06-action-capture.md:3-7` assumes no broker sequence/event ID.

**Issue:** Receive time cannot by itself prove broker lifecycle ordering.

**Cause/risk:** A late cancellation or partial-fill update may incorrectly regress or override a newer state.

**Recommended solution:** Define status precedence, cumulative-quantity monotonicity, verified broker-time use, platform receive time as evidence only, equal-precedence conflict handling, and manual reconciliation override.

**Status:** `EVIDENCE-BLOCKED`

## AUD-H08 — RPO is not an acceptance target

**Evidence:** `03-non-functional.md:29-38`, `06-operational.md:35-44`.

**Issue:** Requirements say RPO will be reported per scenario but do not define acceptable loss by boundary.

**Cause/risk:** A test can report an RPO without proving it meets a business requirement.

**Recommended solution:** Define RPO separately for raw packets, candles, instructions, audit, postbacks, Executor attempts, and EOD data, or assign each as explicitly unresolved with owner and release gate.

**Status:** `OPEN`

## AUD-H09 — One-VM loss lacks N+1 resource and recovery criteria

**Evidence:** `09-platform-runtime.md:16-20,64-73`.

**Issue:** The topology combines Fluss and Flink workload on three VMs but does not define post-loss CPU, memory, network, storage, slots, quorum, or catch-up headroom.

**Cause/risk:** A system may pass a nominal failover test while violating sustained load, checkpoint, or recovery requirements after loss.

**Recommended solution:** Require per-node resource budgets, post-loss capacity, Fluss re-replication behavior, checkpoint download bandwidth, catch-up rate, maximum backlog, and a failure timeline.

**Status:** `OPEN`

## AUD-H10 — Security wording permits unsupported plaintext paths

**Evidence:** `03-non-functional.md:72-80` says TLS is required “where supported.”

**Issue:** A production money-moving path must not become plaintext merely because a component lacks support.

**Recommended solution:** Make encryption mandatory for broker, OpenAlgo, S3, operator control, secrets, and cross-host sensitive/state traffic. Permit exceptions only for isolated non-sensitive traffic with explicit risk acceptance.

**Status:** `RECONCILIATION-REQUIRED`

## AUD-H11 — Observability outage degradation is not component-specific

**Evidence:** `01-ingestion.md:138-142`, `07-executor.md:80-84`, `08-observability.md:3-5`.

**Issue:** Readiness depends on observability delivery, but the safe behavior differs between ingestion, Action Capture, and Executor.

**Cause/risk:** An OpenObserve outage may unnecessarily stop data capture, or telemetry may be treated as equivalent to durable audit.

**Recommended solution:** Define per-component behavior. Continue ingestion/postback capture when durable evidence works and bounded local buffering is available; Executor halts new calls when mandatory safety audit/alert visibility is unavailable.

**Status:** `OPEN`

## AUD-H12 — Failure-detection boundaries are undefined

**Evidence:** `03-non-functional.md:15-27` measures from failure/uncertainty detection.

**Issue:** Detection delay is excluded from the five- and thirty-second objectives.

**Recommended solution:** Define detector, threshold, detection timestamp, gate-block timestamp, recovery-complete timestamp, and report fault→detection, detection→halt, and fault→recovery.

**Status:** `OPEN`
