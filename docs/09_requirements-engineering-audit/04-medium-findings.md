# Medium Findings

<!-- markdownlint-disable MD013 -->

These findings reduce traceability, human readability, and implementation confidence. They should be corrected after critical/high architecture decisions are closed.

## AUD-M01 — Requirement ID families are inconsistent

**Evidence:** Requirements use `REQ-FLS`, `REQ-SS`, and `REQ-BB`; implementation traceability refers to `REQ-ST`, `REQ-BL`, and `REQ-BAB` in `docs/08_implementation/99-traceability.md:68-75` and component dossiers.

**Cause:** Machine traceability and acceptance linking can silently miss requirements.

**Recommendation:** Choose one ID family per segment and update requirements, contracts, dossiers, tests, and traceability in one change.

## AUD-M02 — Segment count is ambiguous

**Evidence:** `02-functional/0.0-index.md:3-17` says nine segments while listing ten files because ranking is both a domain file and an in-job boundary.

**Recommendation:** State “nine deployable ownership segments plus one in-operator ranking contract,” or restructure the index.

## AUD-M03 — Functional index omits Positions from Action Capture writes

**Evidence:** `02-functional/0.0-index.md:15-16` lists lifecycle and quarantine but not the required `Positions` projection.

**Recommendation:** Add `Positions` and the projection ledger/equivalent.

## AUD-M04 — Table naming and casing are inconsistent

**Evidence:** `raw_table_1`, `Signal_Candidates`, `Ranking_Results`, `Trade_Decisions`, and `Fills_table` use mixed conventions.

**Recommendation:** Establish one naming convention before final DDL generation, or record deliberate exceptions.

## AUD-M05 — Candle `ingest_ts` is semantically misnamed

**Evidence:** `02-functional/03-compute.md:40-54` defines it as final output acknowledgement time.

**Recommendation:** Rename to `computed_at`, `sink_ack_ts`, or `visible_at` according to the actual event.

## AUD-M06 — Candidate/evaluation lifecycle is incomplete

**Evidence:** `04-business-logic.md:19-23`, `10-ranking.md:31-40`.

**Missing detail:** Evaluation trigger, candidate expiry, invalidation, active-state removal, timer reevaluation, simultaneous updates, and maximum active candidate count.

**Recommendation:** Define lifecycle transitions and state bounds.

## AUD-M07 — Closed-candle in-process contract is missing

**Evidence:** Business Logic consumes closed candles in `04-business-logic.md:3-5`, but Compute specifies only forming-bar handoff in `03-compute.md:65-69`.

**Recommendation:** Define typed closed-candle and forming-bar event schemas separately.

## AUD-M08 — State bounds are qualitative

**Evidence:** State is named across Compute, Business Logic, Action Capture, and Executor but cardinality and serialized-size limits are absent.

**Recommendation:** Add maximum entries/key, serialized size, cleanup, restore size, checkpoint contribution, and skew behavior to every state contract.

## AUD-M09 — OpenAlgo reconciliation capability is not a requirement

**Evidence:** `07-executor.md:52-68` requires reconciliation but does not define query-by-reference/order, list history, fills/positions, consistency delay, rate limits, or horizon.

**Recommendation:** Add a minimum broker/OpenAlgo reconciliation capability gate.

## AUD-M10 — Control-plane interfaces are missing

**Missing:** Halt, reconciliation, approval, manual disposition, quarantine disposition, gate inspection, and audit retrieval APIs.

**Recommendation:** Add versioned control contracts with authorization, idempotency, concurrency, expiry, errors, and audit behavior.

## AUD-M11 — Seven-year audit policy lacks retention controls

**Missing:** WORM/object lock, legal hold, deletion evidence, key rotation, access review, retrieval SLA, export format, and reconstruction integrity.

**Recommendation:** Add compliance owner and binary policy/evidence gates.

## AUD-M12 — EOD offload controller ownership is unclear

**Evidence:** Storage owns policy but no deployable owner is defined in `02-functional/02-storage.md:92-96`.

**Recommendation:** Name the controller, trigger, durable manifest state machine, retry owner, retention extension mechanism, and manual override.

## AUD-M13 — Acceptance criteria are grouped rather than binary and traceable

**Evidence:** Many segment endings say “tests SHALL prove” without test IDs, thresholds, fixtures, owners, or artifacts.

**Recommendation:** Assign unique acceptance IDs and map every requirement to at least one binary test/evidence record.

## AUD-M14 — Formatting and historical issue debris remain in requirements

**Evidence:** trailing quote/hash markers and resolved commentary in `02-functional/05-babysitter.md:77`, `06-action-capture.md:88-90`, `07-executor.md:97-103`, `09-platform-runtime.md:79-83`, and `10-ranking.md:56-62`.

**Recommendation:** Remove formatting debris and move resolved commentary to issue history.
