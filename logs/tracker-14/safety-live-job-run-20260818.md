# Safety live job run — 2026-08-18 (USER-APPROVAL-GATED, Item E)

Evidence for 18-signal-job-remaining-work-plan.md Item E (SafetyHaltJob live FlusSource consume path).

Approval: explicit user approval 2026-08-18 ("ok go ahead").

Build: cd code/02_services/02_compute && mvn -o -DskipTests package → BUILD SUCCESS shaded compute.jar

Run: SafetyHaltJob main against dev Fluss FLUSS_BOOTSTRAP_SERVERS=localhost:9123, CHECKPOINTS_DIRECTORY=file:///tmp/safetyhalt-checkpoints RETAIN_ON_CANCELLATION EXACTLY_ONCE 10s/30s/1, OffsetsInitializer.full() (one-off full replay), parallelism 1 (see finding), runner SafetyLiveJobRun.java (MiniCluster, topology identical to main + checkpoint contract)

Writes: SafetyHaltWriter 21-column v3 UNSAFE FEED_STALLED epoch 1786994318032 → RECOVERED epoch 1786994318033 (epoch+1)

Logs: slot hft-0 UNSAFE -> NEW_UNSAFE (epoch 1786994318032, reason FEED_STALLED) then RECOVERED -> RECOVERED (epoch 1786994318033) in changelog order, same subtask; safety.transitions.applied 53 increments; safety.rows.malformed 0 from own rows (pre-existing contract_version=21 malformed counted+skipped as expected)

Checkpoint: clean cancel retained chk-3 _metadata under /tmp/safetyhalt-checkpoints/<jobId>/chk-3/ with shared/ + taskowned/

Findings:
- NotSerializableException SlotAssignmentResolver at graph build — FIXED SlotAssignment extends Serializable + SlotEntry implements Serializable (common/src/main/java/com/trading/common/safety/*; guard test SlotAssignmentResolverTest.serializableRoundTrip common 340→341)
- parallelism-16 splits UNSAFE/RECOVERED across subtasks (per-task tracker) — RECORDED, consumer runs at 1 until broadcast state (future work)

C6 truth bumped 340/234/292→341/234/292 in 01-foundation L42 + scanner constants.
