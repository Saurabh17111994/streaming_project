# Compute — Signal and Babysitter implementation handoff

> **Status:** implementation not started. Use the implementation dossiers as the build contracts.
>
> **Live money:** disabled until checkpoint, state, connector, decision, and Executor safety evidence passes.

## MVP topology

There are exactly two Flink jobs:

1. **Signal job:** raw ingestion validation, bounded fingerprint deduplication, event-time 15-second candles, forming-bar state, Business Logic, in-operator Ranking, reservations, immutable candidates/ranking/decisions.
2. **Babysitter job:** consumes `Positions`, checkpoints observation state, and emits zero actions in MVP.

Ranking is not a separate job and the Signal job does not read feature tables back for strategy execution.

## Implementation checklist

- [ ] Pin Flink/Fluss connector and state/checkpoint versions.
- [ ] Implement raw source/schema/validity and event-time watermarks.
- [ ] Implement bounded fingerprint deduplication.
- [ ] Implement final 15-second candles and late-event policy.
- [ ] Implement forming-bar typed handoff and Business Logic.
- [ ] Implement deterministic in-operator Ranking and reservations.
- [ ] Implement immutable `Signal_Candidates`, `Ranking_Results`, and `Trade_Decisions` outputs.
- [ ] Implement Signal job submission/status/checkpoint verification.
- [ ] Implement separate checkpointed Babysitter no-op.
- [ ] Pass deterministic replay, checkpoint, failure, and workload tests.

## Do not implement

- A separate feature-compute deployment.
- A separate Ranking deployment.
- `seq_no`-based deduplication.
- Mutable `Trade_Decisions` execution fields.
- Broker REST calls from Flink jobs.

## References

- Requirements: `../../../docs/02_requirements/02-functional/03-compute.md`, `04-business-logic.md`, `05-babysitter.md`, `10-ranking.md`
- Contracts: `../../../docs/04_contracts/03-compute.md`, `04-business-logic.md`, `05-babysitter.md`, `10-ranking.md`
- Signal dossier: `../../../docs/08_implementation/04-signal-job.md`
- Babysitter dossier: `../../../docs/08_implementation/06-babysitter.md`
