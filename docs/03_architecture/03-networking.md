# Networking

## Environment-specific topology

### Local and integration

Docker Compose uses an isolated single-host bridge network for deterministic development and integration testing. Service names provide internal DNS. Host exposure is limited to the endpoints explicitly required by local operators and tests.

### Production

Docker Swarm uses separate encrypted overlay networks or equivalent TLS-protected cross-host transport. The three workload VMs host Fluss replicas/quorum and Flink workload capacity with anti-co-location. The fourth VM hosts observability and is not required for order-safety correctness.

Compose is not production HA evidence. Production placement, resources, restart policy, health checks, ingress, rollback, and persistent storage are defined in the Swarm stack and validated by one-workload-VM failure tests.

## Logical communication paths

```text
Arrow market-data endpoint ─TLS─► Ingestion
Ingestion ─Fluss client─► Fluss coordinator/tablets
Signal and Babysitter jobs ─version-pinned connector─► Fluss
Arrow postback endpoint ─TLS─► Action Capture
Executor ─Fluss changelog─► execution inputs/state
Executor ─evidence-approved REST/TLS─► OpenAlgo ─► broker
All components ─supported telemetry protocol─► OpenObserve
Operators ─controlled authenticated interface─► gate/reconciliation controls
```

Exact ports, endpoint paths, authentication fields, compression, subscription limits, timeout values, and protocol versions are evidence-gated. They must not be copied from historical examples into production contracts.

## Network exposure rules

Production exposes only explicitly required operator, health, or API endpoints through controlled ingress and firewall rules. Fluss tablet/internal RPC, checkpoint storage, service-to-service data paths, and execution state are not publicly exposed.

The observability VM cannot authorize orders, change the Executor gate, or erase execution audit if OpenObserve is unavailable. Local UI exposure is for development only and is not a production security model.

## Identity and authorization boundaries

- Ingestion can append to market-data tables and write discontinuity/quarantine evidence.
- Signal jobs can write signal/candle/ranking/instruction outputs and their checkpoint state.
- Action Capture can append postback audit, update lifecycle/position projections, and write quarantine.
- Executor can read immutable instructions and lifecycle/position health, and write only execution-owned state.
- Babysitter reads `Positions` and writes no action in MVP.
- Operators can perform authenticated reconciliation/approval operations but cannot bypass the gate with an unaudited call.
- OpenObserve receives telemetry and cannot authorize execution.

All service identities are least-privilege and separate. Exact Fluss ACLs, mTLS support, and credential mechanisms are version-gated.

## Connectivity failure behavior

- Broker disconnect or authentication exhaustion makes the affected service not ready and alerts operations.
- Fluss unavailability or append uncertainty follows the component retry policy, exposes uncertainty, and prevents false loss claims.
- Changelog discontinuity, checkpoint failure affecting order correctness, or durable Executor state loss halts new money-moving calls.
- OpenObserve outage does not erase local durable audit; lack of required observability can make the platform not ready for live-money operation.
- Cross-host network degradation is tested for bounded backlog, recovery, replication behavior, and safe-halt latency.

## References

- Runtime requirements: `../02_requirements/02-functional/09-platform-runtime.md`
- Operational requirements: `../02_requirements/06-operational.md`
- Security requirements: `../02_requirements/03-non-functional.md` §3.6
- Platform contract: `../04_contracts/09-platform-runtime.md`
