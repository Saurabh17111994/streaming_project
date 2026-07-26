# Segment Build Contract — Platform and Runtime

## Environments

Local development/integration uses Docker Compose. Production uses Docker Swarm on three workload VMs plus one observability VM. Compose is not production HA evidence.

## Production requirements

- Exact image digests and Java/Python/Flink/Fluss/OpenAlgo/protocol versions; no `latest`.
- Three-node Fluss replication/quorum with anti-co-location.
- Flink checkpoints/savepoints in encrypted S3.
- Encrypted Swarm overlay/TLS-protected cross-host traffic.
- Docker Swarm secrets and least-privilege service identities.
- Durable volumes and encrypted seven-year audit/lake storage.
- Executor fencing: one active owner per account/order partition.

## Readiness

Fluss quorum/schemas, Flink jobs/checkpoints, broker subscriptions/schema, Executor durable state/changelog/OpenAlgo, observability, and gate state are checked independently. Startup never auto-enables order placement.

## Capacity and failure gates

Pass 75,000 ticks/s full session, 112,500 for 30 minutes, 150,000 for 60 minutes, one workload VM loss at normal rate, data recovery <30 seconds, safe-halt <5 seconds, decision p99 <100 ms, and EOD manifest <30 minutes target.

## Security and rollout

Money-moving deployments begin halted and require reconciliation/two-person enablement. Rollback defaults halted when uncertain. Secret rotation, network exposure, TLS, redaction, least privilege, audit access, vulnerability policy, and compromised-credential recovery are tested.

## Requirement traceability

- Functional: `REQ-PF-001` through `REQ-PF-010`
- Cross-cutting: `03-non-functional.md` §§3.1–3.8; `04-data.md` §§4.1, 4.3, 4.6–4.7; `05-interfaces.md` §§5.1–5.11; `06-operational.md` §§6.1–6.10

See `../02_requirements/02-functional/09-platform-runtime.md` and `../02_requirements/06-operational.md`.
