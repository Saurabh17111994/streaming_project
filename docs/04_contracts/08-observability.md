# Segment Build Contract — Observability

## Boundary

OpenObserve receives operational logs, metrics, and supported traces. Correlation IDs and immutable execution audit remain mandatory even when distributed trace propagation is unavailable.

## Required proof

Data/compute metrics prove throughput, latency percentiles, fingerprint behavior, invalid/late data, watermark/backpressure, and checkpoints.

Order metrics prove gate epoch/state, halt latency, attempt outcomes, unknowns, mappings/quarantine, reconciliation, approvals, changelog continuity, and Arrow REST responses.

Storage/runtime metrics prove replication/quorum, node health, checkpoint store, EOD manifest/retry/expiry margin, storage pressure, secrets, and security events.

## MVP alerts

Order safety, streaming health, storage safety, and security alert groups are configured, routed, and tested. Thresholds are versioned configuration. Health separates liveness, readiness, job health, trading readiness, and durability readiness.

## Security

Credentials, original payloads, and unnecessary account identifiers are redacted. Audit access is role-restricted and audited. Execution, gate, order, fill, correlation, approval, reconciliation, and future position-action audit is immutable, encrypted, and retained seven years.

## Acceptance

Metric/alert emission, redaction, cardinality, backend outage behavior, health transitions, safe-halt alerts, offload expiry alerts, unauthorized resume alerts, and release-gate reconstruction tests pass.

## Component-specific degradation

OpenObserve outage SHALL not erase durable execution audit or authorize orders. Ingestion and Action Capture MAY continue bounded evidence capture when durable source/audit writes, local buffering, and readiness policy remain healthy. Executor SHALL halt new money-moving calls when mandatory execution audit, safety-control acknowledgement, or alert visibility is unavailable. Each component SHALL expose its degraded reason and buffer bounds.

## Requirement traceability

- Functional: `REQ-OBS-001` through `REQ-OBS-008`
- Cross-cutting: `03-non-functional.md` §§3.1–3.8; `04-data.md` §§4.1, 4.3–4.7; `05-interfaces.md` §§5.9–5.11; `06-operational.md` §§6.3, 6.5–6.10

See `../02_requirements/02-functional/08-observability.md`.
