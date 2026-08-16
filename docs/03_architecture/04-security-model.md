# Security Model

## Status

This is the target security architecture for the pre-production platform. Exact credential fields, endpoint behavior, TLS/mTLS support, image provenance, and authorization APIs remain evidence-gated. Live-money operation is blocked until security tests and release gates pass.

## Security objectives

1. Prevent unauthorized money-moving calls.
2. Halt the order path when identity, state, continuity, or broker outcome is uncertain.
3. Preserve immutable execution and broker evidence.
4. Minimize service and operator privileges.
5. Protect data in transit and at rest.
6. Make security behavior observable and auditable.

## Secrets and identities

Local development may use an ignored `.env` file. Production uses Docker Swarm secrets. Secrets are never committed, embedded in images, placed in stack files, exposed on command lines, dumped in environments, or written to logs.

Separate least-privilege identities are required for:

- Ingestion and market-data append
- Signal and Babysitter Flink jobs
- Action Capture and projection writes
- Executor state, gate control, and Arrow REST access
- Fluss administration
- S3 checkpoint/lake access
- OpenObserve telemetry
- Authenticated operators

Credential rotation and revocation covers broker/Arrow, S3, OpenObserve, TLS material, Fluss access, and operator identities. Expiry or revocation sets affected readiness false and alerts operations.

## Order-path safety controls

The Executor owns and enforces the final gate before every new or position-changing broker call:

```text
HALTED
  → RECONCILING
  → APPROVAL_PENDING
  → ENABLED
  → HALTED
```

Every call validates the current gate epoch. Before a call, the Executor durably records `execution_attempt_id`, request hash, `client_order_ref`, gate epoch, and prepared state. An ambiguous outcome becomes `UNKNOWN`, halts the gate, and cannot be retried automatically as a new order.

Resumption requires broker order reconciliation, fill/position reconciliation, changelog continuity, signal/checkpoint health, resolution of unknown attempts (and, pre-2026-08-15, reservations — REMOVED CHG-005), and two distinct authenticated operators approving the same epoch and evidence hash. Automatic resume and unaudited bypass are prohibited.

One fenced Executor owns each `execution_partition_id`. Leadership loss, durable-state loss, fencing-token mismatch, network partition, or stale ownership halts submissions.

## Data protection

- Original broker packets and postback payloads are retained for evidence but are never written to ordinary logs.
- Credentials, tokens, raw payloads, and unnecessary account identifiers are redacted from logs and traces.
- Fluss volumes, S3 checkpoints, Iceberg/lake data, and money-moving audit are encrypted at rest.
- Production cross-host traffic uses mandatory encrypted overlay/TLS-protected transport for all sensitive paths (broker, Arrow REST, S3, operator control, secret delivery, and cross-host money-moving/state traffic). Exact mechanism remains evidence-gated but encryption is not optional.
- Broker and Arrow REST communication uses the evidence-approved secure transport.
- Seven-year money-moving audit retention is encrypted with WORM/Object Lock immutability, legal-hold capability, key rotation with historical decryptability, role-restricted access with access audit, retrieval target under 15 minutes from cold storage, event-to-manifest hash-chain integrity, and two-person authorized deletion where policy permits. Exact mechanisms remain evidence-gated but the controls are architecturally mandatory. **2026-08-14: on the configured store (Cloudflare R2) the WORM mechanism is 'bucket locks' — prefix retention rules (duration / until-date / indefinite) via the Cloudflare dashboard/Wrangler/API; an indefinite rule on the audit prefix is the WORM-equivalent (the S3 Object Lock API is not implemented on R2).**

The exact encryption modes, keys, rotation cadence, and legal retention/deletion policy require deployment and compliance evidence.

## Service ownership and authorization

| Principal | Allowed responsibility | Prohibited responsibility |
| --- | --- | --- |
| Ingestion | Append raw market events and discontinuity evidence | Strategy or order placement |
| Signal job | Write candles, candidates (**rankings and immutable instructions REMOVED 2026-08-15, CHG-005**) | Broker calls or fill authority |
| Action Capture | Append postback audit; project lifecycle/positions | Strategy or order submission |
| Babysitter | Read positions; emit zero actions in MVP | Direct broker calls or lifecycle mutation |
| Executor | Gate, attempts, mappings, reconciliation, safety-halt consumption, fencing, Arrow REST | Strategy mutation or authoritative fill capture |
| Observability | Receive telemetry and alerts | Authorize orders or mutate execution state |
| Operator | Authenticated reconcile/approve according to gate epoch; publish safety-halt requests | Unilateral or unaudited resume |

## Security monitoring and response

Required signals include authentication failures, credential age/expiry, secret exposure, redaction failures, TLS failures, unauthorized or mismatched approvals, gate transitions, fencing events, anomalous Arrow REST responses, audit access, and compromised-credential recovery.

Security incidents halt affected order flow, preserve evidence, rotate/revoke credentials, and require reconciliation plus two-person approval before resumption.

## Security acceptance

Tests must cover network exposure, TLS/transport protection, secret scanning and redaction, credential rotation/revocation, least privilege, unauthorized gate actions, encrypted storage, audit access, immutable image/SBOM/vulnerability policy, Executor fencing, and recovery from compromised credentials.

## References

- Security requirements: `../02_requirements/03-non-functional.md` §3.6 and `../02_requirements/02-functional/09-platform-runtime.md` §REQ-PF-010
- Executor contract: `../04_contracts/07-executor.md`
- Runtime contract: `../04_contracts/09-platform-runtime.md`
