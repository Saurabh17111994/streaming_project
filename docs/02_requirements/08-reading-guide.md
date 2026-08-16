# Requirements Reading Guide

This guide makes the requirements easier to review. It is a navigation aid, not a replacement for the individual requirement files.

## Read every requirement using this order

1. **Status and owner** — Is the requirement active, blocked, or evidence-gated? Who is accountable?
2. **Problem and scope** — What behavior is required, and what is explicitly excluded?
3. **Inputs and outputs** — Which component owns each boundary and what is the delivery guarantee?
4. **Identity and key** — What identifies the event or aggregate, and what scope does it belong to?
5. **State and lifecycle** — Where is state stored, who writes it, how is it bounded, and how is it rebuilt?
6. **Time and ordering** — Which clock/timestamp is authoritative? What happens to late, duplicate, or out-of-order data?
7. **Failure and recovery** — What is the safe state? What is automatic, and what requires reconciliation or approval?
8. **Observability** — Which signals prove the requirement and trigger alerts?
9. **Acceptance** — Which binary test or evidence artifact proves completion?

## Project-wide scopes

- `account_scope_id`: broker/account isolation boundary.
- ~~`portfolio_id`: ranking, reservation, and capacity boundary~~ — **REMOVED 2026-08-15 (CHG-005).**
- `execution_partition_id`: Executor ownership/fencing boundary when account scope is subdivided.

These scopes must be carried consistently through attempts, mappings, lifecycle, positions, gates, and audit. (**Instructions and reservations REMOVED 2026-08-15, CHG-005.**)

## Project-wide timestamp vocabulary

- `event_time`: verified external event time.
- `receive_time`: local receipt time.
- `persist_start_time`: append start time.
- `persist_ack_time`: durable append acknowledgement.
- `visible_time`: first verified reader visibility.
- `checkpoint_commit_time`: checkpoint/sink commit time, when applicable.
- `processing_time`: local processing timestamp.

Do not use `commit` without naming which boundary it means.

## Canonical ownership

- Signal job: compute, candidates. **(Ranking/reservations/instructions REMOVED 2026-08-15, CHG-005.)**
- Action Capture: postback audit, order lifecycle, position projection, quarantine, projection recovery.
- Executor: gate, attempts, fencing, correlation, reconciliation, execution audit, Arrow REST calls.
- Babysitter: position observation; zero actions in MVP.
- Fluss: durable tables/changelogs/replication; not strategy or broker safety decisions.

## Canonical safety rule

When state, identity, changelog continuity, broker outcome, or reconciliation is uncertain:

- do not guess;
- represent `UNKNOWN` explicitly;
- preserve evidence;
- halt affected money-moving flow;
- reconcile before retry or release;
- require the approved resume process.

## Status vocabulary

- `READY`: dependency and contract checks pass.
- `DEGRADED`: process operates with bounded capability loss; no unsafe side effect is permitted.
- `NOT_READY`: required dependency or contract is unavailable.
- `HALTED`: Executor blocks money-moving calls.
- `UNKNOWN`: outcome or state cannot be safely classified.
- `EVIDENCE-BLOCKED`: external/version-specific behavior is not proven.
