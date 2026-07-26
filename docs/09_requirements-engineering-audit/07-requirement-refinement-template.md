# Requirement Refinement Template

<!-- markdownlint-disable MD013 -->

Use this template when converting an audit finding into an authoritative requirement. Do not mark the finding closed until all linked artifacts are updated and tested.

```markdown
## REQ-<DOMAIN>-<NUMBER>: <Short title>

**Status:** Draft | Evidence-blocked | Implementation-ready | Validated
**Owner:** <single accountable owner>
**Readers/consumers:** <components>
**Source decision/contract:** <DEC/contract references>
**Related audit findings:** <AUD-Cxx/AUD-Hxx/AUD-Mxx>

### Problem and scope

<What problem this requirement solves. State non-goals.>

### Inputs

| Input | Owner | Schema/version | Delivery | Invalid/unknown behavior |
|---|---|---|---|---|

### Outputs and side effects

| Output/effect | Owner | Identity/key | Delivery/idempotency | Visibility boundary |
|---|---|---|---|---|

### State and ownership

| State | Type | Key | Writer | Readers | Bound/cleanup | Recovery source |
|---|---|---|---|---|---|---|

### Required behavior

1. <Mandatory behavior>
2. <Mandatory behavior>

### Ordering and time semantics

- Event-time definition:
- Receive/persist/visibility timestamps:
- Ordering scope:
- Watermark/idleness/finality:
- Late and out-of-order behavior:

### Failure and degraded behavior

| Failure | Detection | Safe state | Automatic action | Manual action | RPO/RTO |
|---|---|---|---|---|---|

### Compatibility and rollout

- Schema/wire/state compatibility:
- Migration order:
- Checkpoint/savepoint impact:
- Rollback/readability:
- Evidence gate:

### Observability

| Signal | Dimensions | Threshold | Owner | Evidence use |
|---|---|---|---|---|

### Acceptance criteria

| ID | Fixture/workload | Action/failure | Expected result | Threshold | Artifact |
|---|---|---|---|---|---|

### Trade-off record

- Recommended option:
- Problem solved:
- Benefit:
- Cost/risk:
- Limitation:
- Rejected simpler alternative:
- Revisit trigger:
```

## Quality check

Reject the requirement as incomplete if it lacks:

- one accountable owner;
- explicit identity and state key;
- input/output behavior;
- duplicate/idempotency behavior;
- failure and recovery behavior;
- measurable acceptance criteria;
- compatibility and rollback impact;
- required observability;
- evidence owner for unresolved external behavior.
