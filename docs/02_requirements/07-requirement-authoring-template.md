# Requirements Authoring Template

Use this template for every new or materially changed requirement. Keep requirements short, explicit, and testable.

## Requirement header

```markdown
## REQ-<DOMAIN>-<NUMBER>: <Short title>

- **Status:** Draft | Evidence-blocked | Implementation-ready | Validated
- **Owner:** <one accountable owner>
- **Readers:** <components or teams>
- **Source:** <decision, contract, or requirement reference>
- **Related audit findings:** <AUD-Cxx/AUD-Hxx/AUD-Mxx or None>
```

## Requirement body

```markdown
### Problem and scope
- **Problem:** <what this requirement solves>
- **In scope:** <included behavior>
- **Out of scope:** <explicit exclusions>

### Inputs
- **Source:** <component/table/API>
- **Schema/version:** <version>
- **Delivery:** at-least-once, exactly-once within tested boundary, or unknown
- **Invalid/unknown input:** <required behavior>

### Outputs and side effects
- **Output:** <component/table/API>
- **Identity:** <event/entity identity>
- **Key/scope:** <partition or aggregate key>
- **Visibility:** <acknowledged, visible, checkpoint-committed, or other defined boundary>
- **Idempotency:** <duplicate behavior>

### State and ownership
- **State:** <state name/type>
- **Writer:** <sole writer or column owner>
- **Readers:** <consumers>
- **Key:** <physical/logical key>
- **Bound:** <maximum cardinality/bytes or evidence gate>
- **Cleanup:** <TTL, terminal cleanup, or retention rule>
- **Recovery source:** <checkpoint, immutable log, backup, or manual reconciliation>

### Required behavior
1. <mandatory behavior>
2. <mandatory behavior>

### Ordering and time
- **Event time:** <meaning>
- **Receive/persist/visible time:** <meaning>
- **Ordering scope:** <per instrument, portfolio, order, etc.>
- **Watermark/finality:** <if applicable>
- **Late/out-of-order behavior:** <required result>

### Failure and recovery
| Failure | Detection | Safe state | Automatic action | Manual action |
|---|---|---|---|---|
| <failure> | <detector> | <state> | <action> | <action> |

### Configuration and compatibility
- **Configuration:** <required values and fail-closed behavior>
- **Version/wire/state impact:** <impact>
- **Migration/rollback:** <plan>
- **Evidence gate:** <unverified external behavior and owner>

### Observability
- **Metrics:** <names/measurements>
- **Logs/audit:** <required evidence>
- **Readiness:** <conditions>
- **Alerts:** <threshold and owner>

### Acceptance criteria
| ID | Fixture/workload | Action/failure | Expected result | Threshold | Evidence artifact |
|---|---|---|---|---|---|
| <AC-ID> | <input> | <event> | <binary result> | <threshold> | <report/test> |

### Decision record
- **Recommendation:** <selected option>
- **Why:** <problem solved>
- **Benefit:** <benefit>
- **Cost/risk:** <trade-off>
- **Rejected alternative:** <simpler/other option and why rejected>
- **Revisit trigger:** <condition>
```

## Quality gate

A requirement is not implementation-ready if it lacks:

- one accountable owner;
- explicit identity, scope, and state key;
- inputs and outputs;
- duplicate/idempotency behavior;
- failure and recovery behavior;
- measurable acceptance criteria;
- compatibility and rollback impact;
- observability needed to prove the behavior.
