# Requirements Engineering Audit

## Purpose

This folder records a production-engineering review of `docs/02_requirements` for the Flink + Fluss streaming trading platform. It is an **audit and remediation proposal**, not an authority layer and not an approval to implement.

## Authority

Use the repository authority order in [`../01_project/00-index.md`](../01_project/00-index.md):

1. Executable implementation and tests
2. Active architectural decisions
3. Validated DDLs
4. Contracts
5. Requirements
6. Summaries and historical prose

Current DDLs remain version-validation blocked. Findings here must be reconciled into requirements, contracts, DDLs, tests, and runbooks before implementation-ready status.

## Documents

| File | Purpose |
| --- | --- |
| [`01-executive-summary.md`](./01-executive-summary.md) | Verdict, scope, and highest-value actions |
| [`02-critical-findings.md`](./02-critical-findings.md) | Findings that block safe implementation or live-money readiness |
| [`03-high-findings.md`](./03-high-findings.md) | Important design and operational gaps |
| [`04-medium-findings.md`](./04-medium-findings.md) | Traceability, clarity, and maintainability gaps |
| [`05-cross-document-inconsistencies.md`](./05-cross-document-inconsistencies.md) | Contradictions and vocabulary mismatches |
| [`06-remediation-roadmap.md`](./06-remediation-roadmap.md) | Ordered resolution plan and exit gates |
| [`07-requirement-refinement-template.md`](./07-requirement-refinement-template.md) | Standard format for rewriting requirements |
| [`08-traceability-matrix.md`](./08-traceability-matrix.md) | Finding-to-artifact and validation mapping |

## Finding statuses

- `OPEN`: unresolved and requires work
- `EVIDENCE-BLOCKED`: cannot close until an external/runtime fact is proven
- `RECONCILIATION-REQUIRED`: documents must be changed consistently
- `READY-FOR-REVIEW`: proposed correction exists but is not approved
- `CLOSED`: evidence and artifact updates complete

## Review rules

- Do not silently promote a recommendation to a requirement.
- Do not use this folder as a substitute for exact Flink/Fluss capability tests.
- Every accepted finding must update all affected requirements, contracts, DDLs, tests, metrics, and runbooks.
- A failed correctness, compatibility, recovery, or safety finding blocks live-money readiness.
