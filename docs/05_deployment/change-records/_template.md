# Change Record Template

File each change as `CHG-<N>.md` in this directory (one record per change),
per `01-foundation.md` "Change control" (orig L205). A change to any of
decision / requirement / DDL-schema / identity-event-contract /
Flink-state-checkpoint-contract / broker-Arrow-REST-adapter / gate-approval /
retention-offload / topology-secret requires a reconciliation record.

Every record MUST name the six required fields below inside a fenced
```text block — `docs-audit` C14 rejects any record missing one of them.
`change_record_id`, `scope`, `owner`, and `date` are recommended for
traceability but not validated.

```text
change_record_id: CHG-001
scope: <decision|requirement|ddl|identity-contract|state-contract|protocol-adapter|gate-behavior|retention|topology-secret> (comma-separated)
owner: <owner>
date: <UTC date>
affected_artifacts: <files, schemas, contracts, or DDLs the change touches — path-shaped tokens (known extensions) must resolve: repo-relative, record-dir-relative, bare name under docs/08_implementation/ or docs/, or a unique repo-wide basename>
compatibility_class: <COMPATIBLE | COMPATIBLE_WITH_LIMITATION | INCOMPATIBLE | UNKNOWN | NOT_APPLICABLE>
savepoint_impact: <none | migration | clean-restart | replay | other — describe state/savepoint/checkpoint effect>
test_updates: <test IDs added or changed, or "none" with justification>
rollback_behavior: <rollback path and state-readability>
plan_tasks: <plan or tracker task references — `tracker-<n>` must match docs/08_implementation/<n>-*.md; `.md` paths must resolve (repo-relative, record-dir-relative, or a bare dossier name); or `none`>
```
