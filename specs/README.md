# specs/ — active implementation planning

This directory holds the **work in progress**: what we're building, phase by phase.
It is deliberately separate from `docs/` (long-lived reference — what the system IS).

## Current contents

| File | Purpose |
|------|---------|
| `roadmap.md` | Build phases 4.1 → 4.7 (lifecycle/maturity axis, not strict order) |
| `status.md` | Current focus & open decisions (working status only) |

## Convention for phase work

When a roadmap phase becomes active, create a directory for it:

```text
specs/
└── phase-x-name/
    ├── proposal.md      what this phase changes and why
    ├── design.md        detailed design deltas
    ├── tasks.md         concrete, checkable task list
    ├── validation.md    how completion is verified
    └── specs/           per-capability spec deltas (OpenSpec-style)
```

- Create `specs/phase-x-name/` **only when starting that phase** — do not pre-create
  empty phase directories.
- One directory per phase. Archive completed phases to `specs/archived/` (create it
  when first needed).

## Note on OpenSpec references

Several files in `docs/04_contracts/` reference a `design/09_openspec/` tree
(`specs/<capability>/spec.md`, `changes/phase-*`) that **does not exist** in this
repository. Until that is reconciled, treat `docs/04_contracts/` as the human-readable
source of truth. Reconciling those references is a separate documentation task, not
part of repository restructuring.

## Related

- `docs/01_project/project-design.md` — closed macro decisions (what we're building)
- `docs/04_contracts/` — build-ready contracts per segment (how each segment is built)
