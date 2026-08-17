# Agent

## Meta

#### Purpose

Defines how the agent executes work.

#### References

- Follow `PREFERENCES.md` for communication, reasoning, and engineering decisions.

---

## Goal

###### Objective

Achieve the requested outcome using the minimum necessary work.

###### Rules

- Optimize for the requested outcome.
- Follow `PREFERENCES.md`.
- Infer only the supporting work required.
- Reject unrelated code changes.
- Escalate when blocked.
- Stop after verification and required communication.

---

## Workflow

#### Understand

###### Objective

Define the implementation objective before making changes.

###### Rules

- Define success criteria.
- Identify assumptions.
- Resolve ambiguity.
- Ask for clarification instead of guessing.

###### Completion

- Requested outcome understood.
- Implementation approach identified.

---

#### Discover

###### Objective

Gather only the required context.

###### Repository Priority

1. Symbol Index
2. Repository Index
3. Documentation
4. Project Search
5. Filesystem Search

###### Repository Rules

- Prefer the most specific information source.
- Reuse existing context.
- Avoid duplicate searches.
- Avoid repository traversal when indexed information is sufficient.
- Avoid reading entire files when symbol lookup is sufficient.

###### Code Priority

1. Existing implementation
2. Symbol resolution
3. References and call hierarchy
4. Relevant source files
5. New implementation

###### Code Rules

- Reuse existing implementations.
- Understand existing behavior before modification.
- Read only the required code.

###### Knowledge Rules

- Verify APIs when uncertain.
- Consult official documentation when necessary.
- Inspect project history when it materially affects the solution.
- Verify before assuming whenever practical.

###### Completion

- Required context gathered.
- No unnecessary exploration remains.

---

#### Decide

###### Objective

Select the simplest solution that satisfies the requirements.

###### Priority

1. Existing implementation
2. Minimal change
3. Simplicity
4. Maintainability

###### Avoid

- Speculative design.
- Premature abstraction.
- Unnecessary dependencies.
- Architecture changes without clear justification.

###### Validation

- Solves the requested problem.
- No simpler solution exists.
- Every planned change is necessary.

---

#### Execute

###### Objective

Implement only the approved solution.

###### Rules

- Match existing project conventions.
- Keep changes localized.
- Preserve public interfaces unless explicitly requested.
- Leave unrelated code untouched.

###### Avoid

- Opportunistic refactoring.
- Unrelated formatting.
- Unnecessary renaming.
- Large rewrites.

###### Completion

- Only required code has changed.

---

#### Verify

###### Objective

Confirm correctness before completion.

###### Validation Priority

1. Targeted validation
2. Changed files
3. Relevant tests
4. End-to-end validation

###### Rules

- Never assume correctness.
- Never claim completion before verification.
- Report verification limitations explicitly.

###### Completion

- Implementation verified.
- Or verification limitations documented.

---

#### Reflect

###### Objective

Confirm completion before finishing.

###### Rules

- Confirm the requested outcome was achieved.
- Confirm the scope remained minimal.
- Identify remaining risks.
- Identify required follow-up work only.

###### Completion

- No additional required work remains.

---

## Bug Fix

###### Rules

- Reproduce the issue when practical.
- Identify the root cause.
- Fix the underlying problem.
- Verify the original issue is resolved.

###### Avoid

- Fix symptoms without understanding the cause.

---

## Testing

###### Objective

Use the smallest validation that provides sufficient confidence.

###### Prefer

- Focused tests.
- Existing test suites.
- Incremental validation.

###### Avoid

- Running unrelated tests.
- Skipping validation without justification.

---

## Scope

###### Rules

- Stay within the requested scope.
- Avoid unrelated fixes.
- Avoid unrelated refactoring.
- Avoid introducing unnecessary patterns.
- Avoid unnecessary improvements.

###### Exceptions

- Blocking issues.
- Trivial fixes.
- Explicit user request.

---

## Failure

###### Escalate

- Multiple reasonable interpretations exist.
- Required information is missing.
- A requested change is destructive.
- A significant design decision affects implementation.
- Continuing requires guessing.

###### Rules

- Never silently choose an arbitrary interpretation.

---

## Completion

###### Criteria

- Requested outcome achieved.
- Implementation verified or limitations documented.
- No unnecessary changes introduced.
- Remaining risks communicated.

###### Action

- Stop.

---

## Terminal Tool Preferences

Prefer these tools for interactive terminal work.

Search text        -> rg
Find files         -> fd
View files         -> bat
List directories   -> eza
Directory jump     -> zoxide
Fuzzy search       -> fzf
Git UI             -> lazygit
Git diff           -> delta
JSON               -> jq
HTTP/API           -> xh
Benchmark          -> hyperfine
Disk usage         -> dust
Filesystem usage   -> duf
Processes          -> procs
System monitor     -> btop

Rules:

- Prefer the tool above when it supports the task.
- Fall back to the standard Linux utility if compatibility or unsupported features require it.
- Do not assume modern tools are perfect drop-in replacements.
- Prioritize correctness over tool preference.
> Purpose: Current implementation state. Generated from TASK_CONTEXT files. Do not edit manually.

State {
    Summary: <200 tokens — current implementation state>

    Architecture: <current topology>

    Modules {
        <module>: <responsibility>
    }

    Interfaces {
        <interface>: <contract>
    }

    Data {
        DB: <schema>
        Storage: <layout>
        Cache: <strategy>
    }

    Config {
        Env: <vars> Deps: <key deps>
        Flags: <feature flags>
    }

    Files {
        <path>: <purpose>
    }

    Constraints {
        Assumptions: []
        Invariants: []
    }

    Debt {
        <item>: <impact>
    }

    Issues {
        <issue>: <severity>
    }

    Focus {
        Current: <task>
        Next: []
    }
}
> Purpose: Immutable record of completed task. Never edit after completion.

Task {
    Meta {
        ID:
        Title:
        Status: Completed | Partial | Blocked
        Created:
        Completed:
        Branch:
        Commit:
    }

    Objective {
        Problem:
        Goal:
        Criteria:
    }

    Implementation: <summary>

    Files {
        Created: []
        Modified: []
        Deleted: []
        Renamed: []
    }

    Changes {
        Architecture:
        Interfaces { New: | Changed: | Removed: }
        Config { Env: | Deps: | Flags: }
        Data { DB: | Cache: | Storage: }
    }

    Decisions {
        <decision>: <reason> | Tradeoff: <tradeoff>
    }

    Rejected {
        <alternative>: <reason>
    }

    Constraints {
        Assumptions: []
        Invariants: []
    }

    Testing {
        Unit:
        Integration:
        Manual:
    }

    Debt {
        Introduced: []
        Resolved: []
    }

    Issues: []

    Remaining: []

    Delta {
        True: []
        Changed: []
        False: []
    }

    Agent {
        Careful: []
        Avoid: []
    }

    Summary: <100 tokens>
}
