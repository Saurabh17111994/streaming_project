# Segment Build Contract — Ranking — REMOVED FROM SCOPE

> **REMOVED 2026-08-15 (CHG-005 — not deferred).** The Ranking/Reservations/Decisions
> segment contract is removed from project scope. This file is retained as a stub so
> contract cross-references remain resolvable.
>
> The in-operator scoring/selection contract, the `portfolio_id` ranking/reservation
> scope, the conservative capacity model (`RESERVED`/`SUBMITTING`/`PENDING`/`OPEN`/
> `RELEASE_PENDING`/`RELEASED`/`UNKNOWN`), and the `Ranking_Results` / `Trade_Decisions`
> outputs are all REMOVED with the feature and are not part of the current system.
>
> Historical note: the full ranking design (versioned score contract, deterministic
> tie-breakers, capacity serialization, p99 < 100 ms latency target) is preserved in
> the 2026-08-15 change record.
