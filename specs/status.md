# Status — current focus & open decisions

> Companion to `project_design.md` and `ROADMAP.md`. Working status only — not design, not process.
> Last updated: 2026-07-20

---

## Current focus

Phase 4.1 (Design & architecture) — **closed**.

Closed so far: who reads the output (OpenAlgo); ingestion (Arrow HFT WS → ingestion → `raw_table_1`; Fluss log live + Iceberg-on-S3 lake; old Parquet backfilled then retired); compute (Flink builds features fresh, initial scope = candles → append-only Fluss log feature tables; 250+ features deferred); strategy (stateful + CEP consume the in-flight feature stream from the feature builder, not raw ticks); execution & lifecycle (fills captured independently by a Fluss Java client service from Arrow Order Stream → `Fills_table` + KV lifecycle; a separate babysitter Flink job reads fills and emits trim/re-entry/exit; OpenAlgo is read-only on the KV and never touches fills); observability (OpenObserve; depth = infra health + SLA pipeline metrics; business/trade metrics out of scope; alerting operational/SLO only); and NFRs (throughput = NSE+MCX ~3k instruments, peak ~75k ticks/sec; failure = data path auto-recovers / order path safe-halts; latency = tens of ms tick→instruction-emitted, excluding the broker REST call which is tracked as a separate SLA; implies OpenAlgo must be push/event-driven). Flink is two jobs: signal + babysitter.

## Open / deferred items (detailed-pass work)

Carried from `project_design.md` §5 — not yet designed:

- **Ingestion:** `raw_table_1` column schema, distribution / bucketing, delivery semantics into Fluss, WS reconnect and gap handling.
- **Compute:** candle window(s) / granularity, event-time and watermarking, feature table column schema, write idempotency / dedup.
- **Observability:** SLA metric list, SLO targets, alert thresholds, dashboards, runbooks (Phase 4.5).
- **Execution:** OpenAlgo push mechanism (KV push vs change-log tail), Java-client writer idempotency.

## Next phase

4.2 MVP (see `ROADMAP.md`).
