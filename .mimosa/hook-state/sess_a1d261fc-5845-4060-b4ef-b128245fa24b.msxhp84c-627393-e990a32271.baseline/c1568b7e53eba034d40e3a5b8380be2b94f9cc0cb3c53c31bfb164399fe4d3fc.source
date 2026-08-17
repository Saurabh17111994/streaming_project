package com.trading.common.schema.drill;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * SCH-25 clean-break drill — the pure-JVM convergence core: proves that
 * reset + full replay from an IMMUTABLE source log reconverges every target
 * projection to the reference state, and fails closed when the source log
 * was mutated (which is exactly why the clean break may only replay from an
 * append-only LOG, never from a mutable store).
 *
 * <p>Model: events are {@code (table, key, sourceVersion, contentHash)}
 * append-only entries; each projection is last-write-wins on
 * {@code (table, key)}. The reference is a from-scratch re-apply of the whole
 * log; the replayed projection is a re-apply from {@code replayFromOffset}.
 * Convergence = every projected row's (version, hash) equals the reference.
 *
 * <p>This is the dry-run half of the drill — the operator half (dropping live
 * tables, restarting the Flink job) is the gated {@code clean_break_drill.py}
 * procedure; the semantics it relies on are pinned here.
 */
public final class CleanBreakSimulation {

    private CleanBreakSimulation() {}

    /** One append-only source event. */
    public record SourceEvent(String table, String key, long sourceVersion,
                              String contentHash, long offset) {}

    /** One projected row: last write wins on (table, key). */
    public record ProjectedRow(long sourceVersion, String contentHash) {}

    /** Per-table convergence result. */
    public record TableResult(String table, long sourceRows, long replayedRows,
                              boolean converged) {}

    public record RunResult(List<TableResult> tables, boolean converged) {}

    /**
     * Run the simulation (reference derived from the immutable log — the
     * known-good reading of an append-only source).
     */
    public static RunResult run(List<SourceEvent> log, List<String> tables,
            long replayFromOffset) {
        return run(log, tables, replayFromOffset, null);
    }

    /**
     * Run the simulation against an externally captured pre-reset reference
     * (the real drill: capture pre-reset state, reset, replay the full log,
     * compare). When {@code preResetReference} is null the reference is a
     * from-scratch re-apply of the whole log.
     *
     * @param log                the source log (append-only)
     * @param tables             the target tables to reset and replay
     * @param replayFromOffset   inclusive replay start (0 = full replay)
     * @param preResetReference  per-table captured pre-reset projections
     */
    public static RunResult run(List<SourceEvent> log, List<String> tables,
            long replayFromOffset,
            Map<String, Map<String, ProjectedRow>> preResetReference) {
        Map<String, ProjectedRow> reference = preResetReference == null
                ? apply(log, 0L, null)
                : flatten(preResetReference);
        // Reset: drop the projections. Replay: re-apply from the replay offset.
        Map<String, ProjectedRow> replayed = apply(log, replayFromOffset, tables);

        List<TableResult> results = new ArrayList<>();
        boolean allConverged = true;
        for (String table : tables) {
            Map<String, ProjectedRow> refTable = select(reference, table);
            Map<String, ProjectedRow> repTable = select(replayed, table);
            long sourceRows = log.stream().filter(e -> e.table().equals(table)).count();
            long replayedRows = log.stream()
                    .filter(e -> e.table().equals(table))
                    .filter(e -> e.offset() >= replayFromOffset)
                    .count();
            boolean converged = refTable.equals(repTable);
            allConverged &= converged;
            results.add(new TableResult(table, sourceRows, replayedRows, converged));
        }
        return new RunResult(results, allConverged);
    }

    private static Map<String, ProjectedRow> flatten(
            Map<String, Map<String, ProjectedRow>> perTable) {
        Map<String, ProjectedRow> out = new LinkedHashMap<>();
        for (Map.Entry<String, Map<String, ProjectedRow>> t : perTable.entrySet()) {
            for (Map.Entry<String, ProjectedRow> row : t.getValue().entrySet()) {
                out.put(t.getKey() + "|" + row.getKey(), row.getValue());
            }
        }
        return out;
    }

    private static Map<String, ProjectedRow> apply(List<SourceEvent> log, long fromOffset,
            List<String> tables) {
        Map<String, ProjectedRow> out = new LinkedHashMap<>();
        for (SourceEvent e : log) {
            if (e.offset() < fromOffset) {
                continue;
            }
            if (tables != null && !tables.contains(e.table())) {
                continue;
            }
            out.put(e.table() + "|" + e.key(), new ProjectedRow(e.sourceVersion(), e.contentHash()));
        }
        return out;
    }

    private static Map<String, ProjectedRow> select(Map<String, ProjectedRow> all, String table) {
        Map<String, ProjectedRow> out = new LinkedHashMap<>();
        for (Map.Entry<String, ProjectedRow> e : all.entrySet()) {
            if (e.getKey().startsWith(table + "|")) {
                out.put(e.getKey().substring(table.length() + 1), e.getValue());
            }
        }
        return out;
    }
}
