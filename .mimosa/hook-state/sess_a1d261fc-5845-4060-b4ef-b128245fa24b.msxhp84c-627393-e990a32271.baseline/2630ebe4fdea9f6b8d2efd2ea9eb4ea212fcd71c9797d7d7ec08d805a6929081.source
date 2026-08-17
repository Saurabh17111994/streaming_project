package com.trading.common.schema.drill;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/** SCH-25 clean-break drill, dry-run half: reset + replay reconvergence. */
class CleanBreakSimulationTest {

    private static List<CleanBreakSimulation.SourceEvent> log() {
        List<CleanBreakSimulation.SourceEvent> events = new ArrayList<>();
        long offset = 0;
        // Two tables, interleaved writes, last-write-wins per key.
        events.add(new CleanBreakSimulation.SourceEvent("Signal_Candidates", "k1", 1, "h1", offset++));
        events.add(new CleanBreakSimulation.SourceEvent("Positions", "p1", 1, "h1", offset++));
        events.add(new CleanBreakSimulation.SourceEvent("Signal_Candidates", "k1", 2, "h2", offset++));
        events.add(new CleanBreakSimulation.SourceEvent("Signal_Candidates", "k2", 1, "h1", offset++));
        events.add(new CleanBreakSimulation.SourceEvent("Positions", "p1", 2, "h2", offset++));
        events.add(new CleanBreakSimulation.SourceEvent("Positions", "p2", 1, "h1", offset++));
        events.add(new CleanBreakSimulation.SourceEvent("Signal_Candidates", "k1", 3, "h3", offset++));
        return List.copyOf(events);
    }

    @Test
    void fullReplayReconvergesEveryTable() {
        CleanBreakSimulation.RunResult r = CleanBreakSimulation.run(
                log(), List.of("Signal_Candidates", "Positions"), 0L);

        assertThat(r.converged()).isTrue();
        assertThat(r.tables()).hasSize(2);
        for (CleanBreakSimulation.TableResult t : r.tables()) {
            assertThat(t.converged()).as(t.table()).isTrue();
            assertThat(t.replayedRows()).isEqualTo(t.sourceRows());
        }
    }

    @Test
    void partialReplayDoesNotReconvergeStaleKeys() {
        // Replaying only the tail cannot rebuild keys whose last write precedes
        // the replay offset — the drill mandates FULL replay from offset 0.
        CleanBreakSimulation.RunResult r = CleanBreakSimulation.run(
                log(), List.of("Signal_Candidates", "Positions"), 5L);

        assertThat(r.converged()).isFalse();
        assertThat(r.tables().get(0).converged()).isFalse();
        assertThat(r.tables().get(1).converged()).isFalse();
    }

    @Test
    void mutatedSourceLogDivergesFromPreResetReference() {
        // The immutable-source guarantee is load-bearing: with a PRE-RESET
        // captured reference (the real drill), a tampered log cannot
        // reconverge and the drill fails closed.
        List<CleanBreakSimulation.SourceEvent> original = log();
        List<CleanBreakSimulation.SourceEvent> mutated = new ArrayList<>(original);
        // Tamper the FINAL write of k1 — an earlier overwrite would be
        // superseded by a later legitimate write and stay hidden.
        mutated.set(6, new CleanBreakSimulation.SourceEvent(
                "Signal_Candidates", "k1", 3, "TAMPERED", 6L));

        // Pre-reset reference captured from the genuine log.
        Map<String, Map<String, CleanBreakSimulation.ProjectedRow>> reference = capture(
                original, List.of("Signal_Candidates"));

        CleanBreakSimulation.RunResult r = CleanBreakSimulation.run(
                mutated, List.of("Signal_Candidates"), 0L, reference);

        assertThat(r.converged()).isFalse();
        assertThat(r.tables().get(0).converged()).isFalse();
    }

    @Test
    void resetThenReplayMatchesPreResetReference() {
        // The real drill semantics: capture the pre-reset projection as the
        // reference, reset, replay the full log, and compare — identical when
        // the log is immutable.
        List<CleanBreakSimulation.SourceEvent> log = log();
        Map<String, Map<String, CleanBreakSimulation.ProjectedRow>> reference =
                capture(log, List.of("Positions"));

        CleanBreakSimulation.RunResult afterReplay = CleanBreakSimulation.run(
                log, List.of("Positions"), 0L, reference);

        assertThat(afterReplay.converged()).isTrue();
        assertThat(afterReplay.tables().get(0).converged()).isTrue();
        assertThat(afterReplay.tables().get(0).sourceRows())
                .isEqualTo(afterReplay.tables().get(0).replayedRows());
    }

    private static Map<String, Map<String, CleanBreakSimulation.ProjectedRow>> capture(
            List<CleanBreakSimulation.SourceEvent> log, List<String> tables) {
        // Reference = from-scratch re-apply of the log, per table.
        Map<String, Map<String, CleanBreakSimulation.ProjectedRow>> out = new java.util.LinkedHashMap<>();
        for (String table : tables) {
            Map<String, CleanBreakSimulation.ProjectedRow> rows =
                    new java.util.LinkedHashMap<>();
            for (CleanBreakSimulation.SourceEvent e : log) {
                if (e.table().equals(table)) {
                    rows.put(e.key(), new CleanBreakSimulation.ProjectedRow(
                            e.sourceVersion(), e.contentHash()));
                }
            }
            out.put(table, rows);
        }
        return out;
    }
}
