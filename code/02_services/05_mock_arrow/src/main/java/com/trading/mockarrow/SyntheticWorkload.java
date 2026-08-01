package com.trading.mockarrow;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.SplittableRandom;

/** Deterministic, per-instrument variable-arrival workload used by benchmarks. */
public final class SyntheticWorkload {
    public enum Profile { BASELINE, PEAK }

    public record Config(List<Long> instruments, long seed, Profile profile, long startMs) {
        public Config {
            if (instruments == null || instruments.isEmpty()) {
                throw new IllegalArgumentException("instrument manifest must not be empty");
            }
            if (profile == null) throw new IllegalArgumentException("profile is required");
            if (startMs < 0) throw new IllegalArgumentException("startMs must be non-negative");
            instruments = List.copyOf(instruments);
        }
    }

    public record Tick(long instrumentToken, long eventTimeMs, long sequence) {}

    private record Due(long timeMs, int instrumentIndex) {}

    private final Config config;
    private final SplittableRandom random;
    private final PriorityQueue<Due> due = new PriorityQueue<>(Comparator.comparingLong(Due::timeMs));
    private long sequence;

    public SyntheticWorkload(Config config) {
        this.config = config;
        this.random = new SplittableRandom(config.seed());
        for (int i = 0; i < config.instruments().size(); i++) {
            due.add(new Due(config.startMs() + initialOffset(i), i));
        }
    }

    public Config config() { return config; }

    /** Returns the next event in deterministic event-time order. */
    public Tick next() {
        Due next = due.remove();
        long interval = nextIntervalMs();
        due.add(new Due(next.timeMs() + interval, next.instrumentIndex()));
        return new Tick(config.instruments().get(next.instrumentIndex()), next.timeMs(), sequence++);
    }

    /** Generate a bounded sample without sleeping or using wall-clock time. */
    public List<Tick> sample(int count) {
        if (count < 0) throw new IllegalArgumentException("count must be non-negative");
        List<Tick> ticks = new ArrayList<>(count);
        for (int i = 0; i < count; i++) ticks.add(next());
        return ticks;
    }

    private long initialOffset(int instrumentIndex) {
        // Stagger instruments so the first measurement window is not a burst.
        return Math.floorMod((long) instrumentIndex * 31L + config.seed(), 1000L);
    }

    private long nextIntervalMs() {
        if (config.profile() == Profile.PEAK) {
            // 33/34 ms gives <=30 ticks/s per instrument and a variable stream.
            return 33L + random.nextLong(2L);
        }
        // 40..60 ms has a 50 ms mean: 20 ticks/s/instrument baseline average.
        return 40L + random.nextLong(21L);
    }
}
