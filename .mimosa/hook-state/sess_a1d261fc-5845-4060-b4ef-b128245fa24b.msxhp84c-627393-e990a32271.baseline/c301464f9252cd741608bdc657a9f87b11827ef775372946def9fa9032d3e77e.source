package com.trading.compute.signaljob;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * Container memory reader for cgroup v2 / v1 (tracker 14 box 906, 2026-08-12).
 *
 * <p>Reads the enclosing container's memory usage and limit from the cgroup
 * hierarchy: cgroup v2 ({@code /sys/fs/cgroup/memory.current} and
 * {@code memory.max}) first, cgroup v1
 * ({@code /sys/fs/cgroup/memory/memory.usage_in_bytes} and
 * {@code memory.limit_in_bytes}) as fallback. The raw files are authoritative
 * because Flink task managers run inside the container; {@code Runtime} /
 * {@code OperatingSystemMXBean} only see host values.
 *
 * <p>Failure contract: any missing/unreadable/unparseable file returns
 * {@code null} (callers then simply do not register the gauges — a metric
 * gap, never a crash). A cgroup v2 limit of the literal {@code "max"} means
 * unlimited and is reported as {@link Snapshot#limitBytes()} = -1.
 *
 * <p>Static factory for the real filesystem paths; package-visible
 * {@link #read(Path, Path, Path, Path)} accepts injected paths for tests.
 */
public final class ContainerMemory {

    // cgroup v2
    public static final Path CGROUP_V2_CURRENT = Path.of("/sys/fs/cgroup/memory.current");
    public static final Path CGROUP_V2_MAX = Path.of("/sys/fs/cgroup/memory.max");
    // cgroup v1
    public static final Path CGROUP_V1_USAGE = Path.of("/sys/fs/cgroup/memory/memory.usage_in_bytes");
    public static final Path CGROUP_V1_LIMIT = Path.of("/sys/fs/cgroup/memory/memory.limit_in_bytes");

    private ContainerMemory() {}

    /** Immutable snapshot of a successful container-memory read. */
    public record Snapshot(long usageBytes, long limitBytes) {

        /** -1 marks an unlimited cgroup v2 limit ("max"). */
        public boolean unlimited() {
            return limitBytes < 0;
        }
    }

    /** Reads from the real cgroup files; {@code null} when unavailable. */
    public static Snapshot read() {
        return read(CGROUP_V2_CURRENT, CGROUP_V2_MAX, CGROUP_V1_USAGE, CGROUP_V1_LIMIT);
    }

    /**
     * Reads usage + limit. Returns {@code null} on ANY failure (missing
     * files, IO error, malformed content) — the caller treats null as
     * "gauges absent".
     */
    static Snapshot read(Path v2Current, Path v2Max, Path v1Usage, Path v1Limit) {
        try {
            Long usageV2 = readLong(v2Current);
            String maxRaw = readTrimmed(v2Max);
            if (usageV2 != null && maxRaw != null) {
                if ("max".equals(maxRaw)) {
                    return new Snapshot(usageV2, -1L);
                }
                return new Snapshot(usageV2, Long.parseLong(maxRaw));
            }
            Long usageV1 = readLong(v1Usage);
            Long limitV1 = readLong(v1Limit);
            if (usageV1 != null && limitV1 != null) {
                return new Snapshot(usageV1, limitV1);
            }
        } catch (IOException | RuntimeException e) {
            return null;
        }
        return null;
    }

    private static Long readLong(Path p) throws IOException {
        String raw = readTrimmed(p);
        return raw == null ? null : Long.parseLong(raw);
    }

    private static String readTrimmed(Path p) throws IOException {
        if (p == null || !Files.isRegularFile(p)) {
            return null;
        }
        byte[] bytes = Files.readAllBytes(p);
        return new String(bytes, StandardCharsets.US_ASCII).trim();
    }
}
