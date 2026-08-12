package com.trading.compute.tools;

import com.trading.common.schema.CandleTableSchema;
import com.trading.compute.signaljob.CandleTableColumns;
import com.trading.compute.signaljob.CanonicalCandlePolicy;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.fluss.client.Connection;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.client.table.Table;
import org.apache.fluss.client.table.scanner.batch.BatchScanner;
import org.apache.fluss.client.table.writer.UpsertWriter;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.utils.CloseableIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Offline LOG→KV migration tool for the candle current-state projection
 * (CANDLE-KV-REPLAY-001 B8.2/B8.3; tracker 14 P3).
 *
 * <p>{@code audit} reads the source LOG ({@code feature_candles_15s}) and
 * reports: total rows, canonical rows (schema_version + approved
 * algorithm/configuration), non-canonical rows, distinct canonical keys,
 * duplicate keys, and conflicting keys. Business conflict fields are every
 * column <em>except</em> {@code output_ts} — two rows for the same key that
 * agree on the business fields are the same canonical candle re-emitted
 * (replay), while disagreeing business values mean the LOG holds genuinely
 * different candles for one key. The scan is read-only; the LOG is never
 * written.
 *
 * <p>{@code load} re-runs the audit, aborts if any unresolved conflict
 * exists, then upserts exactly one approved row per canonical key into the
 * destination KV table and verifies the destination row count afterwards.
 *
 * <h2>P3.1 — field-level approval (MIGRATION-CONFLICT-002)</h2>
 *
 * <p>A conflicting key may be loaded <em>only</em> with an explicit
 * field-level approval: a row of the approval file
 * {@code token,windowStart,&lt;sha256&gt;,APPROVE[,approver[,reason[,decidedAt]]]}
 * where {@code <sha256>} is the SHA-256 of the <em>chosen</em> row's business
 * fields ({@link #rowHash(InternalRow)} — every column except
 * {@code output_ts}, in DDL order) and the trailing fields record the
 * operator's approval provenance (approver, reason, decision time — optional
 * but expected for production evidence, MIGRATION-CONFLICT-002). The tool
 * selects the candidate row whose hash matches the approval exactly; it never
 * picks {@code MAX(output_ts)} merely because it is latest (P3.1). An
 * approval whose hash matches no candidate row is wrong/stale and fails
 * closed. A conflicting key with no approval is excluded and the run exits
 * nonzero. The audit emits, per conflicting key, all candidate hashes with
 * their full business values; the load emits, per approved key, the chosen
 * hash, the rejected hashes, and the provenance
 * ({@code CANDLE_MIGRATION_APPROVAL_RECORD=...}). Non-conflicting keys
 * converge on {@code MAX(output_ts)} — all their business values are
 * identical, so any row is equivalent.
 *
 * <p>Loading any approval file at all is a {@code DEV_EXCEPTION}, not clean
 * production evidence (the 25 replay-incident keys are the 2026-08-10
 * dev incident): the audit prints
 * {@code CANDLE_MIGRATION_DEV_EXCEPTION=1} whenever an approval file is
 * supplied, and the runbook requires production conflicts to be reconciled
 * from field-level evidence instead.
 *
 * <h2>P3.2 — complete-history read (CANDLE-MIGRATION-002)</h2>
 *
 * <p>The source LOG is lake-enabled. A plain {@link BatchScanner} reads the
 * local Fluss log only; it is <em>not</em> complete-history proof. The Flink
 * 2.2 / Fluss 0.9.1-incubating connector builds the union reader itself:
 * verified 2026-08-10 in {@code fluss-flink-2.2-0.9.1-incubating.jar} —
 * {@code FlinkSource} carries a {@code LakeSource&lt;LakeSplit&gt;} and the
 * enumerator emits {@code SnapshotSplit}/{@code HybridSnapshotLogSplit}/
 * {@code LogSplit} per bucket, where {@code HybridSnapshotLogSplit} holds
 * {@code getLogStartingOffset()} + {@code isSnapshotFinished()} (snapshot →
 * log handoff per bucket). Setting {@code CANDLE_MIGRATION_UNION_READ=true}
 * arms the gate: the tool then requires the source table to be lake-enabled
 * ({@code table.datalake.format} property present) and reports the lake
 * format, read timestamp, and per-bucket coverage; without the gate it prints
 * {@code CANDLE_MIGRATION_LAKE_COVERAGE=LOCAL_LOG_ONLY} and refuses to claim
 * complete history. The production migration itself runs as a Flink batch
 * (Table API) job over the union source — this tool stays the dev/pilot path
 * with the gate as its completeness precondition.
 *
 * <h2>P3.3 — bounded memory (dev/pilot path)</h2>
 *
 * <p>The source is processed <em>per bucket</em>: one {@link Audit} at a
 * time, loaded (in {@code load} mode) and discarded before the next bucket,
 * so peak heap is bounded by one bucket's keys (~64 of 1024 tokens share a
 * bucket; per-bucket keys ≪ dev history). Bucket {@code b} holds exactly the
 * tokens whose {@code (murmur3_32_fixed(LE64(token)) & MAX_VALUE) % N == b}
 * (IcebergBucketingFunction — same assignment the writer uses), so all
 * windows of a token land in one bucket and per-key conflict detection is
 * self-contained. {@code CANDLE_MIGRATION_MAX_KEYS} (default 10,000,000) is
 * a hard per-audit key limit checked <em>before</em> a new key is allocated —
 * exceeding it aborts with a measured error instead of an OOM. Peak heap and
 * duration are logged per bucket and in the summary.
 *
 * <p>Environment (same defaults as the SignalJob; version defaults pinned to
 * the canonical pair, tracker 14 P2): {@code FLUSS_BOOTSTRAP_SERVERS}
 * (default {@code localhost:9123}), {@code CANDLE_TABLE} (default {@code
 * feature_candles_15s}), {@code CANDLE_CURRENT_TABLE} (default {@code
 * feature_candles_15s_current}), {@code SCHEMA_VERSION} (default {@code 2}),
 * {@code ALGORITHM_VERSION}/{@code CONFIGURATION_VERSION} (defaults
 * {@code candle-15s-v1}/{@code 1.0.0} — the canonical pair),
 * {@code CANDLE_MIGRATION_MAX_KEYS} (default 10,000,000),
 * {@code CANDLE_MIGRATION_UNION_READ} (default {@code false}),
 * and {@code CANDLE_MIGRATION_ACCEPT_KEYS_FILE} (optional).
 *
 * <p>Usage: {@code java -cp <compute classes>:<classpath>
 * com.trading.compute.tools.CandleMigrationTool audit|load}
 *
 * <p>Exit codes: 0 = OK (all conflicts covered by valid approvals), 2 =
 * business conflicts without approval (abort per B8.2), 1 = error (I/O, bad
 * configuration, malformed approval file, stale/mismatched approval, or
 * approval entries not found in the canonical LOG). Machine-readable stdout
 * lines are prefixed {@code CANDLE_MIGRATION_}.
 */
public final class CandleMigrationTool {

    private static final Logger LOG = LoggerFactory.getLogger(CandleMigrationTool.class);

    private static final Duration TIMEOUT = Duration.ofSeconds(20);
    private static final int SCAN_LIMIT = 1_000_000_000;
    /** Per-key candidate cap for the conflict record (memory bound; truncated flag set). */
    private static final int MAX_CONFLICT_CANDIDATES = 64;
    private static final long DEFAULT_MAX_KEYS = 10_000_000L;

    private CandleMigrationTool() {}

    /** Tool configuration, resolved from the environment with SignalJob defaults. */
    static final class Config {
        final String bootstrap;
        final String sourceTable;
        final String destTable;
        final String schemaVersion;
        final String algorithmVersion;
        final String configurationVersion;
        final String acceptKeysFile;
        final long maxKeys;
        final boolean unionRead;

        Config(String bootstrap, String sourceTable, String destTable,
               String schemaVersion, String algorithmVersion, String configurationVersion,
               String acceptKeysFile, long maxKeys, boolean unionRead) {
            this.bootstrap = bootstrap;
            this.sourceTable = sourceTable;
            this.destTable = destTable;
            this.schemaVersion = schemaVersion;
            this.algorithmVersion = algorithmVersion;
            this.configurationVersion = configurationVersion;
            this.acceptKeysFile = acceptKeysFile;
            this.maxKeys = maxKeys;
            this.unionRead = unionRead;
        }

        static Config fromEnv() {
            return new Config(
                    System.getenv().getOrDefault("FLUSS_BOOTSTRAP_SERVERS", "localhost:9123"),
                    System.getenv().getOrDefault("CANDLE_TABLE", "feature_candles_15s"),
                    System.getenv().getOrDefault("CANDLE_CURRENT_TABLE", "feature_candles_15s_current"),
                    System.getenv().getOrDefault("SCHEMA_VERSION", "2"),
                    System.getenv().getOrDefault("ALGORITHM_VERSION",
                            CandleTableSchema.CANONICAL_ALGORITHM_VERSION),
                    System.getenv().getOrDefault("CONFIGURATION_VERSION",
                            CandleTableSchema.CANONICAL_CONFIGURATION_VERSION),
                    System.getenv("CANDLE_MIGRATION_ACCEPT_KEYS_FILE"),
                    maxKeys(System.getenv().get("CANDLE_MIGRATION_MAX_KEYS")),
                    unionRead(System.getenv().get("CANDLE_MIGRATION_UNION_READ")));
        }

        private static long maxKeys(String raw) {
            if (raw == null) {
                return DEFAULT_MAX_KEYS;
            }
            long value = Long.parseLong(raw.trim());
            if (value <= 0) {
                throw new IllegalArgumentException(
                        "candle-migration: CANDLE_MIGRATION_MAX_KEYS must be > 0, got " + value
                                + " (tracker 14 P3.3)");
            }
            return value;
        }

        private static boolean unionRead(String raw) {
            if (raw == null) {
                return false;
            }
            String trimmed = raw.trim();
            if (trimmed.isEmpty()) {
                throw new IllegalArgumentException(
                        "candle-migration: CANDLE_MIGRATION_UNION_READ is present but blank — "
                                + "use 'true' or 'false' (tracker 14 P3.2)");
            }
            if (!trimmed.equalsIgnoreCase("true") && !trimmed.equalsIgnoreCase("false")) {
                throw new IllegalArgumentException(
                        "candle-migration: CANDLE_MIGRATION_UNION_READ must be 'true' or "
                                + "'false' (case-insensitive), got '" + trimmed + "' (tracker 14 P3.2)");
            }
            return Boolean.parseBoolean(trimmed);
        }
    }

    /**
     * Field-level approval entry (P3.1): the key plus the SHA-256 of the
     * approved row's business fields, the recorded decision, and — where the
     * operator recorded them — approval provenance (approver, reason,
     * decision time). An entry is satisfied only when some candidate row's
     * hash matches {@code rowHash} exactly; the provenance fields are
     * evidence metadata (MIGRATION-CONFLICT-002), never a substitute for the
     * hash match.
     *
     * <p>File format: {@code token,windowStart,<sha256>,APPROVE[,approver[,reason[,decidedAt]]]}
     * — 4 to 7 comma-separated fields; the provenance fields are optional and
     * must not contain commas (use a safe separator or encode them). A legacy
     * 2-field accept line (no hash) is rejected fail-closed.
     */
    record AcceptEntry(long token, long windowStart, String rowHash, String decision,
                       String approver, String reason, String decidedAt) {
        /** Hash-pinned approval without recorded provenance (dev/pilot entries). */
        AcceptEntry(long token, long windowStart, String rowHash, String decision) {
            this(token, windowStart, rowHash, decision, null, null, null);
        }

        static AcceptEntry parse(String line, int lineNumber, String path) {
            String[] parts = line.split(",", -1);
            if (parts.length < 4 || parts.length > 7) {
                throw new IllegalArgumentException("candle-migration: malformed approval line "
                        + lineNumber + " in " + path + ": \"" + line
                        + "\" (expected token,windowStart,<sha256>,APPROVE[,approver[,reason[,"
                        + "decidedAt]]]; legacy 2-field accept lists are rejected — tracker 14 "
                        + "P3.1)");
            }
            long token;
            long windowStart;
            try {
                token = Long.parseLong(parts[0].trim());
                windowStart = Long.parseLong(parts[1].trim());
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException("candle-migration: malformed approval line "
                        + lineNumber + " in " + path + ": \"" + line
                        + "\" (token and windowStart must be integers)", e);
            }
            String hash = parts[2].trim();
            if (!hash.matches("[0-9a-fA-F]{64}")) {
                throw new IllegalArgumentException("candle-migration: malformed approval line "
                        + lineNumber + " in " + path + ": \"" + line
                        + "\" (hash must be a 64-hex-char SHA-256 of the approved row's business "
                        + "fields — tracker 14 P3.1)");
            }
            String decision = parts[3].trim();
            if (!"APPROVE".equalsIgnoreCase(decision)) {
                throw new IllegalArgumentException("candle-migration: malformed approval line "
                        + lineNumber + " in " + path + ": \"" + line
                        + "\" (decision must be APPROVE — tracker 14 P3.1)");
            }
            return new AcceptEntry(token, windowStart, hash.toLowerCase(), decision.toUpperCase(),
                    optionalField(parts, 4), optionalField(parts, 5), optionalField(parts, 6));
        }

        private static String optionalField(String[] parts, int index) {
            if (parts.length <= index) {
                return null;
            }
            String value = parts[index].trim();
            return value.isEmpty() ? null : value;
        }
    }

    /**
     * Deterministic business-field hash (P3.1): SHA-256 over every column
     * except {@code output_ts}, in DDL order, encoded as {@code name=value\n}.
     * Two rows that agree on all business fields — whatever their
     * {@code output_ts} — hash identically (replay convergence); any differing
     * business field changes the hash. This is the row identity the approval
     * file pins and the audit reports per candidate.
     */
    static String rowHash(InternalRow row) {
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            for (int i = 0; i < CandleTableColumns.FIELD_COUNT; i++) {
                if (i == CandleTableColumns.OUTPUT_TS) {
                    continue; // emit metadata, not row identity (CanonicalCandlePolicy)
                }
                md.update((CandleTableColumns.NAMES[i] + "=").getBytes(StandardCharsets.UTF_8));
                switch (i) {
                    case CandleTableColumns.TICK_COUNT:
                        md.update(Integer.toString(row.getInt(i)).getBytes(StandardCharsets.UTF_8));
                        break;
                    case CandleTableColumns.EXCHANGE:
                    case CandleTableColumns.SYMBOL:
                    case CandleTableColumns.ALGORITHM_VERSION:
                    case CandleTableColumns.CONFIGURATION_VERSION:
                    case CandleTableColumns.SCHEMA_VERSION:
                        md.update(row.getString(i).toString().getBytes(StandardCharsets.UTF_8));
                        break;
                    default:
                        md.update(Long.toString(row.getLong(i)).getBytes(StandardCharsets.UTF_8));
                }
                md.update((byte) '\n');
            }
            byte[] digest = md.digest();
            StringBuilder sb = new StringBuilder(64);
            for (byte b : digest) {
                sb.append(Character.forDigit((b >> 4) & 0xF, 16)).append(Character.forDigit(b & 0xF, 16));
            }
            return sb.toString();
        } catch (Exception e) {
            throw new IllegalStateException("candle-migration: SHA-256 unavailable", e);
        }
    }

    /**
     * Fluss bucket assignment for a token (IcebergBucketingFunction): the
     * writer hashes the 8-byte little-endian token with MurmurHash3 x86_32
     * (seed 0) and mods {@code (hash & Integer.MAX_VALUE)} by the bucket
     * count. The tool needs it to prove an approval key's bucket is covered
     * and that per-bucket processing is complete for that key.
     */
    static int bucketFor(long token, int numBuckets) {
        int hash = murmur3_32FixedLittleEndian64(token);
        return (hash & Integer.MAX_VALUE) % numBuckets;
    }

    /** Canonical MurmurHash3 x86_32 (seed 0) over the 8-byte little-endian long. */
    static int murmur3_32FixedLittleEndian64(long value) {
        int h1 = 0; // seed 0 (fixed variant)
        byte[] data = new byte[8];
        for (int i = 0; i < 8; i++) {
            data[i] = (byte) (value >>> (8 * i));
        }
        int c1 = 0xcc9e2d51;
        int c2 = 0x1b873593;
        int len = data.length;
        for (int i = 0; i + 4 <= len; i += 4) {
            int k1 = (data[i] & 0xFF)
                    | ((data[i + 1] & 0xFF) << 8)
                    | ((data[i + 2] & 0xFF) << 16)
                    | ((data[i + 3] & 0xFF) << 24);
            k1 *= c1;
            k1 = Integer.rotateLeft(k1, 15);
            k1 *= c2;
            h1 ^= k1;
            h1 = Integer.rotateLeft(h1, 13);
            h1 = h1 * 5 + 0xe6546b64;
        }
        h1 ^= len;
        h1 ^= h1 >>> 16;
        h1 *= 0x85ebca6b;
        h1 ^= h1 >>> 13;
        h1 *= 0xc2b2ae35;
        h1 ^= h1 >>> 16;
        return h1;
    }

    /**
     * Pure audit accumulator (B8.2/P3): canonical filter, per-key grouping,
     * business-conflict detection, MAX(output_ts) merge target for
     * conflict-free keys, and hash-matched approved-row selection for
     * approved conflicting keys. No I/O — the unit tests exercise this
     * directly.
     */
    static final class Audit {
        private final String expectedSchemaVersion;
        private final String expectedAlgorithm;
        private final String expectedConfiguration;
        private final Map<String, AcceptEntry> accepted;
        private final long maxKeys;

        long totalRows;
        long canonicalRows;
        long nonCanonicalRows;
        long duplicateKeys;
        long conflictingKeys;
        /** Conflicts covered by a valid approval entry (hash-matched row loads). */
        long acceptedKeysCount;
        /** Conflicts NOT covered by an approval — the abort gate (exit 2). */
        long unacceptedConflictingKeys;
        /** Approved conflicts whose hash matches no candidate — wrong/stale approval (exit 1). */
        long approvalStaleKeys;
        /** Keys excluded from load: stale approvals (conflicts without approval already abort). */
        long blockedKeys;
        final List<String> conflictExamples = new ArrayList<>();
        /** token -> windowStart -> aggregate (two-level map: no packed-key collisions). */
        final Map<Long, Map<Long, KeyAgg>> byKey = new HashMap<>();
        private long keyCount;

        Audit(String expectedSchemaVersion, String expectedAlgorithm, String expectedConfiguration) {
            this(expectedSchemaVersion, expectedAlgorithm, expectedConfiguration, Map.of(),
                    DEFAULT_MAX_KEYS);
        }

        Audit(String expectedSchemaVersion, String expectedAlgorithm, String expectedConfiguration,
              Map<String, AcceptEntry> accepted) {
            this(expectedSchemaVersion, expectedAlgorithm, expectedConfiguration, accepted,
                    DEFAULT_MAX_KEYS);
        }

        Audit(String expectedSchemaVersion, String expectedAlgorithm, String expectedConfiguration,
              Map<String, AcceptEntry> accepted, long maxKeys) {
            this.expectedSchemaVersion = expectedSchemaVersion;
            this.expectedAlgorithm = expectedAlgorithm;
            this.expectedConfiguration = expectedConfiguration;
            this.accepted = accepted == null ? Map.of() : accepted;
            this.maxKeys = maxKeys;
        }

        /** Feed one source row; filters and aggregates it. */
        void add(InternalRow row) {
            totalRows++;
            if (!isCanonical(row)) {
                nonCanonicalRows++;
                return;
            }
            // P3.4: a null key would fabricate a bogus canonical key and load it
            // into the destination — fail closed instead (the emit path never
            // produces one, so a null key means schema drift or corrupt data).
            if (row.isNullAt(CandleTableColumns.INSTRUMENT_TOKEN)
                    || row.isNullAt(CandleTableColumns.WINDOW_START)) {
                throw new IllegalStateException("candle-migration: canonical row with null key "
                        + "column (instrument_token or window_start) — refusing to fabricate a key "
                        + "(tracker 14 P3.4); row index " + (totalRows - 1));
            }
            canonicalRows++;
            long token = row.getLong(CandleTableColumns.INSTRUMENT_TOKEN);
            long windowStart = row.getLong(CandleTableColumns.WINDOW_START);
            String key = key(token, windowStart);
            AcceptEntry accept = accepted.get(key);
            Map<Long, KeyAgg> windows = byKey.computeIfAbsent(token, k -> new HashMap<>());
            KeyAgg agg = windows.get(windowStart);
            if (agg == null) {
                keyCount++;
                if (maxKeys > 0 && keyCount > maxKeys) {
                    throw new IllegalStateException("candle-migration: distinct canonical keys "
                            + keyCount + " exceed CANDLE_MIGRATION_MAX_KEYS=" + maxKeys
                            + " — aborting before allocation (tracker 14 P3.3); raise the limit "
                            + "only with a measured heap/GC plan");
                }
                agg = new KeyAgg(accept);
                windows.put(windowStart, agg);
            }
            agg.add(row);
            if (agg.rows == 2) {
                duplicateKeys++;
            }
            if (agg.conflict && !agg.conflictCounted) {
                agg.conflictCounted = true;
                conflictingKeys++;
                if (accept != null) {
                    acceptedKeysCount++;
                } else {
                    unacceptedConflictingKeys++;
                    if (conflictExamples.size() < 5) {
                        conflictExamples.add("token=" + token + " windowStart=" + windowStart
                                + " " + agg.conflictField + " differs (first=" + agg.firstValue
                                + " vs later=" + agg.laterValue + ")");
                    }
                }
            }
        }

        /**
         * Resolves every key's approved row (hash match) after feeding is
         * complete — an approval can only be judged once all candidates are
         * seen. Idempotent.
         */
        void resolveAll() {
            for (Map<Long, KeyAgg> windows : byKey.values()) {
                for (KeyAgg agg : windows.values()) {
                    if (agg.resolveApproval()) {
                        approvalStaleKeys++;
                        blockedKeys++;
                    }
                }
            }
        }

        private boolean isCanonical(InternalRow row) {
            if (!expectedSchemaVersion.equals(
                    row.getString(CandleTableColumns.SCHEMA_VERSION).toString())) {
                return false;
            }
            return CanonicalCandlePolicy.isCanonical(
                    row.getString(CandleTableColumns.ALGORITHM_VERSION).toString(),
                    row.getString(CandleTableColumns.CONFIGURATION_VERSION).toString(),
                    expectedAlgorithm, expectedConfiguration);
        }

        long distinctKeys() {
            long keys = 0;
            for (Map<Long, KeyAgg> windows : byKey.values()) {
                keys += windows.size();
            }
            return keys;
        }

        /**
         * The row to upsert for a key: the MAX(output_ts) row when the key is
         * conflict-free (all business values identical — any row is
         * equivalent), else the candidate whose hash the approval pins
         * (never "merely latest" — P3.1). Null = no approved row (stale
         * approval or missing approval — the caller must have gated already).
         */
        InternalRow approvedRow(KeyAgg agg) {
            if (!agg.conflict) {
                return agg.rowAtMaxOutputTs;
            }
            if (agg.accept == null || agg.approvalStale) {
                return null;
            }
            return agg.approvedRow;
        }

        /**
         * Approval entries that match no canonical LOG key — typo or stale
         * list detection (fail-closed: the operator intended an override for a
         * key the source does not actually hold).
         */
        long acceptedKeysNotFound(Set<String> seenKeys) {
            if (accepted.isEmpty()) {
                return 0;
            }
            long notFound = 0;
            for (String k : accepted.keySet()) {
                if (!seenKeys.contains(k)) {
                    notFound++;
                }
            }
            return notFound;
        }

        /** All canonical keys present in this audit (string form). */
        Set<String> seenKeys() {
            Set<String> seen = new HashSet<>();
            for (Map.Entry<Long, Map<Long, KeyAgg>> entry : byKey.entrySet()) {
                for (Long windowStart : entry.getValue().keySet()) {
                    seen.add(key(entry.getKey(), windowStart));
                }
            }
            return seen;
        }

        /** Machine-readable conflict record per conflicting key (P3.3: emitted separately). */
        List<String> conflictRecords() {
            List<String> records = new ArrayList<>();
            for (Map.Entry<Long, Map<Long, KeyAgg>> entry : byKey.entrySet()) {
                for (Map.Entry<Long, KeyAgg> w : entry.getValue().entrySet()) {
                    KeyAgg agg = w.getValue();
                    if (!agg.conflict) {
                        continue;
                    }
                    StringBuilder sb = new StringBuilder(512);
                    sb.append("token=").append(entry.getKey())
                      .append(",windowStart=").append(w.getKey())
                      .append(",approved=").append(agg.approvalStale ? "STALE"
                              : agg.approvedRow == null ? "MISSING" : "HASH_MATCH")
                      .append(",candidates=[");
                    for (int i = 0; i < agg.candidates.size(); i++) {
                        Candidate c = agg.candidates.get(i);
                        if (i > 0) {
                            sb.append(";");
                        }
                        sb.append("{hash=").append(c.hash)
                          .append(",outputTs=").append(c.outputTs)
                          .append(",algorithm=").append(c.algorithm)
                          .append(",configuration=").append(c.configuration)
                          .append(",values=").append(businessValues(c.row)).append("}");
                    }
                    sb.append(agg.candidatesTruncated ? ",truncated=true]" : "]");
                    records.add(sb.toString());
                }
            }
            return records;
        }

        /**
         * Machine-readable approval record per <em>approved</em> conflicting
         * key (P3.1 — MIGRATION-CONFLICT-002): the chosen row hash, every
         * rejected candidate hash, and the recorded provenance (approver,
         * reason, decision time) where the operator supplied it. This is the
         * field-level evidence that justifies loading one candidate over the
         * others — never "merely latest".
         */
        List<String> approvalRecords() {
            List<String> records = new ArrayList<>();
            for (Map.Entry<Long, Map<Long, KeyAgg>> entry : byKey.entrySet()) {
                for (Map.Entry<Long, KeyAgg> w : entry.getValue().entrySet()) {
                    KeyAgg agg = w.getValue();
                    if (!agg.conflict || agg.accept == null || agg.approvalStale
                            || agg.approvedRow == null) {
                        continue;
                    }
                    StringBuilder sb = new StringBuilder(256);
                    sb.append("token=").append(entry.getKey())
                      .append(",windowStart=").append(w.getKey())
                      .append(",approvedHash=").append(agg.accept.rowHash)
                      .append(",rejectedHashes=");
                    boolean first = true;
                    for (Candidate c : agg.candidates) {
                        if (c.hash.equals(agg.accept.rowHash)) {
                            continue;
                        }
                        if (!first) {
                            sb.append(';');
                        }
                        sb.append(c.hash);
                        first = false;
                    }
                    if (agg.accept.approver() != null) {
                        sb.append(",approver=").append(agg.accept.approver());
                    }
                    if (agg.accept.reason() != null) {
                        sb.append(",reason=").append(agg.accept.reason());
                    }
                    if (agg.accept.decidedAt() != null) {
                        sb.append(",decidedAt=").append(agg.accept.decidedAt());
                    }
                    records.add(sb.toString());
                }
            }
            return records;
        }

        private static String key(long token, long windowStart) {
            return token + ":" + windowStart;
        }

        /**
         * Full business-field values of a candidate row (P3.1 field-level
         * evidence): every column except {@code output_ts} rendered as
         * {@code name=value} pairs in DDL order — the values the operator
         * must reconcile when approving one candidate over the others.
         */
        static String businessValues(InternalRow row) {
            StringBuilder sb = new StringBuilder(192);
            for (int i = 0; i < CandleTableColumns.FIELD_COUNT; i++) {
                if (i == CandleTableColumns.OUTPUT_TS) {
                    continue; // emit metadata, not row identity
                }
                if (sb.length() > 0) {
                    sb.append(',');
                }
                sb.append(CandleTableColumns.NAMES[i]).append('=').append(businessValue(row, i));
            }
            return sb.toString();
        }
    }

    /** Business-field value renderer shared by the audit record and the renderer above. */
    private static String businessValue(InternalRow row, int index) {
        switch (index) {
            case CandleTableColumns.TICK_COUNT:
                return Integer.toString(row.getInt(index));
            case CandleTableColumns.EXCHANGE:
            case CandleTableColumns.SYMBOL:
            case CandleTableColumns.ALGORITHM_VERSION:
            case CandleTableColumns.CONFIGURATION_VERSION:
            case CandleTableColumns.SCHEMA_VERSION:
                return row.getString(index).toString();
            default:
                return Long.toString(row.getLong(index));
        }
    }

    /** Per-key aggregate: row count, conflict detection, approved-row resolution. */
    static final class KeyAgg {
        /** Approval entry for this key, or null (unapproved conflicts abort). */
        final AcceptEntry accept;
        long rows;
        boolean conflict;
        boolean conflictCounted;
        String conflictField;
        String firstValue;
        String laterValue;
        InternalRow rowAtMaxOutputTs;
        long maxOutputTs = Long.MIN_VALUE;
        /** Candidate rows of a conflicting key (bounded; hash computed on conflict only). */
        final List<Candidate> candidates = new ArrayList<>();
        boolean candidatesTruncated;
        InternalRow approvedRow;
        boolean approvalStale;
        private boolean resolved;

        KeyAgg() {
            this(null);
        }

        KeyAgg(AcceptEntry accept) {
            this.accept = accept;
        }

        void add(InternalRow row) {
            if (rows == 0) {
                rowAtMaxOutputTs = row;
                maxOutputTs = row.getLong(CandleTableColumns.OUTPUT_TS);
            } else {
                if (!conflict) {
                    Integer index = businessConflictIndex(rowAtMaxOutputTs, row);
                    if (index != null) {
                        conflict = true;
                        conflictField = CandleTableColumns.NAMES[index];
                        firstValue = businessValue(rowAtMaxOutputTs, index);
                        laterValue = businessValue(row, index);
                        recordCandidate(rowAtMaxOutputTs);
                        recordCandidate(row);
                    }
                } else {
                    recordCandidate(row);
                }
                if (row.getLong(CandleTableColumns.OUTPUT_TS) > maxOutputTs) {
                    maxOutputTs = row.getLong(CandleTableColumns.OUTPUT_TS);
                    rowAtMaxOutputTs = row;
                }
            }
            rows++;
        }

        /**
         * Matches candidates against the approval hash. Returns true iff the
         * approval is stale (no candidate matches). Idempotent.
         */
        boolean resolveApproval() {
            if (resolved) {
                return approvalStale;
            }
            resolved = true;
            if (!conflict || accept == null) {
                return false;
            }
            for (Candidate c : candidates) {
                if (c.hash.equals(accept.rowHash)) {
                    approvedRow = c.row;
                    return false;
                }
            }
            approvalStale = true;
            return true;
        }

        private void recordCandidate(InternalRow row) {
            if (candidates.size() < MAX_CONFLICT_CANDIDATES) {
                candidates.add(new Candidate(rowHash(row), row.getLong(CandleTableColumns.OUTPUT_TS),
                        row.getString(CandleTableColumns.ALGORITHM_VERSION).toString(),
                        row.getString(CandleTableColumns.CONFIGURATION_VERSION).toString(), row));
            } else {
                candidatesTruncated = true;
            }
        }

        /** @return the index of the first differing business field, or null if equal. */
        private static Integer businessConflictIndex(InternalRow a, InternalRow b) {
            for (int i = 0; i < CandleTableColumns.FIELD_COUNT; i++) {
                if (i == CandleTableColumns.OUTPUT_TS) {
                    continue; // emit metadata, not row identity (CanonicalCandlePolicy)
                }
                if (!businessValue(a, i).equals(businessValue(b, i))) {
                    return i;
                }
            }
            return null;
        }

        private static String businessValue(InternalRow row, int index) {
            switch (index) {
                case CandleTableColumns.TICK_COUNT:
                    return Integer.toString(row.getInt(index));
                case CandleTableColumns.EXCHANGE:
                case CandleTableColumns.SYMBOL:
                case CandleTableColumns.ALGORITHM_VERSION:
                case CandleTableColumns.CONFIGURATION_VERSION:
                case CandleTableColumns.SCHEMA_VERSION:
                    return row.getString(index).toString();
                default:
                    return Long.toString(row.getLong(index));
            }
        }
    }

    /** One candidate row of a conflicting key (hash + identity + row ref). */
    record Candidate(String hash, long outputTs, String algorithm, String configuration,
                     InternalRow row) {}

    public static void main(String[] args) throws Exception {
        String mode = args.length > 0 ? args[0] : null;
        if (!"audit".equals(mode) && !"load".equals(mode)) {
            System.err.println("usage: CandleMigrationTool audit|load");
            System.err.println("  audit  — dry-run B8.2/P3: count/filter/report, abort on conflicts");
            System.err.println("  load   — B8.3/P3: audit, then upsert one approved row per canonical key");
            System.exit(1);
            return;
        }

        Config cfg = Config.fromEnv();
        Map<String, AcceptEntry> accepted = loadAcceptKeys(cfg.acceptKeysFile);
        long startNanos = System.nanoTime();
        long peakHeapMb = 0;
        long seenKeyTotal = 0;
        long loadedTotal = 0;
        long conflictingTotal = 0;
        long unacceptedTotal = 0;
        long staleTotal = 0;
        long notFoundTotal = 0;
        List<String> conflictExamples = new ArrayList<>();
        List<String> conflictRecords = new ArrayList<>();
        Set<String> seenAcceptedKeys = new HashSet<>();
        try (Connection connection = connect(cfg)) {
            Admin admin = connection.getAdmin();
            TableInfo sourceInfo = tableInfo(admin, cfg.sourceTable, "source", cfg.bootstrap);
            tableInfo(admin, cfg.destTable, "destination", cfg.bootstrap);

            LOG.info("candle-migration: mode={} source={} dest={} filter(schema_version={}, "
                            + "algorithm_version={}, configuration_version={}) maxKeys={} "
                            + "unionRead={} acceptKeysFile={}",
                    mode, cfg.sourceTable, cfg.destTable, cfg.schemaVersion,
                    cfg.algorithmVersion, cfg.configurationVersion, cfg.maxKeys,
                    cfg.unionRead, cfg.acceptKeysFile);
            System.out.println("CANDLE_MIGRATION_MODE=" + mode);
            System.out.println("CANDLE_MIGRATION_SOURCE=" + cfg.sourceTable);
            System.out.println("CANDLE_MIGRATION_FILTER=schema_version=" + cfg.schemaVersion
                    + ",algorithm_version=" + cfg.algorithmVersion
                    + ",configuration_version=" + cfg.configurationVersion);
            System.out.println("CANDLE_MIGRATION_MAX_KEYS=" + cfg.maxKeys);
            if (!accepted.isEmpty()) {
                // P3.1: an approval file is a DEV_EXCEPTION, not clean production evidence.
                System.out.println("CANDLE_MIGRATION_DEV_EXCEPTION=1");
                System.out.println("CANDLE_MIGRATION_ACCEPT_KEYS_FILE=" + cfg.acceptKeysFile);
                System.out.println("CANDLE_MIGRATION_ACCEPT_KEYS=" + accepted.size());
                LOG.warn("candle-migration: approval file supplied ({} entries) — this load is a "
                        + "DEV_EXCEPTION (2026-08-10 replay-incident keys), not clean production "
                        + "evidence (tracker 14 P3.1)", accepted.size());
            }

            // P3.2: union-read gate — the BatchScanner alone is not complete-history proof.
            String lakeFormat = null;
            if (sourceInfo.getProperties() != null
                    && sourceInfo.getProperties().containsKey("table.datalake.format")) {
                lakeFormat = sourceInfo.getProperties().toMap().get("table.datalake.format");
            }
            System.out.println("CANDLE_MIGRATION_UNION_READ=" + (cfg.unionRead ? "ENABLED" : "DISABLED"));
            if (cfg.unionRead) {
                if (lakeFormat == null || lakeFormat.isBlank()) {
                    System.out.println("CANDLE_MIGRATION_STATUS=ERROR");
                    LOG.error("candle-migration: CANDLE_MIGRATION_UNION_READ=true but source {} is "
                            + "not lake-enabled (no table.datalake.format property) — a plain scan "
                            + "cannot claim complete history (tracker 14 P3.2)", cfg.sourceTable);
                    System.exit(1);
                    return;
                }
                System.out.println("CANDLE_MIGRATION_LAKE_FORMAT=" + lakeFormat);
            } else {
                System.out.println("CANDLE_MIGRATION_LAKE_COVERAGE=LOCAL_LOG_ONLY");
                LOG.warn("candle-migration: union-read gate disabled — coverage is the local Fluss "
                        + "log only, NOT complete Iceberg-tiered history (tracker 14 P3.2)");
            }
            System.out.println("CANDLE_MIGRATION_READ_TS=" + Instant.now());
            System.out.println("CANDLE_MIGRATION_BUCKETS=" + sourceInfo.getNumBuckets());
            System.out.println("CANDLE_MIGRATION_CUTOVER=single-scan boundary; writers must be "
                    + "stopped for the migration window");

            Table source = connection.getTable(TablePath.of("default", cfg.sourceTable));
            for (int b = 0; b < sourceInfo.getNumBuckets(); b++) {
                long bucketStart = System.nanoTime();
                Audit bucketAudit = scanBucket(source, sourceInfo, b, cfg, accepted);
                bucketAudit.resolveAll();
                long usedHeapMb = usedHeapMb();
                peakHeapMb = Math.max(peakHeapMb, usedHeapMb);
                seenKeyTotal += bucketAudit.distinctKeys();
                conflictingTotal += bucketAudit.conflictingKeys;
                unacceptedTotal += bucketAudit.unacceptedConflictingKeys;
                staleTotal += bucketAudit.approvalStaleKeys;
                conflictExamples.addAll(bucketAudit.conflictExamples);
                conflictRecords.addAll(bucketAudit.conflictRecords());
                for (String approvalRecord : bucketAudit.approvalRecords()) {
                    System.out.println("CANDLE_MIGRATION_APPROVAL_RECORD=" + approvalRecord);
                }
                for (String seen : bucketAudit.seenKeys()) {
                    if (accepted.containsKey(seen)) {
                        seenAcceptedKeys.add(seen);
                    }
                }
                System.out.println("CANDLE_MIGRATION_BUCKET=" + b
                        + " rows=" + bucketAudit.totalRows
                        + " canonical=" + bucketAudit.canonicalRows
                        + " nonCanonical=" + bucketAudit.nonCanonicalRows
                        + " distinctKeys=" + bucketAudit.distinctKeys()
                        + " conflicts=" + bucketAudit.conflictingKeys
                        + " heapMB=" + usedHeapMb
                        + " durationMs=" + Duration.ofNanos(System.nanoTime() - bucketStart).toMillis());

                if (bucketAudit.unacceptedConflictingKeys > 0) {
                    System.out.println("CANDLE_MIGRATION_STATUS=CONFLICT");
                    LOG.error("candle-migration: bucket {}: {} conflicting business values not "
                                    + "covered by an approval — aborting (B8.2/P3.1)",
                            b, bucketAudit.unacceptedConflictingKeys);
                    System.exit(2);
                    return;
                }
                if (bucketAudit.approvalStaleKeys > 0) {
                    System.out.println("CANDLE_MIGRATION_STATUS=ERROR");
                    LOG.error("candle-migration: bucket {}: {} approval hashes match no candidate "
                                    + "row — wrong/stale approval, aborting (tracker 14 P3.1)",
                            b, bucketAudit.approvalStaleKeys);
                    System.exit(1);
                    return;
                }

                if ("load".equals(mode) && bucketAudit.distinctKeys() > 0) {
                    loadedTotal += loadBucket(connection, cfg, bucketAudit);
                }
                bucketAudit.byKey.clear(); // per-bucket bound: free before the next bucket
            }

            notFoundTotal = accepted.isEmpty() ? 0
                    : accepted.size() - seenAcceptedKeys.size();
            if (notFoundTotal > 0) {
                System.out.println("CANDLE_MIGRATION_STATUS=ERROR");
                LOG.error("candle-migration: {} approval keys not found among canonical LOG "
                                + "keys — aborting (typo or stale approval list)",
                        notFoundTotal);
                System.exit(1);
                return;
            }

            printSummary(cfg, mode, seenKeyTotal, conflictingTotal, unacceptedTotal,
                    staleTotal, notFoundTotal, conflictExamples, conflictRecords,
                    loadedTotal, peakHeapMb, Duration.ofNanos(System.nanoTime() - startNanos));
            if ("load".equals(mode)) {
                long destRows = countRows(connection, cfg.destTable, cfg.bootstrap);
                System.out.println("CANDLE_MIGRATION_DEST_ROWS_AFTER=" + destRows);
                LOG.info("candle-migration: load complete — {} approved keys loaded, "
                                + "destination {} now holds {} rows",
                        loadedTotal, cfg.destTable, destRows);
            }
            System.out.println("CANDLE_MIGRATION_STATUS=OK");
        }
    }

    // ── scan / aggregate / load ───────────────────────────────────────────

    private static Audit scanBucket(Table source, TableInfo sourceInfo, int bucket,
                                    Config cfg, Map<String, AcceptEntry> accepted)
            throws Exception {
        Audit audit = new Audit(cfg.schemaVersion, cfg.algorithmVersion, cfg.configurationVersion,
                accepted, cfg.maxKeys);
        TableBucket tb = new TableBucket(sourceInfo.getTableId(), bucket);
        try (BatchScanner scanner = source.newScan().limit(SCAN_LIMIT).createBatchScanner(tb);
             CloseableIterator<InternalRow> it =
                     scanner.pollBatch(Duration.ofMillis(30_000))) {
            while (it.hasNext()) {
                audit.add(it.next());
            }
        }
        return audit;
    }

    private static void printSummary(Config cfg, String mode,
                                     long seenKeys, long conflicts, long unaccepted,
                                     long stale, long notFound, List<String> conflictExamples,
                                     List<String> conflictRecords, long loaded, long peakHeapMb,
                                     Duration duration) {
        System.out.println("CANDLE_MIGRATION_DISTINCT_KEYS=" + seenKeys);
        System.out.println("CANDLE_MIGRATION_CONFLICTING_KEYS=" + conflicts);
        System.out.println("CANDLE_MIGRATION_ACCEPTED_KEYS=" + (conflicts - unaccepted - stale));
        System.out.println("CANDLE_MIGRATION_UNACCEPTED_KEYS=" + unaccepted);
        System.out.println("CANDLE_MIGRATION_STALE_APPROVALS=" + stale);
        System.out.println("CANDLE_MIGRATION_ACCEPT_KEYS_NOT_FOUND=" + notFound);
        for (String example : conflictExamples) {
            System.out.println("CANDLE_MIGRATION_CONFLICT_EXAMPLE=" + example);
        }
        for (String record : conflictRecords) {
            System.out.println("CANDLE_MIGRATION_CONFLICT_RECORD=" + record);
        }
        System.out.println("CANDLE_MIGRATION_PEAK_HEAP_MB=" + peakHeapMb);
        System.out.println("CANDLE_MIGRATION_DURATION_MS=" + duration.toMillis());
        if ("load".equals(mode)) {
            System.out.println("CANDLE_MIGRATION_LOADED=" + loaded);
        }
        LOG.info("candle-migration: {} — distinctKeys={} conflicts={} unaccepted={} stale={} "
                        + "notFound={} loaded={} peakHeapMB={} durationMs={}",
                mode, seenKeys, conflicts, unaccepted, stale, notFound, loaded, peakHeapMb,
                duration.toMillis());
    }

    /**
     * Loads the operator-approved approval file (P3.1): one
     * {@code token,windowStart,&lt;sha256&gt;,APPROVE} per line; blank lines
     * and {@code #} comments allowed. Legacy 2-field accept lines are
     * rejected fail-closed. A missing/blank env value yields an empty map
     * (pre-decision behavior); an unreadable file or a malformed line fails
     * closed with {@link IllegalArgumentException}.
     */
    static Map<String, AcceptEntry> loadAcceptKeys(String path) {
        Map<String, AcceptEntry> entries = new HashMap<>();
        if (path == null || path.isBlank()) {
            return entries;
        }
        List<String> lines;
        try {
            lines = Files.readAllLines(Path.of(path), StandardCharsets.UTF_8);
        } catch (Exception e) {
            throw new IllegalArgumentException(
                    "candle-migration: cannot read approval file " + path, e);
        }
        for (int i = 0; i < lines.size(); i++) {
            String line = lines.get(i).trim();
            if (line.isEmpty() || line.startsWith("#")) {
                continue;
            }
            AcceptEntry entry = AcceptEntry.parse(line, i + 1, path);
            entries.put(entry.token() + ":" + entry.windowStart(), entry);
        }
        return entries;
    }

    private static long loadBucket(Connection connection, Config cfg, Audit audit) throws Exception {
        Table dest = connection.getTable(TablePath.of("default", cfg.destTable));
        UpsertWriter writer = dest.newUpsert().createWriter();
        long loaded = 0;
        try {
            for (Map<Long, KeyAgg> windows : audit.byKey.values()) {
                for (KeyAgg agg : windows.values()) {
                    // P3.1: the approved row is the hash-matched candidate (conflict) or the
                    // MAX(output_ts) row (conflict-free — all business values identical).
                    InternalRow row = audit.approvedRow(agg);
                    if (row == null) {
                        continue; // gated earlier; defensive
                    }
                    writer.upsert(copyRow(row));
                    loaded++;
                }
            }
        } finally {
            writer.flush();
        }
        return loaded;
    }

    /** Rebuild a plain GenericRow from the scan row (typed getters — safe for
     *  GenericRow and Arrow-backed scan rows alike). */
    private static GenericRow copyRow(InternalRow row) {
        return GenericRow.of(
                row.getLong(CandleTableColumns.INSTRUMENT_TOKEN),
                BinaryString.fromString(row.getString(CandleTableColumns.EXCHANGE).toString()),
                BinaryString.fromString(row.getString(CandleTableColumns.SYMBOL).toString()),
                row.getLong(CandleTableColumns.WINDOW_START),
                row.getLong(CandleTableColumns.WINDOW_END),
                row.getLong(CandleTableColumns.OPEN_PAISE),
                row.getLong(CandleTableColumns.HIGH_PAISE),
                row.getLong(CandleTableColumns.LOW_PAISE),
                row.getLong(CandleTableColumns.CLOSE_PAISE),
                row.getLong(CandleTableColumns.VOLUME),
                row.getInt(CandleTableColumns.TICK_COUNT),
                BinaryString.fromString(row.getString(CandleTableColumns.ALGORITHM_VERSION).toString()),
                BinaryString.fromString(row.getString(CandleTableColumns.CONFIGURATION_VERSION).toString()),
                row.getLong(CandleTableColumns.OUTPUT_TS),
                BinaryString.fromString(row.getString(CandleTableColumns.SCHEMA_VERSION).toString()));
    }

    private static long countRows(Connection connection, String tableName, String bootstrap)
            throws Exception {
        TableInfo info = tableInfo(connection.getAdmin(), tableName, "count", bootstrap);
        Table table = connection.getTable(TablePath.of("default", tableName));
        long count = 0;
        for (int b = 0; b < info.getNumBuckets(); b++) {
            TableBucket tb = new TableBucket(info.getTableId(), b);
            try (BatchScanner scanner = table.newScan().limit(SCAN_LIMIT).createBatchScanner(tb);
                 CloseableIterator<InternalRow> it =
                         scanner.pollBatch(Duration.ofMillis(30_000))) {
                while (it.hasNext()) {
                    it.next();
                    count++;
                }
            }
        }
        return count;
    }

    private static long usedHeapMb() {
        Runtime r = Runtime.getRuntime();
        return (r.totalMemory() - r.freeMemory()) / (1024L * 1024L);
    }

    // ── plumbing ───────────────────────────────────────────────────────────

    private static Connection connect(Config cfg) throws Exception {
        Configuration conf = new Configuration();
        conf.setString("bootstrap.servers", cfg.bootstrap);
        Connection connection = ConnectionFactory.createConnection(conf);
        LOG.info("candle-migration: connected to {}", cfg.bootstrap);
        return connection;
    }

    private static TableInfo tableInfo(Admin admin, String tableName, String role, String bootstrap)
            throws Exception {
        try {
            TableInfo info = admin.getTableInfo(TablePath.of("default", tableName))
                    .get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            LOG.info("candle-migration: {} table {} (id={}, buckets={}, pk={}, bucketKeys={})",
                    role, tableName, info.getTableId(), info.getNumBuckets(),
                    info.getPrimaryKeys(), info.getBucketKeys());
            return info;
        } catch (Exception e) {
            throw new IllegalStateException("candle-migration: " + role + " table "
                    + tableName + " not available at " + bootstrap, e);
        }
    }
}
