package com.trading.ingestion;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.trading.ingestion.config.IngestionConfig;
import com.trading.ingestion.discontinuity.DiscontinuitySink;
import com.trading.ingestion.discontinuity.DiscontinuityWriter;
import com.trading.ingestion.health.NtpClockChecker;
import com.trading.ingestion.model.Instrument;
import com.trading.ingestion.model.RawTick;
import com.trading.ingestion.model.TickPacket;
import com.trading.ingestion.quarantine.QuarantineSink;
import com.trading.ingestion.quarantine.QuarantineWriter;
import com.trading.ingestion.safety.SafetyHaltWriter;
import com.trading.ingestion.safety.SafetySink;
import com.trading.ingestion.write.FlussRowConverter;
import com.trading.ingestion.write.RawTickWriter;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * ING-DQ-011: seeded property-based fuzz of the tick parser.
 *
 * <p>A plain JDK {@link Random} with a fixed seed (no external property
 * library, per the hardening plan) generates a mixed adversarial corpus —
 * well-formed ticks, schema violations, malformed JSON, non-JSON garbage,
 * oversized lines, control characters, duplicate keys, wrong-typed fields —
 * and every line is driven through the REAL {@code IngestionService}
 * pipeline via the no-op-sink test seam (ING-DQ-010). The properties asserted
 * per run:
 *
 * <pre>{@code
 *   nothing propagates out of processLine     (reflection InvocationTargetException fails the test)
 *   frameCount == linesFed                    (every line entered the pipeline)
 *   appends + quarantineWrites == linesFed    (no silent drop: every line maps to exactly one evidence outcome)
 *   INTERNAL_ERROR writes == errorCount       (evidence and the error counter agree)
 *   tracker drains to zero                    (no leaked reservations)
 * }</pre>
 *
 * <p><b>Determinism:</b> the corpus regenerates byte-identically from the
 * fixed seed, so a failing seed is a reproducible regression pin. The three
 * seeds below are pinned in the run — a fourth seed that exposes a new parser
 * failure is itself a bug report, not a test change.
 */
@DisplayName("ING-DQ-011: seeded property-based fuzz — no crash, no silent drop, reproducible")
class FuzzIngestionTest {

    private static final long TOKEN_A = 100_000L;
    private static final long TOKEN_B = 100_100L;

    /** Pinned seeds — a corpus from any of these must satisfy every property. */
    private static final long[] SEEDS = {0x0BAD_F00DL, 0xDEAD_BEEFL, 0x5EED_CAFEL};

    /** Fixed epoch for all generated timestamps — keeps the corpus fully deterministic. */
    private static final long NOW_MS = 1_800_000_000_000L;

    /** Freshness window from the test config: [now - 5000, now + 2000]. */
    private static final long STALE_OFFSET_MS = 60_000L;

    @Test
    @DisplayName("fuzz corpus: no uncaught exception, no silent drop, ledger reconciles, reproducible")
    void fuzzCorpusNeverCrashesOrDrops() throws Exception {
        // Re-runnability pin: the same seed must regenerate the identical corpus.
        List<FuzzLine> corpus = generateCorpus(SEEDS[0], NOW_MS);
        assertEquals(corpus, generateCorpus(SEEDS[0], NOW_MS),
                "fixed seed must regenerate the identical corpus");

        IngestionConfig config = buildConfig();
        CountingConverter converter = new CountingConverter();
        CountingQuarantine quarantine = new CountingQuarantine();
        NtpClockChecker clock = new NtpClockChecker("127.0.0.1:9", 100, false);
        IngestionService service = new IngestionService(
                "ing-dq-011", instruments(), converter, config, clock,
                quarantine, new CountingDiscontinuity(), new CountingSafety());

        Method processLine = IngestionService.class.getDeclaredMethod("processLine", String.class);
        processLine.setAccessible(true);

        long totalLines = 0;
        for (long seed : SEEDS) {
            List<FuzzLine> batch = generateCorpus(seed, NOW_MS);
            totalLines += batch.size();
            for (FuzzLine line : batch) {
                processLine.invoke(service, line.line());
                // an escaping exception surfaces as InvocationTargetException → test fails
            }
        }

        long appends = converter.appendCalls.get();
        long quarantineWrites = quarantine.writes.get();
        assertEquals(totalLines, frameCount(service), "every fed line must be processed");
        assertEquals(totalLines, appends + quarantineWrites,
                "no silent drop: appends + quarantines must reconcile exactly with lines fed "
                        + "(appends=" + appends + ", quarantines=" + quarantineWrites + ")");
        assertEquals(quarantine.internalErrors.get(), errorCount(service),
                "INTERNAL_ERROR quarantine evidence must match the error counter — an untyped "
                        + "crash path means the parser did not classify the input");
        assertEquals(0, service.tracker().pendingRecords(), "no leaked record reservations");
        assertEquals(0, service.tracker().pendingBytes(), "no leaked byte reservations");
        assertTrue(converter.packets.stream().allMatch(p -> p.eventFingerprint() != null),
                "every appended packet carries a fingerprint");
        assertTrue(converter.packets.stream()
                        .allMatch(p -> p.raw() != null && p.raw().rawPayload() != null),
                "every appended packet preserves the raw payload bytes");
    }

    @Test
    @DisplayName("guaranteed-valid ticks always append — fuzz mutations never break the happy path")
    void validTicksAlwaysAppend() throws Exception {
        IngestionConfig config = buildConfig();
        CountingConverter converter = new CountingConverter();
        CountingQuarantine quarantine = new CountingQuarantine();
        NtpClockChecker clock = new NtpClockChecker("127.0.0.1:9", 100, false);
        IngestionService service = new IngestionService(
                "ing-dq-011-valid", instruments(), converter, config, clock,
                quarantine, new CountingDiscontinuity(), new CountingSafety());

        Method processLine = IngestionService.class.getDeclaredMethod("processLine", String.class);
        processLine.setAccessible(true);

        List<Long> expectedTokens = new ArrayList<>();
        Random rnd = new Random(0x5EED); // deterministic fixture set
        for (int i = 0; i < 200; i++) {
            String mode = MODES[rnd.nextInt(MODES.length)];
            long token = rnd.nextBoolean() ? TOKEN_A : TOKEN_B;
            long ltp = mode.equals("quote") ? 0 : 1 + rnd.nextInt(500_000);
            long tsMs = NOW_MS + rnd.nextInt(4001) - 2000; // strictly inside the freshness window
            expectedTokens.add(token);
            processLine.invoke(service, validTick(mode, token, ltp, tsMs));
        }

        assertEquals(200, converter.appendCalls.get(),
                "every guaranteed-valid tick must append — quarantine writes=" + quarantine.writes.get());
        assertEquals(0, quarantine.writes.get(), "no valid tick may be quarantined");
        assertEquals(expectedTokens, converter.packets.stream().map(TickPacket::instrumentToken).toList(),
                "appended tokens must match the fed order exactly");
        assertEquals(0, errorCount(service));
    }

    @Test
    @DisplayName("null-node lines are consumed without evidence — the reject bucket")
    void rejectBucketConsumedWithoutEvidence() throws Exception {
        IngestionConfig config = buildConfig();
        CountingConverter converter = new CountingConverter();
        CountingQuarantine quarantine = new CountingQuarantine();
        NtpClockChecker clock = new NtpClockChecker("127.0.0.1:9", 100, false);
        IngestionService service = new IngestionService(
                "ing-dq-011-reject", instruments(), converter, config, clock,
                quarantine, new CountingDiscontinuity(), new CountingSafety());

        Method processLine = IngestionService.class.getDeclaredMethod("processLine", String.class);
        processLine.setAccessible(true);

        List<String> rejects = List.of("", "   ", "null");
        for (String line : rejects) {
            processLine.invoke(service, line);
        }
        assertEquals(rejects.size(), frameCount(service), "reject lines still enter the pipeline");
        assertEquals(0, converter.appendCalls.get(), "no reject line may become a tick");
        assertEquals(0, quarantine.writes.get(), "no reject line may produce quarantine evidence");
        assertEquals(0, errorCount(service), "no reject line may hit the error path");
    }

    // ---- corpus generation (deterministic from seed) ----

    /** One generated line; the pipeline decides its outcome (no per-line prediction). */
    private record FuzzLine(String line) {}

    private static List<FuzzLine> generateCorpus(long seed, long now) {
        Random rnd = new Random(seed);
        List<FuzzLine> lines = new ArrayList<>();
        for (int i = 0; i < 800; i++) {
            lines.add(switch (rnd.nextInt(10)) {
                case 0, 1, 2, 3, 4, 5 -> mutatedTick(rnd, now);
                case 6, 7 -> malformedJson(rnd, now);
                case 8 -> garbageText(rnd);
                default -> schemaViolation(rnd, now);
            });
        }
        // Pinned adversarial edges (outside the random draw so they always run).
        lines.add(new FuzzLine("{}"));                                  // empty object
        lines.add(new FuzzLine("{\"feed\":"));                          // truncated JSON
        lines.add(new FuzzLine("{\"feed\":\"hft\",\"mode\":\"ltp\",}")); // trailing comma
        lines.add(new FuzzLine("12345"));                               // JSON scalar
        lines.add(new FuzzLine("[]"));                                  // JSON array
        lines.add(new FuzzLine("\"hello\""));                           // JSON string
        lines.add(new FuzzLine("x".repeat(1_000_000)));                 // 1 MB non-JSON
        lines.add(new FuzzLine(deepNest(120)));                         // deep nesting
        lines.add(new FuzzLine(deepNest(600)));                         // very deep nesting
        lines.add(new FuzzLine("{\"feed\":\"hft\",\"mode\":\"ltp\"} trailing garbage")); // trailing tokens
        return lines;
    }

    private static final String[] MODES = {"ltp", "ltpc", "full", "quote"};
    private static final String[] FEED_JUNK = {"standard", "HFT", "hft ", "hft\u0000x", "", "\u20B9hft"};

    private static FuzzLine mutatedTick(Random rnd, long now) {
        StringBuilder j = new StringBuilder("{");
        boolean first = true;
        first = put(rnd, j, first, "record_type", q("tick"), 0.95);
        first = put(rnd, j, first, "feed", feedValue(rnd), 0.95);
        first = put(rnd, j, first, "mode", modeValue(rnd), 0.95);
        first = put(rnd, j, first, "token", tokenValue(rnd), 0.95);
        first = put(rnd, j, first, "ltp_paise", ltpValue(rnd), 0.95);
        first = put(rnd, j, first, "ts_ms", tsValue(rnd, now), 0.98);
        first = put(rnd, j, first, "received_ts_ms", Long.toString(now), 0.90);
        PayloadPair pp = payloadPair(rnd);
        // Base64 payload and hex hash MUST be quoted JSON strings (the bridge
        // emits them quoted); unquoted, the whole line fails JSON parsing.
        first = put(rnd, j, first, "raw_payload", q(pp.raw()), 0.92);
        first = put(rnd, j, first, "payload_hash", q(pp.hash()), 0.92);
        if (rnd.nextInt(10) < 4) first = put(rnd, j, first, "connection_epoch", Integer.toString(rnd.nextInt(1000)), 1.0);
        if (rnd.nextInt(10) < 4) first = put(rnd, j, first, "slot_id", q("hft-" + rnd.nextInt(4)), 1.0);
        if (rnd.nextInt(10) < 6) first = put(rnd, j, first, "bid_px", "[" + rnd.nextInt(10_000) + "," + rnd.nextInt(10_000) + "]", 1.0);
        if (rnd.nextInt(10) < 6) first = put(rnd, j, first, "ask_px", "[" + rnd.nextInt(10_000) + "," + rnd.nextInt(10_000) + "]", 1.0);
        if (rnd.nextInt(10) < 3) first = put(rnd, j, first, "volume", Integer.toString(rnd.nextInt(1_000_000)), 1.0);
        if (rnd.nextInt(10) < 3) first = put(rnd, j, first, "junk_" + rnd.nextInt(50), q(randomStr(rnd, 6)), 1.0);
        j.append('}');
        return new FuzzLine(j.toString());
    }

    /** A well-formed tick that MUST append (per ING-DQ-011 validTicksAlwaysAppend). */
    private static String validTick(String mode, long token, long ltpPaise, long tsMs) {
        byte[] payload = ("fuzz-payload-" + token + "-" + mode).getBytes(StandardCharsets.UTF_8);
        return "{\"record_type\":\"tick\",\"feed\":\"hft\",\"mode\":\"" + mode + "\",\"token\":" + token
                + ",\"ltp_paise\":" + ltpPaise + ",\"ts_ms\":" + tsMs
                + ",\"received_ts_ms\":" + NOW_MS
                + ",\"raw_payload\":\"" + Base64.getEncoder().encodeToString(payload)
                + "\",\"payload_hash\":\"" + sha256Hex(payload) + "\"}";
    }

    private static FuzzLine malformedJson(Random rnd, long now) {
        return switch (rnd.nextInt(8)) {
            case 0 -> new FuzzLine("{\"feed\":\"hft\",\"mode\":");
            case 1 -> new FuzzLine("{\"feed\":\"hft\",\"mode\":\"ltp\"");
            case 2 -> new FuzzLine("{");
            case 3 -> new FuzzLine("[1,2,3");
            case 4 -> new FuzzLine("\"unterminated");
            case 5 -> new FuzzLine("'single-quoted'");
            case 6 -> new FuzzLine("{" + "\"a\":".repeat(50) + "1" + "}".repeat(50) + " extra");
            default -> new FuzzLine("{\"feed\":\"hft\"} " + randomStr(rnd, 20));
        };
    }

    private static FuzzLine garbageText(Random rnd) {
        byte[] bytes = new byte[1 + rnd.nextInt(2000)];
        rnd.nextBytes(bytes);
        if (rnd.nextInt(10) < 4) bytes[rnd.nextInt(bytes.length)] = 0; // NUL byte
        return new FuzzLine(new String(bytes, StandardCharsets.UTF_8));
    }

    private static FuzzLine schemaViolation(Random rnd, long now) {
        byte[] payload = ("schema-" + rnd.nextInt(1_000_000)).getBytes(StandardCharsets.UTF_8);
        String valid = "{\"record_type\":\"tick\",\"feed\":\"hft\",\"mode\":\"ltp\",\"token\":" + TOKEN_A
                + ",\"ltp_paise\":100,\"ts_ms\":" + now + ",\"received_ts_ms\":" + now
                + ",\"raw_payload\":\"" + Base64.getEncoder().encodeToString(payload)
                + "\",\"payload_hash\":\"" + sha256Hex(payload) + "\"}";
        return switch (rnd.nextInt(8)) {
            // String token — Jackson coerces "100000" → long: still a valid tick (append).
            case 0 -> new FuzzLine(valid.replace("\"token\":" + TOKEN_A, "\"token\":\"100000\""));
            case 1 -> new FuzzLine(valid.replace("\"feed\":\"hft\"", "\"feed\":{}"));       // object feed
            case 2 -> new FuzzLine(valid.replace("\"mode\":\"ltp\"", "\"mode\":{}"));       // object mode
            case 3 -> new FuzzLine(valid.replace("\"ts_ms\":" + now, "\"ts_ms\":{\"x\":1}")); // object ts
            case 4 -> new FuzzLine(valid.replace("\"bid_px\"", "\"bid_px\":\"nope\""));     // string for array
            case 5 -> new FuzzLine("{\"feed\":\"hft\",\"feed\":\"standard\",\"mode\":\"ltp\"}"); // duplicate key
            case 6 -> new FuzzLine(valid.replace("\"ts_ms\":" + now, "\"ts_ms\":" + (now - 2 * STALE_OFFSET_MS))); // stale
            default -> new FuzzLine(valid.replace("\"raw_payload\":\"" + Base64.getEncoder().encodeToString(payload)
                    + "\"", "\"raw_payload\":{\"nested\":true}"));                            // object payload
        };
    }

    // ---- generation helpers ----

    /** Appends {@code "key":value} unless the keep-draw fails (field omitted); returns whether a field was written. */
    private static boolean put(Random rnd, StringBuilder j, boolean first, String key, String value, double keepProb) {
        if (rnd.nextDouble() > keepProb) return first;
        if (!first) j.append(',');
        j.append('"').append(key).append("\":").append(value);
        return false;
    }

    private static String q(String s) {
        return "\"" + s + "\"";
    }

    private static String feedValue(Random rnd) {
        return switch (rnd.nextInt(10)) {
            case 0, 1, 2, 3, 4, 5, 6, 7 -> q("hft");
            case 8 -> q(FEED_JUNK[rnd.nextInt(FEED_JUNK.length)]);
            default -> rnd.nextBoolean() ? "123" : "null";
        };
    }

    private static String modeValue(Random rnd) {
        return switch (rnd.nextInt(10)) {
            case 0, 1, 2, 3, 4, 5, 6 -> q(MODES[rnd.nextInt(MODES.length)]);
            case 7 -> q(randomStr(rnd, 6));
            case 8 -> "null";
            default -> "123";
        };
    }

    private static String tokenValue(Random rnd) {
        return switch (rnd.nextInt(10)) {
            case 0, 1, 2, 3, 4, 5, 6 -> Long.toString(rnd.nextBoolean() ? TOKEN_A : TOKEN_B);
            case 7 -> "999999";
            case 8 -> rnd.nextBoolean() ? "0" : "-1";
            default -> "99999999999999999999"; // beyond long → Jackson mapping error → MALFORMED_JSON
        };
    }

    private static String ltpValue(Random rnd) {
        return switch (rnd.nextInt(10)) {
            case 0, 1, 2, 3, 4, 5, 6, 7 -> Integer.toString(1 + rnd.nextInt(500_000));
            case 8 -> "0";
            default -> rnd.nextBoolean() ? "-100" : "999999999999999";
        };
    }

    private static String tsValue(Random rnd, long now) {
        return switch (rnd.nextInt(10)) {
            case 0, 1, 2, 3, 4, 5, 6 -> Long.toString(now + rnd.nextInt(4001) - 3000); // [-3000, +1000] fresh
            case 7 -> Long.toString(now - STALE_OFFSET_MS);                            // stale
            case 8 -> Long.toString(now + STALE_OFFSET_MS);                            // future
            default -> rnd.nextBoolean() ? "0" : "-1";
        };
    }

    private record PayloadPair(String raw, String hash) {}

    private static PayloadPair payloadPair(Random rnd) {
        byte[] bytes = new byte[8 + rnd.nextInt(57)];
        rnd.nextBytes(bytes);
        return switch (rnd.nextInt(10)) {
            case 0, 1, 2, 3, 4, 5, 6 -> new PayloadPair(
                    Base64.getEncoder().encodeToString(bytes), sha256Hex(bytes));        // valid pair
            case 7 -> new PayloadPair(
                    Base64.getEncoder().encodeToString(bytes), sha256Hex(new byte[] {bytes[0]})); // hash mismatch
            case 8 -> new PayloadPair("!!not-base64!!", sha256Hex(bytes));               // malformed payload
            default -> new PayloadPair(Base64.getEncoder().encodeToString(bytes), "ZZZ"); // malformed hash
        };
    }

    private static String randomStr(Random rnd, int maxLen) {
        int len = rnd.nextInt(maxLen + 1);
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < len; i++) {
            char c = switch (rnd.nextInt(6)) {
                case 0 -> (char) ('a' + rnd.nextInt(26));
                case 1 -> (char) ('A' + rnd.nextInt(26));
                case 2 -> (char) ('0' + rnd.nextInt(10));
                case 3 -> rnd.nextBoolean() ? '\u0000' : ' ';
                case 4 -> '\u20B9'; // ₹
                default -> (char) (0x4E00 + rnd.nextInt(0x3000)); // CJK
            };
            sb.append(c);
        }
        return sb.toString();
    }

    private static String deepNest(int depth) {
        return "{".repeat(depth) + "\"a\":1" + "}".repeat(depth);
    }

    // ---- fixtures and harness ----

    private static IngestionConfig buildConfig() throws Exception {
        Map<String, String> env = new HashMap<>();
        env.put("ARROW_APP_ID", "test-app");
        env.put("ARROW_APP_SECRET", "test-secret");
        env.put("ARROW_TOKEN", "test-token");
        env.put("FLUSS_BOOTSTRAP", "localhost:9123");
        env.put("RAW_TABLE_NAME", "raw_table_1");
        env.put("ARROW_MAX_EVENT_AGE_MS", "5000");
        env.put("ARROW_MAX_FUTURE_EVENT_SKEW_MS", "2000");
        Method validateFrom = IngestionConfig.class.getDeclaredMethod("validateFrom", Map.class);
        validateFrom.setAccessible(true);
        return (IngestionConfig) validateFrom.invoke(null, env);
    }

    private static List<Instrument> instruments() {
        return List.of(
                new Instrument.Builder().instrumentToken(TOKEN_A).tradingSymbol("SYM1-EQ")
                        .exchange("NSE").segment("CM").lotSize(1).manifestVersion(1).build(),
                new Instrument.Builder().instrumentToken(TOKEN_B).tradingSymbol("SYM2-EQ")
                        .exchange("NSE").segment("CM").lotSize(1).manifestVersion(1).build());
    }

    private static String sha256Hex(byte[] data) {
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            byte[] digest = md.digest(data);
            StringBuilder sb = new StringBuilder(digest.length * 2);
            for (byte b : digest) {
                sb.append(Character.forDigit((b >> 4) & 0xF, 16)).append(Character.forDigit(b & 0xF, 16));
            }
            return sb.toString();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    // ---- counting sinks (ING-DQ-010 no-op seam, but counting) ----

    static final class CountingQuarantine implements QuarantineSink {
        final AtomicLong writes = new AtomicLong();
        final AtomicLong internalErrors = new AtomicLong();

        @Override
        public void write(byte[] rawPayload, QuarantineWriter.Reason reason, String detail) {
            count(reason);
        }

        @Override
        public void write(byte[] rawPayload, QuarantineWriter.Reason reason, String detail,
                          Long instrumentToken, String exchange, String symbol) {
            count(reason);
        }

        private void count(QuarantineWriter.Reason reason) {
            writes.incrementAndGet();
            if (reason == QuarantineWriter.Reason.INTERNAL_ERROR) internalErrors.incrementAndGet();
        }

        @Override
        public void close() {
        }
    }

    static final class CountingDiscontinuity implements DiscontinuitySink {
        @Override
        public void write(DiscontinuityWriter.Reason reason, String note,
                          DiscontinuityWriter.LastTickSnapshot before) {
        }

        @Override
        public void write(DiscontinuityWriter.Reason reason, String note,
                          DiscontinuityWriter.LastTickSnapshot before,
                          Long instrumentToken, String exchange, String symbol) {
        }

        @Override
        public void writeBridgeEvent(com.trading.ingestion.bridge.BridgeEvent event,
                                     DiscontinuityWriter.LastTickSnapshot before) {
        }

        @Override
        public void close() {
        }
    }

    static final class CountingSafety implements SafetySink {
        @Override
        public String write(String slotId, long connectionEpoch, SafetyHaltWriter.SafetyState state,
                            SafetyHaltWriter.ReasonCode reasonCode, String assignedTokenHash,
                            String evidenceReference, long detectedTsMs) {
            return "fuzz-halt";
        }

        @Override
        public void close() {
        }
    }

    /** Fake converter: counts appends, completes instantly, records packets. */
    static final class CountingConverter implements FlussRowConverter {
        final AtomicInteger appendCalls = new AtomicInteger();
        final List<TickPacket> packets = new CopyOnWriteArrayList<>();

        @Override
        public CompletableFuture<RawTickWriter.AppendResult> append(TickPacket packet) {
            appendCalls.incrementAndGet();
            packets.add(packet);
            return CompletableFuture.completedFuture(new RawTickWriter.AppendResult(1, "p0"));
        }

        @Override
        public int estimatedRowSize(TickPacket packet) {
            return 256;
        }

        @Override
        public void close() {
        }
    }

    // ---- reflection accessors (private state; IngestionServiceTest uses the same pattern) ----

    private static long frameCount(IngestionService service) throws Exception {
        Field f = IngestionService.class.getDeclaredField("frameCount");
        f.setAccessible(true);
        return ((AtomicLong) f.get(service)).get();
    }

    private static long errorCount(IngestionService service) throws Exception {
        Field f = IngestionService.class.getDeclaredField("errorCount");
        f.setAccessible(true);
        return ((AtomicLong) f.get(service)).get();
    }
}
