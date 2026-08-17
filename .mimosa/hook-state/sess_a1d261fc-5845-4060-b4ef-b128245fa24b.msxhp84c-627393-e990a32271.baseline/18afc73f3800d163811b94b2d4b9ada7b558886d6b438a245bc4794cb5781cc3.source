package com.trading.compute.signaljob;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import com.trading.common.model.FormingBar;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import org.apache.fluss.client.Connection;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.types.DataTypes;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * FORMING-BAR-REHYDRATE-001 live-cluster evidence for the forming-bar KV
 * persistence phase (2026-08-16): the {@code forming_bar} current-state home
 * exercised through the PRODUCTION rehydration store
 * ({@link FlussFormingBarStateStore}) — the DEC-038 authority read/write path
 * — against a live Fluss cluster.
 *
 * <p>Proves the restart-rehydration claim end to end at the durable layer:
 * <ol>
 *   <li><b>Current-state semantics</b> — multiple updates within one window
 *       converge to the LATEST bar (last-write-wins on the single per-
 *       instrument PK); a window rollover replaces the durable row (same key,
 *       new window's bar) — never per-tick history, never append-only.</li>
 *   <li><b>Cold-restart rehydration</b> — close the store, reopen a FRESH
 *       one (simulated restart), read each instrument's latest bar back from
 *       Fluss authority: exact OHLCV/volume/tick-count/last-event state, and
 *       {@code windowEnd/exchange/symbol = null} per the v1 projection (the
 *       caller restores them from the completed-candle stream).</li>
 *   <li><b>No stale guess</b> — an unwritten instrument reads empty, never a
 *       fabricated bar.</li>
 * </ol>
 *
 * <p>Gates: {@code @Tag("integration")}, skipped unless
 * {@code COMPUTE_INT_TEST_FORMING_BAR_REHYDRATE=true}; Fluss at
 * {@code FLUSS_BOOTSTRAP} (default {@code localhost:9123}). Runs on a
 * SCRATCH {@code forming_bar}-shaped table (PK {@code instrument_token},
 * 16 buckets, {@code bucket.key = instrument_token}, {@code kv.format-version
 * = 2}) created and dropped by the test — the live platform table is never
 * touched. Run against the dev cluster:
 * {@code COMPUTE_INT_TEST_FORMING_BAR_REHYDRATE=true mvn -o -f code/02_services/02_compute/pom.xml test -Dtest=FormingBarRehydrationIntegrationTest}.
 */
@Tag("integration")
@DisplayName("FORMING-BAR-REHYDRATE-001: forming_bar KV rehydration (live Fluss)")
class FormingBarRehydrationIntegrationTest {

    private static final Logger LOG =
            LoggerFactory.getLogger(FormingBarRehydrationIntegrationTest.class);

    private static final Duration TIMEOUT = Duration.ofSeconds(30);
    private static final String SCRATCH = "forming_bar_rehydrate_it";

    /** Epoch-aligned base: 1_710_000_000_000 / 15000 = 114_000_000 exactly. */
    private static final long T0 = 1_710_000_000_000L;

    private static String bootstrap;
    private static Connection connection;
    private static Admin admin;

    @BeforeAll
    static void connect() {
        assumeTrue("true".equalsIgnoreCase(System.getenv().getOrDefault(
                        "COMPUTE_INT_TEST_FORMING_BAR_REHYDRATE", "false")),
                "Skipping — set COMPUTE_INT_TEST_FORMING_BAR_REHYDRATE=true");
        bootstrap = System.getenv().getOrDefault("FLUSS_BOOTSTRAP", "localhost:9123");
        try {
            Configuration conf = new Configuration();
            conf.setString("bootstrap.servers", bootstrap);
            connection = ConnectionFactory.createConnection(conf);
            admin = connection.getAdmin();
            createScratchTable();
            LOG.info("fb-rehydrate: connected to {}, scratch table {}", bootstrap, SCRATCH);
        } catch (Exception e) {
            LOG.warn("fb-rehydrate: cannot connect to {} — {}", bootstrap, e.getMessage());
            assumeTrue(false, "Fluss cluster not available at " + bootstrap);
        }
    }

    @AfterAll
    static void cleanup() throws Exception {
        if (admin != null) {
            try {
                admin.dropTable(TablePath.of("default", SCRATCH), false)
                        .get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
                LOG.info("fb-rehydrate: dropped scratch table {}", SCRATCH);
            } catch (Exception e) {
                LOG.warn("fb-rehydrate: drop {} failed: {}", SCRATCH, e.getMessage());
            }
            admin.close();
        }
        if (connection != null) {
            connection.close();
        }
    }

    /** The DDL-04 descriptor under a scratch name (platform table untouched). */
    private static void createScratchTable() throws Exception {
        Schema schema = Schema.newBuilder()
                .column("instrument_token", DataTypes.BIGINT())
                .column("window_start", DataTypes.BIGINT())
                .column("open_paise", DataTypes.BIGINT())
                .column("high_paise", DataTypes.BIGINT())
                .column("low_paise", DataTypes.BIGINT())
                .column("close_paise", DataTypes.BIGINT())
                .column("volume", DataTypes.BIGINT())
                .column("tick_count", DataTypes.INT())
                .column("last_event_time", DataTypes.BIGINT())
                .column("last_event_fingerprint", DataTypes.STRING())
                .column("schema_version", DataTypes.STRING())
                .primaryKey("instrument_token")
                .build();
        TableDescriptor td = TableDescriptor.builder()
                .schema(schema)
                .distributedBy(16, "instrument_token")
                .property("table.kv.format-version", "2")
                .build();
        admin.createTable(TablePath.of("default", SCRATCH), td, false)
                .get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
    }

    private static FlussFormingBarStateStore store() throws Exception {
        return FlussFormingBarStateStore.open(bootstrap, "default", SCRATCH, TIMEOUT);
    }

    private static FormingBar bar(long token, long windowStart, long close, long volume,
            long tickCount, String fingerprint) {
        return new FormingBar(token, windowStart, windowStart + 15_000L,
                close - 1, close, close - 2, close, volume, tickCount,
                windowStart + 1_000L, fingerprint, "NSE", "TEST");
    }

    @Test
    void latestWinsWithinWindowAndRolloverReplaces() throws Exception {
        try (FlussFormingBarStateStore s = store()) {
            s.put(bar(7L, T0, 100, 5, 1, "fp-1"));
            s.put(bar(7L, T0, 102, 7, 2, "fp-2"));
            s.put(bar(7L, T0, 99, 10, 3, "fp-3")); // latest of window W
            assertFormingBarEquals(bar(7L, T0, 99, 10, 3, "fp-3"),
                    s.read(7L).orElseThrow(),
                    "three updates of the same window converge to the LATEST (last-write-wins)");

            // Window rollover: the same per-instrument key now holds the new
            // window's bar — the durable row is current-state, not history.
            long t1 = T0 + 15_000L;
            s.put(bar(7L, t1, 150, 1, 1, "fp-4"));
            assertFormingBarEquals(bar(7L, t1, 150, 1, 1, "fp-4"),
                    s.read(7L).orElseThrow(),
                    "window rollover replaces the durable row on the same PK");
        }
    }

    @Test
    void coldRestartRehydratesExactLatestState() throws Exception {
        // Write through store #1 (the writer-side authority path).
        try (FlussFormingBarStateStore s1 = store()) {
            s1.put(bar(11L, T0, 111, 3, 1, "a"));
            s1.put(bar(11L, T0, 222, 6, 2, "b")); // latest for 11
            s1.put(bar(22L, T0, 333, 9, 3, null)); // nullable fingerprint
            s1.put(bar(33L, T0, 444, 12, 4, "d"));
        }
        // Simulated restart: a FRESH store/connection reads Fluss authority.
        try (FlussFormingBarStateStore s2 = store()) {
            assertFormingBarEquals(bar(11L, T0, 222, 6, 2, "b"),
                    s2.read(11L).orElseThrow(),
                    "cold restart rehydrates the exact latest bar (not the first write)");
            assertFormingBarEquals(bar(22L, T0, 333, 9, 3, null),
                    s2.read(22L).orElseThrow(),
                    "nullable fingerprint round-trips through the KV row");
            assertFormingBarEquals(bar(33L, T0, 444, 12, 4, "d"),
                    s2.read(33L).orElseThrow(),
                    "per-instrument state is exact and independent");
        }
    }

    @Test
    void missingInstrumentReturnsEmpty() throws Exception {
        try (FlussFormingBarStateStore s = store()) {
            assertTrue(s.read(999_999L).isEmpty(),
                    "no row -> empty — rehydration never fabricates a bar");
        }
    }

    /** Compares every persisted field; asserts the v1-projection non-persisted fields. */
    private static void assertFormingBarEquals(FormingBar expected, FormingBar actual,
            String msg) {
        assertEquals(expected.instrumentToken(), actual.instrumentToken(), msg + " token");
        assertEquals(expected.windowStart(), actual.windowStart(), msg + " windowStart");
        assertEquals(expected.openPaise(), actual.openPaise(), msg + " open");
        assertEquals(expected.highPaise(), actual.highPaise(), msg + " high");
        assertEquals(expected.lowPaise(), actual.lowPaise(), msg + " low");
        assertEquals(expected.closePaise(), actual.closePaise(), msg + " close");
        assertEquals(expected.volume(), actual.volume(), msg + " volume");
        assertEquals(expected.tickCount(), actual.tickCount(), msg + " tickCount");
        assertEquals(expected.lastEventTime(), actual.lastEventTime(), msg + " lastEventTime");
        assertEquals(expected.lastFingerprint(), actual.lastFingerprint(), msg + " fingerprint");
        assertEquals(0L, actual.windowEnd(), "v1 projection does not persist windowEnd");
        assertEquals(null, actual.exchange(), "v1 projection does not persist exchange");
        assertEquals(null, actual.symbol(), "v1 projection does not persist symbol");
    }
}
