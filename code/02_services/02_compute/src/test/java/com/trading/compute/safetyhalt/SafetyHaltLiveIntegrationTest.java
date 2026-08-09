package com.trading.compute.safetyhalt;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import com.trading.common.safety.SafetyHaltRequestParser;
import com.trading.common.safety.SafetyStateTracker;
import com.trading.common.safety.SlotAssignmentResolver;
import com.trading.common.safety.SlotSafetyRequest;
import com.trading.common.safety.SlotSafetyStatus;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.fluss.client.Connection;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.client.table.Table;
import org.apache.fluss.client.table.writer.UpsertResult;
import org.apache.fluss.client.table.writer.UpsertWriter;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.row.InternalRow;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * SAFETY-INT-001: live {@code Safety_Halt_Requests} consume-and-suppress
 * harness (plan.md &sect; "Slot-scoped safety propagation", Signal Job row).
 *
 * <p>Set {@code COMPUTE_INT_TEST_SAFETY=true} to run. Requires a live Fluss
 * cluster at {@code FLUSS_BOOTSTRAP_SERVERS} (default localhost:9123) with the
 * approved v3 {@code Safety_Halt_Requests} DDL applied (offline gate — never
 * created here). The test writes an {@code UNSAFE} and a {@code RECOVERED}
 * row through the same append path the ingestion writer uses, reads each row
 * back by primary key, bridges it through the production
 * {@link SafetyHaltRowDataBridge} + {@link SafetyHaltRequestParser}, and
 * asserts the tracker's slot-scoped suppression window opens on UNSAFE and
 * closes on RECOVERED.
 *
 * <p>The table is immutable evidence: rows are never deleted. The
 * connection epoch is taken from the wall clock so re-runs append distinct
 * {@code halt_request_id} tuples and never collide on the PK.
 */
@DisplayName("SAFETY-INT-001: live Safety_Halt_Requests consume-and-suppress")
class SafetyHaltLiveIntegrationTest {

    private static final String TABLE_DB = "default";
    private static final String TABLE_NAME = "Safety_Halt_Requests";
    private static final String SLOT_ID = "hft-0";
    private static final Duration APPEND_TIMEOUT = Duration.ofSeconds(15);

    @Test
    @DisplayName("UNSAFE suppresses the slot's tokens; RECOVERED admits post-recovery input")
    void unsafeThenRecoveredAgainstLiveFluss() throws Exception {
        assumeTrue("true".equalsIgnoreCase(
                System.getenv().getOrDefault("COMPUTE_INT_TEST_SAFETY", "false")),
                "Skipping — set COMPUTE_INT_TEST_SAFETY=true");

        String bootstrap = System.getenv().getOrDefault(
                "FLUSS_BOOTSTRAP_SERVERS", "localhost:9123");

        // Deterministic manifest-derived assignment: one slot owns all tokens.
        // Single-slot assignment makes the slot hash equal the manifest
        // fingerprint, which is exactly what a 1-connection deployment emits.
        List<Long> tokens = List.of(1000L, 1001L, 1L);
        SlotAssignmentResolver assignment = SlotAssignmentResolver.of(tokens, 1, 1024);
        String manifestFingerprint = assignment.manifestFingerprint();
        String slotTokenHash = assignment.tokenSetHashOf(SLOT_ID);
        assertNotNull(slotTokenHash, "hft-0 must own tokens");

        // Epoch = connection-instance boundary; wall clock keeps re-runs
        // strictly increasing (distinct halt_request_id tuples).
        long unsafeEpoch = System.currentTimeMillis();

        Configuration conf = new Configuration();
        conf.setString("bootstrap.servers", bootstrap);

        try (Connection connection = ConnectionFactory.createConnection(conf)) {
            final Table table;
            try {
                table = connection.getTable(TablePath.of(TABLE_DB, TABLE_NAME));
            } catch (Exception e) {
                LOG_OR_ASSUME("Safety_Halt_Requests not available at " + bootstrap
                        + " — apply the approved v3 DDL offline first: " + e.getMessage());
                assumeTrue(false, "Safety_Halt_Requests table missing at " + bootstrap);
                return;
            }

            // --- UNSAFE transition (reason mandatory for UNSAFE rows) ---
            String unsafeId = appendRow(table, manifestFingerprint, SLOT_ID, unsafeEpoch,
                    "UNSAFE", "FEED_STALLED", slotTokenHash);
            SlotSafetyRequest unsafe = bridge(lookupById(table, unsafeId));

            SafetyStateTracker tracker = new SafetyStateTracker(assignment);
            assertEquals(SafetyStateTracker.ApplyResult.NEW_UNSAFE, tracker.apply(unsafe),
                    "first UNSAFE opens the suppression window");
            assertTrue(tracker.isUnsafe(SLOT_ID));
            for (long token : tokens) {
                assertTrue(tracker.isTokenSuppressed(token),
                        "token " + token + " of the unsafe slot must be suppressed");
            }
            assertFalse(tracker.isTokenSuppressed(999_999L),
                    "tokens outside the assignment are never suppressed");

            // --- RECOVERED requires a strictly greater connection epoch ---
            long recoveredEpoch = unsafeEpoch + 1;
            String recoveredId = appendRow(table, manifestFingerprint, SLOT_ID, recoveredEpoch,
                    "RECOVERED", "", slotTokenHash);
            SlotSafetyRequest recovered = bridge(lookupById(table, recoveredId));

            assertEquals(SafetyStateTracker.ApplyResult.RECOVERED, tracker.apply(recovered),
                    "RECOVERED at a strictly greater epoch clears the slot");
            assertFalse(tracker.isUnsafe(SLOT_ID));
            for (long token : tokens) {
                assertFalse(tracker.isTokenSuppressed(token),
                        "token " + token + " admitted after recovery");
            }
        }
    }

    /** Mirrors SafetyHaltWriter: 21-column v3 row, KV upsert writer, async ack. */
    private static String appendRow(Table table, String manifestFingerprint, String slotId,
                                    long epoch, String state, String reasonCode,
                                    String assignedTokenHash) throws Exception {
        String haltRequestId = computeHaltRequestId(
                manifestFingerprint, slotId, epoch, state, reasonCode);
        GenericRow row = GenericRow.of(
                bs(haltRequestId),
                bs("SAFETY-INT-TEST"),                  // account_scope_id
                null,                                   // portfolio_id
                null,                                   // execution_partition_id
                bs("INGESTION"),                        // source_component
                bs("safety-int-test"),                  // source_instance
                bs(reasonCode),                         // reason_code
                null,                                   // reason_detail
                System.currentTimeMillis(),             // detection_time
                epoch,                                  // source_epoch
                bs(assignedTokenHash),                  // evidence_hash
                bs("OPEN"),                             // application_result
                null,                                   // applied_ts
                bs("v2"),                               // schema_version
                bs(slotId),                             // slot_id
                epoch,                                  // connection_epoch
                bs(manifestFingerprint),                // manifest_fingerprint
                bs(assignedTokenHash),                  // assigned_token_set_hash
                bs(state),                              // state
                bs("safety-int-test"),                  // evidence_reference
                2                                       // contract_version
        );
        UpsertWriter writer = table.newUpsert().createWriter();
        CompletableFuture<UpsertResult> future = writer.upsert(row);
        writer.flush();
        future.get(APPEND_TIMEOUT.toMillis(), java.util.concurrent.TimeUnit.MILLISECONDS);
        return haltRequestId;
    }

    private static InternalRow lookupById(Table table, String haltRequestId) throws Exception {
        // KV table: halt_request_id IS the physical primary key, so the
        // primary-key lookuper (no lookupBy) is the only supported path —
        // prefix lookup is rejected by the client when lookup columns equal
        // the primary key.
        var lookuper = table.newLookup().createLookuper();
        var result = lookuper.lookup(GenericRow.of(bs(haltRequestId)))
                .get(APPEND_TIMEOUT.toMillis(), java.util.concurrent.TimeUnit.MILLISECONDS);
        assertNotNull(result, "lookup must return a result for " + haltRequestId);
        InternalRow row = result.getSingletonRow();
        assertNotNull(row, "row must exist for halt_request_id " + haltRequestId);
        return row;
    }

    /**
     * Live-row bridge: fluss {@link InternalRow} &rarr; flink
     * {@link RowData} &rarr; {@link SlotSafetyRequest} through the production
     * {@link SafetyHaltRowDataBridge}. Column indexes mirror DDL v3
     * (18_safety_halt_requests.sql).
     */
    private static SlotSafetyRequest bridge(InternalRow row) {
        GenericRowData flink = GenericRowData.of(
                StringData.fromString(row.getString(0).toString()), // halt_request_id
                null,                            // account_scope_id
                null,                            // portfolio_id
                null,                            // execution_partition_id
                StringData.fromString(row.getString(4).toString()), // source_component
                null,                            // source_instance
                StringData.fromString(row.getString(6).toString()), // reason_code
                null,                            // reason_detail
                row.getLong(8),                  // detection_time
                null,                            // source_epoch
                null,                            // evidence_hash
                null,                            // application_result
                null,                            // applied_ts
                null,                            // schema_version
                StringData.fromString(row.getString(14).toString()), // slot_id
                row.getLong(15),                 // connection_epoch
                StringData.fromString(row.getString(16).toString()), // manifest_fingerprint
                StringData.fromString(row.getString(17).toString()), // assigned_token_set_hash
                StringData.fromString(row.getString(18).toString()), // state
                null,                            // evidence_reference
                row.getInt(20)                   // contract_version
        );
        return SafetyHaltRowDataBridge.toRequest(flink);
    }

    /** halt_request_id = sha256(manifest_fingerprint|slot_id|epoch|state|reason). */
    private static String computeHaltRequestId(String manifestFingerprint, String slotId,
                                               long epoch, String state, String reasonCode) {
        String tuple = manifestFingerprint + "|" + slotId + "|" + epoch
                + "|" + state + "|" + reasonCode;
        try {
            byte[] digest = java.security.MessageDigest.getInstance("SHA-256")
                    .digest(tuple.getBytes(java.nio.charset.StandardCharsets.UTF_8));
            return java.util.HexFormat.of().formatHex(digest);
        } catch (Exception e) {
            throw new IllegalStateException("SHA-256 unavailable", e);
        }
    }

    private static BinaryString bs(String s) {
        return s != null ? BinaryString.fromString(s) : null;
    }

    /** stdout warn (no logging dep in this module yet). */
    private static void LOG_OR_ASSUME(String message) {
        System.out.println("[SafetyHaltLiveIntegrationTest] " + message);
    }
}
