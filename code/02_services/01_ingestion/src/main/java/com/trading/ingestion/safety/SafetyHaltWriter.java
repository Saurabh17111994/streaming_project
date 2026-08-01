package com.trading.ingestion.safety;

import java.security.MessageDigest;
import java.time.Instant;
import java.util.HexFormat;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.apache.fluss.client.Connection;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.client.table.Table;
import org.apache.fluss.client.table.writer.AppendResult;
import org.apache.fluss.client.table.writer.AppendWriter;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.GenericRow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Appends slot-scoped safety requests to {@code Safety_Halt_Requests}
 * (plan Amendment §Slot-scoped safety propagation).
 *
 * <p>One immutable row per unsafe/recovered transition. {@code halt_request_id}
 * is the SHA-256 hex of {@code manifest_fingerprint|slot_id|connection_epoch|
 * state|reason_code}, so re-emitting the same tuple is a duplicate (deduped by
 * the caller — never a second state transition). The request never contains
 * credentials, raw payload bytes, token lists, symbol lists, or free-form SDK
 * exceptions.
 */
public final class SafetyHaltWriter implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(SafetyHaltWriter.class);

    private static final String TABLE_DB = "default";
    private static final String TABLE_NAME = "Safety_Halt_Requests";
    private static final String CONTRACT_VERSION = "2";

    /** Ingestion safety reason codes (plan Amendment — exact list). */
    public enum ReasonCode {
        FEED_STALLED,
        HEARTBEAT_FAILED,
        READ_FAILURE,
        SUBSCRIPTION_PARTIAL,
        SUBSCRIPTION_TIMEOUT,
        AUTH_FAILURE,
        DECODE_ERROR_BURST,
        BRIDGE_EXIT,
        RESOURCE_EXHAUSTED
    }

    /** Safety state of a slot as seen by the Signal job. */
    public enum SafetyState {
        UNSAFE,
        RECOVERED
    }

    private final AppendWriter writer;
    private final String sourceInstance;
    private final String manifestFingerprint;
    private final String accountScopeId;

    /**
     * @param bootstrapServers    Fluss coordinator bootstrap
     * @param sourceInstance      ingestion instance id (source_instance)
     * @param manifestFingerprint deterministic manifest fingerprint (plan)
     * @param accountScopeId      account scope id (existing DDL column)
     */
    public SafetyHaltWriter(String bootstrapServers, String sourceInstance,
                            String manifestFingerprint, String accountScopeId) {
        this.sourceInstance = sourceInstance;
        this.manifestFingerprint = manifestFingerprint;
        this.accountScopeId = accountScopeId;

        Configuration conf = new Configuration();
        conf.setString("bootstrap.servers", bootstrapServers);
        try {
            Connection connection = ConnectionFactory.createConnection(conf);
            TablePath path = TablePath.of(TABLE_DB, TABLE_NAME);
            Table table = connection.getTable(path);
            this.writer = table.newAppend().createWriter();
            LOG.info("safety-halt-writer: connected (table={}, instance={})", path, sourceInstance);
        } catch (Exception e) {
            LOG.error("safety-halt-writer: failed to connect to Fluss: {}", e.getMessage(), e);
            throw new RuntimeException("Cannot create SafetyHaltWriter", e);
        }
    }

    /**
     * Write one safety request row.
     *
     * @param slotId             slot id (hft-N)
     * @param connectionEpoch    slot epoch at the transition
     * @param state              UNSAFE or RECOVERED
     * @param reasonCode         exact plan reason code (UNSAFE only)
     * @param assignedTokenHash  SHA-256 of the slot's sorted token set
     * @param evidenceReference  optional link to discontinuity/quarantine evidence
     * @param detectedTsMs       epoch ms of the transition
     * @return the computed halt_request_id (for caller-side dedup)
     */
    public String write(String slotId, long connectionEpoch, SafetyState state,
                        ReasonCode reasonCode, String assignedTokenHash,
                        String evidenceReference, long detectedTsMs) {
        String reason = reasonCode != null ? reasonCode.name() : "";
        String haltRequestId = computeHaltRequestId(
                manifestFingerprint, slotId, connectionEpoch, state.name(), reason);
        Instant now = Instant.now();

        GenericRow row = GenericRow.of(
                bs(haltRequestId),
                bs(accountScopeId),
                null,                                   // portfolio_id
                null,                                   // execution_partition_id
                bs("INGESTION"),                        // source_component
                bs(sourceInstance),                     // source_instance
                bs(reason),                             // reason_code
                bs(reason.isEmpty() ? null : reason),   // reason_detail (scrubbed)
                detectedTsMs,                           // detection_time
                connectionEpoch,                        // source_epoch
                bs(assignedTokenHash),                  // evidence_hash
                bs("OPEN"),                             // application_result
                null,                                   // applied_ts
                bs("v2"),                               // schema_version
                bs(slotId),                             // slot_id
                connectionEpoch,                        // connection_epoch
                bs(manifestFingerprint),                // manifest_fingerprint
                bs(assignedTokenHash),                  // assigned_token_set_hash
                bs(state.name()),                       // state
                bs(evidenceReference != null ? evidenceReference : ""), // evidence_reference
                Integer.parseInt(CONTRACT_VERSION)      // contract_version
        );

        try {
            @SuppressWarnings("unused")
            CompletableFuture<AppendResult> future = writer.append(row);
            LOG.info("safety-halt-writer: wrote {} (slot={}, state={}, reason={}, epoch={})",
                    haltRequestId, slotId, state, reason, connectionEpoch);
        } catch (Exception e) {
            LOG.error("safety-halt-writer: append failed (id={}, reason={}): {}",
                    haltRequestId, reason, e.getMessage());
        }
        return haltRequestId;
    }

    /**
     * halt_request_id = sha256(manifest_fingerprint|slot_id|connection_epoch|state|reason_code)
     */
    public static String computeHaltRequestId(String manifestFingerprint, String slotId,
                                              long connectionEpoch, String state, String reasonCode) {
        String tuple = manifestFingerprint + "|" + slotId + "|" + connectionEpoch
                + "|" + state + "|" + reasonCode;
        try {
            byte[] digest = MessageDigest.getInstance("SHA-256").digest(
                    tuple.getBytes(java.nio.charset.StandardCharsets.UTF_8));
            return HexFormat.of().formatHex(digest);
        } catch (Exception e) {
            throw new IllegalStateException("SHA-256 unavailable", e);
        }
    }

    /** SHA-256 of the slot's sorted token set (assigned_token_set_hash). */
    public static String computeAssignedTokenHash(java.util.List<Long> tokens) {
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            java.util.List<Long> sorted = new java.util.ArrayList<>(tokens);
            sorted.sort(Long::compareTo);
            for (long token : sorted) {
                md.update((byte) (token >>> 56));
                md.update((byte) (token >>> 48));
                md.update((byte) (token >>> 40));
                md.update((byte) (token >>> 32));
                md.update((byte) (token >>> 24));
                md.update((byte) (token >>> 16));
                md.update((byte) (token >>> 8));
                md.update((byte) (token));
            }
            return HexFormat.of().formatHex(md.digest());
        } catch (Exception e) {
            throw new IllegalStateException("SHA-256 unavailable", e);
        }
    }

    private static BinaryString bs(String s) {
        return s != null ? BinaryString.fromString(s) : null;
    }

    @Override
    public void close() {
        try {
            writer.flush();
        } catch (Exception e) {
            LOG.warn("safety-halt-writer: close failed: {}", e.getMessage());
        }
    }
}
