package com.trading.ingestion.quarantine;

import com.trading.ingestion.model.ValidityClassification;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.util.HexFormat;
import java.util.UUID;
import java.util.regex.Pattern;
import java.util.concurrent.CompletableFuture;
import org.apache.fluss.client.Connection;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.client.table.Table;
import org.apache.fluss.client.table.writer.AppendWriter;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.client.table.writer.AppendResult;
import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.GenericRow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Writes ingestion-side quarantine evidence to {@code ingestion_quarantine}
 * in Fluss. This is intentionally separate from Action Capture's
 * {@code Postback_Quarantine} schema.
 *
 * <p>Per dossier §B6:</p>
 * <blockquote>
 * Quarantine unsupported/malformed packet evidence.
 * Append to Postback_Quarantine with bytes + reason + timestamp.
 * </blockquote>
 *
 * <p>Column mapping (16_postback_quarantine.sql):</p>
 * <pre>
 * quarantine_id      STRING   — UUID
 * postback_event_id  STRING   — null (ingestion, not postback)
 * reason             STRING   — classifier
 * broker_order_id    STRING   — null
 * client_order_ref   STRING   — null
 * broker_status      STRING   — null
 * broker_timestamp   BIGINT   — null
 * instrument_token   BIGINT   — may be null if missing
 * exchange           STRING   — may be null
 * symbol             STRING   — may be null
 * raw_payload        BYTES    — original NDJSON line bytes
 * payload_hash       STRING   — SHA-256 hex
 * detected_ts        BIGINT   — epoch ms
 * status             STRING   — OPEN
 * resolution_ts      BIGINT   — null
 * resolution_note    STRING   — null
 * operator_identity  STRING   — null
 * schema_version     STRING   — v1
 * </pre>
 */
public class QuarantineWriter implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(QuarantineWriter.class);

    private static final String TABLE_DB = "default";
    private static final String TABLE_NAME = "ingestion_quarantine";
    private static final int BUCKET_COUNT = 8;
    private static final Pattern SECRET_PATTERN = Pattern.compile(
            "(?i)(ARROW_APP_SECRET|ARROW_PASSWORD|ARROW_TOTP_KEY|ARROW_TOKEN|access_token|authorization|appID|token)([=:][^&\\s,}]+)");
    /** Runs first so `Bearer <token>` (space-separated) is consumed before the
     *  name=value pattern can eat only the literal `Bearer` and leak the token. */
    private static final Pattern BEARER_PATTERN = Pattern.compile(
            "(?i)\\bBearer[=:\\s]+[^\\s,}]+");

    private final AppendWriter writer;
    private final String instanceId;

    /**
     * Reason — deliberately broad; ingestion can't classify as
     * MISSING_BROKER_ID etc. Those are postback-only categories.
     */
    public enum Reason {
        /** NDJSON line is not valid JSON. */
        MALFORMED_JSON,
        /** JSON parsed but required fields missing or of wrong type. */
        INVALID_SCHEMA,
        /** Instrument token not found in the daily manifest. */
        MISSING_INSTRUMENT,
        /** Price/timestamp/volume failed ValidityClassification. */
        INVALID_VALUES,
        FUTURE_BROKER_TIMESTAMP,
        STALE_BROKER_TIMESTAMP,
        BROKER_LIMIT_VIOLATION,
        HASH_MISMATCH,
        /** Ingestion-internal processing exception. */
        INTERNAL_ERROR,
        /** Canonical fingerprint could not be computed. */
        FINGERPRINT_FAILURE
    }

    /**
     * Creates a quarantine writer connected to the Fluss coordinator.
     * Inline creates its own {@link AppendWriter} so this table is
     * independent of the raw_table_1 writer.
     */
    public QuarantineWriter(String bootstrapServers, String instanceId) {
        this.instanceId = instanceId;

        Configuration conf = new Configuration();
        conf.setString("bootstrap.servers", bootstrapServers);

        try {
            Connection connection = ConnectionFactory.createConnection(conf);
            TablePath path = TablePath.of(TABLE_DB, TABLE_NAME);
            Table table = connection.getTable(path);
            this.writer = table.newAppend().createWriter();
            LOG.info("quarantine-writer: connected (table={}, instanceId={})",
                    path, instanceId);
        } catch (Exception e) {
            LOG.error("quarantine-writer: failed to connect to Fluss: {}", e.getMessage(), e);
            throw new RuntimeException("Cannot create QuarantineWriter", e);
        }
    }

    /**
     * Write a quarantined tick. Never throws — failures are logged
     * at ERROR and must not block the ingestion pipeline.
     *
     * @param rawPayload  raw NDJSON bytes (may be null if we have no bytes)
     * @param reason      why this is quarantined
     * @param detail      human-readable detail for operator, logged inline
     */
    public void write(byte[] rawPayload, Reason reason, String detail) {
        write(rawPayload, reason, detail, null, null, null);
    }

    /**
     * Write with optional instrument context.
     */
    public void write(byte[] rawPayload, Reason reason, String detail,
                      Long instrumentToken, String exchange, String symbol) {

        String quarantineId = instanceId + "-" + UUID.randomUUID();
        Instant now = Instant.now();
        String payloadHash = computePayloadHash(rawPayload);
        byte[] payload = rawPayload != null ? rawPayload : new byte[0];

        String safeDetail = sanitizeDetail(detail);
        GenericRow row = GenericRow.of(
                bs(quarantineId), bs(reason.name()), instrumentToken,
                bs(exchange), bs(symbol), payload, bs(payloadHash),
                now.toEpochMilli(), bs(safeDetail), bs("v1"));

        try {
            @SuppressWarnings("unused")
            CompletableFuture<AppendResult> future = writer.append(row);
            LOG.debug("quarantine-writer: wrote {} (reason={}, detail={})",
                    quarantineId, reason, safeDetail);
        } catch (Exception e) {
            LOG.error("quarantine-writer: append failed (id={}, reason={}): {}",
                    quarantineId, reason, e.getMessage());
        }
    }

    /** Shorthand to convert a String to Fluss's internal BinaryString type. */
    /** Shorthand to convert a String to Fluss's internal BinaryString type. Returns null for null input (nullable column). */
    private static BinaryString bs(String s) {
        return s != null ? BinaryString.fromString(s) : null;
    }

    @Override
    public void close() {
        try {
            writer.flush();
            // AppendWriter (TableWriter) does not have close() in Fluss 0.9.1-incubating
        } catch (Exception e) {
            LOG.warn("quarantine-writer: close failed: {}", e.getMessage());
        }
    }

    /** Best-effort SHA-256 hash of payload bytes. */
    private static String computePayloadHash(byte[] data) {
        if (data == null || data.length == 0) return "";
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            byte[] hash = md.digest(data);
            return HexFormat.of().formatHex(hash);
        } catch (NoSuchAlgorithmException e) {
            return ""; // unreachable
        }
    }

    static String sanitizeDetail(String value) {
        if (value == null) return "";
        // Two-pass: Bearer tokens first (space-separated), then name=value pairs.
        String safe = BEARER_PATTERN.matcher(value).replaceAll("Bearer=[REDACTED]");
        safe = SECRET_PATTERN.matcher(safe).replaceAll("$1=[REDACTED]");
        return safe.length() > 512 ? safe.substring(0, 512) : safe;
    }
}
