package com.trading.ingestion.discontinuity;

import com.trading.ingestion.bridge.BridgeEvent;
import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;
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
 * Writes suspected data-gap evidence to {@code suspected_discontinuities}
 * in Fluss. Records ingestion-side discontinuities: bridge crashes,
 * reconnections (epoch bumps), heartbeat timeouts, NTP clock jumps,
 * and feed-health signals.
 *
 * <p>Per dossier §B5:</p>
 * <blockquote>
 * Write evidence to suspected_discontinuities Fluss table on connection
 * loss, heartbeat timeout, reconnect, or feed-health warning.
 * </blockquote>
 *
 * <p>Column mapping (19_suspected_discontinuities.sql):</p>
 * <pre>
 * discontinuity_id           STRING  — UUID
 * source                     STRING  — who detected the gap (bridge instance / slot)
 * reason                     STRING  — DROP|HEARTBEAT_GAP|TIME_JUMP|FEED_HEALTH|RECONNECT
 * connection_epoch           BIGINT  — monotonically increasing
 * last_tick_ts               BIGINT  — epoch ms of last tick before gap
 * last_tick_fingerprint      STRING  — fingerprint before gap (null on startup crash)
 * last_tick_token            BIGINT  — bucket key; null for connection-wide events
 * last_tick_exchange         STRING  — null for connection-wide
 * last_tick_symbol           STRING  — null for connection-wide
 * detected_ts                BIGINT  — epoch ms when gap was detected
 * schema_version             STRING  — v1
 * </pre>
 */
public class DiscontinuityWriter implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(DiscontinuityWriter.class);

    private static final String TABLE_DB = "default";
    private static final String TABLE_NAME = "suspected_discontinuities";

    /** Reason per DDL enum. */
    public enum Reason {
        /** Connection dropped unexpectedly (bridge crash, stdin EOF). */
        DROP,
        /** No tick received within heartbeat window. */
        HEARTBEAT_GAP,
        /** NTP clock offset exceeded limit. */
        TIME_JUMP,
        /** Feed-health signal from broker (e.g. broker-side gap warning). */
        FEED_HEALTH,
        /** Reconnection occurred — epoch bumped while pipeline is continuous. */
        RECONNECT
    }

    private final AppendWriter writer;
    private final String instanceId;
    private final String connectionId;
    private final AtomicLong connectionEpoch;

    /**
     * Snapshot of the most recent tick for before/after gap evidence.
     */
    public static class LastTickSnapshot {
        public final long timestampMs;
        public final String fingerprint;
        public final long instrumentToken;
        public final String exchange;
        public final String symbol;

        public LastTickSnapshot(long timestampMs, String fingerprint,
                                long instrumentToken, String exchange, String symbol) {
            this.timestampMs = timestampMs;
            this.fingerprint = fingerprint;
            this.instrumentToken = instrumentToken;
            this.exchange = exchange;
            this.symbol = symbol;
        }
    }

    /**
     * Creates a discontinuity writer connected to the Fluss coordinator.
     * Maintains its own independent {@link AppendWriter}.
     */
    public DiscontinuityWriter(String bootstrapServers, String instanceId,
                                String connectionId, AtomicLong connectionEpoch) {
        this.instanceId = instanceId;
        this.connectionId = connectionId;
        this.connectionEpoch = connectionEpoch;

        Configuration conf = new Configuration();
        conf.setString("bootstrap.servers", bootstrapServers);

        try {
            Connection connection = ConnectionFactory.createConnection(conf);
            TablePath path = TablePath.of(TABLE_DB, TABLE_NAME);
            Table table = connection.getTable(path);
            this.writer = table.newAppend().createWriter();
            LOG.info("discontinuity-writer: connected (table={}, instanceId={})",
                    path, instanceId);
        } catch (Exception e) {
            LOG.error("discontinuity-writer: failed to connect to Fluss: {}", e.getMessage(), e);
            throw new RuntimeException("Cannot create DiscontinuityWriter", e);
        }
    }

    /**
     * Write a connection-wide discontinuity (no instrument context).
     * Used for bridge crashes and reconnects.
     *
     * @param reason        detection reason
     * @param note          free-text operator note
     * @param before        last tick before gap (null if none)
     * @param after         first tick after gap (null if none yet)
     */
    public void write(Reason reason, String note,
                       LastTickSnapshot before, LastTickSnapshot after) {
        write(reason, note, before, after, null, null, null);
    }

    /**
     * Map a bridge lifecycle event to discontinuity evidence (plan §DiscontinuityWriter).
     *
     * <pre>
     * DISCONNECTED / BRIDGE_EXIT  -> DROP
     * HEARTBEAT_FAILED / FEED_STALLED -> HEARTBEAT_GAP
     * RECONNECT                    -> RECONNECT
     * SUBSCRIPTION_PARTIAL         -> FEED_HEALTH
     * AUTH_FAILURE                 -> DROP
     * </pre>
     *
     * <p>One immutable row per transition, using the event's own connection
     * epoch and source. Never includes secrets or full raw lines.
     */
    public void writeBridgeEvent(BridgeEvent event, LastTickSnapshot before) {
        Reason reason = mapEventToReason(event.event());
        if (reason == null) {
            return; // not a discontinuity-bearing event
        }
        writeWithEpoch(reason, event.connectionEpoch(), event.slotId(),
                event.connectionId(), before, event.reason());
    }

    /** Map a bridge event name to a discontinuity {@link Reason}, or null if not applicable. */
    static Reason mapEventToReason(String eventName) {
        if (eventName == null) return null;
        return switch (eventName) {
            case "disconnect", "bridge_exit", "auth_failure" -> Reason.DROP;
            case "heartbeat_failed", "feed_stalled" -> Reason.HEARTBEAT_GAP;
            case "reconnect" -> Reason.RECONNECT;
            default -> null;
        };
    }

    /**
     * Write a connection-wide discontinuity with an explicit epoch and source
     * (used for per-slot lifecycle events where the shared connectionEpoch
     * counter is not the slot's epoch).
     */
    private void writeWithEpoch(Reason reason, long epoch, String source,
                                String connectionIdValue, LastTickSnapshot before, String note) {
        String discontinuityId = instanceId + "-" + UUID.randomUUID();
        Instant now = Instant.now();
        // source is NOT NULL in the DDL — never allow null through.
        // Prefer the connection id (the bridge process instance that detected
        // the gap); fall back to the slot id, then a constant.
        String sourceValue = connectionIdValue != null ? connectionIdValue
                : (source != null ? source : "ingestion");
        GenericRow row = GenericRow.of(
                bs(discontinuityId),
                bs(sourceValue),
                bs(reason.name()),
                epoch,
                before != null ? before.timestampMs : null,
                before != null ? bs(before.fingerprint) : null,
                before != null ? before.instrumentToken : null,
                before != null ? bs(before.exchange) : null,
                before != null ? bs(before.symbol) : null,
                now.toEpochMilli(),
                bs("v1")
        );
        try {
            @SuppressWarnings("unused")
            CompletableFuture<AppendResult> future = writer.append(row);
            LOG.info("discontinuity-writer: wrote {} (reason={}, source={}, epoch={}, note={})",
                    discontinuityId, reason, source, epoch, note);
        } catch (Exception e) {
            LOG.error("discontinuity-writer: append failed (id={}, reason={}): {}",
                    discontinuityId, reason, e.getMessage());
        }
    }

    /**
     * Write an instrument-scoped discontinuity.
     */
    public void write(Reason reason, String note,
                       LastTickSnapshot before, LastTickSnapshot after,
                       Long instrumentToken, String exchange, String symbol) {

        String discontinuityId = instanceId + "-" + UUID.randomUUID();
        Instant now = Instant.now();

        long instToken = instrumentToken != null ? instrumentToken
                : (before != null ? before.instrumentToken : 0L);
        String exch = exchange != null ? exchange
                : (before != null ? before.exchange : null);
        String sym = symbol != null ? symbol
                : (before != null ? before.symbol : null);

        GenericRow row = GenericRow.of(
                bs(discontinuityId),
                bs(connectionId),
                bs(reason.name()),
                connectionEpoch.get(),
                before != null ? before.timestampMs : null,
                before != null ? bs(before.fingerprint) : null,
                instToken,
                bs(exch),
                bs(sym),
                now.toEpochMilli(),
                bs("v1")
        );

        try {
            @SuppressWarnings("unused")
            CompletableFuture<AppendResult> future = writer.append(row);
            LOG.info("discontinuity-writer: wrote {} (reason={}, note={})",
                    discontinuityId, reason, note);
        } catch (Exception e) {
            LOG.error("discontinuity-writer: append failed (id={}, reason={}): {}",
                    discontinuityId, reason, e.getMessage());
        }
    }

    /** Shorthand to convert a String to Fluss's internal BinaryString type. */
    private static BinaryString bs(String s) {
        return s != null ? BinaryString.fromString(s) : BinaryString.EMPTY_UTF8;
    }

    @Override
    public void close() {
        try {
            writer.flush();
            // AppendWriter (TableWriter) does not have close() in Fluss 0.9.1-incubating;
            // only the Connection needs closing. The DiscontinuityWriter creates
            // its own Connection, which is closed by the creator.
        } catch (Exception e) {
            LOG.warn("discontinuity-writer: close failed: {}", e.getMessage());
        }
    }
}
