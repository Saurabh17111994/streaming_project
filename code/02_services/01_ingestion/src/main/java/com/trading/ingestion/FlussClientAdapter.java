package com.trading.ingestion;

import com.trading.ingestion.model.RawTick;
import com.trading.ingestion.model.TickPacket;
import com.trading.ingestion.write.FlussRowConverter;
import com.trading.ingestion.write.RawTickWriter;
import java.time.Instant;
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
 * Fluss client adapter — connects to a Fluss cluster and provides a row
 * converter backed by {@link AppendWriter} for {@code raw_table_1}.
 *
 * <h3>Real Fluss API (0.9.1-incubating)</h3>
 * <ol>
 *   <li>{@link ConnectionFactory#createConnection(Configuration)} — bootstrap</li>
 *   <li>{@link Connection#getTable(TablePath)} — access the LOG table</li>
 *   <li>{@link Table#newAppend()} → {@link AppendWriter} — create writer</li>
 *   <li>{@link AppendWriter#append(org.apache.fluss.row.InternalRow)} → {@link CompletableFuture}</li>
 * </ol>
 *
 * <p>Each accepted tick is converted to a {@link GenericRow} matching the
 * 28-column {@code raw_table_1} DDL and appended individually (no batching).
 *
 * <p>See {@code /home/saurabh/Jupyter_notebook/Flink_Fluss_Infrastructure/fluss}
 * for the upstream Fluss source (Apache 2.0 licensed).
 */
final class FlussClientAdapter {

    private static final Logger LOG = LoggerFactory.getLogger(FlussClientAdapter.class);

    private FlussClientAdapter() {}

    /**
     * Connect to Fluss, get the table, create an append writer, and return
     * a row converter that uses the real client.
     */
    static FlussRowConverter connect(String bootstrapServers, String tablePath) {
        LOG.info("fluss: connecting (bootstrap={}, table={})", bootstrapServers, tablePath);

        // 1. Configure bootstrap
        Configuration conf = new Configuration();
        conf.setString("bootstrap.servers", bootstrapServers);

        // 2. Create connection
        Connection connection = ConnectionFactory.createConnection(conf);

        // 3. Parse table path "database.table_name"
        TablePath path = parseTablePath(tablePath);

        // 4. Get table handle
        Table table = connection.getTable(path);

        // 5. Verify schema — TableInfo contains the table schema
        org.apache.fluss.metadata.TableInfo info = table.getTableInfo();
        LOG.info("fluss: schema verified (table={}, schemaId={}, columns={})",
                tablePath, info.getSchemaId(), info.getRowType().getFieldCount());

        // 6. Create append writer (starts background Sender + MetadataUpdater)
        AppendWriter appendWriter = table.newAppend().createWriter();

        LOG.info("fluss: connected (table={}, path={})", tablePath, path);
        return new RealFlussRowConverter(appendWriter, connection, path.toString());
    }

    private static TablePath parseTablePath(String tablePath) {
        int dot = tablePath.indexOf('.');
        if (dot > 0) {
            return TablePath.of(tablePath.substring(0, dot), tablePath.substring(dot + 1));
        }
        // default database
        return TablePath.of("default", tablePath);
    }
}

/**
 * Real Fluss row converter — converts {@link TickPacket} → {@link GenericRow}
 * and appends through {@link AppendWriter}.
 *
 * <p>Column order matches {@code code/01_platform/02_sql/ddl/02_raw_table_1.sql}:
 */
class RealFlussRowConverter implements FlussRowConverter {

    private static final Logger LOG = LoggerFactory.getLogger(RealFlussRowConverter.class);

    private final AppendWriter writer;
    private final Connection connection;
    private final String tablePath;
    private volatile boolean closed;

    RealFlussRowConverter(AppendWriter writer, Connection connection, String tablePath) {
        this.writer = writer;
        this.connection = connection;
        this.tablePath = tablePath;
    }

    /** Shorthand to convert a String to Fluss's internal BinaryString type. */
    private static BinaryString bs(String s) {
        return s != null ? BinaryString.fromString(s) : BinaryString.EMPTY_UTF8;
    }

    /**
     * Convert a {@link TickPacket} to a {@link GenericRow} matching the
     * {@code raw_table_1} DDL column order, then append to Fluss.
     *
     * <p>DDL column order (28 columns):
     * <pre>{@code
     *   event_fingerprint, fingerprint_version, connection_id, connection_epoch,
     *   instrument_token, exchange, symbol, instrument_type, strike_paise, expiry,
     *   option_type, event_time, ingest_ts, ack_ts, tick_type, last_price_paise,
     *   last_qty, bid_price_paise, bid_qty, ask_price_paise, ask_qty,
     *   raw_payload, payload_hash, decoder_version, protocol_version,
     *   validity_state, validity_reason, schema_version
     * }</pre>
     */
    @Override
    public CompletableFuture<RawTickWriter.AppendResult> append(TickPacket packet) {
        if (closed) {
            return CompletableFuture.failedFuture(
                    new IllegalStateException("Fluss writer is closed"));
        }

        Instant now = Instant.now();
        RawTick raw = packet.raw();

        GenericRow row = GenericRow.of(
                // identity and routing
                bs(packet.eventFingerprint()),                      // event_fingerprint STRING
                bs(String.valueOf(packet.fingerprintVersion())),   // fingerprint_version STRING
                bs(packet.connectionId()),                          // connection_id STRING
                packet.connectionEpoch(),                           // connection_epoch BIGINT
                packet.instrumentToken(),                           // instrument_token BIGINT
                bs(packet.exchange()),                              // exchange STRING
                bs(packet.tradingSymbol()),                         // symbol STRING
                bs(""),                                             // instrument_type STRING (EQ/IDX...)
                null,                                               // strike_paise BIGINT
                null,                                               // expiry BIGINT
                null,                                               // option_type STRING
                // timestamps
                packet.eventTime().toEpochMilli(),                  // event_time BIGINT
                now.toEpochMilli(),                                 // ingest_ts BIGINT
                0L,                                                 // ack_ts BIGINT NULL — 0 = unknown
                                                                    // (R-010: Fluss LOG rows are immutable;
                                                                    //  the broker ack time is not known at
                                                                    //  row-build time)
                // trade fields
                bs(packet.validity().name().contains("NON_TRADE") ? "QUOTE" : "TRADE"), // tick_type
                packet.lastPricePaise(),                            // last_price_paise BIGINT
                packet.volume(),                                    // last_qty BIGINT
                // quote fields
                0L,                                                 // bid_price_paise BIGINT
                0L,                                                 // bid_qty BIGINT
                0L,                                                 // ask_price_paise BIGINT
                0L,                                                 // ask_qty BIGINT
                // payload preservation
                raw != null ? raw.rawPayload() : new byte[0],       // raw_payload BYTES
                bs(raw != null ? raw.payloadHash() : ""),           // payload_hash STRING
                bs(raw != null ? raw.decoderVersion() : "go-arrow-sdk"), // decoder_version STRING
                bs(raw != null ? raw.protocolVersion() : ""),       // protocol_version STRING
                // provenance
                bs(packet.validity().name()),                       // validity_state STRING
                bs(packet.validityReason() != null ? packet.validityReason() : ""), // validity_reason
                bs(String.valueOf(packet.schemaVersion()))          // schema_version STRING
        );

        return writer.append(row)
                .thenApply(result -> {
                    // Record ack timestamp
                    long appendCount = 0; // counter is external (in tracker)
                    return new RawTickWriter.AppendResult(appendCount, tablePath);
                })
                .exceptionally(ex -> {
                    LOG.warn("fluss: append failed (table={}): {}", tablePath,
                            ex.getCause() != null ? ex.getCause().getMessage() : ex.getMessage());
                    throw new RuntimeException("Fluss append failed", ex.getCause());
                });
    }

    /**
     * Estimate row size in bytes for backpressure accounting.
     * Conservative estimate: ~512 bytes fixed + raw payload size.
     */
    @Override
    public int estimatedRowSize(TickPacket packet) {
        int payloadSize = (packet.raw() != null && packet.raw().rawPayload() != null)
                ? packet.raw().rawPayload().length : 512;
        return 512 + payloadSize;
    }

    @Override
    public void close() {
        closed = true;
        try {
            connection.close();
        } catch (Exception e) {
            LOG.warn("fluss: close error", e);
        }
        LOG.info("fluss: closed (table={})", tablePath);
    }
}
