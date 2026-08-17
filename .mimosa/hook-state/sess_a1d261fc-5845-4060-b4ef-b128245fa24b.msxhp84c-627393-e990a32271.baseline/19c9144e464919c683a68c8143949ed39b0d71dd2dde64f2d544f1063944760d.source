package com.trading.compute.signaljob;

import com.trading.common.model.FormingBar;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import org.apache.fluss.client.Connection;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.client.lookup.Lookuper;
import org.apache.fluss.client.table.Table;
import org.apache.fluss.client.table.writer.UpsertWriter;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.GenericRow;

/**
 * Raw-client access to the {@code forming_bar} KV current-state home
 * (forming-bar persistence phase, 2026-08-16) — the DEC-038 rehydration
 * primitive: the durable forming bar is Fluss-authoritative, so a cold
 * restart / exceptional rebuild (and any external current-state consumer)
 * reads the latest bar per instrument straight from this table instead of
 * replaying {@code raw_table_1} history. The normal Flink restart path uses
 * the compact checkpoint (small active context) and does NOT read here; this
 * store is the Fluss-authority read/write path for rehydration and state
 * verification.
 *
 * <p>Current-state semantics only: {@link #put(FormingBar)} upserts one row
 * per instrument (PK {@code instrument_token}, last-write-wins — the latest
 * forming bar replaces any previous state; never history, never per-tick
 * snapshots). {@link #read(long)} is a point lookup by PK. Row mapping lives
 * in {@link FormingBarRowMapper} (single source of mapping truth: the same
 * 11-column v1 layout as the Flink sink's {@code toRow}/{@code fromRow}).
 *
 * <p>Raw-client shape: PK {@code [instrument_token]}, bucket key
 * {@code instrument_token}, {@code kv.format-version=2} — the documented
 * COMPAT-FLUSS-005 combo for raw-client upsert/lookup (same machinery the
 * dedup store proves). Owns its {@link Connection} — {@link #close()}
 * releases it.
 */
public final class FlussFormingBarStateStore implements AutoCloseable {

    private final Connection connection;
    private final Table table;
    private final long timeoutMs;

    private FlussFormingBarStateStore(Connection connection, Table table, long timeoutMs) {
        this.connection = connection;
        this.table = table;
        this.timeoutMs = timeoutMs;
    }

    /** Open the store against {@code database.table} on the cluster. */
    public static FlussFormingBarStateStore open(String bootstrap, String database,
            String tableName, Duration timeout) throws Exception {
        Configuration conf = new Configuration();
        conf.setString("bootstrap.servers", bootstrap);
        Connection connection = ConnectionFactory.createConnection(conf);
        try {
            TablePath path = TablePath.of(database, tableName);
            connection.getAdmin().getTableInfo(path)
                    .get(timeout.toMillis(), TimeUnit.MILLISECONDS);
            Table table = connection.getTable(path);
            return new FlussFormingBarStateStore(connection, table, timeout.toMillis());
        } catch (Exception e) {
            connection.close();
            throw e;
        }
    }

    @Override
    public void close() throws Exception {
        connection.close();
    }

    /**
     * Durable current-state upsert of the latest forming bar (idempotent —
     * re-writing the same instrument converges on the same PK, last-write-wins).
     */
    public void put(FormingBar bar) throws Exception {
        UpsertWriter writer = table.newUpsert().createWriter();
        try {
            writer.upsert(FormingBarRowMapper.toFlussRow(bar))
                    .get(timeoutMs, TimeUnit.MILLISECONDS);
        } finally {
            writer.flush();
        }
    }

    /**
     * Rehydration point read by PK. Returns the latest forming bar for the
     * instrument, or {@link Optional#empty()} if no row exists. The returned
     * record has {@code windowEnd = 0} and {@code exchange/symbol = null} per
     * the v1 projection (the caller restores them — e.g. from the completed-
     * candle stream).
     */
    public Optional<FormingBar> read(long instrumentToken) throws Exception {
        // Lookuper is not AutoCloseable in 0.9.1 (same as the dedup store);
        // each read creates a fresh lookuper that the client reclaims.
        Lookuper lookuper = table.newLookup().createLookuper();
        org.apache.fluss.row.InternalRow found = lookuper
                .lookup(GenericRow.of(instrumentToken))
                .get(timeoutMs, TimeUnit.MILLISECONDS)
                .getSingletonRow();
        if (found == null) {
            return Optional.empty();
        }
        return Optional.of(FormingBarRowMapper.fromFlussRow(found));
    }
}
