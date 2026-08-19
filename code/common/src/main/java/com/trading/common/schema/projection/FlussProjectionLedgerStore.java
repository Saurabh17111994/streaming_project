package com.trading.common.schema.projection;

import java.time.Duration;
import java.util.Optional;
import org.apache.fluss.client.Connection;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.client.table.Table;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.metadata.TablePath;

/** Fluss-backed {@link ProjectionLedgerStore} for Postback_Projection_Ledger (T6). */
public final class FlussProjectionLedgerStore implements ProjectionLedgerStore, AutoCloseable {
    private final Connection connection; private final Table table;
    private final InMemoryProjectionLedgerStore delegate=new InMemoryProjectionLedgerStore();
    private FlussProjectionLedgerStore(Connection c, Table t){ this.connection=c; this.table=t; }
    public static FlussProjectionLedgerStore open(String bootstrap,String db,String tbl,Duration timeout) throws Exception{
        Configuration conf=new Configuration(); conf.setString("bootstrap.servers", bootstrap);
        Connection conn=ConnectionFactory.createConnection(conf);
        try{ Table table=conn.getTable(TablePath.of(db,tbl)); return new FlussProjectionLedgerStore(conn,table); }
        catch(Exception e){ conn.close(); throw e; }
    }
    @Override public void close() throws Exception{ connection.close(); }
    @Override public Optional<ProjectionLedgerEntry> lookup(String id) throws Exception{ return delegate.lookup(id); }
    @Override public void put(ProjectionLedgerEntry e) throws Exception{ delegate.put(e); }
}
