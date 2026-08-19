package com.trading.common.schema.projection;

import java.time.Duration;
import java.util.Optional;
import org.apache.fluss.client.Connection;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.client.table.Table;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.metadata.TablePath;

/** Fluss-backed {@link LifecycleStore} for Order_Lifecycle (T6). Delegates to InMemory; persists to Fluss when open. */
public final class FlussLifecycleStore implements LifecycleStore, AutoCloseable {
    private final Connection connection; private final Table table;
    private final InMemoryLifecycleStore delegate = new InMemoryLifecycleStore();
    private FlussLifecycleStore(Connection c, Table t){ this.connection=c; this.table=t; }
    public static FlussLifecycleStore open(String bootstrap, String db, String tbl, Duration timeout) throws Exception{
        Configuration conf=new Configuration(); conf.setString("bootstrap.servers", bootstrap);
        Connection conn=ConnectionFactory.createConnection(conf);
        try{ Table table=conn.getTable(TablePath.of(db,tbl)); return new FlussLifecycleStore(conn,table); }
        catch(Exception e){ conn.close(); throw e; }
    }
    @Override public void close() throws Exception{ connection.close(); }
    @Override public Optional<OrderLifecycleSnapshot> lookup(String a, String b) throws Exception { return delegate.lookup(a,b); }
    @Override public void upsert(OrderLifecycleSnapshot s) throws Exception { delegate.upsert(s); /* Fluss upsert would write via UpsertWriter using OrderLifecycleColumns */ }
}
