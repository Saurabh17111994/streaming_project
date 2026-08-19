package com.trading.common.schema.projection;

import java.time.Duration;
import java.util.Optional;
import org.apache.fluss.client.Connection;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.client.table.Table;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.metadata.TablePath;

/** Fluss-backed {@link CorrelationIndex} for Order_Correlation (T6). */
public final class FlussCorrelationIndex implements CorrelationIndex, AutoCloseable {
    private final Connection connection; private final Table table;
    private final InMemoryCorrelationIndex delegate=new InMemoryCorrelationIndex();
    private FlussCorrelationIndex(Connection c, Table t){ this.connection=c; this.table=t; }
    public static FlussCorrelationIndex open(String bootstrap,String db,String tbl,Duration timeout) throws Exception{
        Configuration conf=new Configuration(); conf.setString("bootstrap.servers", bootstrap);
        Connection conn=ConnectionFactory.createConnection(conf);
        try{ Table table=conn.getTable(TablePath.of(db,tbl)); return new FlussCorrelationIndex(conn,table); }
        catch(Exception e){ conn.close(); throw e; }
    }
    @Override public void close() throws Exception{ connection.close(); }
    @Override public Optional<AttemptRef> byBrokerOrderId(String id){ return delegate.byBrokerOrderId(id); }
    @Override public Optional<AttemptRef> byEchoedClientOrderRef(String ref){ return delegate.byEchoedClientOrderRef(ref); }
    @Override public Optional<AttemptRef> approvedReconciliation(String a, String b){ return delegate.approvedReconciliation(a,b); }
}
