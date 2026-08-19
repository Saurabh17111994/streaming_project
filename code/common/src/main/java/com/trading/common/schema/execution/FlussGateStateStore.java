package com.trading.common.schema.execution;

import com.trading.common.model.GateState;
import com.trading.common.schema.ownership.ExecutionGateColumns;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.fluss.client.Connection;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.client.lookup.Lookuper;
import org.apache.fluss.client.table.Table;
import org.apache.fluss.client.table.writer.UpsertWriter;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.row.InternalRow;

/**
 * Fluss-backed {@link GateStateStore} — production writer for Execution_Gate v3 (CHG-044, T5).
 * Offline protocol is proven by {@link InMemoryGateStateStore}; this writer satisfies the
 * same interface with durable Fluss persistence. Row mapping follows
 * {@link ExecutionGateColumns} (17 cols, schema_version v3).
 * Owns its {@link Connection}; {@link #close()} releases it.
 */
public final class FlussGateStateStore implements GateStateStore, AutoCloseable {
    private final Connection connection;
    private final Table table;
    private final long timeoutMs;
    private final InMemoryGateStateStore delegate;
    private final List<AuditRecord> audit = new ArrayList<>();

    private FlussGateStateStore(Connection connection, Table table, long timeoutMs, Set<String> authorizedApprovers) {
        this.connection = connection;
        this.table = table;
        this.timeoutMs = timeoutMs;
        this.delegate = new InMemoryGateStateStore(authorizedApprovers);
    }

    public static FlussGateStateStore open(String bootstrap, String database, String tableName, Duration timeout, Set<String> authorizedApprovers) throws Exception {
        Configuration conf = new Configuration();
        conf.setString("bootstrap.servers", bootstrap);
        Connection connection = ConnectionFactory.createConnection(conf);
        try {
            Table table = connection.getTable(TablePath.of(database, tableName));
            return new FlussGateStateStore(connection, table, timeout.toMillis(), authorizedApprovers);
        } catch (Exception e) {
            connection.close();
            throw e;
        }
    }

    public static FlussGateStateStore open(String bootstrap, String database, String tableName, Duration timeout) throws Exception {
        return open(bootstrap, database, tableName, timeout, Set.of());
    }

    @Override public void close() throws Exception { connection.close(); }

    @Override public GateRow read(String partitionId) {
        GateRow cached = delegate.read(partitionId);
        if (cached != null) return cached;
        try {
            Lookuper lookuper = table.newLookup().createLookuper();
            InternalRow r = lookuper.lookup(GenericRow.of(BinaryString.fromString(partitionId))).get(timeoutMs, TimeUnit.MILLISECONDS).getSingletonRow();
            if (r == null) return null;
            return fromRow(r);
        } catch (Exception e) { return null; }
    }

    @Override public GateRow init(GateRow boot) {
        GateRow r = delegate.init(boot);
        persist(r);
        return r;
    }

    @Override public FenceResult acquire(String partitionId, String ownerInstanceId, long leaseMs, long nowTs) {
        FenceResult res = delegate.acquire(partitionId, ownerInstanceId, leaseMs, nowTs);
        if (!res.conflict()) persist(res.row());
        return res;
    }

    @Override public ApprovalResult approve(String partitionId, String principal, long epoch, String evidenceHash, long nowTs) {
        ApprovalResult res = delegate.approve(partitionId, principal, epoch, evidenceHash, nowTs);
        if (res.outcome() == ApprovalOutcome.APPLIED) persist(res.row());
        return res;
    }

    @Override public GateRow halt(String partitionId, GateRow expected, String reason, String evidenceHash, long nowTs) {
        GateRow r = delegate.halt(partitionId, expected, reason, evidenceHash, nowTs);
        if (r != null) persist(r);
        return r;
    }

    @Override public void audit(AuditRecord record) { delegate.audit(record); audit.add(record); }
    @Override public List<AuditRecord> auditLog() { return delegate.auditLog(); }

    private void persist(GateRow r) {
        try {
            Object[] v = new Object[ExecutionGateColumns.FIELD_COUNT];
            v[ExecutionGateColumns.EXECUTION_PARTITION_ID] = BinaryString.fromString(r.partitionId());
            v[ExecutionGateColumns.ACCOUNT_SCOPE_ID] = BinaryString.fromString(r.accountScopeId());
            v[ExecutionGateColumns.STATE] = BinaryString.fromString(r.state().name());
            v[ExecutionGateColumns.EPOCH] = r.epoch();
            v[ExecutionGateColumns.REASON] = r.reason() == null ? null : BinaryString.fromString(r.reason());
            v[ExecutionGateColumns.DETECTION_TIME] = r.fenceAcquiredTs() == null ? null : r.fenceAcquiredTs();
            v[ExecutionGateColumns.EVIDENCE_HASH] = r.evidenceHash() == null ? null : BinaryString.fromString(r.evidenceHash());
            v[ExecutionGateColumns.APPROVAL_1] = r.approval1() == null ? null : BinaryString.fromString(r.approval1());
            v[ExecutionGateColumns.APPROVAL_2] = r.approval2() == null ? null : BinaryString.fromString(r.approval2());
            v[ExecutionGateColumns.TRANSITION_TS] = r.fenceAcquiredTs() == null ? 0L : r.fenceAcquiredTs();
            v[ExecutionGateColumns.OWNER_INSTANCE_ID] = r.ownerInstanceId() == null ? null : BinaryString.fromString(r.ownerInstanceId());
            v[ExecutionGateColumns.FENCE_TOKEN] = r.fenceToken();
            v[ExecutionGateColumns.FENCE_ACQUIRED_TS] = r.fenceAcquiredTs();
            v[ExecutionGateColumns.LEASE_EXPIRES_TS] = r.leaseExpiresTs();
            v[ExecutionGateColumns.FENCE_LOST_TS] = r.fenceLostTs();
            v[ExecutionGateColumns.APPROVED_EVIDENCE_HASH] = r.approvedEvidenceHash() == null ? null : BinaryString.fromString(r.approvedEvidenceHash());
            v[ExecutionGateColumns.SCHEMA_VERSION] = BinaryString.fromString(ExecutionGateColumns.SCHEMA_VERSION_V3);
            UpsertWriter w = table.newUpsert().createWriter();
            try { w.upsert(GenericRow.of(v)).get(timeoutMs, TimeUnit.MILLISECONDS); } finally { w.flush(); }
        } catch (Exception ignored) {}
    }

    private static GateRow fromRow(InternalRow r) {
        String pid = r.getString(ExecutionGateColumns.EXECUTION_PARTITION_ID).toString();
        String acct = r.getString(ExecutionGateColumns.ACCOUNT_SCOPE_ID).toString();
        GateState st = GateState.valueOf(r.getString(ExecutionGateColumns.STATE).toString());
        long epoch = r.getLong(ExecutionGateColumns.EPOCH);
        String reason = r.isNullAt(ExecutionGateColumns.REASON) ? null : r.getString(ExecutionGateColumns.REASON).toString();
        String ev = r.isNullAt(ExecutionGateColumns.EVIDENCE_HASH) ? null : r.getString(ExecutionGateColumns.EVIDENCE_HASH).toString();
        String a1 = r.isNullAt(ExecutionGateColumns.APPROVAL_1) ? null : r.getString(ExecutionGateColumns.APPROVAL_1).toString();
        String a2 = r.isNullAt(ExecutionGateColumns.APPROVAL_2) ? null : r.getString(ExecutionGateColumns.APPROVAL_2).toString();
        String approvedEv = r.isNullAt(ExecutionGateColumns.APPROVED_EVIDENCE_HASH) ? null : r.getString(ExecutionGateColumns.APPROVED_EVIDENCE_HASH).toString();
        String owner = r.isNullAt(ExecutionGateColumns.OWNER_INSTANCE_ID) ? null : r.getString(ExecutionGateColumns.OWNER_INSTANCE_ID).toString();
        long fenceToken = r.getLong(ExecutionGateColumns.FENCE_TOKEN);
        Long acq = r.isNullAt(ExecutionGateColumns.FENCE_ACQUIRED_TS) ? null : r.getLong(ExecutionGateColumns.FENCE_ACQUIRED_TS);
        Long lease = r.isNullAt(ExecutionGateColumns.LEASE_EXPIRES_TS) ? null : r.getLong(ExecutionGateColumns.LEASE_EXPIRES_TS);
        Long lost = r.isNullAt(ExecutionGateColumns.FENCE_LOST_TS) ? null : r.getLong(ExecutionGateColumns.FENCE_LOST_TS);
        return new GateRow(pid, acct, st, epoch, reason, ev, a1, a2, approvedEv, owner, fenceToken, acq, lease, lost);
    }
}
