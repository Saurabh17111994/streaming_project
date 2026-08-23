package com.trading.execution.gateway;

import java.util.*;
import java.util.function.Consumer;
import org.apache.fluss.row.InternalRow;
import com.trading.common.schema.execution.SafetyHaltRequest;

/** Offline ControlStateStore — holds SafetyHaltRequests in memory, no Fluss. */
public final class InMemoryControlStateStore implements ControlStateStore {
    private final List<SafetyHaltRequest> halts = new ArrayList<>();
    private final Map<String,SafetyHaltRequest> byId = new LinkedHashMap<>();
    public void add(SafetyHaltRequest r){
        if(!r.idValid()) throw new IllegalArgumentException("halt_request_id != deterministic SHA256");
        halts.add(r); byId.putIfAbsent(r.haltRequestId(), r);
    }
    @Override public Lookup lookup(String t, List<Object> k){ return new Lookup(Status.NOT_FOUND,null,"offline"); }
    @Override public void replaySafetyHalts(Consumer<InternalRow> c){ /* not used offline */ }
    public void replaySafetyHaltsTyped(Consumer<SafetyHaltRequest> c){ List.copyOf(halts).forEach(c); }
    @Override public void close(){}
}
