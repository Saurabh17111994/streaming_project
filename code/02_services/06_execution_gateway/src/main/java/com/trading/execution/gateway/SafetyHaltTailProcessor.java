package com.trading.execution.gateway;

import com.trading.common.schema.execution.GateRow;
import com.trading.common.schema.execution.GateStateStore;
import com.trading.common.schema.execution.SafetyHaltRequest;
import java.util.HashSet;
import java.util.Set;

/** Offline KV tail: Safety_Halt_Requests → GateStateStore.halt(). Idempotent, cross-scope reject. */
public final class SafetyHaltTailProcessor {
    public enum ApplyResult { APPLIED, DUPLICATE, CROSS_SCOPE_REJECT, NOT_FOUND, INVALID_ID }
    private final GateStateStore gates;
    private final Set<String> appliedIds = new HashSet<>();
    public SafetyHaltTailProcessor(GateStateStore gates){ this.gates=gates; }
    public ApplyResult apply(SafetyHaltRequest req, long nowTs){
        if(!req.idValid()) return ApplyResult.INVALID_ID;
        if(!appliedIds.add(req.haltRequestId())) return ApplyResult.DUPLICATE;
        GateRow row = gates.read(req.executionPartitionId());
        if(row==null) return ApplyResult.NOT_FOUND;
        if(!row.accountScopeId().equals(req.accountScopeId())) {
            appliedIds.remove(req.haltRequestId());
            return ApplyResult.CROSS_SCOPE_REJECT;
        }
        gates.halt(req.executionPartitionId(), row, req.reasonCode()+":"+req.reasonDetail(), req.evidenceHash(), nowTs);
        return ApplyResult.APPLIED;
    }
    public void replay(InMemoryControlStateStore store, long nowTs){
        store.replaySafetyHaltsTyped(r -> apply(r, nowTs));
    }
}
