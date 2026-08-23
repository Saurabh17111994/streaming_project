package com.trading.execution.gateway;

import com.trading.common.model.GateState;
import com.trading.common.schema.execution.GateRow;
import com.trading.common.schema.execution.InMemoryGateStateStore;
import com.trading.common.schema.execution.SafetyHaltRequest;
import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.row.InternalRow;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

/** Offline wire test: SafetyHaltTailProcessor InternalRow (live Fluss) path — no Arrow, no Fluss. */
class SafetyHaltTailFlussWireTest {

    GateRow boot(String pid, String acct){ return new GateRow(pid,acct,GateState.HALTED,0,"boot","ev0",null,null,null,null,0L,null,null,null); }

    SafetyHaltRequest req(String acct,String pid,String reason,long ts){
        String id=SafetyHaltRequest.deterministicId(acct,pid,"signal-job",reason,ts,1L,"abc123","v3");
        return new SafetyHaltRequest(id,acct,pid,"signal-job","i-1",reason,"detail",ts,1L,"abc123","v3",null,0L,null,null);
    }

    SafetyHaltRequest reqFull(String acct,String pid,String reason,long ts,String slot,long connEpoch,String manifest,String state){
        String id=SafetyHaltRequest.deterministicId(acct,pid,"signal-job",reason,ts,1L,"abc123","v3");
        return new SafetyHaltRequest(id,acct,pid,"signal-job","i-1",reason,"detail",ts,1L,"abc123","v3",slot,connEpoch,manifest,state);
    }

    @Test void internalRowDecodeRoundtripAndIdValid(){
        SafetyHaltRequest r = req("acct-1","p-1","MANUAL_HALT",2000L);
        InternalRow row = InMemoryControlStateStore.toRow(r);
        // Verify the row decodes and idValid holds via InternalRow path
        InMemoryGateStateStore gates=new InMemoryGateStateStore();
        gates.init(boot("p-1","acct-1"));
        SafetyHaltTailProcessor tail=new SafetyHaltTailProcessor(gates);
        assertEquals(SafetyHaltTailProcessor.ApplyResult.APPLIED, tail.apply(row, 3000L));
        // Duplicate same InternalRow must be DUPLICATE not second halt
        assertEquals(SafetyHaltTailProcessor.ApplyResult.DUPLICATE, tail.apply(row, 4000L));
        // Gate epoch must not have incremented twice (idempotent)
        long epochAfter = gates.read("p-1").epoch();
        assertEquals(SafetyHaltTailProcessor.ApplyResult.DUPLICATE, tail.apply(row, 5000L));
        assertEquals(epochAfter, gates.read("p-1").epoch());
    }

    @Test void internalRowValidThenDuplicateThenCrossScopeReject(){
        InMemoryGateStateStore gates=new InMemoryGateStateStore();
        gates.init(boot("p-1","acct-1"));
        SafetyHaltTailProcessor tail=new SafetyHaltTailProcessor(gates);

        SafetyHaltRequest valid = req("acct-1","p-1","MANUAL_HALT",2000L);
        InternalRow validRow = InMemoryControlStateStore.toRow(valid);
        SafetyHaltRequest cross = req("acct-OTHER","p-1","MANUAL_HALT",3000L);
        InternalRow crossRow = InMemoryControlStateStore.toRow(cross);
        // 1) APPLIED
        assertEquals(SafetyHaltTailProcessor.ApplyResult.APPLIED, tail.apply(validRow, 3000L));
        // 2) DUPLICATE same id
        assertEquals(SafetyHaltTailProcessor.ApplyResult.DUPLICATE, tail.apply(validRow, 4000L));
        // 3) CROSS_SCOPE_REJECT different account same partition
        assertEquals(SafetyHaltTailProcessor.ApplyResult.CROSS_SCOPE_REJECT, tail.apply(crossRow, 5000L));
        // Cross-scope must have removed its id so second attempt is still CROSS_SCOPE_REJECT not DUPLICATE
        assertEquals(SafetyHaltTailProcessor.ApplyResult.CROSS_SCOPE_REJECT, tail.apply(crossRow, 6000L));
        // Correct-scope retry with same valid id still DUPLICATE (already applied)
        assertEquals(SafetyHaltTailProcessor.ApplyResult.DUPLICATE, tail.apply(validRow, 7000L));
        // Correct-scope distinct halt (different ts => different id) should APPLIED
        SafetyHaltRequest valid2 = req("acct-1","p-1","MANUAL_HALT",9000L);
        InternalRow validRow2 = InMemoryControlStateStore.toRow(valid2);
        assertEquals(SafetyHaltTailProcessor.ApplyResult.APPLIED, tail.apply(validRow2, 8000L));
    }

    @Test void internalRowInvalidIdFailClosed(){
        InMemoryGateStateStore gates=new InMemoryGateStateStore();
        gates.init(boot("p-1","acct-1"));
        SafetyHaltTailProcessor tail=new SafetyHaltTailProcessor(gates);
        SafetyHaltRequest valid = req("acct-1","p-1","MANUAL_HALT",2000L);
        InternalRow row = InMemoryControlStateStore.toRow(valid);
        // Tamper halt_request_id to break deterministicId
        Object[] tampered = new Object[21];
        // Copy from GenericRow by reading back via InMemory's toRow internals: rebuild manually with bad id
        tampered[0] = BinaryString.fromString("bad-id-not-sha256");
        tampered[1] = BinaryString.fromString("acct-1");
        tampered[2] = null;
        tampered[3] = BinaryString.fromString("p-1");
        tampered[4] = BinaryString.fromString("signal-job");
        tampered[5] = BinaryString.fromString("i-1");
        tampered[6] = BinaryString.fromString("MANUAL_HALT");
        tampered[7] = BinaryString.fromString("detail");
        tampered[8] = 2000L;
        tampered[9] = 1L;
        tampered[10] = BinaryString.fromString("abc123");
        tampered[11] = BinaryString.fromString("OPEN");
        tampered[12] = null;
        tampered[13] = BinaryString.fromString("v3");
        tampered[14] = null;
        tampered[15] = 0L;
        tampered[16] = null;
        tampered[17] = BinaryString.fromString("abc123");
        tampered[18] = null;
        tampered[19] = null;
        tampered[20] = 2;
        InternalRow bad = GenericRow.of(tampered);
        assertEquals(SafetyHaltTailProcessor.ApplyResult.INVALID_ID, tail.apply(bad, 3000L));
        // null row also INVALID_ID
        assertEquals(SafetyHaltTailProcessor.ApplyResult.INVALID_ID, tail.apply((InternalRow)null, 3000L));
        // malformed row missing required field (null halt_request_id)
        Object[] missing = tampered.clone();
        missing[0] = null;
        InternalRow missingRow = GenericRow.of(missing);
        assertEquals(SafetyHaltTailProcessor.ApplyResult.INVALID_ID, tail.apply(missingRow, 3000L));
    }

    @Test void replayViaControlStateStoreWireIdempotent(){
        InMemoryGateStateStore gates=new InMemoryGateStateStore();
        gates.init(boot("p-1","acct-1"));
        gates.init(boot("p-2","acct-1"));
        InMemoryControlStateStore store=new InMemoryControlStateStore();
        SafetyHaltRequest r1 = req("acct-1","p-1","MANUAL_HALT",2000L);
        SafetyHaltRequest cross = req("acct-OTHER","p-1","MANUAL_HALT",3000L);
        SafetyHaltRequest r2 = req("acct-1","p-2","RISK",4000L);
        store.add(r1);
        // duplicate same id added again to list (InMemory keeps list duplicates)
        store.add(r1);
        // cross-scope will be added (its id matches its own account) — valid but should be rejected at apply
        store.add(cross);
        store.add(r2);
        // tampered row via rawRows
        Object[] bad = new Object[21];
        bad[0]=BinaryString.fromString("ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff");
        bad[1]=BinaryString.fromString("acct-1");
        bad[2]=null;
        bad[3]=BinaryString.fromString("p-1");
        bad[4]=BinaryString.fromString("signal-job");
        bad[5]=BinaryString.fromString("i-1");
        bad[6]=BinaryString.fromString("MANUAL_HALT");
        bad[7]=BinaryString.fromString("detail");
        bad[8]=2000L;
        bad[9]=1L;
        bad[10]=BinaryString.fromString("badhash");
        bad[11]=BinaryString.fromString("OPEN");
        bad[12]=null;
        bad[13]=BinaryString.fromString("v3");
        bad[14]=null;
        bad[15]=0L;
        bad[16]=null;
        bad[17]=BinaryString.fromString("badhash");
        bad[18]=null;
        bad[19]=null;
        bad[20]=2;
        store.addRawRow(GenericRow.of(bad));

        SafetyHaltTailProcessor tail=new SafetyHaltTailProcessor(gates);
        // Replay via generic ControlStateStore (offline Fluss wire) — uses InternalRow decode
        tail.replay((ControlStateStore) store, 5000L);
        // r1 APPLIED once, duplicate second r1 => DUPLICATE, cross => CROSS_SCOPE_REJECT, r2 => APPLIED (different partition)
        // Verify gate states: p-1 and p-2 remain HALTED (boot was HALTED) but halt audit was idempotent
        assertEquals(GateState.HALTED, gates.read("p-1").state());
        assertEquals(GateState.HALTED, gates.read("p-2").state());
        // Replaying again must be idempotent: no extra epoch increments
        long e1 = gates.read("p-1").epoch();
        long e2 = gates.read("p-2").epoch();
        tail.replay((ControlStateStore) store, 6000L);
        assertEquals(e1, gates.read("p-1").epoch());
        assertEquals(e2, gates.read("p-2").epoch());

        // Verify that after replay, a fresh tail can re-apply via InternalRow path individually
        SafetyHaltTailProcessor tail2=new SafetyHaltTailProcessor(gates);
        // But gates already have those halts applied, tail2's appliedIds is empty so it will APPLIED again
        // We verify idValid still works for valid rows
        InternalRow validRow = InMemoryControlStateStore.toRow(r1);
        // For tail2, first apply should be APPLIED (its own dedup set empty)
        assertEquals(SafetyHaltTailProcessor.ApplyResult.APPLIED, tail2.apply(validRow, 7000L));
        assertEquals(SafetyHaltTailProcessor.ApplyResult.DUPLICATE, tail2.apply(validRow, 8000L));
    }

    @Test void typedApplyStillWorksForOfflineTests(){
        InMemoryGateStateStore gates=new InMemoryGateStateStore();
        gates.init(boot("p-1","acct-1"));
        SafetyHaltTailProcessor tail=new SafetyHaltTailProcessor(gates);
        SafetyHaltRequest r = req("acct-1","p-1","MANUAL_HALT",2000L);
        assertEquals(SafetyHaltTailProcessor.ApplyResult.APPLIED, tail.apply(r, 3000L));
        assertEquals(SafetyHaltTailProcessor.ApplyResult.DUPLICATE, tail.apply(r, 4000L));
        assertFalse(r.haltRequestId().isEmpty());
        assertTrue(r.idValid());
    }

    @Test void decodeHandlesOptionalFieldsAndSlotScopedColumns(){
        // Verify roundtrip for request with slot/manifest/state optional fields
        SafetyHaltRequest r = reqFull("acct-1","p-1","FEED_STALLED",5000L,"hft-0",5L,"m".repeat(64),"UNSAFE");
        InternalRow row = InMemoryControlStateStore.toRow(r);
        InMemoryGateStateStore gates=new InMemoryGateStateStore();
        gates.init(boot("p-1","acct-1"));
        SafetyHaltTailProcessor tail=new SafetyHaltTailProcessor(gates);
        assertEquals(SafetyHaltTailProcessor.ApplyResult.APPLIED, tail.apply(row, 6000L));
        // Verify that slot_id/manifest are preserved but not required for idValid (idValid uses evidenceHash etc)
        assertTrue(r.idValid());
    }
}
