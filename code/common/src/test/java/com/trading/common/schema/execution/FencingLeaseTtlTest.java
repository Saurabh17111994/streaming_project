package com.trading.common.schema.execution;

import com.trading.common.model.GateState;
import org.junit.jupiter.api.Test;
import java.util.Set;
import static org.junit.jupiter.api.Assertions.*;

class FencingLeaseTtlTest {
    @Test void acquireAndExpiryAndRefresh() {
        InMemoryGateStateStore store = new InMemoryGateStateStore(Set.of("saurabh"));
        GateRow boot = new GateRow("p1","acct1",GateState.HALTED,0,"boot","h0",null,null,null,null,0L,null,null,null);
        store.init(boot);
        long now = 1000L;
        var res = store.acquire("p1","owner1", 5000L, now);
        assertFalse(res.conflict());
        GateRow row = store.read("p1");
        assertTrue(row.fenceValidFor("owner1", row.fenceToken(), now));
        assertFalse(row.fenceExpiredAt(now));
        // still valid before expiry
        assertTrue(row.fenceValidFor("owner1", row.fenceToken(), now + 4000));
        // expired after lease
        assertTrue(row.fenceExpiredAt(now + 6000));
        assertFalse(row.fenceValidFor("owner1", row.fenceToken(), now + 6000));
        // stale token
        assertFalse(row.fenceValidFor("owner1", row.fenceToken()-1, now));
        // different owner rejected when live
        var conflict = store.acquire("p1","owner2", 5000L, now + 100);
        assertTrue(conflict.conflict());
        // after expiry, different owner can acquire
        var after = store.acquire("p1","owner2", 5000L, now + 6000);
        assertFalse(after.conflict());
        assertTrue(after.row().fenceToken() > row.fenceToken());
    }
    @Test void haltBootsHaltedNoSecondEpoch() {
        InMemoryGateStateStore store = new InMemoryGateStateStore(Set.of("saurabh"));
        GateRow boot = new GateRow("p1","acct1",GateState.HALTED,0,"boot","h0",null,null,null,null,0L,null,null,null);
        store.init(boot);
        long epoch0 = store.read("p1").epoch();
        store.halt("p1", store.read("p1"), "test", "h1", 2000L);
        assertEquals(epoch0, store.read("p1").epoch(), "HALTED halt must not increment epoch");
    }
}
