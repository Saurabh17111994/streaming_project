package com.trading.common.schema.execution;

import com.trading.common.model.GateState;
import org.junit.jupiter.api.Test;

import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Fencing lease renew/revoke coverage — offline, no Arrow, HALTED default.
 *
 * Proves the missing paths on top of {@link FencingLeaseTtlTest}:
 * 1. TTL expiry => fenceExpiredAt true, fenceValidFor false
 * 2. renew before expiry extends lease (fenceValidFor true longer, fenceToken unchanged)
 * 3. renew after expiry fails (conflict)
 * 4. revoke clears fence (after revoke, new owner can acquire immediately, fenceToken increments)
 * 5. different owner cannot renew another's fence
 *
 * Also pins HALTED default: HALT clears fence (offline fenced-off).
 */
class FencingLeaseRenewRevokeTest {

    private static final long LEASE_MS = 5000L;

    private InMemoryGateStateStore freshStore() {
        InMemoryGateStateStore store = new InMemoryGateStateStore(Set.of("saurabh"));
        GateRow boot = new GateRow("p1", "acct1", GateState.HALTED, 0, "boot", "h0",
                null, null, null, null, 0L, null, null, null);
        store.init(boot);
        return store;
    }

    @Test
    void ttlExpiryMeansExpiredAndInvalid() {
        InMemoryGateStateStore store = freshStore();
        long now = 1000L;
        var res = store.acquire("p1", "owner1", LEASE_MS, now);
        assertFalse(res.conflict());
        GateRow row = store.read("p1");
        // before expiry valid
        assertFalse(row.fenceExpiredAt(now + 4000));
        assertTrue(row.fenceValidFor("owner1", row.fenceToken(), now + 4000));
        // after expiry expired and invalid
        assertTrue(row.fenceExpiredAt(now + 6000));
        assertFalse(row.fenceValidFor("owner1", row.fenceToken(), now + 6000));
        assertFalse(row.fenceValidFor("owner1", row.fenceToken() - 1, now));
    }

    @Test
    void renewBeforeExpiryExtendsLeaseWithoutChangingToken() {
        InMemoryGateStateStore store = freshStore();
        long now = 1000L;
        var acquired = store.acquire("p1", "owner1", LEASE_MS, now);
        assertFalse(acquired.conflict());
        long token = acquired.row().fenceToken();
        GateRow before = store.read("p1");
        assertEquals(now + LEASE_MS, before.leaseExpiresTs());

        // renew at 3000 (2s before original expiry 6000) for another 5000ms => new expiry 8000
        long renewAt = 3000L;
        var renewed = store.renew("p1", "owner1", token, LEASE_MS, renewAt);
        assertFalse(renewed.conflict(), "renew before expiry must succeed");
        GateRow after = store.read("p1");
        assertEquals(token, after.fenceToken(), "fenceToken must not increment on renew");
        assertEquals(renewAt + LEASE_MS, after.leaseExpiresTs(), "lease must be extended to renewAt+leaseMs");
        // would have expired at 6000, now valid at 7000
        assertTrue(after.fenceValidFor("owner1", token, 7000L), "renewed lease must be valid past original expiry");
        // but still expires at new horizon
        assertTrue(after.fenceExpiredAt(9000L));
        assertFalse(after.fenceValidFor("owner1", token, 9000L));
        // fenceAcquiredTs stays original
        assertEquals(before.fenceAcquiredTs(), after.fenceAcquiredTs());
        // fenceLostTs stays null
        assertNull(after.fenceLostTs());
        // audit recorded
        assertTrue(store.auditLog().stream().anyMatch(a -> "FENCE_RENEW".equals(a.eventType())));
    }

    @Test
    void renewAfterExpiryFailsWithConflict() {
        InMemoryGateStateStore store = freshStore();
        long now = 1000L;
        var acquired = store.acquire("p1", "owner1", LEASE_MS, now);
        long token = acquired.row().fenceToken();
        long originalExpiry = store.read("p1").leaseExpiresTs();

        // try renew at 7000 (after expiry 6000)
        var conflict = store.renew("p1", "owner1", token, LEASE_MS, 7000L);
        assertTrue(conflict.conflict(), "renew after expiry must be rejected");
        GateRow still = store.read("p1");
        assertEquals(originalExpiry, still.leaseExpiresTs(), "expiry must not change on failed renew");
        assertEquals(token, still.fenceToken());
        assertTrue(still.fenceExpiredAt(7000L));
        assertFalse(still.fenceValidFor("owner1", token, 7000L));
    }

    @Test
    void revokeClearsFenceAndAllowsImmediateReacquireWithIncrementedToken() {
        InMemoryGateStateStore store = freshStore();
        long now = 1000L;
        var first = store.acquire("p1", "owner1", LEASE_MS, now);
        long token1 = first.row().fenceToken();
        assertTrue(store.read("p1").fenceValidFor("owner1", token1, now));

        // revoke by holder at 2000
        long revokeAt = 2000L;
        GateRow revoked = store.revoke("p1", "owner1", revokeAt);
        assertNotNull(revoked);
        assertNull(revoked.ownerInstanceId(), "owner must be cleared");
        assertEquals(0L, revoked.fenceToken(), "token must be 0 after revoke");
        assertNull(revoked.leaseExpiresTs(), "lease must be null after revoke");
        assertNull(revoked.fenceAcquiredTs(), "acquired ts must be null after revoke");
        assertEquals(revokeAt, revoked.fenceLostTs(), "lost ts must be revoke time");
        // fence no longer valid for old owner
        assertFalse(revoked.fenceValidFor("owner1", token1, revokeAt));
        assertFalse(revoked.fenceValidFor("owner1", token1, revokeAt + 100));

        // new owner can acquire immediately before original lease would have expired (original expiry 6000)
        var second = store.acquire("p1", "owner2", LEASE_MS, revokeAt + 1);
        assertFalse(second.conflict(), "after revoke new owner must acquire immediately");
        assertTrue(second.token() > token1, "fenceToken must increment monotonically after revoke");
        GateRow afterAcquire = store.read("p1");
        assertEquals("owner2", afterAcquire.ownerInstanceId());
        assertEquals(second.token(), afterAcquire.fenceToken());
        assertTrue(afterAcquire.fenceValidFor("owner2", second.token(), revokeAt + 1));
        assertFalse(afterAcquire.fenceValidFor("owner1", token1, revokeAt + 1));

        // renew on revoked (no fence) must fail
        var renewAfterRevoke = store.renew("p1", "owner2", second.token() + 99, LEASE_MS, revokeAt + 2);
        // wrong token => conflict
        assertTrue(renewAfterRevoke.conflict());
        // correct token but after revoke holder is owner2, so owner1 cannot renew
        var wrongOwnerRenew = store.renew("p1", "owner1", second.token(), LEASE_MS, revokeAt + 2);
        assertTrue(wrongOwnerRenew.conflict());

        // audit
        assertTrue(store.auditLog().stream().anyMatch(a -> "FENCE_REVOKE".equals(a.eventType())));
    }

    @Test
    void differentOwnerCannotRenewAnothersFence() {
        InMemoryGateStateStore store = freshStore();
        long now = 1000L;
        var acquired = store.acquire("p1", "owner1", LEASE_MS, now);
        long token = acquired.row().fenceToken();
        long originalExpiry = store.read("p1").leaseExpiresTs();

        // owner2 tries to renew with same token but different owner
        var conflict = store.renew("p1", "owner2", token, LEASE_MS, now + 100);
        assertTrue(conflict.conflict(), "different owner must not renew another's fence");
        GateRow unchanged = store.read("p1");
        assertEquals(originalExpiry, unchanged.leaseExpiresTs(), "lease must not change on rejected renew");
        assertEquals("owner1", unchanged.ownerInstanceId());
        assertEquals(token, unchanged.fenceToken());
        // still valid for real owner
        assertTrue(unchanged.fenceValidFor("owner1", token, now + 100));
        assertFalse(unchanged.fenceValidFor("owner2", token, now + 100));
    }

    @Test
    void revokeByNonHolderDoesNotClearFence() {
        InMemoryGateStateStore store = freshStore();
        long now = 5000L;
        var acquired = store.acquire("p1", "owner1", LEASE_MS, now);
        long token = acquired.row().fenceToken();

        // non-holder tries revoke
        GateRow after = store.revoke("p1", "owner2", now + 100);
        // should not clear
        assertEquals("owner1", after.ownerInstanceId());
        assertEquals(token, after.fenceToken());
        assertNotNull(after.leaseExpiresTs());
        assertTrue(after.fenceValidFor("owner1", token, now + 100));
        // holder still cannot be superseded while live
        var conflict = store.acquire("p1", "owner2", LEASE_MS, now + 200);
        assertTrue(conflict.conflict());
    }

    @Test
    void renewWithStaleTokenFails() {
        InMemoryGateStateStore store = freshStore();
        long now = 1000L;
        var acquired = store.acquire("p1", "owner1", LEASE_MS, now);
        long token = acquired.row().fenceToken();
        var stale = store.renew("p1", "owner1", token - 1, LEASE_MS, now + 100);
        assertTrue(stale.conflict());
        // also wrong future token
        var future = store.renew("p1", "owner1", token + 99, LEASE_MS, now + 100);
        assertTrue(future.conflict());
    }

    @Test
    void haltClearsFenceAndHaltedDefault() {
        InMemoryGateStateStore store = freshStore();
        long now = 1000L;
        var acquired = store.acquire("p1", "owner1", LEASE_MS, now);
        long token = acquired.row().fenceToken();
        assertTrue(store.read("p1").fenceValidFor("owner1", token, now));

        // halt at 2000 — even though fence is live, HALTED default must clear fence
        GateRow halted = store.halt("p1", store.read("p1"), "test-halt", "h1", 2000L);
        assertEquals(GateState.HALTED, halted.state());
        assertNull(halted.ownerInstanceId(), "HALT must clear owner");
        assertEquals(0L, halted.fenceToken(), "HALT must clear fenceToken to 0");
        assertNull(halted.leaseExpiresTs(), "HALT must clear lease");
        assertNull(halted.fenceAcquiredTs(), "HALT must clear acquiredTs");
        assertNotNull(halted.fenceLostTs(), "HALT must record fenceLostTs");
        assertFalse(halted.fenceValidFor("owner1", token, 2000L));

        // after halt, renew must fail (no live lease)
        var renewAfterHalt = store.renew("p1", "owner1", token, LEASE_MS, 2100L);
        assertTrue(renewAfterHalt.conflict());

        // new owner can acquire immediately after halt (no need to wait for TTL)
        var reacquire = store.acquire("p1", "owner2", LEASE_MS, 2100L);
        assertFalse(reacquire.conflict());
        assertTrue(reacquire.token() > token);
        assertEquals("owner2", store.read("p1").ownerInstanceId());

        // idempotent halt while already HALTED must not increment epoch a second time, but fence stays cleared
        long epochAfterFirstHalt = halted.epoch();
        GateRow secondHalt = store.halt("p1", store.read("p1"), "again", "h2", 3000L);
        assertEquals(epochAfterFirstHalt, secondHalt.epoch(), "HALTED halt must not increment epoch");
        assertNull(secondHalt.ownerInstanceId());
        assertEquals(0L, secondHalt.fenceToken());
    }

    @Test
    void renewOnHaltedWithNoFenceFails() {
        InMemoryGateStateStore store = freshStore();
        // boot HALTED has no fence — renew must fail
        GateRow boot = store.read("p1");
        assertNull(boot.ownerInstanceId());
        var res = store.renew("p1", "owner1", 1L, LEASE_MS, 1000L);
        assertTrue(res.conflict());
    }

    @Test
    void releaseAliasClearsFence() {
        InMemoryGateStateStore store = freshStore();
        long now = 1000L;
        store.acquire("p1", "owner1", LEASE_MS, now);
        GateRow viaRelease = store.release("p1", "owner1", 2000L);
        assertNull(viaRelease.ownerInstanceId());
        assertEquals(0L, viaRelease.fenceToken());
        // after release, new acquire works
        var reacq = store.acquire("p1", "owner2", LEASE_MS, 2001L);
        assertFalse(reacq.conflict());
    }
}
