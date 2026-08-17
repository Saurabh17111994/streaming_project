package com.trading.ingestion.safety;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import org.apache.fluss.client.table.writer.AppendResult;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Phase 6A — slot-scoped safety propagation (plan Amendment).
 *
 * <p>Verifies the pure identity computations: halt_request_id is the SHA-256 of
 * {@code manifest_fingerprint|slot_id|connection_epoch|state|reason_code} (so a
 * re-emitted tuple is a duplicate), and assigned_token_set_hash is the SHA-256
 * of the sorted token set (deterministic regardless of input order).
 */
@DisplayName("ING-SAFE-004: safety request identity")
class SafetyHaltWriterTest {

    @Test
    @DisplayName("halt_request_id is deterministic over the tuple")
    void haltRequestIdDeterministic() {
        String id1 = SafetyHaltWriter.computeHaltRequestId("fp-abc", "hft-0", 3, "UNSAFE", "FEED_STALLED");
        String id2 = SafetyHaltWriter.computeHaltRequestId("fp-abc", "hft-0", 3, "UNSAFE", "FEED_STALLED");
        assertEquals(id1, id2, "same tuple must produce the same id");
        assertEquals(64, id1.length(), "SHA-256 hex is 64 chars");
        assertNotEquals(SafetyHaltWriter.computeHaltRequestId("fp-abc", "hft-0", 3, "UNSAFE", "AUTH_FAILURE"),
                id1, "different reason → different id");
        assertNotEquals(SafetyHaltWriter.computeHaltRequestId("fp-abc", "hft-0", 4, "UNSAFE", "FEED_STALLED"),
                id1, "different epoch → different id");
    }

    @Test
    @DisplayName("assigned token hash is order-independent")
    void assignedTokenHashOrderIndependent() {
        String h1 = SafetyHaltWriter.computeAssignedTokenHash(List.of(3L, 1L, 2L));
        String h2 = SafetyHaltWriter.computeAssignedTokenHash(List.of(1L, 2L, 3L));
        assertEquals(h1, h2, "hash must be over sorted tokens");
        assertEquals(64, h1.length());
    }

    @Test
    @DisplayName("recovered differs from unsafe for the same slot")
    void recoveredDiffersFromUnsafe() {
        String unsafe = SafetyHaltWriter.computeHaltRequestId("fp", "hft-0", 5, "UNSAFE", "FEED_STALLED");
        String recovered = SafetyHaltWriter.computeHaltRequestId("fp", "hft-0", 5, "RECOVERED", "");
        assertNotEquals(unsafe, recovered);
    }

    @Test
    @DisplayName("observe() propagates async append failures (R-034)")
    void observePropagatesAsyncAppendFailures() {
        CompletableFuture<AppendResult> failed =
                CompletableFuture.failedFuture(new RuntimeException("broker down"));
        CompletableFuture<AppendResult> guarded = SafetyHaltWriter.observe(
                failed, "halt-1", "hft-0", SafetyHaltWriter.SafetyState.UNSAFE,
                SafetyHaltWriter.ReasonCode.FEED_STALLED.name(), 7L);
        assertThrows(CompletionException.class, guarded::join,
                "async failure must surface so the halt is not silently lost");
    }

    @Test
    @DisplayName("observe() completes normally on successful append (R-034)")
    void observeCompletesOnSuccess() {
        CompletableFuture<AppendResult> ok = CompletableFuture.completedFuture(null);
        CompletableFuture<AppendResult> guarded = SafetyHaltWriter.observe(
                ok, "halt-2", "hft-0", SafetyHaltWriter.SafetyState.RECOVERED, "", 8L);
        assertEquals(null, guarded.join());
    }
}
