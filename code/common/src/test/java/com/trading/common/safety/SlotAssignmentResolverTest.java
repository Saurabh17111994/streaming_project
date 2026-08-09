package com.trading.common.safety;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Slot-assignment parity with the Go {@code BuildSubscriptionPlan}
 * (go-bridge/subscription_plan.go): sorted tokens chunked into contiguous
 * connections of {@code connectionLimit}; slot ids {@code hft-N}.
 */
@DisplayName("SlotAssignmentResolver: Go BuildSubscriptionPlan parity")
class SlotAssignmentResolverTest {

    @Test
    @DisplayName("1024 tokens in one slot → hft-0 owns all, token lookup works")
    void singleSlotChunk() {
        List<Long> tokens = range(1L, 1024L);
        SlotAssignmentResolver r = SlotAssignmentResolver.of(tokens, 1, 1024);
        assertEquals(List.of("hft-0"), r.slotIds());
        assertEquals("hft-0", r.slotIdOf(1L));
        assertEquals("hft-0", r.slotIdOf(1024L));
        assertNull(r.slotIdOf(0L));
        assertNull(r.slotIdOf(1025L));
        assertEquals(TokenSetHash.of(tokens), r.tokenSetHashOf("hft-0"));
        assertEquals(TokenSetHash.of(tokens), r.manifestFingerprint());
    }

    @Test
    @DisplayName("2048 tokens across two slots; third slot skipped (Go break)")
    void multiSlotChunking() {
        List<Long> tokens = range(1L, 2048L);
        SlotAssignmentResolver r = SlotAssignmentResolver.of(tokens, 3, 1024);
        assertEquals(List.of("hft-0", "hft-1"), r.slotIds());
        assertEquals("hft-0", r.slotIdOf(1L));
        assertEquals("hft-0", r.slotIdOf(1024L));
        assertEquals("hft-1", r.slotIdOf(1025L));
        assertEquals("hft-1", r.slotIdOf(2048L));
        assertEquals(TokenSetHash.of(range(1L, 1024L)), r.tokenSetHashOf("hft-0"));
        assertEquals(TokenSetHash.of(range(1025L, 2048L)), r.tokenSetHashOf("hft-1"));
        assertNull(r.tokenSetHashOf("hft-2"));
    }

    @Test
    @DisplayName("partial last slot: 1500 tokens → hft-0=1024, hft-1=476")
    void partialLastSlot() {
        SlotAssignmentResolver r = SlotAssignmentResolver.of(range(1L, 1500L), 2, 1024);
        assertEquals(List.of("hft-0", "hft-1"), r.slotIds());
        assertEquals("hft-1", r.slotIdOf(1400L));
        assertEquals("hft-1", r.slotIdOf(1500L));
        assertEquals(TokenSetHash.of(range(1025L, 1500L)), r.tokenSetHashOf("hft-1"));
    }

    @Test
    @DisplayName("unsorted input yields identical assignment")
    void unsortedInput() {
        List<Long> shuffled = new ArrayList<>(range(1L, 100L));
        Collections.shuffle(shuffled);
        SlotAssignmentResolver a = SlotAssignmentResolver.of(range(1L, 100L), 1, 1024);
        SlotAssignmentResolver b = SlotAssignmentResolver.of(shuffled, 1, 1024);
        assertEquals(a.manifestFingerprint(), b.manifestFingerprint());
        assertEquals(a.slotIds(), b.slotIds());
    }

    @Test
    @DisplayName("validation mirrors Go: empty, capacity, duplicates, domain, ranges")
    void validation() {
        assertThrows(IllegalArgumentException.class,
                () -> SlotAssignmentResolver.of(List.of(), 1, 1024));
        assertThrows(IllegalArgumentException.class,
                () -> SlotAssignmentResolver.of(range(1L, 2049L), 2, 1024)); // 2049 > 2*1024
        assertThrows(IllegalArgumentException.class,
                () -> SlotAssignmentResolver.of(List.of(1L, 1L), 1, 1024)); // duplicate
        assertThrows(IllegalArgumentException.class,
                () -> SlotAssignmentResolver.of(List.of(0L), 1, 1024));     // non-positive
        assertThrows(IllegalArgumentException.class,
                () -> SlotAssignmentResolver.of(List.of(-5L), 1, 1024));    // negative
        assertThrows(IllegalArgumentException.class,
                () -> SlotAssignmentResolver.of(List.of(1L), 0, 1024));     // slots < 1
        assertThrows(IllegalArgumentException.class,
                () -> SlotAssignmentResolver.of(List.of(1L), 4, 1024));     // slots > 3
        assertThrows(IllegalArgumentException.class,
                () -> SlotAssignmentResolver.of(List.of(1L), 1, 0));        // limit < 1
        assertThrows(IllegalArgumentException.class,
                () -> SlotAssignmentResolver.of(List.of(1L), 1, 1025));     // limit > 1024
    }

    private static List<Long> range(long from, long toInclusive) {
        List<Long> list = new ArrayList<>((int) (toInclusive - from + 1));
        for (long v = from; v <= toInclusive; v++) {
            list.add(v);
        }
        return list;
    }

    @Test
    @DisplayName("slotIdOf covers the full subscribed domain")
    void domainCoverage() {
        SlotAssignmentResolver r = SlotAssignmentResolver.of(range(1L, 100L), 1, 1024);
        for (long t = 1L; t <= 100L; t++) {
            assertTrue("hft-0".equals(r.slotIdOf(t)), "token " + t);
        }
    }
}
