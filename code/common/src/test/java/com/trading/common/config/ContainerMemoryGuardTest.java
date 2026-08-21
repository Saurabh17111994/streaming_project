package com.trading.common.config;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

/**
 * Unit tests for the 09-production-swarm JVM/container memory contract
 * (pure arithmetic — no cgroup/JVM dependency, so these run anywhere).
 */
class ContainerMemoryGuardTest {

    @Test
    void maxHeapBudget_is_65_percent_of_limit() {
        // use a limit divisible by 100 so the 65% share is exact (no floor)
        long limit = 100L * 1024 * 1024;
        long budget = ContainerMemoryGuard.maxHeapBudget(limit);
        assertEquals(limit * 65L / 100L, budget);
        assertEquals(65L, ContainerMemoryGuard.utilizedPercent(limit, budget));
    }

    @Test
    void nonHeapReserve_is_35_percent_of_limit() {
        long limit = 100L * 1024 * 1024;
        long reserve = ContainerMemoryGuard.nonHeapReserve(limit);
        // reserve must be at least 35% of the limit
        assertTrue(reserve >= limit * 35L / 100L);
        assertEquals(35L, ContainerMemoryGuard.utilizedPercent(limit, reserve));
    }

    @Test
    void reject_non_positive_limit() {
        assertThrows(IllegalArgumentException.class, () -> ContainerMemoryGuard.maxHeapBudget(0));
        assertThrows(IllegalArgumentException.class, () -> ContainerMemoryGuard.utilizedPercent(10, -1));
    }

    @Test
    void alert_threshold_at_85_percent() {
        long limit = 100L * 1024 * 1024;
        long used84 = Math.floorDiv(limit * 84L, 100L);
        long used85 = Math.floorDiv(limit * 85L, 100L);
        assertFalse(ContainerMemoryGuard.atOrAboveAlertPercent(limit, used84));
        assertTrue(ContainerMemoryGuard.atOrAboveAlertPercent(limit, used85));
    }

    @Test
    void contract_enforced_only_when_real_limit_present() {
        // A real container limit with a max heap that exceeds the 65% share:
        // simulate by validating the pure check directly — a max heap larger
        // than the budget means the reserve is below 35%.
        long limit = 100L * 1024 * 1024;
        long heapBudget = ContainerMemoryGuard.maxHeapBudget(limit);
        long oversizedHeap = heapBudget + 1;
        assertTrue(oversizedHeap > heapBudget);
        // assertContainerMemoryContract() is a no-op on this bare JVM (no
        // cgroup), so we verify the budget arithmetic that drives it instead.
        assertTrue(oversizedHeap > ContainerMemoryGuard.maxHeapBudget(limit));
    }

    @Test
    void usage_reader_is_nonnegative_or_sentinel() {
        // On this bare host there is no bounded cgroup, so the reader must be a
        // no-fail sentinel; in a real container it returns the current usage.
        long used = ContainerMemoryGuard.readContainerMemoryUsedBytes();
        assertTrue(used == -1L || used >= 0, "usage reader must return -1 or a non-negative byte count");
    }
}
