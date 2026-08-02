package com.trading.common.workitem;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * R-269 — BLOCKED transitions must record owner, missing evidence, and the
 * unblock condition. Also proves the package now lives in the compiled module.
 */
@DisplayName("R-269: WorkItem BLOCKED requires evidence fields")
class WorkItemRegressionTest {

    @Test
    @DisplayName("entering BLOCKED without all three fields is rejected")
    void blockedRequiresEvidence() {
        WorkItem item = WorkItem.create("WI-1", "ops");

        // No owner.
        IllegalStateException e1 = assertThrows(IllegalStateException.class,
                () -> item.transitionTo(WorkItemState.BLOCKED, null, "evidence", "condition"));
        assertTrue(e1.getMessage().contains("owner"));

        // No missing evidence.
        IllegalStateException e2 = assertThrows(IllegalStateException.class,
                () -> item.transitionTo(WorkItemState.BLOCKED, "ops", null, "condition"));
        assertTrue(e2.getMessage().contains("missingEvidence"));

        // No unblock condition.
        IllegalStateException e3 = assertThrows(IllegalStateException.class,
                () -> item.transitionTo(WorkItemState.BLOCKED, "ops", "evidence", "  "));
        assertTrue(e3.getMessage().contains("unblockCondition"));

        // A fully-specified BLOCKED transition succeeds.
        WorkItem blocked = item.transitionTo(
                WorkItemState.BLOCKED, "ops", "no test env", "CI available");
        assertTrue(blocked.state() == WorkItemState.BLOCKED);
        assertTrue(blocked.blockedFrom() == WorkItemState.OPEN);
    }
}
