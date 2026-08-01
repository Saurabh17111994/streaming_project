package common.workitem;

import java.util.List;

/**
 * Transition rules for {@link WorkItemState}. Encodes the forward lifecycle
 * chain plus the rule that BLOCKED is reachable from any state and resumable
 * to any state.
 */
public final class WorkItemLifecycle {

    /** Ordered forward chain excluding the BLOCKED escape state. */
    private static final List<WorkItemState> FORWARD_CHAIN = List.of(
            WorkItemState.OPEN,
            WorkItemState.DOCUMENTING,
            WorkItemState.DOCUMENTATION_READY,
            WorkItemState.APPROVED_FOR_IMPLEMENTATION,
            WorkItemState.IMPLEMENTED,
            WorkItemState.VERIFIED,
            WorkItemState.STRUCK_THROUGH);

    private WorkItemLifecycle() {
    }

    /** True if {@code to} is a legal next state from {@code from}. */
    public static boolean canTransition(WorkItemState from,
                                        WorkItemState to) {
        if (from == null || to == null) {
            return false;
        }
        if (from.isTerminal()) {
            return false; // STRUCK_THROUGH is final
        }
        if (to == WorkItemState.BLOCKED) {
            return true; // any live state can block
        }
        if (from == WorkItemState.BLOCKED) {
            return true; // resume from blocked to any state
        }
        int fromIdx = FORWARD_CHAIN.indexOf(from);
        int toIdx = FORWARD_CHAIN.indexOf(to);
        return fromIdx >= 0 && toIdx == fromIdx + 1;
    }
}
