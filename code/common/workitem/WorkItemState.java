package common.workitem;

/**
 * Work-item lifecycle states (01-foundation.md, "Work item lifecycle").
 *
 * <pre>
 * OPEN -> DOCUMENTING -> DOCUMENTATION_READY -> APPROVED_FOR_IMPLEMENTATION
 *      -> IMPLEMENTED -> VERIFIED -> STRUCK_THROUGH
 * </pre>
 *
 * BLOCKED may be entered from any state when a required external fact,
 * approval, or test environment is unavailable. A blocked item must record
 * the owner, missing evidence, and unblock condition.
 */
public enum WorkItemState {
    OPEN,
    DOCUMENTING,
    DOCUMENTATION_READY,
    APPROVED_FOR_IMPLEMENTATION,
    IMPLEMENTED,
    VERIFIED,
    STRUCK_THROUGH,
    BLOCKED;

    /** Terminal states from which no further transition is allowed. */
    public boolean isTerminal() {
        return this == STRUCK_THROUGH;
    }
}
