package common.workitem;

/**
 * A tracked work item carrying its lifecycle state and, when blocked, the
 * owner, missing evidence, and unblock condition required by the spec.
 */
public record WorkItem(
        String id,
        WorkItemState state,
        WorkItemState blockedFrom,
        String owner,
        String missingEvidence,
        String unblockCondition) {

    /** Creates a new work item in the OPEN state. */
    public static WorkItem create(String id, String owner) {
        return new WorkItem(id, WorkItemState.OPEN, null, owner, null, null);
    }

    /**
     * Returns a copy in {@code next} state, enforcing
     * {@link WorkItemLifecycle#canTransition}. When entering BLOCKED the
     * prior state is recorded so it can be resumed.
     */
    public WorkItem transitionTo(WorkItemState next,
                                 String owner,
                                 String missingEvidence,
                                 String unblockCondition) {
        if (!WorkItemLifecycle.canTransition(this.state, next)) {
            throw new IllegalStateException(
                    "Illegal transition " + this.state + " -> " + next);
        }
        WorkItemState blockedFrom =
                (next == WorkItemState.BLOCKED) ? this.state : null;
        return new WorkItem(
                this.id, next, blockedFrom, owner, missingEvidence,
                unblockCondition);
    }
}
