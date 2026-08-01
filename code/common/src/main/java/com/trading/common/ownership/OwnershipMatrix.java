package com.trading.common.ownership;

import java.util.EnumSet;
import java.util.Set;

/**
 * Ownership matrix (docs/08_implementation/01-foundation.md &rarr; "Ownership matrix", orig L615).
 *
 * <p>Encodes, per data/behavior: the sole writer owner, the allowed readers, and the
 * prohibited owners. Enforcement helpers let a component ask "may I write/read this?".
 */
public final class OwnershipMatrix {

    private OwnershipMatrix() {}

    public enum Component {
        INGESTION, SIGNAL_JOB, ACTION_CAPTURE, EXECUTOR, BABYSITTER, STORAGE, PLATFORM_HEALTH
    }

    public static final class Rule {
        public final String target;
        public final Component soleOwner;
        public final Set<Component> readers;
        public final Set<Component> prohibitedOwners;

        public Rule(String target, Component soleOwner, Set<Component> readers,
                    Set<Component> prohibitedOwners) {
            this.target = target;
            this.soleOwner = soleOwner;
            this.readers = readers;
            this.prohibitedOwners = prohibitedOwners;
        }
    }

    /** True only if {@code actor} is the sole owner and not prohibited. */
    public static boolean canWrite(Rule rule, Component actor) {
        return rule.soleOwner == actor && !rule.prohibitedOwners.contains(actor);
    }

    /** True only if {@code actor} is an allowed reader and not prohibited. */
    public static boolean canRead(Rule rule, Component actor) {
        return rule.readers.contains(actor) && !rule.prohibitedOwners.contains(actor);
    }

    public static Rule logRule(String table, Component owner) {
        return new Rule(table, owner, EnumSet.allOf(Component.class), EnumSet.noneOf(Component.class));
    }
}
