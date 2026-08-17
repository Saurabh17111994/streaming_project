package com.trading.common.schema;

/**
 * Schema lifecycle state machine
 * (docs/08_implementation/01-foundation.md &rarr; "Schema states", orig L386).
 */
public enum SchemaState {
    PROPOSED,   // authored, not yet approved for application
    APPROVED,   // approved for the pinned matrix; may be applied
    APPLYING,   // being applied to an empty catalog
    OBSERVED,   // applied and verified against the manifest (parity passed)
    REJECTED;   // failed validation / parity; must not be applied

    /** A DDL becomes executable authority only once it reaches APPROVED for the pinned matrix. */
    public boolean isExecutableAuthority() {
        return this == APPROVED;
    }
}
