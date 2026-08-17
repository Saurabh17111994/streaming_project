package com.trading.common.evidence;

/**
 * Project status vocabulary used across docs and trackers
 * (docs/08_implementation/01-foundation.md &rarr; "Status vocabulary", orig L119).
 */
public enum StatusVocabulary {
    DESIGN_CLOSED,                  // design accepted
    IMPLEMENTATION_ACTIVE,          // being built
    RUNTIME_VALIDATION_PENDING,     // built, not yet proven in runtime
    LIVE_MONEY_BLOCKED;             // must not move real money

    /** No foundation stage ever permits live money until explicitly cleared downstream. */
    public boolean isLiveMoneyAllowed() {
        return false;
    }
}
