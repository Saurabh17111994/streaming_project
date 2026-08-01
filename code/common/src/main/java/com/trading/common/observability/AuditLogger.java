package com.trading.common.observability;

import java.util.Set;

/**
 * Immutable audit logging with mandatory redaction
 * (docs/08_implementation/01-foundation.md &rarr; "Observability invariant", orig L727).
 */
public final class AuditLogger {

    private AuditLogger() {}

    public static final Set<String> REDACTED_FIELDS =
        Set.of("broker_order_id_secret", "auth_token", "api_key", "password", "secret");

    /** Redact sensitive fields before they reach any audit sink. */
    public static String redact(String field, String value) {
        if (value == null) {
            return null;
        }
        return REDACTED_FIELDS.contains(field) ? "***REDACTED***" : value;
    }

    /** An audit record is append-only; mutation in place is forbidden. */
    public static boolean isMutableUpdateAllowed() {
        return false;
    }
}
