package com.trading.common.observability;

import java.util.Set;
import java.util.Locale;

/**
 * Immutable audit logging with mandatory redaction
 * (docs/08_implementation/01-foundation.md &rarr; "Observability invariant", orig L727).
 */
public final class AuditLogger {

    private AuditLogger() {}

    public static final Set<String> REDACTED_FIELDS =
        Set.of("broker_order_id_secret", "auth_token", "api_key", "password", "secret");

    /**
     * R-134: redaction is case-insensitive and token-contained. The old
     * exact-match {@code REDACTED_FIELDS.contains(field)} leaked data for any
     * casing/format variant (apiKey, authToken, API_KEY, api-key ...). We
     * normalize by stripping non-alphanumerics and compare case-insensitively.
     */
    public static String redact(String field, String value) {
        if (value == null) {
            return null;
        }
        if (field == null) {
            return value;
        }
        String normalized = field.replaceAll("[^A-Za-z0-9]", "").toLowerCase(Locale.ROOT);
        for (String sensitive : REDACTED_FIELDS) {
            String token = sensitive.replaceAll("[^A-Za-z0-9]", "").toLowerCase(Locale.ROOT);
            if (normalized.contains(token)) {
                return "***REDACTED***";
            }
        }
        return value;
    }

    /** An audit record is append-only; mutation in place is forbidden. */
    public static boolean isMutableUpdateAllowed() {
        return false;
    }
}
