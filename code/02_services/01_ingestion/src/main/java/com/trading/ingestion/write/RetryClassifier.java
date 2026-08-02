package com.trading.ingestion.write;

/**
 * Classifies Fluss append failures as retryable or fatal.
 *
 * <h3>Retryable</h3>
 * Transient failures where the Fluss client's built-in retry should handle
 * recovery: network timeouts, temporary tablet unavailability, coordinator
 * re-election, connection reset.
 *
 * <h3>Fatal</h3>
 * Failures that cannot succeed on retry and should trigger the halt gate:
 * authentication failure, table not found, schema mismatch, authorization
 * denied, table deleted.
 *
 * <p>Dossier reference: {@code docs/08_implementation/03-ingestion.md} §G8.
 */
public final class RetryClassifier {

    private RetryClassifier() {}

    /** Outcome of classifying an exception. */
    public enum Classification {
        /** Safe to retry — transient error. */
        RETRYABLE,
        /** Fatal — halt the append path, open the safety gate. */
        FATAL
    }

    /**
     * Classify a Throwable from a failed append.
     * Inspects the cause chain for known error categories.
     *
     * <p>Fatal patterns take precedence across the <em>entire</em> cause
     * chain (R-038): a retryable wrapper (e.g. an {@code ExecutionException}
     * whose message mentions "connection" or "timeout") must not mask an
     * underlying fatal cause such as {@code AuthenticationException}. Only
     * after the full chain has been checked without finding a fatal cause
     * is the outcome RETRYABLE.
     */
    public static Classification classify(Throwable t) {
        if (t == null) return Classification.RETRYABLE;

        // Walk the entire cause chain; a fatal cause anywhere wins.
        Throwable current = t;
        while (current != null) {
            String msg = current.getMessage();
            String name = current.getClass().getName();

            if (isFatal(name, msg)) return Classification.FATAL;
            // Retryable patterns are only noted — the walk continues so a
            // deeper fatal cause is not masked by a retryable wrapper.

            current = current.getCause();
        }

        // Default: assume retryable (Fluss client handles its own retries)
        return Classification.RETRYABLE;
    }

    /** Fatal patterns — return true if this link of the chain is fatal. */
    private static boolean isFatal(String name, String msg) {
        if (name.contains("Authentication")
                || name.contains("AccessControl")
                || name.contains("Security")) {
            return true;
        }
        if (name.contains("TableNotExist")
                || name.contains("NoSuchTable")
                || name.contains("UnknownTable")) {
            return true;
        }
        if (name.contains("Schema") && (name.contains("Mismatch")
                || name.contains("Exception")
                || name.contains("Validation"))) {
            return true;
        }
        if (msg != null) {
            String lower = msg.toLowerCase();
            if (lower.contains("table") && lower.contains("not found")) {
                return true;
            }
            if (lower.contains("unauthorized")
                    || lower.contains("forbidden")
                    || lower.contains("access denied")) {
                return true;
            }
        }
        return false;
    }
}
