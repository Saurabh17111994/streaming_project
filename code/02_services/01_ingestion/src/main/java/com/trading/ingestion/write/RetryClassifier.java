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
     */
    public static Classification classify(Throwable t) {
        if (t == null) return Classification.RETRYABLE;

        // Walk the cause chain
        Throwable current = t;
        while (current != null) {
            String msg = current.getMessage();
            String name = current.getClass().getName();

            // ---- Fatal patterns ----
            if (name.contains("Authentication")
                    || name.contains("AccessControl")
                    || name.contains("Security")) {
                return Classification.FATAL;
            }
            if (name.contains("TableNotExist")
                    || name.contains("NoSuchTable")
                    || name.contains("UnknownTable")) {
                return Classification.FATAL;
            }
            if (name.contains("Schema") && (name.contains("Mismatch")
                    || name.contains("Exception")
                    || name.contains("Validation"))) {
                return Classification.FATAL;
            }
            if (msg != null) {
                String lower = msg.toLowerCase();
                if (lower.contains("table") && lower.contains("not found")) {
                    return Classification.FATAL;
                }
                if (lower.contains("unauthorized")
                        || lower.contains("forbidden")
                        || lower.contains("access denied")) {
                    return Classification.FATAL;
                }
            }

            // ---- Retryable patterns ----
            if (name.contains("Timeout")
                    || name.contains("Retriable")
                    || name.contains("NotEnough")
                    || name.contains("Unavailable")
                    || name.contains("Busy")
                    || name.contains("Network")) {
                return Classification.RETRYABLE;
            }
            if (msg != null) {
                String lower = msg.toLowerCase();
                if (lower.contains("timeout")
                        || lower.contains("connection")
                        || lower.contains("refused")
                        || lower.contains("reset")
                        || lower.contains("unavailable")
                        || lower.contains("leader")
                        || lower.contains("coordinator")) {
                    return Classification.RETRYABLE;
                }
            }

            current = current.getCause();
        }

        // Default: assume retryable (Fluss client handles its own retries)
        return Classification.RETRYABLE;
    }
}
