package com.trading.common.observability;

import org.slf4j.MDC;

import java.util.Map;

/**
 * Structured logging with mandatory correlation context
 * (docs/08_implementation/01-foundation.md &rarr; "Observability invariant", orig L727).
 */
public final class StructuredLogger {

    private StructuredLogger() {}

    /** Run an action with a correlation context on the SLF4J MDC, then restore previous state. */
    public static void withContext(Runnable action, Map<String, String> context) {
        Map<String, String> previous = MDC.getCopyOfContextMap();
        try {
            if (context != null) {
                context.forEach(MDC::put);
            }
            action.run();
        } finally {
            if (previous != null) {
                MDC.setContextMap(previous);
            } else {
                MDC.clear();
            }
        }
    }

    /** Stable correlation identity: service / instance / version / trace. */
    public static String correlationId(String service, String instance, String version, String traceId) {
        return service + "/" + instance + "/" + version + "/" + traceId;
    }
}
