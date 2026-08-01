package com.trading.common.version;

import java.util.List;

/**
 * Runtime version gate
 * (docs/08_implementation/01-foundation.md &rarr; "Implementation rules", orig L347).
 *
 * <p>The platform must fail to start if a required version is absent or equals {@code "latest"}.
 * A placeholder value (see {@link PlaceholderVersions}) is not accepted for a live-money path.
 */
public final class VersionGate {

    private VersionGate() {}

    public static final String LATEST = "latest";

    /** Throws if the resolved version is absent, blank, or literally "latest". */
    public static String requirePinned(String what, String resolved) {
        if (resolved == null || resolved.isBlank()) {
            throw new IllegalStateException("Required version for '" + what + "' is absent; refusing to start.");
        }
        if (LATEST.equalsIgnoreCase(resolved.trim())) {
            throw new IllegalStateException(
                "Required version for '" + what + "' is 'latest'; pin an exact version. Refusing to start.");
        }
        return resolved;
    }

    /**
     * True only when the value is a real, pinned version (present, not latest, not a placeholder)
     * AND capability evidence has been recorded.
     */
    public static boolean isPinnedAndVerified(String what, String resolved, boolean capabilityVerified) {
        try {
            requirePinned(what, resolved);
        } catch (IllegalStateException e) {
            return false;
        }
        if (PlaceholderVersions.isPlaceholder(resolved)) {
            return false;
        }
        return capabilityVerified;
    }

    /** Convenience: fail unless every matrix entry is pinned (used by CI before building images). */
    public static void requireAllPinned(List<String> entries) {
        for (String e : entries) {
            requirePinned("matrix-entry", e);
        }
    }
}
