package com.trading.common.schema.audit;

import java.util.Map;

/**
 * Key-versioned master key material for the encrypted export pipeline. Rotation
 * = a new version added to the store; old versions are retained so previously
 * wrapped data keys stay decryptable (decrypt-old/write-new on rotation).
 * A version missing from the store fails closed — an unwrappable bundle is
 * never silently treated as verified.
 */
public interface MasterKeyStore {

    /** The version new bundles are wrapped with. */
    int currentVersion();

    /** The master key for {@code version} — absent versions fail closed. */
    byte[] keyFor(int version);

    /**
     * Static, test-friendly store from a version -> key map. The current
     * version is the highest present.
     */
    static MasterKeyStore of(Map<Integer, byte[]> keys) {
        int current = keys.keySet().stream().mapToInt(Integer::intValue).max().orElse(0);
        return new MasterKeyStore() {
            @Override
            public int currentVersion() {
                return current;
            }

            @Override
            public byte[] keyFor(int version) {
                byte[] key = keys.get(version);
                if (key == null) {
                    throw new IllegalArgumentException("master key version " + version
                            + " not present (rotation dropped it?)");
                }
                return key;
            }
        };
    }
}
