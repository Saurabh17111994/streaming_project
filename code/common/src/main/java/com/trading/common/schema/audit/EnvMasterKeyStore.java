package com.trading.common.schema.audit;

import java.util.Base64;
import java.util.Map;
import java.util.function.Function;

/**
 * Production {@link MasterKeyStore} driven by environment variables:
 * {@code EOD_EXPORT_MASTER_KEY_B64} is the current (v1) 32-byte master key in
 * Base64; {@code EOD_EXPORT_MASTER_KEY_V<v>_B64} holds rotated versions
 * ({@code EOD_EXPORT_MASTER_KEY_V2_B64}, ...). Version 1 is required — an
 * export pipeline with no master key is a fail-closed configuration error.
 */
public final class EnvMasterKeyStore implements MasterKeyStore {

    private final Map<Integer, byte[]> keys;
    private final int current;

    private EnvMasterKeyStore(Map<Integer, byte[]> keys) {
        this.keys = keys;
        this.current = keys.keySet().stream().mapToInt(Integer::intValue).max().orElse(0);
    }

    /** Build from an env accessor (the test twin passes a map; prod passes System::getenv). */
    public static EnvMasterKeyStore fromEnv(Function<String, String> env) {
        String v1 = env.apply("EOD_EXPORT_MASTER_KEY_B64");
        if (v1 == null || v1.isBlank()) {
            throw new IllegalArgumentException(
                    "EOD_EXPORT_MASTER_KEY_B64 is required for the encrypted export pipeline "
                            + "(fail-closed: no master key, no export)");
        }
        java.util.TreeMap<Integer, byte[]> keys = new java.util.TreeMap<>();
        keys.put(1, decode(v1));
        for (int v = 2; ; v++) {
            String k = env.apply("EOD_EXPORT_MASTER_KEY_V" + v + "_B64");
            if (k == null || k.isBlank()) {
                break;
            }
            keys.put(v, decode(k));
        }
        return new EnvMasterKeyStore(keys);
    }

    private static byte[] decode(String b64) {
        try {
            return Base64.getDecoder().decode(b64.trim());
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("master key is not valid Base64", e);
        }
    }

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
}
