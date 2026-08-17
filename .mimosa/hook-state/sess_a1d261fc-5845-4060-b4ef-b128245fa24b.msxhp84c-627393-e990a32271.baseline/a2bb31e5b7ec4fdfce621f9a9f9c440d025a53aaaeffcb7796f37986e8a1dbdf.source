package com.trading.common.schema.audit;

import java.security.SecureRandom;
import javax.crypto.Cipher;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.SecretKeySpec;

/**
 * AES-256-GCM envelope encryption for the encrypted export pipeline: every
 * export bundle gets a fresh 256-bit data key; the payload is sealed with the
 * data key (AAD = the record identity), and the data key itself is wrapped
 * with the master key for the bundle's key version. The plaintext data key is
 *
 * <p>JDK-only (no BouncyCastle). AEAD: 128-bit tag, 96-bit IV.
 */
public final class EnvelopeCrypto {

    private EnvelopeCrypto() {}

    private static final String TRANSFORM = "AES/GCM/NoPadding";
    private static final int TAG_BITS = 128;
    private static final int IV_BYTES = 12;
    private static final int DATA_KEY_BYTES = 32;

    private static final SecureRandom RNG = new SecureRandom();

    /** A fresh 256-bit data key. */
    public static byte[] newDataKey() {
        byte[] key = new byte[DATA_KEY_BYTES];
        RNG.nextBytes(key);
        return key;
    }

    /** Sealed payload layout: {@code iv(12) || ciphertext || tag(16)}. */
    public static byte[] seal(byte[] dataKey, byte[] plaintext, byte[] aad) {
        try {
            Cipher cipher = Cipher.getInstance(TRANSFORM);
            byte[] iv = new byte[IV_BYTES];
            RNG.nextBytes(iv);
            cipher.init(Cipher.ENCRYPT_MODE, new SecretKeySpec(dataKey, "AES"),
                    new GCMParameterSpec(TAG_BITS, iv));
            cipher.updateAAD(aad);
            byte[] ct = cipher.doFinal(plaintext);
            byte[] out = new byte[IV_BYTES + ct.length];
            System.arraycopy(iv, 0, out, 0, IV_BYTES);
            System.arraycopy(ct, 0, out, IV_BYTES, ct.length);
            return out;
        } catch (Exception e) {
            throw new IllegalStateException("AES-GCM seal failed", e);
        }
    }

    /** Reverse of {@link #seal}. Throws (fail-closed) on wrong key / tamper / bad AAD. */
    public static byte[] open(byte[] dataKey, byte[] sealed, byte[] aad) {
        try {
            if (sealed.length <= IV_BYTES) {
                throw new IllegalArgumentException("sealed payload too short");
            }
            Cipher cipher = Cipher.getInstance(TRANSFORM);
            cipher.init(Cipher.DECRYPT_MODE, new SecretKeySpec(dataKey, "AES"),
                    new GCMParameterSpec(TAG_BITS, sealed, 0, IV_BYTES));
            cipher.updateAAD(aad);
            return cipher.doFinal(sealed, IV_BYTES, sealed.length - IV_BYTES);
        } catch (Exception e) {
            throw new IllegalStateException("AES-GCM open failed (wrong key, tamper, or AAD)",
                    e);
        }
    }

    /** Wrap a data key with a master key; same {@code iv || ciphertext || tag} layout. */
    public static byte[] wrap(byte[] masterKey, byte[] dataKey, byte[] aad) {
        return seal(masterKey, dataKey, aad);
    }

    /** Unwrap a wrapped data key with the master key for its version. */
    public static byte[] unwrap(byte[] masterKey, byte[] wrapped, byte[] aad) {
        return open(masterKey, wrapped, aad);
    }
}
