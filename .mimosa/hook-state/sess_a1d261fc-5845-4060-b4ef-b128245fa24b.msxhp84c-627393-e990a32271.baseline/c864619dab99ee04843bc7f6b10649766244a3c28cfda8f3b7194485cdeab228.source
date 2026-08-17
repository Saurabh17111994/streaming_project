package com.trading.compute.signaljob;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import org.apache.flink.api.common.serialization.SerializerConfigImpl;
import org.apache.flink.api.java.typeutils.runtime.kryo.KryoSerializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Tracker 14 P5.1 — bytes-per-entry measurement backing the dedup gauge's
 * {@code bytes.estimate}. The gauge's per-entry constant
 * ({@link FingerprintDedupFunction#PER_ENTRY_ESTIMATE_BYTES}) must be an
 * UPPER BOUND on the actually serialized size of a
 * {@code fingerprint → (first_seen, nominal_expiry)} MapState entry — measured
 * here with the same Flink type serializers the HashMap state backend uses.
 * Since CHG-023 item 2 (2026-08-17) the constant includes the +8 B/entry TTL
 * timestamp RocksDB stores per map entry (Flink TTL docs); the expiry-index
 * bucket contribution is gone with the index.
 */
@DisplayName("Dedup per-entry bytes measurement (tracker 14 P5.1)")
class DedupStateSizeTest {

    private static byte[] serialize(Object value, Class<?> type) {
        @SuppressWarnings({"unchecked", "rawtypes"})
        KryoSerializer<Object> serializer = new KryoSerializer(type,
                new SerializerConfigImpl());
        DataOutputSerializer out = new DataOutputSerializer(64);
        try {
            serializer.serialize(value, out);
            return out.getCopyOfBuffer();
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    @Test
    @DisplayName("136 B/entry upper bound covers the serialized key+value for a real state key")
    void perEntryEstimateCoversSerializedSize() {
        // A realistic state key: version | token | fingerprint (raw_table_1 v2
        // fingerprints are ~40 chars).
        String stateKey = "v2|1660|fp-1c8a2e9f4b7d3e6a5c8f0b1d2e3a4c5d6e7f8a9b";
        FingerprintDedupFunction.DedupEntry entry = new FingerprintDedupFunction.DedupEntry(
                1_700_000_000_000L, 1_700_300_000_000L);

        int keyBytes = serialize(stateKey, String.class).length;
        int valueBytes = serialize(entry, FingerprintDedupFunction.DedupEntry.class).length;

        // HashMapStateBackend stores key + value + HashMap overhead (~32-48 B
        // per node); the gauge constant must dominate the serialized payload
        // plus the 8 B TTL timestamp RocksDB adds per map entry.
        assertTrue(keyBytes + valueBytes + 8 < FingerprintDedupFunction.PER_ENTRY_ESTIMATE_BYTES,
                "serialized key+value+ttl " + (keyBytes + valueBytes + 8)
                        + " B must fit the " + FingerprintDedupFunction.PER_ENTRY_ESTIMATE_BYTES
                        + " B/entry upper bound");
    }

    @Test
    @DisplayName("gauge estimate for a full 1.0 GB-scale state stays within the same order")
    void estimateScalesLinearly() {
        long count = 1_000_000L; // 1M live fingerprints
        long estimate = count * FingerprintDedupFunction.PER_ENTRY_ESTIMATE_BYTES;
        // 136 B/entry — 1M entries ≈ 136 MB, same order as the dossier's
        // per-entry envelope; the gauge is an estimate, not a memory profiler.
        assertTrue(estimate >= 100L * 1024L * 1024L, "estimate=" + estimate);
    }
}
