package com.trading.common.schema.eod;

import com.trading.common.schema.audit.EnvelopeCrypto;
import com.trading.common.schema.audit.MasterKeyStore;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.time.Instant;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * The encrypted offload half: envelope-encrypts a trading day's source data
 * into an immutable, key-versioned export bundle on the staging target and
 * verifies it by re-opening. Replaces the fail-closed
 * {@link NotConfiguredEodOffloadExecutor} / {@link MockEodOffloadExecutor}
 * when a master key is configured.
 *
 * <p><b>Pipeline</b> (per record): {@link BundleSource#read} acquires the
 * source payload (the Fluss reader is the documented integration — the
 * {@code FileBundleSource} twin serves drills); a fresh 256-bit data key
 * seals the payload (AAD = record id); the data key is wrapped with the
 * master key for {@link MasterKeyStore#currentVersion()}; the sealed payload,
 * the wrapped key, and a JSON manifest (version, key version, row/byte counts,
 * source + target hashes) land on the staging dir. {@link #verify} re-opens
 * with the manifest's key version and re-checks the source hash — fail-closed
 * on wrong key, tamper, or missing version.
 *
 * <p>The R2 push of the staging bundle rides the existing Python tooling
 * ({@code audit_r2.py}) — documented runbook step, not a Java S3 client.
 * {@code icebergSnapshotId} carries the manifest name so the controller's
 * evidence is content-addressed.
 */
public final class EncryptedExportEodOffloadExecutor implements EodOffloadExecutor {

    /** Acquires the plaintext bundle for one offload record. */
    public interface BundleSource {
        byte[] read(EodOffloadRecord record) throws Exception;
    }

    private final Path stagingDir;
    private final MasterKeyStore keyStore;
    private final BundleSource source;
    private final long rowCountProvider;

    public EncryptedExportEodOffloadExecutor(Path stagingDir, MasterKeyStore keyStore,
            BundleSource source, long rowCountProvider) {
        this.stagingDir = stagingDir;
        this.keyStore = keyStore;
        this.source = source;
        this.rowCountProvider = rowCountProvider;
    }

    private static String recordId(EodOffloadRecord r) {
        return r.tradingDate() + "__" + r.tableName();
    }

    private static String hex(byte[] bytes) {
        StringBuilder sb = new StringBuilder(bytes.length * 2);
        for (byte b : bytes) {
            sb.append(String.format("%02x", b));
        }
        return sb.toString();
    }

    private static byte[] sha256(byte[] payload) {
        try {
            return MessageDigest.getInstance("SHA-256").digest(payload);
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    public OffloadResult offload(EodOffloadRecord record) {
        String id = recordId(record);
        try {
            byte[] payload = source.read(record);
            byte[] dataKey = EnvelopeCrypto.newDataKey();
            int keyVersion = keyStore.currentVersion();
            byte[] aad = id.getBytes(StandardCharsets.UTF_8);

            byte[] sealed = EnvelopeCrypto.seal(dataKey, payload, aad);
            byte[] wrapped = EnvelopeCrypto.wrap(keyStore.keyFor(keyVersion), dataKey, aad);

            Path sealedPath = stagingDir.resolve(id + ".enc");
            Path keyPath = stagingDir.resolve(id + ".key.v" + keyVersion);
            Files.createDirectories(stagingDir);
            Files.write(sealedPath, sealed);
            Files.write(keyPath, wrapped);

            Map<String, Object> manifest = new LinkedHashMap<>();
            manifest.put("record_id", id);
            manifest.put("trading_date", record.tradingDate());
            manifest.put("table_name", record.tableName());
            manifest.put("schema_version", record.schemaVersion());
            manifest.put("key_version", keyVersion);
            manifest.put("aad", id);
            manifest.put("sealed_file", sealedPath.getFileName().toString());
            manifest.put("wrapped_key_file", keyPath.getFileName().toString());
            manifest.put("row_count", rowCountProvider);
            manifest.put("byte_count", payload.length);
            manifest.put("source_sha256", hex(sha256(payload)));
            manifest.put("target_sha256", hex(sha256(sealed)));
            manifest.put("created_utc", Instant.now().toString());
            String manifestJson = new com.fasterxml.jackson.databind.ObjectMapper()
                    .writeValueAsString(manifest);
            Path manifestPath = stagingDir.resolve(id + ".manifest.json");
            Files.write(manifestPath, manifestJson.getBytes(StandardCharsets.UTF_8));

            return new OffloadResult(true,
                    record.sourceOffsetStart(), record.sourceOffsetEnd(),
                    rowCountProvider, payload.length,
                    hex(sha256(payload)), hex(sha256(sealed)),
                    "env-export-v1:" + manifestPath.getFileName(), null);
        } catch (Exception e) {
            return OffloadResult.failure("encrypted export failed for " + id + ": " + e);
        }
    }

    @Override
    public boolean verify(EodOffloadRecord committed) {
        String id = recordId(committed);
        try {
            Path manifestPath = stagingDir.resolve(id + ".manifest.json");
            if (!Files.exists(manifestPath)) {
                return false;
            }
            @SuppressWarnings("unchecked")
            Map<String, Object> manifest = new com.fasterxml.jackson.databind.ObjectMapper()
                    .readValue(Files.readAllBytes(manifestPath), Map.class);
            int keyVersion = ((Number) manifest.get("key_version")).intValue();
            byte[] wrapped = Files.readAllBytes(stagingDir.resolve(
                    (String) manifest.get("wrapped_key_file")));
            byte[] sealed = Files.readAllBytes(stagingDir.resolve(
                    (String) manifest.get("sealed_file")));
            byte[] aad = ((String) manifest.get("aad")).getBytes(StandardCharsets.UTF_8);

            byte[] dataKey = EnvelopeCrypto.unwrap(keyStore.keyFor(keyVersion), wrapped, aad);
            byte[] payload = EnvelopeCrypto.open(dataKey, sealed, aad);

            String expectedSource = (String) manifest.get("source_sha256");
            return hex(sha256(payload)).equals(expectedSource)
                    && ((Number) manifest.get("row_count")).longValue() == rowCountProvider
                    && ((Number) manifest.get("byte_count")).longValue() == payload.length;
        } catch (Exception e) {
            return false; // fail-closed: any mismatch is a failed verification
        }
    }
}
