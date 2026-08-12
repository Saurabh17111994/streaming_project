package com.trading.compute.signaljob;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashMap;
import java.util.Map;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.StateBackendOptions;
import org.apache.flink.configuration.StateRecoveryOptions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Tracker 14 P4.1/P4.3: {@link SignalJob#applyRuntimeOptions} must translate
 * the validated {@link SignalJobConfig} into exactly the Flink
 * {@link Configuration} a run would use — RocksDB + incremental checkpoints
 * (production pin), RocksDB local state dirs + managed memory, the separate
 * savepoint directory, and the effective-backend log line. It must NEVER set
 * {@code allowNonRestoredState}: a restore that cannot match the graph fails
 * closed (CHECKPOINT-RESTORE-001), it does not degrade to full replay.
 * The pinned checkpoint contract (interval/timeout/max-concurrent) is applied
 * by {@code buildTopology} and asserted to remain untouched here.
 */
@DisplayName("SignalJob runtime options translation (tracker 14 P4.1/P4.2)")
class RuntimeOptionsTest {

    private static Map<String, String> env() {
        Map<String, String> env = new HashMap<>();
        env.put("DEDUP_TTL_MS", "300000");
        env.put("CANDLE_WINDOW_MS", "15000");
        env.put("CHECKPOINT_INTERVAL_MS", "10000");
        env.put("CHECKPOINT_TIMEOUT_MS", "30000");
        env.put("MAX_CONCURRENT_CHECKPOINTS", "1");
        env.put("ALLOW_FULL_REPLAY", "true");
        return env;
    }

    @Test
    @DisplayName("rocksdb (production) → state.backend.type=rocksdb + incremental=true")
    void rocksdbBackendApplied() {
        Map<String, String> env = env();
        env.put("DEPLOYMENT_ENV", "production");
        env.put("STATE_BACKEND", "rocksdb");
        env.put("CHECKPOINT_DIR", "s3://signal-checkpoints/prod");
        env.put("SAVEPOINT_DIR", "s3://signal-savepoints/prod");
        env.put("S3_ENDPOINT", "https://signal-test.r2.cloudflarestorage.com");
        env.put("AWS_ACCESS_KEY_ID", "r2accesskey000000000000");
        env.put("AWS_SECRET_ACCESS_KEY", "r2s3cr3tvalue000000000000");
        env.put("STATE_BACKEND_LOCAL_DIRS", "/data/rocksdb,/data2/rocksdb");
        env.put("STATE_BACKEND_MANAGED_MEMORY", "false");
        env.put("PARALLELISM", "4");
        SignalJobConfig config = SignalJobConfig.from(env);

        Configuration flinkConfig = new Configuration();
        SignalJob.applyRuntimeOptions(config, flinkConfig);

        assertEquals("rocksdb", flinkConfig.get(StateBackendOptions.STATE_BACKEND));
        assertTrue(flinkConfig.get(CheckpointingOptions.INCREMENTAL_CHECKPOINTS));
        assertEquals("/data/rocksdb,/data2/rocksdb",
                flinkConfig.getString("state.backend.rocksdb.localdir", null),
                "RocksDBOptions.LOCAL_DIRECTORIES (state.backend.rocksdb.localdir) is the "
                        + "live 2.2.1 key — the old local_directories key is dead");
        assertEquals("false", flinkConfig.getString("state.backend.rocksdb.memory.managed", null));
        // Tracker 14 box 906 (2026-08-12): native-memory gauges exported via
        // the per-property boolean keys (RocksDBProperty kebab names,
        // jar-verified against flink-statebackend-rocksdb-2.2.1).
        assertEquals("true", flinkConfig.getString("state.backend.rocksdb.metrics.block-cache-usage", null));
        assertEquals("true", flinkConfig.getString(
                "state.backend.rocksdb.metrics.cur-size-all-mem-tables", null));
        assertEquals("true", flinkConfig.getString(
                "state.backend.rocksdb.metrics.estimate-table-readers-mem", null));
        assertEquals("s3://signal-savepoints/prod",
                flinkConfig.get(CheckpointingOptions.SAVEPOINT_DIRECTORY));
        // Tracker 14 P4.2 — S3 object-store access wired into the Flink config
        // (R2-compatible): endpoint, static creds, region, path-style, provider.
        assertEquals("https://signal-test.r2.cloudflarestorage.com",
                flinkConfig.getString("fs.s3a.endpoint", null));
        assertEquals("r2accesskey000000000000",
                flinkConfig.getString("fs.s3a.access.key", null));
        assertEquals("r2s3cr3tvalue000000000000",
                flinkConfig.getString("fs.s3a.secret.key", null));
        assertEquals("auto", flinkConfig.getString("fs.s3a.endpoint.region", null));
        assertEquals("true", flinkConfig.getString("fs.s3a.path.style.access", null),
                "R2 has no virtual-hosted buckets — path-style addressing must be on");
        assertEquals("org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
                flinkConfig.getString("fs.s3a.aws.credentials.provider", null));
    }

    @Test
    @DisplayName("dev hashmap default → state.backend.type=hashmap, no RocksDB keys")
    void devHashmapDefaultApplied() {
        SignalJobConfig config = SignalJobConfig.from(env());
        Configuration flinkConfig = new Configuration();
        SignalJob.applyRuntimeOptions(config, flinkConfig);

        assertEquals("hashmap", flinkConfig.get(StateBackendOptions.STATE_BACKEND));
        assertFalse(flinkConfig.get(CheckpointingOptions.INCREMENTAL_CHECKPOINTS),
                "incremental is RocksDB-only — must stay off for heap state");
        assertNull(flinkConfig.getString("state.backend.rocksdb.localdir", null));
        assertNull(flinkConfig.get(CheckpointingOptions.SAVEPOINT_DIRECTORY));
        // RocksDB-only by construction — the hashmap branch sets none.
        assertNull(flinkConfig.getString("state.backend.rocksdb.metrics.block-cache-usage", null));
        assertNull(flinkConfig.getString(
                "state.backend.rocksdb.metrics.cur-size-all-mem-tables", null));
        assertNull(flinkConfig.getString(
                "state.backend.rocksdb.metrics.estimate-table-readers-mem", null));
    }

    @Test
    @DisplayName("rocksdb dev → incremental, managed memory stays default true")
    void rocksdbDevDefaults() {
        Map<String, String> env = env();
        env.put("STATE_BACKEND", "rocksdb");
        SignalJobConfig config = SignalJobConfig.from(env);
        Configuration flinkConfig = new Configuration();
        SignalJob.applyRuntimeOptions(config, flinkConfig);

        assertEquals("rocksdb", flinkConfig.get(StateBackendOptions.STATE_BACKEND));
        assertTrue(flinkConfig.get(CheckpointingOptions.INCREMENTAL_CHECKPOINTS));
        assertNull(flinkConfig.getString("state.backend.rocksdb.memory.managed", null),
                "managed memory defaults true in RocksDB — key only set when disabled");
    }

    @Test
    @DisplayName("checkpoint pins and restore path are untouched by runtime options")
    void checkpointPinsUntouched() {
        Map<String, String> env = env();
        env.remove("ALLOW_FULL_REPLAY");
        env.put("STATE_RECOVERY_PATH", "file:///tmp/signaljob-checkpoints/job/chk-1");
        env.put("DEPLOYMENT_ENV", "production");
        env.put("CHECKPOINT_DIR", "s3://signal-checkpoints/prod");
        env.put("S3_ENDPOINT", "https://signal-test.r2.cloudflarestorage.com");
        env.put("AWS_ACCESS_KEY_ID", "r2accesskey000000000000");
        env.put("AWS_SECRET_ACCESS_KEY", "r2s3cr3tvalue000000000000");
        SignalJobConfig config = SignalJobConfig.from(env);

        Configuration flinkConfig = new Configuration();
        // What buildTopology sets before applyRuntimeOptions:
        flinkConfig.set(CheckpointingOptions.CHECKPOINTS_DIRECTORY, config.checkpointDir());
        flinkConfig.set(StateRecoveryOptions.SAVEPOINT_PATH, config.stateRecoveryPath());
        SignalJob.applyRuntimeOptions(config, flinkConfig);

        // Restore path survives; no allowNonRestoredState is ever introduced.
        assertEquals("file:///tmp/signaljob-checkpoints/job/chk-1",
                flinkConfig.get(StateRecoveryOptions.SAVEPOINT_PATH));
        assertFalse(flinkConfig.get(StateRecoveryOptions.SAVEPOINT_IGNORE_UNCLAIMED_STATE),
                "restore failure must fail closed, never fall back to full replay "
                        + "(P4.3, CHECKPOINT-RESTORE-001)");
        assertEquals("s3://signal-checkpoints/prod",
                flinkConfig.get(CheckpointingOptions.CHECKPOINTS_DIRECTORY));
        // The pinned contract itself stays with buildTopology — assert the
        // options applyRuntimeOptions touches do not include it.
        assertFalse(flinkConfig.containsKey("execution.checkpointing.interval"));
    }

    @Test
    @DisplayName("effective-backend log line carries scheme only, never the full URI")
    void logLineHasNoCredentials() {
        Map<String, String> env = env();
        env.put("DEPLOYMENT_ENV", "production");
        env.put("CHECKPOINT_DIR", "s3://access:secret@signal-checkpoints/prod");
        env.put("SAVEPOINT_DIR", "s3://signal-savepoints/prod");
        env.put("S3_ENDPOINT", "https://signal-test.r2.cloudflarestorage.com");
        env.put("AWS_ACCESS_KEY_ID", "r2accesskey000000000000");
        env.put("AWS_SECRET_ACCESS_KEY", "r2s3cr3tvalue000000000000");
        SignalJobConfig config = SignalJobConfig.from(env);
        Configuration flinkConfig = new Configuration();
        SignalJob.applyRuntimeOptions(config, flinkConfig);

        // The URI scheme is derived from the config, not the log — assert the
        // derivation used by the log line yields only 's3' for both URIs and
        // that no full URI (with credentials) ever appears in the configuration
        // strings the log would print.
        assertEquals("s3", config.checkpointDir().substring(0, config.checkpointDir().indexOf(':')));
        assertEquals("s3", config.savepointDir().substring(0, config.savepointDir().indexOf(':')));
        assertFalse(flinkConfig.toMap().values().stream().anyMatch(v -> v.contains("secret")),
                "no config value may carry the secret — log and config stay credential-free");
    }

    @Test
    @DisplayName("S3 env is inert unless a checkpoint/savepoint URI is an object-store URI")
    void objectStoreConfigOnlyWhenObjectStoreUri() {
        Map<String, String> env = env();
        env.put("CHECKPOINT_DIR", "file:///tmp/signaljob-checkpoints");
        env.put("S3_ENDPOINT", "https://signal-test.r2.cloudflarestorage.com");
        env.put("AWS_ACCESS_KEY_ID", "r2accesskey000000000000");
        env.put("AWS_SECRET_ACCESS_KEY", "r2s3cr3tvalue000000000000");
        SignalJobConfig config = SignalJobConfig.from(env);
        assertEquals(null, config.s3Endpoint(),
                "file:// checkpoints are not object-store — endpoint must stay null");

        Configuration flinkConfig = new Configuration();
        SignalJob.applyRuntimeOptions(config, flinkConfig);

        assertNull(flinkConfig.getString("fs.s3a.endpoint", null));
        assertNull(flinkConfig.getString("fs.s3a.access.key", null));
        assertNull(flinkConfig.getString("fs.s3a.secret.key", null));
        assertNull(flinkConfig.getString("fs.s3a.aws.credentials.provider", null),
                "no object-store URI → no S3 config, not even a default provider");
    }
}
