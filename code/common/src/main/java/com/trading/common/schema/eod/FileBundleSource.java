package com.trading.common.schema.eod;

import java.nio.file.Files;
import java.nio.file.Path;

/**
 * {@link EncryptedExportEodOffloadExecutor.BundleSource} that reads the
 * plaintext bundle for a record from {@code <recordId>.plain} on the source
 * dir — the drill/dev twin of the (documented, env-gated) Fluss reader: a
 * staging file produced by the export tooling or a prior dump.
 */
public final class FileBundleSource implements EncryptedExportEodOffloadExecutor.BundleSource {

    private final Path sourceDir;

    public FileBundleSource(Path sourceDir) {
        this.sourceDir = sourceDir;
    }

    @Override
    public byte[] read(EodOffloadRecord record) throws Exception {
        Path p = sourceDir.resolve(
                record.tradingDate() + "__" + record.tableName() + ".plain");
        if (!Files.exists(p)) {
            throw new java.io.FileNotFoundException("bundle source missing: " + p);
        }
        return Files.readAllBytes(p);
    }
}
