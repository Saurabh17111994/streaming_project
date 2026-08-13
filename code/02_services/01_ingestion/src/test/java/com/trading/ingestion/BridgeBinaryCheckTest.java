package com.trading.ingestion;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Startup step 6 (plan §IngestionService): the Go arrow-bridge binary must
 * exist and be runnable before launch. A missing or non-runnable binary is a
 * FATAL startup error — {@link IngestionService#requireBridgeBinary} throws
 * with a clear message so the JVM exits non-zero instead of failing
 * ambiguously at {@code ProcessBuilder.start()}.
 */
@DisplayName("ING start: bridge binary presence + runnable check")
class BridgeBinaryCheckTest {

    @TempDir
    Path tmp;

    @Test
    @DisplayName("missing binary throws with clear message")
    void missingBinaryThrowsWithClearMessage() {
        Path missing = tmp.resolve("no-such-bridge");
        IllegalStateException e = assertThrows(IllegalStateException.class,
                () -> IngestionService.requireBridgeBinary(missing.toString()));
        assertTrue(e.getMessage().contains("not found"), e.getMessage());
        assertTrue(e.getMessage().contains(missing.toString()), e.getMessage());
        assertTrue(e.getMessage().contains("ARROW_BRIDGE_BIN"), e.getMessage());
    }

    @Test
    @DisplayName("empty path throws")
    void emptyPathThrows() {
        IllegalStateException e = assertThrows(IllegalStateException.class,
                () -> IngestionService.requireBridgeBinary("  "));
        assertTrue(e.getMessage().contains("empty"), e.getMessage());
    }

    @Test
    @DisplayName("existing file without execute permission throws")
    void nonExecutableFileThrows() throws IOException {
        Path bin = tmp.resolve("bridge");
        Files.writeString(bin, "#!/bin/sh\necho hi\n");
        // Deterministic: clear the execute bit via POSIX perms when available.
        try {
            Files.setPosixFilePermissions(bin, PosixFilePermissions.fromString("rw-r--r--"));
        } catch (UnsupportedOperationException e) {
            if (!bin.toFile().setExecutable(false)) {
                throw new IllegalStateException("cannot clear execute bit on " + bin);
            }
        }
        IllegalStateException e = assertThrows(IllegalStateException.class,
                () -> IngestionService.requireBridgeBinary(bin.toString()));
        assertTrue(e.getMessage().contains("not runnable"), e.getMessage());
    }

    @Test
    @DisplayName("regular executable file passes")
    void executableFilePasses() throws IOException {
        Path bin = tmp.resolve("bridge");
        Files.writeString(bin, "#!/bin/sh\necho hi\n");
        if (!bin.toFile().setExecutable(true)) {
            throw new IllegalStateException("cannot set execute bit on " + bin);
        }
        assertDoesNotThrow(() -> IngestionService.requireBridgeBinary(bin.toString()));
    }
}
