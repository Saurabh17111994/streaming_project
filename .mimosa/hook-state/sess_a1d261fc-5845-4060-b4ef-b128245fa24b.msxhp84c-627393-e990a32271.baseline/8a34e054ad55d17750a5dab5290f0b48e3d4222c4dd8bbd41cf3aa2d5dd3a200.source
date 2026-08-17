package com.trading.ingestion.health;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;

/** Atomic readiness marker for container health checks. */
public final class ReadinessFile {
    private final Path path;
    public ReadinessFile(Path path) { this.path = path; }

    public synchronized void setReady(boolean ready) throws IOException {
        if (ready) {
            Path parent = path.toAbsolutePath().getParent();
            if (parent != null) Files.createDirectories(parent);
            Path tmp = path.resolveSibling(path.getFileName() + ".tmp");
            Files.writeString(tmp, "ready\n", StandardCharsets.UTF_8);
            // R-208: ATOMIC_MOVE is not supported on some NFS/bind-mounted
            // volumes; fall back to a plain replace so the readiness marker
            // still gets written (the marker drives the container probe).
            try {
                Files.move(tmp, path, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
            } catch (AtomicMoveNotSupportedException e) {
                Files.move(tmp, path, StandardCopyOption.REPLACE_EXISTING);
            }
        } else {
            Files.deleteIfExists(path);
        }
    }

    public void clear() throws IOException { setReady(false); }
}
