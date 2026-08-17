package com.trading.ingestion.health;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;

class ReadinessFileTest {
    @Test
    void transitionsAtomically() throws Exception {
        Path path = Files.createTempDirectory("readiness").resolve("ready");
        ReadinessFile marker = new ReadinessFile(path);
        marker.setReady(true); assertTrue(Files.exists(path));
        marker.setReady(false); assertFalse(Files.exists(path));
        marker.clear();
    }
}
