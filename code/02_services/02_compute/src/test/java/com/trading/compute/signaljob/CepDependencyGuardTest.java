package com.trading.compute.signaljob;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * SIG-UNIT-007 dependency scan: the compute module must never declare or import
 * Apache Flink CEP (project rule in 01-foundation.md — no CEP in the MVP order
 * path). Mirrors the repo's `cep_guard.sh` gate as an in-suite test: scans the
 * module pom.xml and java/scala sources for the forbidden dependency/import
 * literals, excluding build artifacts (target/.git/node_modules).
 *
 * <p>This test class deliberately never spells the forbidden literals in its
 * own source (it would trip itself): the patterns are assembled from parts.
 */
@DisplayName("SIG-UNIT-007 dependency scan — no Flink CEP dependency or import in the compute module")
class CepDependencyGuardTest {

    /** Assembled from parts so this file's own source never matches. */
    private static final Pattern FORBIDDEN = Pattern.compile(
            "flink-" + "cep|org\\.apache\\.flink\\." + "cep");

    private static final List<String> SCANNED_FILENAMES = List.of("pom.xml");
    private static final List<String> SCANNED_SUFFIXES = List.of(".java", ".scala");
    private static final List<String> SKIPPED_DIRS = List.of(".git", "target", "node_modules");

    @Test
    @DisplayName("no Flink CEP dependency declaration or CEP import exists in the module")
    void noFlinkCepDependencyOrImport() throws IOException {
        Path root = moduleRoot();
        List<Path> hits = new ArrayList<>();
        try (Stream<Path> walk = Files.walk(root)) {
            walk.filter(Files::isRegularFile)
                    .filter(this::isScannedFile)
                    .forEach(p -> {
                        if (containsForbidden(p)) {
                            hits.add(root.relativize(p));
                        }
                    });
        }
        assertTrue(hits.isEmpty(),
                "Flink CEP is forbidden in the compute module (01-foundation.md); found in: " + hits);
    }

    private boolean isScannedFile(Path p) {
        String name = p.getFileName().toString();
        if (SCANNED_FILENAMES.contains(name)) {
            return true;
        }
        for (String suffix : SCANNED_SUFFIXES) {
            if (name.endsWith(suffix)) {
                return true;
            }
        }
        return false;
    }

    private boolean containsForbidden(Path p) {
        if (isSkipped(p)) {
            return false;
        }
        try {
            String content = Files.readString(p, StandardCharsets.UTF_8);
            return FORBIDDEN.matcher(content).find();
        } catch (IOException e) {
            // A file that cannot be read must fail the scan loudly, like the
            // shell guard's set -euo pipefail — never silently pass.
            throw new IllegalStateException("cannot scan " + p + " for the CEP guard", e);
        }
    }

    private boolean isSkipped(Path p) {
        for (Path part : p) {
            if (SKIPPED_DIRS.contains(part.toString())) {
                return true;
            }
        }
        return false;
    }

    /**
     * Surefire runs with the module basedir as working directory; also accept
     * a repo-root working directory (IDE runs). Fail loudly if neither is
     * found — a scan of nothing must never pass (mirrors cep_guard.sh R-091).
     */
    private static Path moduleRoot() {
        for (String candidate : List.of("pom.xml", "code/02_services/02_compute/pom.xml")) {
            Path pom = Path.of(candidate);
            if (Files.isRegularFile(pom)) {
                return pom.toAbsolutePath().getParent();
            }
        }
        throw new AssertionError(
                "cannot locate the compute module root (no pom.xml in cwd or repo-relative path)");
    }
}
