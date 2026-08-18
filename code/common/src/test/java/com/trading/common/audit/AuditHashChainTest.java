package com.trading.common.audit;

import static org.assertj.core.api.Assertions.assertThat;

import com.trading.common.schema.ImmutabilityProtocol;

import java.util.List;

import org.junit.jupiter.api.Test;

/** Unit tests for the policy-controlled audit hash chain. */
class AuditHashChainTest {

    private static final String H1 = ImmutabilityProtocol.canonicalHash("event-one");
    private static final String H2 = ImmutabilityProtocol.canonicalHash("event-two");
    private static final String H3 = ImmutabilityProtocol.canonicalHash("event-three");

    private static AuditHashChain.Manifest manifest(String date, String table, String schema,
                                                    List<AuditHashChain.AuditEvent> events) {
        return new AuditHashChain.Manifest(date, table, schema, events);
    }

    private static AuditHashChain.Manifest manifest20250101() {
        return manifest("2025-01-01", "Execution_Audit", "1",
                List.of(new AuditHashChain.AuditEvent("ev-1", H1),
                        new AuditHashChain.AuditEvent("ev-2", H2)));
    }

    private static AuditHashChain.Manifest manifest20250102() {
        return manifest("2025-01-02", "Execution_Audit", "1",
                List.of(new AuditHashChain.AuditEvent("ev-3", H3)));
    }

    @Test
    void manifestHashIsDeterministic() {
        assertThat(manifest20250101().hash()).isEqualTo(manifest20250101().hash());
        assertThat(manifest20250101().hash()).hasSize(AuditHashChain.HASH_HEX_LENGTH);
    }

    @Test
    void canonicalIsStableAcrossRebuilds() {
        AuditHashChain.Manifest a = new AuditHashChain.ManifestBuilder("2025-01-01", "Execution_Audit", "1")
                .addEvent("ev-1", H1)
                .addEvent("ev-2", H2)
                .build();
        AuditHashChain.Manifest b = new AuditHashChain.ManifestBuilder("2025-01-01", "Execution_Audit", "1")
                .addEvent("ev-1", H1)
                .addEvent("ev-2", H2)
                .build();
        assertThat(a.canonical()).isEqualTo(b.canonical());
        assertThat(a.hash()).isEqualTo(b.hash());
    }

    @Test
    void verifyManifestAgainstSourceValidOnExactReplay() {
        AuditHashChain.Manifest m = manifest20250101();
        assertThat(AuditHashChain.verifyManifestAgainstSource(m, m.events()))
                .isEqualTo(AuditHashChain.Verification.VALID);
    }

    @Test
    void detectTamperedEventContent() {
        AuditHashChain.Manifest m = manifest20250101();
        List<AuditHashChain.AuditEvent> tampered = List.of(
                new AuditHashChain.AuditEvent("ev-1", H1),
                new AuditHashChain.AuditEvent("ev-2", H3)); // ev-2 content changed
        assertThat(AuditHashChain.verifyManifestAgainstSource(m, tampered))
                .isEqualTo(AuditHashChain.Verification.TAMPERED);
    }

    @Test
    void detectMissingEvent() {
        AuditHashChain.Manifest m = manifest20250101();
        List<AuditHashChain.AuditEvent> partial =
                List.of(new AuditHashChain.AuditEvent("ev-1", H1)); // ev-2 dropped
        assertThat(AuditHashChain.verifyManifestAgainstSource(m, partial))
                .isEqualTo(AuditHashChain.Verification.MISSING_EVENT);
    }

    @Test
    void detectReorderedEvents() {
        AuditHashChain.Manifest m = manifest20250101();
        List<AuditHashChain.AuditEvent> reordered = List.of(
                new AuditHashChain.AuditEvent("ev-2", H2),
                new AuditHashChain.AuditEvent("ev-1", H1));
        assertThat(AuditHashChain.verifyManifestAgainstSource(m, reordered))
                .isEqualTo(AuditHashChain.Verification.REORDERED);
    }

    @Test
    void detectDuplicateEventInsideManifest() {
        AuditHashChain.Manifest m = manifest("2025-01-01", "Execution_Audit", "1",
                List.of(new AuditHashChain.AuditEvent("ev-1", H1),
                        new AuditHashChain.AuditEvent("ev-1", H1)));
        assertThat(AuditHashChain.verifyManifestAgainstSource(m, m.events()))
                .isEqualTo(AuditHashChain.Verification.DUPLICATE_EVENT);
    }

    @Test
    void chainRootMatchesAcrossRebuilds() {
        List<AuditHashChain.Manifest> manifests =
                List.of(manifest20250101(), manifest20250102());
        assertThat(AuditHashChain.rootHash(manifests))
                .isEqualTo(AuditHashChain.rootHash(List.of(manifest20250101(), manifest20250102())));
        assertThat(AuditHashChain.rootHash(manifests))
                .hasSize(AuditHashChain.HASH_HEX_LENGTH);
    }

    @Test
    void chainVerifiesValidWhenRootMatches() {
        List<AuditHashChain.Manifest> manifests =
                List.of(manifest20250101(), manifest20250102());
        assertThat(AuditHashChain.verifyChain(manifests, AuditHashChain.rootHash(manifests)))
                .isEqualTo(AuditHashChain.Verification.VALID);
    }

    @Test
    void chainDetectsTamperedManifest() {
        // Verify the tampered chain against the root of the ORIGINAL chain —
        // the recomputed root must no longer match.
        List<AuditHashChain.Manifest> original =
                List.of(manifest20250101(), manifest20250102());
        String expectedRoot = AuditHashChain.rootHash(original);
        AuditHashChain.Manifest tampered = manifest("2025-01-01", "Execution_Audit", "1",
                List.of(new AuditHashChain.AuditEvent("ev-1", H1),
                        new AuditHashChain.AuditEvent("ev-2", H3)));
        List<AuditHashChain.Manifest> manifests = List.of(tampered, manifest20250102());
        assertThat(AuditHashChain.verifyChain(manifests, expectedRoot))
                .isEqualTo(AuditHashChain.Verification.TAMPERED);
    }

    @Test
    void chainDetectsDuplicateEventAcrossManifests() {
        // Same event id in two different-day manifests — a duplicate across
        // the chain (distinct trading dates so the ordering check passes first).
        AuditHashChain.Manifest dayTwo = manifest("2025-01-02", "Execution_Audit", "1",
                List.of(new AuditHashChain.AuditEvent("ev-1", H1)));
        List<AuditHashChain.Manifest> manifests = List.of(manifest20250101(), dayTwo);
        assertThat(AuditHashChain.verifyChain(manifests, AuditHashChain.rootHash(manifests)))
                .isEqualTo(AuditHashChain.Verification.DUPLICATE_EVENT);
    }

    @Test
    void chainDetectsOutOfOrderManifests() {
        List<AuditHashChain.Manifest> manifests = List.of(manifest20250102(), manifest20250101());
        assertThat(AuditHashChain.verifyChain(manifests, AuditHashChain.rootHash(manifests)))
                .isEqualTo(AuditHashChain.Verification.BROKEN_LINK);
    }

    @Test
    void emptyChainIsValidAgainstEmptyRoot() {
        assertThat(AuditHashChain.verifyChain(List.of(), AuditHashChain.rootHash(List.of())))
                .isEqualTo(AuditHashChain.Verification.VALID);
    }
}
