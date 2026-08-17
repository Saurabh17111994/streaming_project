package com.trading.common.audit;

import com.trading.common.schema.ImmutabilityProtocol;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Seven-year audit hash chain
 * (docs/08_implementation/01-foundation.md &rarr; "Seven-year audit boundary", orig L537;
 * docs/02_requirements/03-non-functional.md &sect;3.4.1 "Reconstruction integrity").
 *
 * <p>Every audit event carries a content hash. Each per-day manifest hashes its
 * events in order, and each manifest hash links to the previous manifest,
 * forming a chain whose root hash fingerprints the whole retained audit set.
 * Reconstruction verifies the chain and therefore detects a tampered, reordered,
 * or missing event.
 */
public final class AuditHashChain {

    private AuditHashChain() {}

    /** SHA-256 hex length — every hash in the chain has this shape. */
    public static final int HASH_HEX_LENGTH = 64;

    public enum Verification {
        VALID,
        TAMPERED,
        MISSING_EVENT,
        REORDERED,
        DUPLICATE_EVENT,
        BROKEN_LINK
    }

    /** One retained audit record: identity plus the content hash of its canonical bytes. */
    public record AuditEvent(String eventId, String contentHash) {
        public AuditEvent {
            requireNonBlank(eventId, "eventId");
            requireNonBlank(contentHash, "contentHash");
        }
    }

    /** One trading day of events for one table — the hashing unit of the chain. */
    public record Manifest(String tradingDate, String table, String schemaVersion,
                           List<AuditEvent> events) {
        public Manifest {
            requireNonBlank(tradingDate, "tradingDate");
            requireNonBlank(table, "table");
            requireNonBlank(schemaVersion, "schemaVersion");
            events = List.copyOf(events);
        }

        /** Deterministic canonical serialization — the byte source of {@link #hash()}. */
        public String canonical() {
            StringBuilder sb = new StringBuilder();
            sb.append("manifest-v1\n");
            sb.append("date=").append(tradingDate).append('\n');
            sb.append("table=").append(table).append('\n');
            sb.append("schema=").append(schemaVersion).append('\n');
            sb.append("count=").append(events.size()).append('\n');
            for (AuditEvent e : events) {
                sb.append("event=").append(e.eventId()).append(':')
                        .append(e.contentHash()).append('\n');
            }
            return sb.toString();
        }

        public String hash() {
            return ImmutabilityProtocol.canonicalHash(canonical());
        }
    }

    /** Accumulates events for one manifest. */
    public static final class ManifestBuilder {
        private final String tradingDate;
        private final String table;
        private final String schemaVersion;
        private final List<AuditEvent> events = new ArrayList<>();

        public ManifestBuilder(String tradingDate, String table, String schemaVersion) {
            this.tradingDate = tradingDate;
            this.table = table;
            this.schemaVersion = schemaVersion;
        }

        public ManifestBuilder addEvent(String eventId, String contentHash) {
            events.add(new AuditEvent(eventId, contentHash));
            return this;
        }

        public Manifest build() {
            return new Manifest(tradingDate, table, schemaVersion, events);
        }
    }

    /**
     * Verifies a manifest against the events observed at reconstruction time.
     * VALID only when every event is present, in manifest order, with an
     * identical content hash.
     */
    public static Verification verifyManifestAgainstSource(Manifest manifest,
                                                           List<AuditEvent> observed) {
        if (manifest == null || observed == null) {
            return Verification.MISSING_EVENT;
        }
        Set<String> manifestIds = new HashSet<>();
        for (AuditEvent e : manifest.events()) {
            if (!manifestIds.add(e.eventId())) {
                return Verification.DUPLICATE_EVENT;
            }
        }
        Set<String> observedIds = new HashSet<>();
        for (AuditEvent e : observed) {
            if (!observedIds.add(e.eventId())) {
                return Verification.DUPLICATE_EVENT;
            }
        }
        if (!observedIds.equals(manifestIds)) {
            return Verification.MISSING_EVENT;
        }
        List<String> expectedOrder = manifest.events().stream().map(AuditEvent::eventId).toList();
        List<String> observedOrder = observed.stream().map(AuditEvent::eventId).toList();
        if (!expectedOrder.equals(observedOrder)) {
            return Verification.REORDERED;
        }
        for (int i = 0; i < manifest.events().size(); i++) {
            if (!manifest.events().get(i).contentHash().equals(observed.get(i).contentHash())) {
                return Verification.TAMPERED;
            }
        }
        return Verification.VALID;
    }

    /**
     * Linked per-manifest hashes: each manifest's hash commits to the previous
     * one, so a change anywhere in the chain changes every subsequent link.
     */
    public static List<String> linkedHashes(List<Manifest> manifests) {
        List<String> out = new ArrayList<>();
        String previous = "";
        for (Manifest m : manifests) {
            String link = ImmutabilityProtocol.canonicalHash(m.canonical() + "prev=" + previous);
            out.add(link);
            previous = link;
        }
        return out;
    }

    /** Root hash of the whole chain — fingerprints the retained audit set. */
    public static String rootHash(List<Manifest> manifests) {
        List<String> links = linkedHashes(manifests);
        return links.isEmpty()
                ? ImmutabilityProtocol.canonicalHash("")
                : links.get(links.size() - 1);
    }

    /**
     * Verifies the chain against the expected root hash: strictly increasing
     * trading dates, no event id repeated across the chain, and the recomputed
     * root must equal the expected root.
     */
    public static Verification verifyChain(List<Manifest> manifests, String expectedRootHash) {
        if (manifests == null) {
            return Verification.BROKEN_LINK;
        }
        for (int i = 1; i < manifests.size(); i++) {
            if (manifests.get(i - 1).tradingDate().compareTo(manifests.get(i).tradingDate()) >= 0) {
                return Verification.BROKEN_LINK;
            }
        }
        Set<String> seen = new HashSet<>();
        for (Manifest m : manifests) {
            for (AuditEvent e : m.events()) {
                if (!seen.add(e.eventId())) {
                    return Verification.DUPLICATE_EVENT;
                }
            }
        }
        if (expectedRootHash == null || !expectedRootHash.equals(rootHash(manifests))) {
            return Verification.TAMPERED;
        }
        return Verification.VALID;
    }

    private static void requireNonBlank(String value, String field) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(field + " must be non-blank");
        }
    }
}
