package com.trading.common.schema.execution;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.Objects;

/** Offline value of one Safety_Halt_Requests KV row. Deterministic PK = SHA256 hex. */
public record SafetyHaltRequest(
        String haltRequestId,
        String accountScopeId,
        String executionPartitionId,
        String sourceComponent,
        String sourceInstance,
        String reasonCode,
        String reasonDetail,
        long detectionTime,
        long sourceEpoch,
        String evidenceHash,
        String schemaVersion,
        String slotId,
        long connectionEpoch,
        String manifestFingerprint,
        String state) {

    public SafetyHaltRequest {
        Objects.requireNonNull(accountScopeId, "accountScopeId");
        Objects.requireNonNull(sourceComponent, "sourceComponent");
        Objects.requireNonNull(reasonCode, "reasonCode");
        Objects.requireNonNull(evidenceHash, "evidenceHash");
        Objects.requireNonNull(schemaVersion, "schemaVersion");
    }

    /** Canonical §Gate tuple: account|partition|source|reason|detectionTime|sourceEpoch|evidenceHash|schemaVersion */
    public static String deterministicId(String accountScopeId, String executionPartitionId,
            String sourceComponent, String reasonCode, long detectionTime,
            long sourceEpoch, String evidenceHash, String schemaVersion) {
        String canonical = String.join("|",
                accountScopeId, executionPartitionId == null ? "" : executionPartitionId,
                sourceComponent, reasonCode,
                Long.toString(detectionTime), Long.toString(sourceEpoch),
                evidenceHash, schemaVersion);
        try {
            MessageDigest d = MessageDigest.getInstance("SHA-256");
            byte[] h = d.digest(canonical.getBytes(StandardCharsets.UTF_8));
            StringBuilder sb = new StringBuilder(h.length*2);
            for (byte b: h) sb.append(String.format("%02x", b));
            return sb.toString();
        } catch (Exception e) { throw new IllegalStateException(e); }
    }
    public static String deterministicId(SafetyHaltRequest r){
        return deterministicId(r.accountScopeId(), r.executionPartitionId(), r.sourceComponent(),
                r.reasonCode(), r.detectionTime(), r.sourceEpoch(), r.evidenceHash(), r.schemaVersion());
    }
    /** Verifies supplied haltRequestId matches canonical — fail-closed on mismatch. */
    public boolean idValid(){ return haltRequestId != null && haltRequestId.equals(deterministicId(this)); }
}
