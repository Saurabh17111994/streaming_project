package com.trading.common.schema.eod;

import com.trading.common.schema.EodControllerState;
import java.time.LocalDate;
import java.util.Map;

/**
 * Durable per-day per-table offload record for the EOD controller (SCH-23;
 * docs/08_implementation/02-schema-storage.md "EOD controller and offload
 * gate").
 *
 * <p>Fields follow the dossier's offload-record spec: trading date, table and
 * schema version, source offset/range, row and byte counts, source/target
 * hashes, Iceberg snapshot/commit ID, verification state, retry count + next
 * retry, and the earliest allowed source expiry.
 *
 * <p>{@code tradingDate} is an ISO-8601 date string (yyyy-MM-dd) — the record
 * is Jackson-serializable without the jsr310 module (the repo pins
 * jackson-databind only). {@link #formatTradingDate}/{@link #parseTradingDate}
 * convert to/from {@link LocalDate} for arithmetic.
 *
 * <p><b>Transitions are validated, never assumed:</b> {@link #transition}
 * enforces the PENDING → WRITING → COMMITTED → VERIFYING → VERIFIED state
 * machine with the two failure exits and the retry/reconcile edges, so a
 * VERIFIED day can never silently regress. Source data cannot expire unless
 * the state is {@link EodControllerState#VERIFIED}; every non-VERIFIED day
 * requires retention extension ({@link #requiresRetentionExtension()}, the
 * load-bearing rule of {@link EodControllerState}).
 *
 * @param tradingDate ISO-8601 trading date (yyyy-MM-dd)
 * @param tableName physical table the record covers
 * @param schemaVersion table contract version at offload time
 * @param sourceOffsetStart inclusive source offset range start (-1 unknown)
 * @param sourceOffsetEnd exclusive source offset range end (-1 unknown)
 * @param rowCount rows copied to the lake target
 * @param byteCount bytes copied to the lake target
 * @param sourceHash source content hash/checksum ("" until computed)
 * @param targetHash target content hash/checksum ("" until verified)
 * @param icebergSnapshotId lake snapshot/commit ID ("" until committed)
 * @param state verification state (never null)
 * @param retryCount retries consumed for the current failure/offload
 * @param nextRetryAtMs epoch-millis of the next automatic retry (0 when none)
 * @param earliestAllowedSourceExpiryMs earliest instant the source may expire;
 *        Long.MAX_VALUE while unverified (never), set at VERIFIED time
 * @param updatedAtMs epoch-millis of the last transition
 */
public record EodOffloadRecord(
        String tradingDate,
        String tableName,
        String schemaVersion,
        long sourceOffsetStart,
        long sourceOffsetEnd,
        long rowCount,
        long byteCount,
        String sourceHash,
        String targetHash,
        String icebergSnapshotId,
        EodControllerState state,
        int retryCount,
        long nextRetryAtMs,
        long earliestAllowedSourceExpiryMs,
        long updatedAtMs) {

    private static final long NEVER = Long.MAX_VALUE;

    /** Legal transition map (state machine in 02-schema-storage.md). */
    private static final Map<EodControllerState, java.util.Set<EodControllerState>> LEGAL = Map.of(
            EodControllerState.PENDING, java.util.Set.of(EodControllerState.WRITING),
            EodControllerState.WRITING, java.util.Set.of(EodControllerState.COMMITTED,
                    EodControllerState.FAILED_RETRYABLE, EodControllerState.FAILED_MANUAL),
            EodControllerState.COMMITTED, java.util.Set.of(EodControllerState.VERIFYING,
                    EodControllerState.FAILED_RETRYABLE, EodControllerState.FAILED_MANUAL),
            EodControllerState.VERIFYING, java.util.Set.of(EodControllerState.VERIFIED,
                    EodControllerState.FAILED_RETRYABLE, EodControllerState.FAILED_MANUAL),
            EodControllerState.FAILED_RETRYABLE, java.util.Set.of(EodControllerState.WRITING,
                    EodControllerState.VERIFYING, EodControllerState.FAILED_MANUAL),
            EodControllerState.FAILED_MANUAL, java.util.Set.of(EodControllerState.PENDING));

    /**
     * New PENDING record for a trading day/table. Earliest allowed source
     * expiry starts at {@code NEVER} (Long.MAX_VALUE) — while unverified the
     * source must not expire; the planner/controller only releases it when the
     * day reaches VERIFIED.
     */
    public static EodOffloadRecord initial(LocalDate tradingDate, String tableName,
            String schemaVersion, long nowMs) {
        return new EodOffloadRecord(formatTradingDate(tradingDate), tableName, schemaVersion,
                -1, -1, 0, 0, "", "", "", EodControllerState.PENDING, 0, 0, NEVER, nowMs);
    }

    /** True when {@code from -> to} is a legal transition of the state machine. */
    public static boolean isLegalTransition(EodControllerState from, EodControllerState to) {
        return LEGAL.getOrDefault(from, java.util.Set.of()).contains(to);
    }

    /**
     * Validate and apply a transition. Returns a new record with the updated
     * state/timestamps; throws {@link IllegalStateException} on any illegal or
     * regressive transition. Side effects:
     *
     * <ul>
     *   <li>entering FAILED_RETRYABLE: retryCount++ and nextRetryAtMs computed
     *       via {@link EodBackoff};</li>
     *   <li>leaving FAILED_RETRYABLE (retry): nextRetryAtMs cleared (the
     *       controller reschedules when the retry fails again);</li>
     *   <li>reaching VERIFIED: earliestAllowedSourceExpiryMs = now — from this
     *       instant the day's data may expire under the retention policy
     *       (permitsSourceExpiry), and the retry state clears.</li>
     * </ul>
     */
    public EodOffloadRecord transition(EodControllerState next, long nowMs) {
        if (!isLegalTransition(state, next)) {
            throw new IllegalStateException("illegal EOD transition " + state + " -> " + next
                    + " for " + tableName + " " + tradingDate);
        }
        int retryCount = this.retryCount;
        long nextRetryAtMs = this.nextRetryAtMs;
        long earliestAllowed = this.earliestAllowedSourceExpiryMs;
        if (next == EodControllerState.FAILED_RETRYABLE) {
            retryCount += 1;
            nextRetryAtMs = EodBackoff.nextRetryAtMs(nowMs, retryCount,
                    EodBackoff.DEFAULT_BASE_MS, EodBackoff.DEFAULT_MAX_MS, EodBackoff.rng());
        } else if (next == EodControllerState.VERIFIED) {
            earliestAllowed = nowMs;
            nextRetryAtMs = 0;
        } else if (state == EodControllerState.FAILED_RETRYABLE) {
            // retrying: the schedule is recomputed on the next failure
            nextRetryAtMs = 0;
        }
        return new EodOffloadRecord(tradingDate, tableName, schemaVersion, sourceOffsetStart,
                sourceOffsetEnd, rowCount, byteCount, sourceHash, targetHash, icebergSnapshotId,
                next, retryCount, nextRetryAtMs, earliestAllowed, nowMs);
    }

    /** Source may expire only when the manifest is VERIFIED. */
    public boolean permitsSourceExpiry() {
        return state.permitsSourceExpiry();
    }

    /** Every non-VERIFIED state requires live retention extension. */
    public boolean requiresRetentionExtension() {
        return state.requiresRetentionExtension();
    }

    public LocalDate tradingDateAsLocalDate() {
        return parseTradingDate(tradingDate);
    }

    public static String formatTradingDate(LocalDate date) {
        return date.toString(); // ISO-8601 yyyy-MM-dd
    }

    public static LocalDate parseTradingDate(String isoDate) {
        return LocalDate.parse(isoDate);
    }
}
