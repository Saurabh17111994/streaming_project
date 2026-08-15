package com.trading.common.schema.eod;

/**
 * The shipped default {@link EodOffloadExecutor}: with no lake
 * target configured, every offload fails closed and every verify refuses —
 * the day stays {@code FAILED_RETRYABLE} and extends retention, and is NEVER
 * silently marked {@code VERIFIED}. The encrypted export pipeline plugs in
 * when a master key is configured.
 */
public final class NotConfiguredEodOffloadExecutor implements EodOffloadExecutor {

    public static final NotConfiguredEodOffloadExecutor INSTANCE =
            new NotConfiguredEodOffloadExecutor();

    private static final String REASON =
            "EOD offload target not configured — the lake/export half is not wired; "
                    + "the day stays FAILED_RETRYABLE (unverified days extend retention)";

    private NotConfiguredEodOffloadExecutor() {}

    @Override
    public OffloadResult offload(EodOffloadRecord record) {
        return OffloadResult.failure(REASON);
    }

    @Override
    public boolean verify(EodOffloadRecord committed) {
        return false;
    }
}
