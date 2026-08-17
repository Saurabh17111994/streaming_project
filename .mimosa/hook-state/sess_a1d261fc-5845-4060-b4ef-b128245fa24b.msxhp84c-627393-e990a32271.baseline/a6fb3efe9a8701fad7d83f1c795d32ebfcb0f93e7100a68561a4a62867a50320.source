package com.trading.common.schema.eod;

/**
 * Drill/dev {@link EodOffloadExecutor} ({@code --offload mock}): a
 * scripted success/failure so the controller's state-machine drive can be
 * exercised end-to-end against a real dev cluster before the real offload
 * pipeline exists. Never used in production posture — the shipped
 *
 * <p>The mock fabricates deterministic content evidence (a fixed source/target
 * hash pair and the configured row count) and records every call so tests can
 * assert the exact drive sequence.
 */
public final class MockEodOffloadExecutor implements EodOffloadExecutor {

    private final boolean succeedOffload;
    private final boolean succeedVerify;
    private final long rowCount;
    private final String sourceHash;
    private final String targetHash;

    private int offloadCalls;
    private int verifyCalls;

    public MockEodOffloadExecutor(boolean succeedOffload, boolean succeedVerify) {
        this(succeedOffload, succeedVerify, 1_000L, "mock-source-hash", "mock-target-hash");
    }

    public MockEodOffloadExecutor(boolean succeedOffload, boolean succeedVerify, long rowCount,
            String sourceHash, String targetHash) {
        this.succeedOffload = succeedOffload;
        this.succeedVerify = succeedVerify;
        this.rowCount = rowCount;
        this.sourceHash = sourceHash;
        this.targetHash = targetHash;
    }

    @Override
    public OffloadResult offload(EodOffloadRecord record) {
        offloadCalls++;
        if (!succeedOffload) {
            return OffloadResult.failure("mock offload failure for " + record.tableName()
                    + " " + record.tradingDate());
        }
        return new OffloadResult(true, 0L, rowCount, rowCount, rowCount * 128L,
                sourceHash, targetHash, "mock-iceberg-snapshot-1", null);
    }

    @Override
    public boolean verify(EodOffloadRecord committed) {
        verifyCalls++;
        return succeedVerify;
    }

    public int offloadCalls() {
        return offloadCalls;
    }

    public int verifyCalls() {
        return verifyCalls;
    }
}
