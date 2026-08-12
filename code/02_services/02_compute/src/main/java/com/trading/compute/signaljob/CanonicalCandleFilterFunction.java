package com.trading.compute.signaljob;

import com.trading.common.schema.CandleTableSchema;
import com.trading.compute.telemetry.ComputeOtlpEmitter;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.table.data.RowData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * KV current-state boundary filter (tracker 14 P2 — CANDLE-CANONICAL-001).
 *
 * <p>The LOG sink ({@code feature_candles_15s}) keeps every candle — it is
 * the immutable audit trail. The KV twin ({@code feature_candles_15s_current})
 * is the <em>canonical current-state projection</em>: only rows whose
 * algorithm/configuration version pair equals the canonical pair exactly
 * ({@link CanonicalCandlePolicy}, pinned in {@link CandleTableSchema}) may be
 * upserted. A non-canonical row (algorithm iteration, unversioned row, blank
 * or padded version) is dropped here and counted on
 * {@code compute.kv.filtered.noncanonical} — it must never overwrite the
 * canonical state of a key with a deviating version.
 *
 * <p>Stateless and side-effect-light: one {@code StringData} comparison per
 * row plus a counter increment on the drop path. The counter is the shared
 * {@link ComputeOtlpEmitter} static (single-JVM scope, matching the emitter's
 * contract) so the operator can alert on non-canonical rows at the KV
 * boundary.
 */
public class CanonicalCandleFilterFunction implements FilterFunction<RowData> {

    private static final Logger LOG = LoggerFactory.getLogger(CanonicalCandleFilterFunction.class);

    private static final long serialVersionUID = 1L;

    @Override
    public boolean filter(RowData row) {
        String algorithm = row.getString(CandleTableColumns.ALGORITHM_VERSION).toString();
        String configuration = row.getString(CandleTableColumns.CONFIGURATION_VERSION).toString();
        if (CanonicalCandlePolicy.isCanonical(algorithm, configuration,
                CandleTableSchema.CANONICAL_ALGORITHM_VERSION,
                CandleTableSchema.CANONICAL_CONFIGURATION_VERSION)) {
            return true;
        }
        ComputeOtlpEmitter.recordKvFilteredNonCanonical();
        LOG.warn("canonical-candle-filter: dropping non-canonical candle token={} windowStart={} "
                        + "algorithm='{}' configuration='{}' (canonical pair {}/{}, tracker 14 P2)",
                row.getLong(CandleTableColumns.INSTRUMENT_TOKEN),
                row.getLong(CandleTableColumns.WINDOW_START),
                algorithm, configuration,
                CandleTableSchema.CANONICAL_ALGORITHM_VERSION,
                CandleTableSchema.CANONICAL_CONFIGURATION_VERSION);
        return false;
    }
}
