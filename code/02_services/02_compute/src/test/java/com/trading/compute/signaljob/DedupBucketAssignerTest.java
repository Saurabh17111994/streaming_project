package com.trading.compute.signaljob;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

import java.util.List;
import org.apache.fluss.bucketing.BucketingFunction;
import org.apache.fluss.metadata.DataLakeFormat;
import org.apache.fluss.row.encode.KeyEncoder;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.types.RowType;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * SIG-STATE-001 regression pin for {@link DedupBucketAssigner}: the bucket a
 * {@code fingerprint_dedup} row lands in is assigned by Fluss's own bucketing
 * machinery from the bucket-key bytes — NOT {@code token % numBuckets}.
 *
 * <p>The pinned numbers are the empirically verified mapping (2026-08-15): the
 * predicted buckets from this exact construction were compared against a live
 * dev-cluster scan and matched row-for-row for both lake modes. The
 * {@code iceberg} mapping is the live dev default
 * ({@code TableConfig.getDataLakeFormat()} returns {@code iceberg} even though
 * DDL 24 comments say lake-off — no property is set, so the cluster default
 * applies); the {@code null} (lake-less) mapping is the plain Fluss hash the
 * production table would use if the DDL blueprint ever pins lake-off. The
 * values are golden pins against Fluss 0.9.1 (the pinned connector) — a
 * connector bump that changes the hash must update this test, which is exactly
 * the cross-boundary pin habit.
 *
 * <p>The pre-fix {@code token % 16} behavior would have scanned bucket 8 for
 * token 7000 and deleted nothing; the fix makes {@code scanExpired} read the
 * bucket the token's rows actually live in.
 */
@DisplayName("SIG-STATE-001: DedupBucketAssigner matches Fluss's bucket assignment")
class DedupBucketAssignerTest {

    private static final RowType ROW = RowType.of(
            new DataType[]{DataTypes.BIGINT()}, new String[]{"instrument_token"});
    private static final List<String> BUCKET_KEYS = List.of("instrument_token");
    private static final int NUM_BUCKETS = 16;

    private static DedupBucketAssigner assigner(DataLakeFormat lake) {
        return new DedupBucketAssigner(NUM_BUCKETS,
                KeyEncoder.ofBucketKeyEncoder(ROW, BUCKET_KEYS, lake),
                BucketingFunction.of(lake));
    }

    @Test
    @DisplayName("lake=iceberg (dev-cluster default): buckets match the observed live layout")
    void icebergBucketsMatchObservedLiveLayout() {
        DedupBucketAssigner a = assigner(DataLakeFormat.ICEBERG);
        assertEquals(15, a.bucket(5000));
        assertEquals(6, a.bucket(5001));
        assertEquals(11, a.bucket(5002));
        assertEquals(9, a.bucket(5003));
        assertEquals(0, a.bucket(5004));
        assertEquals(8, a.bucket(5005));
        assertEquals(4, a.bucket(7000));
        assertEquals(3, a.bucket(7001));
        assertEquals(7, a.bucket(7002));
        assertEquals(5, a.bucket(8000));
    }

    @Test
    @DisplayName("lake=null (lake-less): buckets use the plain Fluss hash — different from token%16")
    void lakeLessBucketsUseFlussHash() {
        DedupBucketAssigner a = assigner(null);
        assertEquals(15, a.bucket(5000));
        assertEquals(8, a.bucket(5001));
        assertEquals(6, a.bucket(5002));
        assertEquals(7, a.bucket(7000));
        assertEquals(10, a.bucket(8000));
    }

    @Test
    @DisplayName("regression guard: the bucket is NOT token % numBuckets (the pre-fix bug)")
    void bucketIsNotTokenModulo() {
        DedupBucketAssigner iceberg = assigner(DataLakeFormat.ICEBERG);
        for (long token : new long[]{5000, 5001, 7000, 7001, 8000}) {
            assertNotEquals((int) (token % NUM_BUCKETS), iceberg.bucket(token),
                    "token " + token + ": token%16 was the pre-fix wrong-bucket bug");
        }
        // The concrete failure that motivated the fix: token 7000 lives in
        // bucket 4, but token%16 would scan bucket 8.
        assertEquals(4, iceberg.bucket(7000));
        assertNotEquals(8, iceberg.bucket(7000));
    }

    @Test
    @DisplayName("deterministic + in range for the accepted token domain")
    void bucketsInRange() {
        DedupBucketAssigner a = assigner(DataLakeFormat.ICEBERG);
        for (long token = 1; token <= 1_000_000; token += 97) {
            int b = a.bucket(token);
            if (b < 0 || b >= NUM_BUCKETS) {
                throw new AssertionError("bucket " + b + " out of range for token " + token);
            }
        }
    }
}
