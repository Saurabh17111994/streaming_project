package com.trading.compute.signaljob;

import org.apache.fluss.bucketing.BucketingFunction;
import org.apache.fluss.metadata.DataLakeFormat;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.row.encode.KeyEncoder;

/**
 * Mirror of the Fluss raw client's {@code HashBucketAssigner} for the
 * {@code fingerprint_dedup} KV state table (DEC-038; design:
 * {@code docs/08_implementation/04-signal-job.md} §Design — fingerprint_dedup
 * dedup state table).
 *
 * <p>Fluss assigns a row's bucket from the <b>bucket-key bytes</b> — NOT the
 * primary key and NOT {@code token % numBuckets}: the bucket-key columns are
 * compact-encoded by the table's bucket-key encoder, then
 * {@link BucketingFunction} maps those bytes to a bucket. The function depends
 * on the table's data-lake format at runtime
 * ({@code TableConfig#getDataLakeFormat()} — {@code null} → the plain Fluss
 * hash; on the dev cluster the default is {@code iceberg}).
 *
 * <p>This class is the fix for the SIG-STATE-001 finding of 2026-08-15: the
 * store previously scanned {@code token % numBuckets}, which is the wrong
 * bucket for the double-hash assignment — so {@code scanExpired} read a bucket
 * that never held the token's rows and the cleanup pass silently missed
 * expired rows (table growth never plateaued at accepted × TTL). The exact
 * writer construction path (verified against the live cluster: predicted
 * buckets == where rows actually land) is reproduced here with Fluss's own
 * public API, so no internal encoding is replicated.
 */
public final class DedupBucketAssigner {

    private final int numBuckets;
    private final KeyEncoder bucketKeyEncoder;
    private final BucketingFunction bucketing;

    /**
     * Build from a live table's own metadata — the exact path the client
     * {@code UpsertWriter} uses for bucket assignment.
     */
    public static DedupBucketAssigner of(TableInfo info) {
        DataLakeFormat lake = info.getTableConfig().getDataLakeFormat().orElse(null);
        return new DedupBucketAssigner(
                info.getNumBuckets(),
                KeyEncoder.ofBucketKeyEncoder(info.getRowType(), info.getBucketKeys(), lake),
                BucketingFunction.of(lake));
    }

    /** Package-private for unit tests — wired from {@link #of(TableInfo)}. */
    DedupBucketAssigner(int numBuckets, KeyEncoder bucketKeyEncoder,
            BucketingFunction bucketing) {
        if (numBuckets <= 0) {
            throw new IllegalArgumentException("bucket count must be positive, got " + numBuckets);
        }
        this.numBuckets = numBuckets;
        this.bucketKeyEncoder = bucketKeyEncoder;
        this.bucketing = bucketing;
    }

    /** The bucket a row with this {@code instrument_token} lands in (0..numBuckets-1). */
    public int bucket(long token) {
        return bucketing.bucketing(bucketKeyEncoder.encodeKey(GenericRow.of(token)), numBuckets);
    }
}
