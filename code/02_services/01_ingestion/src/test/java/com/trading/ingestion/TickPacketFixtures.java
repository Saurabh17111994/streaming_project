package com.trading.ingestion;

import com.trading.ingestion.model.RawTick;
import com.trading.ingestion.model.TickPacket;
import com.trading.ingestion.model.ValidityClassification;
import java.time.Instant;

/** Test fixture factory — creates valid TickPacket instances without real broker data. */
public final class TickPacketFixtures {

    private TickPacketFixtures() {}

    /** Create a valid trade tick with synthetic data. Token increments per call. */
    public static TickPacket validTrade(int index) {
        long token = 100000L + (index % 50) * 100L + (index % 10);
        return new TickPacket.Builder()
                .raw(new RawTick.Builder()
                        .rawPayload(new byte[]{1, 2, 3})
                        .payloadHash("abc123")
                        .hashAlgorithm("SHA-256")
                        .protocolVersion("go-arrow-v0")
                        .decoderVersion("test")
                        .receiveTime(Instant.now())
                        .receiveTimeNanos(System.nanoTime())
                        .build())
                .validity(ValidityClassification.VALID_TRADE)
                .instrumentToken(token)
                .tradingSymbol("SYM" + (index % 50 + 1) + "-EQ")
                .exchange("NSE")
                .eventTime(Instant.now().minusMillis(100))
                .ingestTs(Instant.now())
                .lastPricePaise(12345L + index)
                .volume(100L)
                .eventFingerprint("fp_" + token + "_" + index)
                .fingerprintVersion(1)
                .connectionId("test")
                .connectionEpoch(0L)
                .instanceId("test-instance")
                .schemaVersion(1)
                .build();
    }
}
