package com.trading.capture;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class PostbackQuarantineTest {

    @Test
    void quarantineFactorySetsTimestampAndFields() {
        long before = System.currentTimeMillis();
        PostbackQuarantine.QuarantineEntry e =
                PostbackQuarantine.quarantine("evt-123", "FINGERPRINT_MISMATCH", "{\"id\":\"BRK-1\"}");
        long after = System.currentTimeMillis();

        assertThat(e.postbackEventId()).isEqualTo("evt-123");
        assertThat(e.reason()).isEqualTo("FINGERPRINT_MISMATCH");
        assertThat(e.rawJson()).isEqualTo("{\"id\":\"BRK-1\"}");
        assertThat(e.quarantinedTs()).isBetween(before, after);
    }

    @Test
    void quarantineEntryIsImmutableRecord() {
        PostbackQuarantine.QuarantineEntry e1 =
                PostbackQuarantine.quarantine("id", "reason", "raw");
        PostbackQuarantine.QuarantineEntry e2 =
                new PostbackQuarantine.QuarantineEntry("id", "reason", "raw", e1.quarantinedTs());
        assertThat(e1).isEqualTo(e2);
    }
}
