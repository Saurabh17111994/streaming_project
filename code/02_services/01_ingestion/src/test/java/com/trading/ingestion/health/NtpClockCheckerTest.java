package com.trading.ingestion.health;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * R-032 / R-064 — NTP clock checker fail-closed fallback and response validation.
 */
@DisplayName("ING-UNIT-013: NTP clock checker hardening")
class NtpClockCheckerTest {

    // ---- R-064: response validation ----

    @Test
    @DisplayName("genuine server response passes validation")
    void validResponsePasses() throws Exception {
        byte[] request = new byte[48];
        request[0] = 0x23; // VN=4, mode=3
        writeUint32(request, 40, 0x10203040L); // transmit ts

        byte[] response = new byte[48];
        response[0] = 0x24; // VN=4, mode=4 (server)
        writeUint32(response, 24, 0x10203040L); // origin echoes transmit

        NtpClockChecker.validateResponse(request, response); // no throw
    }

    @Test
    @DisplayName("short datagram is rejected")
    void shortDatagramRejected() {
        byte[] response = new byte[20];
        assertThrows(NtpClockChecker.NtpException.class,
                () -> NtpClockChecker.validateResponse(null, response));
    }

    @Test
    @DisplayName("non-server mode is rejected")
    void wrongModeRejected() {
        byte[] response = new byte[48];
        response[0] = 0x23; // mode 3 (client) — not a server response
        assertThrows(NtpClockChecker.NtpException.class,
                () -> NtpClockChecker.validateResponse(null, response));
    }

    @Test
    @DisplayName("origin timestamp not echoing our transmit is rejected")
    void originMismatchRejected() throws Exception {
        byte[] request = new byte[48];
        writeUint32(request, 40, 0x11111111L);
        byte[] response = new byte[48];
        response[0] = 0x24;
        writeUint32(response, 24, 0x22222222L); // wrong echo
        assertThrows(NtpClockChecker.NtpException.class,
                () -> NtpClockChecker.validateResponse(request, response));
    }

    // ---- R-032: fail-closed fallback ----

    @Test
    @DisplayName("unreachable servers fail closed when not required (R-032)")
    void unreachableServersFailClosed() {
        // Unroutable address with the hardcoded NTP port (123) — the query
        // fails fast and reaches the fallback path.
        NtpClockChecker checker = new NtpClockChecker(
                "10.255.255.1", 100, false);
        try {
            checker.measureOffsetMs();
        } catch (NtpClockChecker.NtpException ignored) {
            // fallback path still reached — assert state below
        }
        assertFalse(checker.isWithinLimit(),
                "unverified clock must NOT be within limit (fail-closed, R-032)");
        assertFalse(checker.isVerified(),
                "unreachable servers must leave the check unverified (R-032)");
    }

    @Test
    @DisplayName("required mode throws when all servers unreachable")
    void requiredModeThrows() {
        NtpClockChecker checker = new NtpClockChecker(
                "10.255.255.1", 100, true);
        assertThrows(NtpClockChecker.NtpException.class, checker::measureOffsetMs);
        assertFalse(checker.isWithinLimit());
    }

    private static void writeUint32(byte[] buf, int offset, long value) {
        buf[offset] = (byte) (value >>> 24);
        buf[offset + 1] = (byte) (value >>> 16);
        buf[offset + 2] = (byte) (value >>> 8);
        buf[offset + 3] = (byte) value;
    }
}
