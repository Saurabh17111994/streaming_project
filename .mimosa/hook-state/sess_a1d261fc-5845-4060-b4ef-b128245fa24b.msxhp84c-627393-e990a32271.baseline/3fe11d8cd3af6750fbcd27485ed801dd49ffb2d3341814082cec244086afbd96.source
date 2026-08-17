package com.trading.common.safety;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

/**
 * Token-set hash parity: byte-identical to Go {@code tokenSetHash}
 * (go-bridge/subscription_plan.go) and Java
 * {@code SafetyHaltWriter.computeAssignedTokenHash} — SHA-256 over the
 * sorted 8-byte-big-endian token encoding, lowercase hex.
 */
@DisplayName("TokenSetHash: pinned vectors + order independence")
class TokenSetHashTest {

    /** Pinned parity vector shared by the Go and Java test suites. */
    static final String PINNED = "8a65b772eeae7692de1f941da206dc6a5b6649568e999dc06fb16a7b0615744c";

    /** sha256("") — the empty token set. */
    static final String EMPTY = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";

    /** sha256(0x0000000000000007). */
    static final String SINGLE_SEVEN = "a3eb8db89fc5123ccfd49585059f292bc40a1c0d550b860f24f84efb4760fbf2";

    @Test
    @DisplayName("pinned parity vector [1000,1001,1]")
    void pinnedVector() {
        assertEquals(PINNED, TokenSetHash.of(1000L, 1001L, 1L));
    }

    @Test
    @DisplayName("order-independent: any input order hashes identically")
    void orderIndependence() {
        assertEquals(TokenSetHash.of(1000L, 1001L, 1L), TokenSetHash.of(1L, 1000L, 1001L));
        assertEquals(TokenSetHash.of(1000L, 1001L, 1L), TokenSetHash.of(1001L, 1L, 1000L));
        assertEquals(PINNED, TokenSetHash.of(List.of(1001L, 1L, 1000L)));
    }

    @Test
    @DisplayName("empty set hashes as sha256 of empty input")
    void emptySet() {
        assertEquals(EMPTY, TokenSetHash.of(List.of()));
    }

    @Test
    @DisplayName("single token encodes as 8 big-endian bytes")
    void singleToken() {
        assertEquals(SINGLE_SEVEN, TokenSetHash.of(7L));
    }

    @Test
    @DisplayName("deterministic across calls")
    void deterministic() {
        assertEquals(TokenSetHash.of(1L, 2L, 3L), TokenSetHash.of(1L, 2L, 3L));
    }
}
