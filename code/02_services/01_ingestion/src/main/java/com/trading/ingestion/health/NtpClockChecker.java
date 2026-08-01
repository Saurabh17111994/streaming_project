package com.trading.ingestion.health;

import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Checks local clock offset against an NTP server.
 *
 * <p>Uses the standard NTP (SNTP) protocol: sends a 48-byte request packet
 * to a configured NTP server and computes the offset from the response.
 * Falls back to a basic sanity check if the NTP server is unreachable
 * (clock must not be before 2024-01-01 UTC).
 *
 * <p>Contract: clock offset &lt;= {@code CLOCK_OFFSET_LIMIT_MS} (default 100ms)
 * for ingestion readiness per {@code docs/08_implementation/03-ingestion.md}.
 */
public final class NtpClockChecker {

    private static final Logger LOG = LoggerFactory.getLogger(NtpClockChecker.class);

    // NTP protocol constants
    private static final int NTP_PORT = 123;
    private static final int NTP_PACKET_SIZE = 48;
    private static final int NTP_MODE_CLIENT = 3;
    private static final long NTP_EPOCH_OFFSET = 2_208_988_800L; // seconds from 1900 to 1970
    private static final int SOCKET_TIMEOUT_MS = 5_000;

    // Fallback: clock must be after this epoch ms (2024-01-01)
    private static final long MIN_WALL_CLOCK_EPOCH_MS = 1_704_067_200_000L;

    private final String[] ntpServers;
    private final long offsetLimitMs;
    private final boolean required;
    private volatile long lastOffsetMs;
    private volatile Instant lastCheckTime;
    private volatile boolean lastCheckPassed;

    /** @param ntpServer      NTP server hostname (e.g. "pool.ntp.org")
     *  @param offsetLimitMs  maximum allowed clock offset in millis */
    public NtpClockChecker(String ntpServer, long offsetLimitMs) {
        this(ntpServer, offsetLimitMs, false);
    }

    /**
     * @param ntpServer      NTP server hostname (or comma-separated list, tried in order)
     * @param offsetLimitMs  maximum allowed clock offset in millis
     * @param required       when true, an unreachable NTP server fails the check
     *                       instead of falling back to a wall-clock sanity check
     */
    public NtpClockChecker(String ntpServer, long offsetLimitMs, boolean required) {
        this.ntpServers = splitServers(ntpServer);
        this.offsetLimitMs = offsetLimitMs;
        this.required = required;
    }

    private static String[] splitServers(String value) {
        if (value == null || value.isBlank()) {
            return new String[] {"ntp.ubuntu.com", "time.google.com", "in.pool.ntp.org"};
        }
        String[] parts = value.split(",");
        List<String> cleaned = new java.util.ArrayList<>();
        for (String p : parts) {
            String t = p.trim();
            if (!t.isEmpty()) cleaned.add(t);
        }
        return cleaned.isEmpty()
                ? new String[] {"ntp.ubuntu.com", "time.google.com", "in.pool.ntp.org"}
                : cleaned.toArray(new String[0]);
    }

    /**
     * Query the NTP server and return the clock offset in milliseconds.
     * Positive = local clock is ahead of NTP. Negative = local clock is behind.
     *
     * @throws NtpException if the server is unreachable or the response is invalid
     */
    public long measureOffsetMs() throws NtpException {
        NtpException lastError = null;
        for (String server : ntpServers) {
            try {
                long offset = queryNtp(server);
                lastOffsetMs = offset;
                lastCheckTime = Instant.now();
                boolean passed = Math.abs(offset) <= offsetLimitMs;
                lastCheckPassed = passed;

                if (!passed) {
                    LOG.warn("ntp-clock: offset {}ms exceeds limit {}ms (server={})",
                            offset, offsetLimitMs, server);
                }
                return offset;
            } catch (NtpException e) {
                lastError = e;
                LOG.warn("ntp-clock: server {} unreachable; trying next", server);
            }
        }

        lastCheckTime = Instant.now();
        if (required) {
            // Strict mode: an unreachable time source is a failure.
            lastCheckPassed = false;
            lastOffsetMs = 0;
            LOG.error("ntp-clock: all servers unreachable and CLOCK_CHECK_REQUIRED=true — clock check FAILED");
            throw lastError != null ? lastError : new NtpException("all NTP servers unreachable", null);
        }
        // Fallback: basic sanity check
        long now = System.currentTimeMillis();
        if (now < MIN_WALL_CLOCK_EPOCH_MS) {
            lastCheckPassed = false;
            throw lastError != null ? lastError : new NtpException("all NTP servers unreachable", null);
        }
        lastOffsetMs = 0;
        lastCheckPassed = true;
        LOG.warn("ntp-clock: all servers unreachable; falling back to wall-clock sanity check");
        return 0;
    }

    /** True if the last check passed (offset within limit or NTP unreachable with sane clock). */
    public boolean isWithinLimit() {
        return lastCheckPassed;
    }

    /** Offset in ms from last check. Positive = local ahead. */
    public long lastOffsetMs() {
        return lastOffsetMs;
    }

    /** When the last check happened, or null if never checked. */
    public Instant lastCheckTime() {
        return lastCheckTime;
    }

    /** The configured limit in ms. */
    public long offsetLimitMs() {
        return offsetLimitMs;
    }

    // ---- NTP query (RFC 5905 SNTP client) ----

    private static long queryNtp(String host) throws NtpException {
        byte[] buffer = new byte[NTP_PACKET_SIZE];

        // Build NTP request (SNTP client, version 4, mode 3)
        buffer[0] = (byte) (0x23); // LI=0, VN=4, Mode=3

        long sendNanos;
        long receiveNanos;

        try (DatagramSocket socket = new DatagramSocket()) {
            socket.setSoTimeout(SOCKET_TIMEOUT_MS);
            InetAddress address = InetAddress.getByName(host);

            DatagramPacket request = new DatagramPacket(buffer, buffer.length, address, NTP_PORT);

            sendNanos = System.nanoTime();
            socket.send(request);

            DatagramPacket response = new DatagramPacket(buffer, buffer.length);
            socket.receive(response);
            receiveNanos = System.nanoTime();

        } catch (Exception e) {
            throw new NtpException("NTP query to " + host + " failed", e);
        }

        // Parse NTP response
        // Transmit timestamp: bytes 40-47 (seconds + fraction)
        long transmitSeconds = readUint32(buffer, 40);
        long transmitFraction = readUint32(buffer, 44);

        // Convert NTP timestamp (1900 epoch) to milliseconds since 1970
        long ntpMillis = (transmitSeconds - NTP_EPOCH_OFFSET) * 1000L
                + (transmitFraction * 1000L) / 0x1_0000_0000L;

        // Local time when response arrived (system time)
        // Account for half the round-trip to approximate the offset
        long rttNanos = receiveNanos - sendNanos;
        long localMillis = System.currentTimeMillis() - (rttNanos / 2_000_000); // half RTT

        return localMillis - ntpMillis;
    }

    private static long readUint32(byte[] buf, int offset) {
        return ((buf[offset] & 0xFFL) << 24)
                | ((buf[offset + 1] & 0xFFL) << 16)
                | ((buf[offset + 2] & 0xFFL) << 8)
                | (buf[offset + 3] & 0xFFL);
    }

    // ---- Exception ----

    public static final class NtpException extends Exception {
        public NtpException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}
