package com.trading.ingestion.write;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.trading.ingestion.write.RetryClassifier.Classification;
import java.util.concurrent.ExecutionException;
import javax.naming.AuthenticationException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * R-038 — fatal patterns take precedence across the entire cause chain.
 *
 * <p>Regression: a retryable wrapper (e.g. an ExecutionException whose message
 * mentions "connection") used to short-circuit the chain walk and return
 * RETRYABLE before an underlying fatal cause (AuthenticationException /
 * AccessControlException / TableNotExistException) could be inspected — turning
 * permanent failures into MAX_RETRY_ATTEMPTS retries that end FAILED instead
 * of FATAL, so the safety-halt gate never opened.
 */
@DisplayName("R-038: RetryClassifier fatal precedence over cause chain")
class RetryClassifierTest {

    @Test
    @DisplayName("null is RETRYABLE")
    void nullIsRetryable() {
        assertEquals(Classification.RETRYABLE, RetryClassifier.classify(null));
    }

    @Test
    @DisplayName("direct AccessControlException is FATAL")
    void directAccessControlIsFatal() {
        Throwable t = new java.security.AccessControlException("denied");
        assertEquals(Classification.FATAL, RetryClassifier.classify(t));
    }

    @Test
    @DisplayName("direct AuthenticationException is FATAL")
    void directAuthenticationIsFatal() {
        assertEquals(Classification.FATAL,
                RetryClassifier.classify(new AuthenticationException("bad credentials")));
    }

    @Test
    @DisplayName("TableNotExist class name is FATAL")
    void tableNotExistIsFatal() {
        // Simulates a Fluss TableNotExistException surfaced through the client:
        // the class name must contain "TableNotExist".
        class FakeTableNotExistException extends RuntimeException {}
        assertEquals(Classification.FATAL,
                RetryClassifier.classify(new FakeTableNotExistException()));
    }

    @Test
    @DisplayName("'table not found' message is FATAL")
    void tableNotFoundMessageIsFatal() {
        assertEquals(Classification.FATAL,
                RetryClassifier.classify(new RuntimeException("table abc not found")));
    }

    @Test
    @DisplayName("retryable wrapper does NOT mask an inner fatal cause (R-038 regression)")
    void retryableWrapperDoesNotMaskInnerFatal() {
        Throwable inner = new AuthenticationException("invalid credentials");
        // Wrapper message contains "connection" → old code returned RETRYABLE here.
        Throwable outer = new ExecutionException("connection reset", inner);
        assertEquals(Classification.FATAL, RetryClassifier.classify(outer));
    }

    @Test
    @DisplayName("fatal wrapper wins even when a deeper cause is retryable")
    void fatalWrapperWinsOverDeeperRetryable() {
        Throwable inner = new RuntimeException("coordinator re-election");
        Throwable outer = new ExecutionException("table not found", inner);
        assertEquals(Classification.FATAL, RetryClassifier.classify(outer));
    }

    @Test
    @DisplayName("pure retryable chain stays RETRYABLE")
    void pureRetryableChainStaysRetryable() {
        Throwable inner = new RuntimeException("leader not available");
        Throwable outer = new ExecutionException("connection refused", inner);
        assertEquals(Classification.RETRYABLE, RetryClassifier.classify(outer));
    }

    @Test
    @DisplayName("pool-exhaustion EOFException is RETRYABLE (R-297)")
    void poolExhaustionEofIsRetryable() {
        // Fluss LazyMemorySegmentPool.waitForSegment throws exactly this when
        // client.writer.buffer.wait-timeout elapses with the pool exhausted
        // (sender wedged on leaderless tables). R-297: transient — retry on
        // the scheduler thread once the wedge clears; never drop the tick.
        Throwable t = new java.io.EOFException(
                "Failed to allocate new segment within the configured max blocking time "
                        + "30000 ms. Total memory: 67108864");
        assertEquals(Classification.RETRYABLE, RetryClassifier.classify(t));
    }

    @Test
    @DisplayName("fatal deep in a three-link chain is still FATAL")
    void fatalDeepInChainIsStillFatal() {
        Throwable innermost = new AuthenticationException("denied");
        Throwable middle = new ExecutionException("timeout", innermost);
        Throwable outer = new ExecutionException("network unavailable", middle);
        assertEquals(Classification.FATAL, RetryClassifier.classify(outer));
    }
}
