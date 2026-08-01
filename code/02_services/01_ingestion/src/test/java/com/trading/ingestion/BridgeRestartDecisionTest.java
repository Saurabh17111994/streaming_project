package com.trading.ingestion;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.trading.ingestion.IngestionService.BridgeRestartDecision;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * ING-UNIT-012: bridge restart policy (plan §IngestionService).
 *
 * <p>An unexpected exit is restarted once; a second unexpected exit in the same
 * process is terminal; a requested exit (code 0) or shutdown is never restarted.
 */
@DisplayName("ING-UNIT-012: bridge restart policy")
class BridgeRestartDecisionTest {

    @Test
    @DisplayName("unexpected exit restarts once")
    void unexpectedExitRestartsOnce() {
        assertEquals(BridgeRestartDecision.RESTART,
                IngestionService.bridgeRestartDecision(true, 1, 0),
                "first unexpected exit (restartCount=0) must restart");
    }

    @Test
    @DisplayName("second unexpected exit is terminal")
    void secondUnexpectedExitIsTerminal() {
        assertEquals(BridgeRestartDecision.TERMINAL,
                IngestionService.bridgeRestartDecision(true, 1, 1),
                "second unexpected exit (restartCount=1) must be terminal");
    }

    @Test
    @DisplayName("clean exit code 0 is never restarted")
    void cleanExitNeverRestarts() {
        assertEquals(BridgeRestartDecision.NO_RESTART,
                IngestionService.bridgeRestartDecision(true, 0, 0),
                "requested/clean exit must not restart");
    }

    @Test
    @DisplayName("shutdown in progress never restarts")
    void shutdownNeverRestarts() {
        assertEquals(BridgeRestartDecision.NO_RESTART,
                IngestionService.bridgeRestartDecision(false, 1, 0),
                "not running must never restart");
    }
}
