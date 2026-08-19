package com.trading.common.schema.projection;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

class PostbackProjectionLedgerTest {

    @Test
    void orderSideAdvanceReachesComplete() {
        PostbackProjectionLedger.State s = PostbackProjectionLedger.State.RECEIVED;
        s = PostbackProjectionLedger.next(s, PostbackProjectionLedger.State.CORRELATED);
        s = PostbackProjectionLedger.next(s, PostbackProjectionLedger.State.AUDIT_WRITTEN);
        s = PostbackProjectionLedger.next(s, PostbackProjectionLedger.State.LIFECYCLE_APPLIED);
        s = PostbackProjectionLedger.next(s,
                PostbackProjectionLedger.State.POSITION_APPLIED_OR_NOT_REQUIRED);
        s = PostbackProjectionLedger.next(s, PostbackProjectionLedger.State.COMPLETE);
        assertThat(s).isEqualTo(PostbackProjectionLedger.State.COMPLETE);
        assertThat(PostbackProjectionLedger.terminal(s)).isTrue();
        assertThat(PostbackProjectionLedger.recoverable(s)).isFalse();
    }

    @Test
    void skipsAreRejected() {
        assertThatThrownBy(() -> PostbackProjectionLedger.next(
                PostbackProjectionLedger.State.RECEIVED,
                PostbackProjectionLedger.State.LIFECYCLE_APPLIED))
                .isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(() -> PostbackProjectionLedger.next(
                PostbackProjectionLedger.State.POSITION_APPLIED_OR_NOT_REQUIRED,
                PostbackProjectionLedger.State.LIFECYCLE_APPLIED))
                .isInstanceOf(IllegalStateException.class);
        // COMPLETE from the final order-side state is legal.
        assertThatCode(() -> PostbackProjectionLedger.next(
                PostbackProjectionLedger.State.POSITION_APPLIED_OR_NOT_REQUIRED,
                PostbackProjectionLedger.State.COMPLETE))
                .doesNotThrowAnyException();
    }

    @Test
    void anyForwardStateMayQuarantineOrFail() {
        for (PostbackProjectionLedger.State s : PostbackProjectionLedger.State.values()) {
            if (PostbackProjectionLedger.terminal(s)) {
                continue;
            }
            assertThat(PostbackProjectionLedger.terminal(PostbackProjectionLedger.next(
                    s, PostbackProjectionLedger.State.QUARANTINED))).isTrue();
            assertThat(PostbackProjectionLedger.terminal(PostbackProjectionLedger.next(
                    s, PostbackProjectionLedger.State.FAILED))).isTrue();
        }
    }

    @Test
    void recoverableStatesResume() {
        assertThat(PostbackProjectionLedger.recoverable(
                PostbackProjectionLedger.State.AUDIT_WRITTEN)).isTrue();
        assertThat(PostbackProjectionLedger.orderSideComplete(
                PostbackProjectionLedger.State.POSITION_APPLIED_OR_NOT_REQUIRED)).isTrue();
    }
}
