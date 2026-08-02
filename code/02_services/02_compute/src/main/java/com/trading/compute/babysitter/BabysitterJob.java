package com.trading.compute.babysitter;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Babysitter Flink job — MVP strict no-op (DEC-017).
 *
 * <p>Consumes versioned {@code Positions} changelog and checkpointed observation
 * state. Emits zero {@code Position_Actions} records for every input in MVP.
 * {@code POSITION_ACTIONS_ENABLED} is hard-coded {@code false}; any attempt to
 * enable it at startup SHALL fail closed.
 *
 * <p>Safety rule: the Babysitter never calls the Arrow REST API or any broker
 * endpoint directly. Babysitter health never implies Executor trading readiness.
 *
 * <p>See docs/08_implementation/06-babysitter.md for the full implementation
 * contract.
 */
public final class BabysitterJob {

    private static final Logger LOG = LoggerFactory.getLogger(BabysitterJob.class);

    /** Hard-coded false for MVP per DEC-017; cannot be overridden. */
    private static final boolean POSITION_ACTIONS_ENABLED = false;

    private BabysitterJob() {
        // utility class
    }

    /**
     * MVP entry point. Validates the action flag, sets up the checkpointed
     * Flink job, consumes Positions changelog, and emits zero actions.
     */
    public static void main(String[] args) throws Exception {
        // Fail closed if anyone tries to override the action flag.
        // R-286: trim the env value — ' false ' or 'false\n' from config-file
        // exports previously triggered the guard spuriously.
        String envValue = System.getenv("POSITION_ACTIONS_ENABLED");
        if (envValue != null
                && !"false".equalsIgnoreCase(envValue.trim())) {
            throw new IllegalStateException(
                "POSITION_ACTIONS_ENABLED must be false in MVP; got '" + envValue + "'"
            );
        }
        LOG.info(
            "babysitter: starting MVP no-op job (POSITION_ACTIONS_ENABLED={})",
            POSITION_ACTIONS_ENABLED
        );

        StreamExecutionEnvironment env =
            StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);

        // R-120: Flink's StreamGraphGenerator rejects a topology with no
        // operators ("No operators defined in streaming topology"). Wire a
        // minimal marker source -> no-op map -> discard sink so the MVP job
        // submits; the real Positions-changelog source replaces this per the
        // implementation dossier.
        env.fromElements(0L)
                .map(value -> {
                    // MVP no-op marker: never produces Position_Actions.
                    return value;
                })
                .name("babysitter-mvp-marker")
                .map(value -> (Void) null)
                .name("babysitter-mvp-discard");

        // TODO: wire Positions changelog source, observation state,
        //   checkpointing, and zero-action emission per the implementation
        //   dossier (docs/08_implementation/06-babysitter.md).

        env.execute("Babysitter MVP no-op job");
    }
}
