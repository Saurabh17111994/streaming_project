package com.trading.execution.gateway;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;
import com.trading.common.schema.fluss.CompositeKeyMatrixVerifier;
import java.time.Duration;
import org.apache.fluss.client.Connection;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.metadata.TablePath;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/** Live T2 evidence; skipped unless an operator explicitly supplies FLUSS_BOOTSTRAP. */
@Tag("integration")
class GatewayFlussIntegrationTest {
    private Connection connection;
    private Admin admin;

    @AfterEach void close() throws Exception {
        if (admin != null) admin.close();
        if (connection != null) connection.close();
    }

    @Test void pinnedRawClientMatrixAndIntentSchemaAreAvailable() throws Exception {
        String bootstrap = System.getenv("FLUSS_BOOTSTRAP");
        assumeTrue(bootstrap != null && !bootstrap.isBlank(), "set FLUSS_BOOTSTRAP for live T2 evidence");
        Configuration c = new Configuration(); c.setString("bootstrap.servers", bootstrap);
        connection = ConnectionFactory.createConnection(c); admin = connection.getAdmin();
        var intent = connection.getTable(TablePath.of("default", "Execution_Intent"));
        assertThat(intent.getTableInfo().getRowType().getFieldCount()).isEqualTo(22);
        var result = CompositeKeyMatrixVerifier.verify(connection, admin,
                "gateway_t2_" + System.nanoTime(), Duration.ofSeconds(20));
        assertThat(result.passed()).as(result.deviations().toString()).isTrue();
        assertThat(result.cells()).hasSize(4);
    }
}
