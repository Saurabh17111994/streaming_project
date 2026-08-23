package com.trading.execution.gateway;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.trading.common.model.GateState;
import com.trading.common.schema.execution.GateRow;
import com.trading.common.schema.execution.InMemoryGateStateStore;
import org.junit.jupiter.api.*;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Map;
import java.util.Set;
import static org.junit.jupiter.api.Assertions.*;

class GatewayHttpServerTest {
    private static final ObjectMapper M = new ObjectMapper();
    private GatewayHttpServer server;
    private InMemoryGateStateStore gates;
    private GatewayConfig cfg;
    private String base;

    @BeforeEach void setUp() throws Exception {
        gates = new InMemoryGateStateStore(Set.of("saurabh"));
        gates.init(new GateRow("p1","acct1",GateState.HALTED,0,"boot","h0",null,null,null,null,0L,null,null,null));
        GateRow pending = new GateRow("p1","acct1",GateState.APPROVAL_PENDING,1,"reconciled","h1",null,null,null,"owner1",1L,1000L, 900000L,null);
        gates.install(pending);
        cfg = new GatewayConfig("localhost:9123","default","Execution_Intent","Execution_Gate","Execution_Attempts","Order_Correlation","Postback_Projection_Ledger","Safety_Halt_Requests","127.0.0.1",0,"http://127.0.0.1:9190/v1/intents","execution-gateway.v1","secret1234567890123456",Duration.ofMillis(2000),Duration.ofMillis(250),"acct1","p1", false);
        server = new GatewayHttpServer(cfg,new GatewayReadiness(), n-> {}, gates);
        server.start();
        int port = serverPort(server);
        base = "http://127.0.0.1:"+port;
    }
    @AfterEach void tearDown(){ if(server!=null) server.close(); }
    private int serverPort(GatewayHttpServer s) throws Exception {
        var f=s.getClass().getDeclaredField("server"); f.setAccessible(true);
        com.sun.net.httpserver.HttpServer hs=(com.sun.net.httpserver.HttpServer)f.get(s);
        return hs.getAddress().getPort();
    }
    private HttpResponse<String> post(String path, String json) throws Exception {
        var c=HttpClient.newHttpClient();
        var req=HttpRequest.newBuilder(URI.create(base+path)).header("Content-Type","application/json").POST(HttpRequest.BodyPublishers.ofString(json)).build();
        return c.send(req, HttpResponse.BodyHandlers.ofString());
    }
    @Test void approve_SaurabhCorrectEpochHash_Enables() throws Exception {
        var r=post("/control/approve", M.writeValueAsString(Map.of("principal","saurabh","executionPartitionId","p1","epoch",1,"evidenceHash","abc123")));
        assertEquals(200,r.statusCode());
        assertTrue(r.body().contains("ENABLED"));
        assertEquals(GateState.ENABLED, gates.read("p1").state());
        assertTrue(gates.read("p1").approvalsComplete());
        assertTrue(gates.read("p1").approvalsCover("abc123"));
    }
    @Test void approve_UnauthorizedPrincipal_403AndHalted() throws Exception {
        var r=post("/control/approve", M.writeValueAsString(Map.of("principal","alice","executionPartitionId","p1","epoch",1,"evidenceHash","abc123")));
        assertEquals(403,r.statusCode());
        assertEquals(GateState.HALTED, gates.read("p1").state());
    }
    @Test void approve_EpochMismatch_409AndHalted() throws Exception {
        var r=post("/control/approve", M.writeValueAsString(Map.of("principal","saurabh","executionPartitionId","p1","epoch",99,"evidenceHash","abc123")));
        assertEquals(409,r.statusCode());
        assertEquals(GateState.HALTED, gates.read("p1").state());
    }
    @Test void approve_MissingEvidence_400() throws Exception {
        var r=post("/control/approve", M.writeValueAsString(Map.of("principal","saurabh","executionPartitionId","p1","epoch",1)));
        assertEquals(400,r.statusCode());
    }
    @Test void approve_WrongMethod_405() throws Exception {
        var c=HttpClient.newHttpClient();
        var req=HttpRequest.newBuilder(URI.create(base+"/control/approve")).GET().build();
        assertEquals(405,c.send(req, HttpResponse.BodyHandlers.ofString()).statusCode());
    }
    @Test void approve_DuplicateAlreadyApplied_Idempotent200() throws Exception {
        post("/control/approve", M.writeValueAsString(Map.of("principal","saurabh","executionPartitionId","p1","epoch",1,"evidenceHash","abc123")));
        var r2=post("/control/approve", M.writeValueAsString(Map.of("principal","saurabh","executionPartitionId","p1","epoch",1,"evidenceHash","abc123")));
        assertEquals(200,r2.statusCode());
        assertTrue(r2.body().contains("ALREADY_APPLIED")||r2.body().contains("ENABLED"));
    }
    @Test void approve_NotFound_404() throws Exception {
        var r=post("/control/approve", M.writeValueAsString(Map.of("principal","saurabh","executionPartitionId","nope","epoch",0,"evidenceHash","h")));
        assertEquals(404,r.statusCode());
    }
    @Test void haltedDefault_NoApproveRemainsHalted() {
        InMemoryGateStateStore fresh=new InMemoryGateStateStore(Set.of("saurabh"));
        fresh.init(new GateRow("p2","acct1",GateState.HALTED,0,"boot","h0",null,null,null,null,0L,null,null,null));
        assertEquals(GateState.HALTED, fresh.read("p2").state());
    }
}
