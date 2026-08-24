//! Minimal HTTP health + private gateway intent endpoint (WP-1 + T4).
//!
//! Serves `GET /healthz` (process alive + gate state) and `GET /readyz` (ready to accept
//! inbound; 503 while draining). And `POST /v1/intents` (private gateway envelope,
//! verified via `gateway_protocol` HMAC + payload hash, fail-closed while gate HALTED).
//!
//! Like the bridge transport it is a deliberately small `tokio` HTTP/1.1 server — no web-framework
//! dependency — because the surface is two static health responses plus one private POST read from
//! a shared snapshot. Safety invariant: health never implies ENABLED; intents never execute while
//! HALTED (503) and never log the shared secret.

use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Context as _, Result};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

use crate::bridge::{BridgeClient, ReportOutcome};
use crate::events;
use crate::gate::ExecState;
use crate::gateway_protocol;
use crate::intent;

/// Shared bridge transport used by the ENABLED sync forward (T4a). `tokio::sync::Mutex` is
/// required because `BridgeClient::send_command` takes `&mut self` across an await point; the
/// `Send` bound is required by the spawned server task (the trait object is used only here,
/// never by the non-`Send` nautilus runtime).
pub type BridgeForwarder = Arc<tokio::sync::Mutex<Box<dyn BridgeClient + Send>>>;

/// Point-in-time health snapshot shared between the server and the shutdown path.
#[derive(Clone)]
pub struct ServerState {
    inner: Arc<Mutex<Snapshot>>,
    forwarder: Option<BridgeForwarder>,
}

impl std::fmt::Debug for ServerState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ServerState")
            .field("inner", &self.inner)
            .field("forwarder", &self.forwarder.is_some())
            .finish()
    }
}

#[derive(Debug)]
struct Snapshot {
    gate: ExecState,
    process_alive: bool,
    draining: bool,
    shared_secret: String,
    protocol_version: String,
    gateway_endpoint: String,
    /// First unresolved UNKNOWN outcome (WP-2 §UNKNOWN): arms the 15 s escalation.
    unknown_first_seen: Option<std::time::Instant>,
    /// Escalation window; production pins 15 s (dossier), tests shrink it.
    unknown_escalation: std::time::Duration,
    /// Set once the escalation fires: an operator must review before any re-enable.
    operator_review_required: bool,
}

impl ServerState {
    /// A fresh server state: process alive, gate as given, not draining, no gateway auth.
    pub fn new(gate: ExecState) -> Self {
        Self {
            inner: Arc::new(Mutex::new(Snapshot {
                gate,
                process_alive: true,
                draining: false,
                shared_secret: String::new(),
                protocol_version: "execution-gateway.v1".into(),
                gateway_endpoint: String::new(),
                unknown_first_seen: None,
                unknown_escalation: Self::UNKNOWN_ESCALATION,
                operator_review_required: false,
            })),
            forwarder: None,
        }
    }

    /// Production escalation window for an unresolved UNKNOWN outcome (dossier
    /// WP-2 §UNKNOWN: 15 s global halt timer + operator review hook).
    pub const UNKNOWN_ESCALATION: std::time::Duration = std::time::Duration::from_secs(15);

    /// With private gateway auth (shared secret + expected protocol version) for POST /v1/intents.
    pub fn with_gateway_auth(
        gate: ExecState,
        shared_secret: String,
        protocol_version: String,
    ) -> Self {
        Self {
            inner: Arc::new(Mutex::new(Snapshot {
                gate,
                process_alive: true,
                draining: false,
                shared_secret,
                protocol_version,
                gateway_endpoint: String::new(),
                unknown_first_seen: None,
                unknown_escalation: Self::UNKNOWN_ESCALATION,
                operator_review_required: false,
            })),
            forwarder: None,
        }
    }

    /// Shrinks the UNKNOWN escalation window (tests only — production keeps 15 s).
    #[cfg(test)]
    fn with_unknown_escalation(self, d: std::time::Duration) -> Self {
        if let Ok(mut s) = self.inner.lock() {
            s.unknown_escalation = d;
        }
        self
    }

    /// Attaches the bridge transport for the ENABLED sync forward (T4a). When absent the
    /// ENABLED path returns the paper 202 ack without executing (offline/test construction).
    #[must_use]
    pub fn with_forwarder(mut self, forwarder: BridgeForwarder) -> Self {
        self.forwarder = Some(forwarder);
        self
    }

    /// Sets the gateway base URL for normalized event emission (A2.4 leg). Empty (default)
    /// disables emission — the 202 then reports `event_emission: disabled`.
    #[must_use]
    pub fn with_gateway_endpoint(self, gateway_endpoint: String) -> Self {
        if let Ok(mut s) = self.inner.lock() {
            s.gateway_endpoint = gateway_endpoint;
        }
        self
    }

    /// Marks the service as draining (shutdown in progress); `/readyz` then returns 503.
    pub fn set_draining(&self, draining: bool) {
        if let Ok(mut s) = self.inner.lock() {
            s.draining = draining;
        }
    }

    fn snapshot(&self) -> Snapshot {
        self.inner.lock().map(|s| s.clone()).unwrap_or(Snapshot {
            gate: ExecState::Halted,
            process_alive: false,
            draining: true,
            shared_secret: String::new(),
            protocol_version: "execution-gateway.v1".into(),
            gateway_endpoint: String::new(),
            unknown_first_seen: None,
            unknown_escalation: Self::UNKNOWN_ESCALATION,
            operator_review_required: true,
        })
    }

    /// Records the first unresolved UNKNOWN outcome and arms its one-shot escalation
    /// task (WP-2 §UNKNOWN: never retried; after 15 s force-HALT + operator review).
    fn record_unknown_outcome(&self) {
        let escalation = {
            let mut s = match self.inner.lock() {
                Ok(s) => s,
                Err(_) => return,
            };
            if s.unknown_first_seen.is_some() || s.operator_review_required {
                return; // already armed / already escalated
            }
            s.unknown_first_seen = Some(std::time::Instant::now());
            s.unknown_escalation
        };
        let st = self.clone();
        tokio::spawn(async move {
            tokio::time::sleep(escalation).await;
            st.escalate_unknown_review();
        });
    }

    /// Escalation hook: force the gate back to HALTED (safety halt from any state)
    /// and raise the operator-review flag. Idempotent; a no-op when the outcome was
    /// resolved (first-seen cleared) or already escalated.
    fn escalate_unknown_review(&self) {
        let mut s = match self.inner.lock() {
            Ok(s) => s,
            Err(_) => return,
        };
        let Some(t0) = s.unknown_first_seen else {
            return;
        };
        if t0.elapsed() < s.unknown_escalation || s.operator_review_required {
            return;
        }
        if s.gate != ExecState::Halted {
            // Forced safety halt from any state (gate invariant: uncertain → HALTED).
            s.gate = ExecState::Halted;
        }
        s.operator_review_required = true;
        tracing::error!(
            "UNKNOWN bridge outcome unresolved for >= {:?}: gate force-HALTED, operator review required before re-enable",
            s.unknown_escalation
        );
    }
}

impl Clone for Snapshot {
    fn clone(&self) -> Self {
        Self {
            gate: self.gate,
            process_alive: self.process_alive,
            draining: self.draining,
            shared_secret: self.shared_secret.clone(),
            protocol_version: self.protocol_version.clone(),
            gateway_endpoint: self.gateway_endpoint.clone(),
            unknown_first_seen: self.unknown_first_seen,
            unknown_escalation: self.unknown_escalation,
            operator_review_required: self.operator_review_required,
        }
    }
}

/// Health document for `/healthz`.
pub fn health_json(state: &ServerState) -> serde_json::Value {
    let s = state.snapshot();
    let trading_ready = s.gate == ExecState::Enabled;
    serde_json::json!({
        "service": "nautilus-execution-service",
        "process_alive": s.process_alive,
        "gate_state": s.gate.as_str(),
        "trading_ready": trading_ready,
        "draining": s.draining,
        "enabled": s.gate == ExecState::Enabled,
        "operator_review_required": s.operator_review_required,
    })
}

fn text(status: u16, body: &str) -> Vec<u8> {
    let mut out = format!(
        "HTTP/1.1 {status}\r\nContent-Type: text/plain\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    )
    .into_bytes();
    out.extend_from_slice(body.as_bytes());
    out
}

fn json(status: u16, v: &serde_json::Value) -> Vec<u8> {
    let body = v.to_string();
    let mut out = format!(
        "HTTP/1.1 {status}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    )
    .into_bytes();
    out.extend_from_slice(body.as_bytes());
    out
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

async fn route(state: &ServerState, method: &str, path: &str, body: &str) -> Vec<u8> {
    match (method, path) {
        ("GET", "/healthz") => json(200, &health_json(state)),
        ("GET", "/readyz") => {
            let s = state.snapshot();
            if s.draining {
                json(
                    503,
                    &serde_json::json!({ "ready": false, "draining": true }),
                )
            } else {
                json(
                    200,
                    &serde_json::json!({ "ready": true, "gate_state": s.gate.as_str() }),
                )
            }
        }
        ("POST", "/v1/intents") => {
            let snap = state.snapshot();
            // Never log the secret.
            let ver = gateway_protocol::verify(
                body,
                &snap.shared_secret,
                &snap.protocol_version,
                now_ms(),
            );
            if !ver.accepted {
                let doc = serde_json::json!({ "accepted": false, "reason": ver.reason });
                // 401 for auth/hash/version, as the gateway protocol defines.
                return json(401, &doc);
            }
            // Fail closed while HALTED — no execution, but prove the private route is wired.
            if snap.gate != ExecState::Enabled {
                let doc = serde_json::json!({
                    "accepted": false,
                    "reason": "gate HALTED",
                    "gate_state": snap.gate.as_str(),
                });
                return json(503, &doc);
            }
            // Gate ENABLED + envelope accepted — T4a sync leg: forward the order to the
            // bridge transport (Fake locally, Http in production) and map the synchronous
            // report. Fail-closed mapping: SUCCESS → 202, REJECTED → 409, UNKNOWN/transport
            // error → 503 (never an ack, never a retry — a lost ack is reconciled by query).
            let Some(forwarder) = &state.forwarder else {
                return json(
                    202,
                    &serde_json::json!({
                        "accepted": true,
                        "gate_state": snap.gate.as_str(),
                        "reason": "no bridge transport configured (paper ack)",
                    }),
                );
            };
            let envelope = ver
                .envelope
                .expect("accepted verification carries the decoded envelope");
            // Optional `action` field: absent → "place" (back-compatible with the
            // pinned schema-v1 payload bytes); "cancel" → cancel mapping (requires
            // broker_order_id). Anything else fails closed with 422.
            let action = envelope
                .payload
                .get("action")
                .and_then(|v| v.as_str())
                .unwrap_or("place")
                .to_ascii_lowercase();
            let cmd_env = match action.as_str() {
                "place" => intent::place_envelope_from_payload(&envelope.payload),
                "cancel" => intent::cancel_envelope_from_payload(&envelope.payload),
                other => Err(format!("unsupported action: {other}")),
            };
            let cmd_env = match cmd_env {
                Ok(p) => p,
                Err(reason) => {
                    return json(
                        422,
                        &serde_json::json!({
                            "accepted": false,
                            "reason": reason,
                            "gate_state": snap.gate.as_str(),
                        }),
                    );
                }
            };
            // Scope the bridge lock to the send only: a concurrent /v1/intents must not
            // serialize behind the gateway emission round-trip (a hung gateway would
            // otherwise stall every subsequent order submit).
            let submit = {
                let mut guard = forwarder.lock().await;
                guard.send_command(cmd_env.clone()).await
            };
            match submit {
                Ok(report) if report.is_success() => {
                    // A2.4 leg: emit the normalized lifecycle+correlation image to the
                    // gateway /v1/events intake. The order is already accepted — a failed
                    // emission never rewrites the 202, it is surfaced as event_emission.
                    let trade_context_id = envelope
                        .payload
                        .get("trade_context_id")
                        .and_then(|v| v.as_str())
                        .unwrap_or("")
                        .to_string();
                    let now = now_ms();
                    let event = events::lifecycle_event_value(
                        &report,
                        &cmd_env,
                        &envelope.account_scope_id,
                        &envelope.execution_partition_id,
                        envelope.gate_epoch,
                        &trade_context_id,
                        now,
                    );
                    // Empty endpoint = emission disabled (offline/paper mode) — distinct
                    // from a real emission failure, which the operator must see.
                    let delivery = if snap.gateway_endpoint.is_empty() {
                        "disabled".to_string()
                    } else {
                        match events::emit_event(
                            &snap.gateway_endpoint,
                            &snap.shared_secret,
                            &snap.protocol_version,
                            &event,
                            now,
                        )
                        .await
                        {
                            Ok(()) => "accepted".to_string(),
                            Err(e) => format!("failed: {e}"),
                        }
                    };
                    json(
                        202,
                        &serde_json::json!({
                            "accepted": true,
                            "gate_state": snap.gate.as_str(),
                            "action": action,
                            "instruction_id": cmd_env.instruction_id,
                            "execution_attempt_id": cmd_env.execution_attempt_id,
                            "client_order_ref": cmd_env.client_order_ref,
                            "broker_order_id": report.broker_order_id,
                            "order_status": report.order_status,
                            "event_emission": delivery,
                        }),
                    )
                }
                Ok(report) if report.outcome() == Some(ReportOutcome::Rejected) => json(
                    409,
                    &serde_json::json!({
                        "accepted": false,
                        "outcome": "REJECTED",
                        "reason": report.reason,
                        "gate_state": snap.gate.as_str(),
                    }),
                ),
                Ok(report) => {
                    state.record_unknown_outcome();
                    json(
                        503,
                        &serde_json::json!({
                            "accepted": false,
                            "outcome": "UNKNOWN",
                            "reason": if report.reason.is_empty() {
                                "bridge UNKNOWN outcome".to_string()
                            } else {
                                report.reason
                            },
                            "gate_state": snap.gate.as_str(),
                        }),
                    )
                }
                Err(e) => {
                    state.record_unknown_outcome();
                    json(
                        503,
                        &serde_json::json!({
                            "accepted": false,
                            "outcome": "UNKNOWN",
                            "reason": e.to_string(),
                            "gate_state": snap.gate.as_str(),
                        }),
                    )
                }
            }
        }
        (m, "/healthz") | (m, "/readyz") if m != "GET" => text(405, "method_not_allowed"),
        (_, "/v1/intents") if method != "POST" => text(405, "method_not_allowed"),
        _ => text(404, "not_found"),
    }
}

/// Binds `addr` and serves health/readiness + private intents until the process exits.
pub async fn serve(addr: SocketAddr, state: ServerState) -> Result<()> {
    let listener = TcpListener::bind(addr)
        .await
        .with_context(|| format!("bind health server {addr}"))?;
    loop {
        let (stream, _) = match listener.accept().await {
            Ok(x) => x,
            Err(_) => continue,
        };
        let st = state.clone();
        tokio::spawn(async move {
            if let Err(e) = handle_conn(stream, st).await {
                tracing::debug!("health connection closed: {e}");
            }
        });
    }
}

async fn handle_conn(mut stream: TcpStream, state: ServerState) -> Result<()> {
    // Read headers + optional body. We support Content-Length only (no chunked).
    let mut buf = Vec::new();
    let mut chunk = [0u8; 4096];
    let header_end = loop {
        match stream.read(&mut chunk).await {
            Ok(0) => return Ok(()),
            Ok(n) => {
                buf.extend_from_slice(&chunk[..n]);
                if let Some(idx) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
                    break idx + 4;
                }
                if buf.len() > 64 * 1024 {
                    return Ok(());
                }
            }
            Err(_) => return Ok(()),
        }
    };
    let h_end = header_end;
    let header_str = String::from_utf8_lossy(&buf[..h_end]).to_string();
    let mut lines = header_str.lines();
    let request_line = lines.next().unwrap_or("");
    let mut parts = request_line.split_whitespace();
    let method = parts.next().unwrap_or("").to_string();
    let raw_path = parts.next().unwrap_or("").to_string();
    let path = raw_path.split('?').next().unwrap_or("").to_string();

    // Parse Content-Length (case-insensitive).
    let mut content_length: usize = 0;
    for line in lines {
        if line.is_empty() {
            break;
        }
        if let Some((k, v)) = line.split_once(':') {
            if k.trim().eq_ignore_ascii_case("content-length") {
                content_length = v.trim().parse::<usize>().unwrap_or(0);
                // Cap to 1 MiB to avoid abuse.
                if content_length > 1024 * 1024 {
                    let resp = text(413, "payload_too_large");
                    let _ = stream.write_all(&resp).await;
                    let _ = stream.flush().await;
                    return Ok(());
                }
            }
        }
    }

    // Body may already be partially in buf beyond header_end; read remainder.
    let mut body_bytes = buf[h_end..].to_vec();
    while body_bytes.len() < content_length {
        let need = content_length - body_bytes.len();
        let to_read = need.min(chunk.len());
        match stream.read(&mut chunk[..to_read]).await {
            Ok(0) => break,
            Ok(n) => body_bytes.extend_from_slice(&chunk[..n]),
            Err(_) => break,
        }
    }
    body_bytes.truncate(content_length);
    let body = String::from_utf8_lossy(&body_bytes).to_string();

    let resp = route(&state, &method, &path, &body).await;
    let _ = stream.write_all(&resp).await;
    let _ = stream.flush().await;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bridge::{CommandScript, FakeBridge};
    use crate::gate::ExecState;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    async fn raw_request(addr: SocketAddr, request: &str) -> (u16, String) {
        let mut stream = TcpStream::connect(addr).await.unwrap();
        stream.write_all(request.as_bytes()).await.unwrap();
        stream.flush().await.unwrap();
        let mut resp = Vec::new();
        let mut chunk = [0u8; 4096];
        loop {
            match stream.read(&mut chunk).await {
                Ok(0) => break,
                Ok(n) => resp.extend_from_slice(&chunk[..n]),
                Err(_) => break,
            }
        }
        let s = String::from_utf8_lossy(&resp);
        let status = s
            .split_whitespace()
            .nth(1)
            .and_then(|x| x.parse::<u16>().ok())
            .unwrap_or(0);
        let body = s.split("\r\n\r\n").nth(1).unwrap_or("").to_string();
        (status, body)
    }

    async fn raw_get(addr: SocketAddr, request: &str) -> (u16, String) {
        raw_request(addr, request).await
    }

    async fn spawn_server(state: ServerState) -> SocketAddr {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            loop {
                let Ok((stream, _)) = listener.accept().await else {
                    continue;
                };
                let st = state.clone();
                tokio::spawn(async move {
                    let _ = handle_conn(stream, st).await;
                });
            }
        });
        addr
    }

    #[tokio::test]
    async fn healthz_reports_halted_and_not_trading() {
        let state = ServerState::new(ExecState::Halted);
        let addr = spawn_server(state.clone()).await;
        let (status, body) = raw_get(addr, "GET /healthz HTTP/1.1\r\nHost: x\r\n\r\n").await;
        assert_eq!(status, 200);
        assert!(body.contains("\"gate_state\":\"HALTED\""), "body: {body}");
        assert!(body.contains("\"trading_ready\":false"), "body: {body}");
        assert!(body.contains("\"enabled\":false"), "body: {body}");
    }

    #[tokio::test]
    async fn readyz_ok_while_running_503_while_draining() {
        let state = ServerState::new(ExecState::Halted);
        let addr = spawn_server(state.clone()).await;
        let (s1, b1) = raw_get(addr, "GET /readyz HTTP/1.1\r\nHost: x\r\n\r\n").await;
        assert_eq!(s1, 200);
        assert!(b1.contains("\"ready\":true"), "body: {b1}");

        state.set_draining(true);
        let (s2, _) = raw_get(addr, "GET /readyz HTTP/1.1\r\nHost: x\r\n\r\n").await;
        assert_eq!(s2, 503);
    }

    #[tokio::test]
    async fn unknown_path_404_and_non_get_405() {
        let state = ServerState::new(ExecState::Halted);
        let addr = spawn_server(state.clone()).await;
        let (s1, _) = raw_get(addr, "GET /nope HTTP/1.1\r\nHost: x\r\n\r\n").await;
        assert_eq!(s1, 404);
        let (s2, _) = raw_get(addr, "POST /healthz HTTP/1.1\r\nHost: x\r\n\r\n").await;
        assert_eq!(s2, 405);
    }

    #[tokio::test]
    async fn intents_requires_post() {
        let state = ServerState::new(ExecState::Halted);
        let addr = spawn_server(state.clone()).await;
        let (s, _) = raw_get(addr, "GET /v1/intents HTTP/1.1\r\nHost: x\r\n\r\n").await;
        assert_eq!(s, 405);
    }

    #[tokio::test]
    async fn intents_rejects_bad_auth_while_halted() {
        let state = ServerState::with_gateway_auth(
            ExecState::Halted,
            "s3cr3t".into(),
            "execution-gateway.v1".into(),
        );
        let addr = spawn_server(state.clone()).await;
        let body = "{\"garbage\":1}";
        let req = format!(
            "POST /v1/intents HTTP/1.1\r\nHost: x\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{}",
            body.len(),
            body
        );
        let (s, b) = raw_request(addr, &req).await;
        assert_eq!(s, 401, "body: {b}");
        assert!(b.contains("accepted"), "body: {b}");
    }

    #[tokio::test]
    async fn intents_halted_returns_503_even_with_valid_envelope() {
        use crate::gateway_protocol::{encode_envelope, sha256_hex, Envelope};
        let payload = serde_json::json!({"instruction_id":"i1"});
        let payload_json = serde_json::to_string(&payload).unwrap();
        let hash = sha256_hex(payload_json.as_bytes());
        let env = Envelope {
            protocol_version: "execution-gateway.v1".into(),
            message_type: "EXECUTION_INTENT".into(),
            request_id: "req-1".into(),
            account_scope_id: "acc-1".into(),
            execution_partition_id: "part-1".into(),
            payload_hash: hash,
            gate_epoch: 1,
            fence_token: "fence-abc".into(),
            deadline_epoch_ms: 9_999_999_999_999,
            payload: payload.clone(),
            authentication: String::new(),
        };
        let encoded = encode_envelope("s3cr3t", &env).unwrap();
        let state = ServerState::with_gateway_auth(
            ExecState::Halted,
            "s3cr3t".into(),
            "execution-gateway.v1".into(),
        );
        let addr = spawn_server(state.clone()).await;
        let req = format!(
            "POST /v1/intents HTTP/1.1\r\nHost: x\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{}",
            encoded.len(),
            encoded
        );
        let (s, b) = raw_request(addr, &req).await;
        assert_eq!(s, 503, "body: {b}");
        assert!(b.contains("HALTED"), "body: {b}");
        assert!(b.contains("\"accepted\":false"), "body: {b}");
    }

    fn bieq_payload_json() -> String {
        serde_json::to_string(&serde_json::json!({
            "instruction_id": "T9-SB-0001",
            "symbol": "BI-EQ",
            "exchange": "NSE",
            "side": "BUY",
            "quantity": 1,
            "order_type": "LIMIT",
            "limit_price_paise": 5050,
            "product_type": "CNC",
            "time_in_force": "DAY",
        }))
        .unwrap()
    }

    async fn encoded_request_with_payload(secret: &str, payload: serde_json::Value) -> String {
        use crate::gateway_protocol::{encode_envelope, sha256_hex, Envelope};
        let payload_json = serde_json::to_string(&payload).unwrap();
        let hash = sha256_hex(payload_json.as_bytes());
        let env = Envelope {
            protocol_version: "execution-gateway.v1".into(),
            message_type: "EXECUTION_INTENT".into(),
            request_id: "req-t4a".into(),
            account_scope_id: "acc-1".into(),
            execution_partition_id: "part-1".into(),
            payload_hash: hash,
            gate_epoch: 1,
            fence_token: "fence-t4a".into(),
            deadline_epoch_ms: 9_999_999_999_999,
            payload,
            authentication: String::new(),
        };
        let encoded = encode_envelope(secret, &env).unwrap();
        format!(
            "POST /v1/intents HTTP/1.1\r\nHost: x\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{}",
            encoded.len(),
            encoded
        )
    }

    async fn encoded_bieq_request(secret: &str) -> String {
        let payload = serde_json::from_str(&bieq_payload_json()).unwrap();
        encoded_request_with_payload(secret, payload).await
    }

    fn fake_forwarder(script: CommandScript) -> BridgeForwarder {
        let mut fake = FakeBridge::new();
        fake.script(script);
        Arc::new(tokio::sync::Mutex::new(
            Box::new(fake) as Box<dyn BridgeClient + Send>
        ))
    }

    fn enabled_state(forwarder: BridgeForwarder) -> ServerState {
        ServerState::with_gateway_auth(
            ExecState::Enabled,
            "s3cr3t".into(),
            "execution-gateway.v1".into(),
        )
        .with_forwarder(forwarder)
    }

    #[tokio::test]
    async fn intents_enabled_forwards_to_bridge_and_returns_start_report() {
        let state = enabled_state(fake_forwarder(CommandScript::Accept));
        let addr = spawn_server(state).await;
        let req = encoded_bieq_request("s3cr3t").await;
        let (s, b) = raw_request(addr, &req).await;
        assert_eq!(s, 202, "body: {b}");
        assert!(b.contains("\"accepted\":true"), "body: {b}");
        // The minted 14-hex deterministic ref is echoed with the bridge's broker id.
        assert!(b.contains("\"client_order_ref\":"), "body: {b}");
        assert!(b.contains("\"broker_order_id\":\"BRK-0001\""), "body: {b}");
        assert!(b.contains("\"execution_attempt_id\":"), "body: {b}");
        assert!(b.contains("\"gate_state\":\"ENABLED\""), "body: {b}");
        let v: serde_json::Value = serde_json::from_str(&b).unwrap();
        let ref_ = v["client_order_ref"].as_str().unwrap();
        assert_eq!(ref_.len(), 14, "deterministic 14-hex ref: {ref_}");
        assert!(ref_.chars().all(|c| c.is_ascii_hexdigit()));
    }

    #[tokio::test]
    async fn intents_enabled_bridge_unknown_returns_503_never_ack() {
        let state = enabled_state(fake_forwarder(CommandScript::Unknown(
            "synthetic-unknown".into(),
        )));
        let addr = spawn_server(state).await;
        let req = encoded_bieq_request("s3cr3t").await;
        let (s, b) = raw_request(addr, &req).await;
        assert_eq!(s, 503, "body: {b}");
        assert!(b.contains("\"accepted\":false"), "body: {b}");
        assert!(b.contains("\"outcome\":\"UNKNOWN\""), "body: {b}");
        assert!(
            !b.contains("broker_order_id"),
            "UNKNOWN must not leak a broker id: {b}"
        );
    }

    #[tokio::test]
    async fn unknown_outcome_escalates_to_halt_and_operator_review() {
        // Shrunk escalation window (production pins 15 s): UNKNOWN → 503, then the
        // watchdog force-HALTs the gate and raises operator_review_required.
        let state = enabled_state(fake_forwarder(CommandScript::Unknown(
            "synthetic-unknown".into(),
        )))
        .with_unknown_escalation(std::time::Duration::from_millis(80));
        let addr = spawn_server(state).await;
        let req = encoded_bieq_request("s3cr3t").await;
        let (s, b) = raw_request(addr, &req).await;
        assert_eq!(s, 503, "body: {b}");
        // Poll /healthz until the escalation fires.
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
        loop {
            let (_, hb) = raw_request(addr, "GET /healthz HTTP/1.1\r\nHost: x\r\n\r\n").await;
            let review = hb.contains("\"operator_review_required\":true");
            if review || std::time::Instant::now() > deadline {
                assert!(review, "escalation never fired; last body: {hb}");
                assert!(
                    hb.contains("\"gate_state\":\"HALTED\""),
                    "watchdog must force HALT; last body: {hb}"
                );
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
    }
    #[tokio::test]
    async fn intents_enabled_bridge_rejected_returns_409() {
        let state = enabled_state(fake_forwarder(CommandScript::Reject(
            "synthetic-reject".into(),
        )));
        let addr = spawn_server(state).await;
        let req = encoded_bieq_request("s3cr3t").await;
        let (s, b) = raw_request(addr, &req).await;
        assert_eq!(s, 409, "body: {b}");
        assert!(b.contains("\"accepted\":false"), "body: {b}");
        assert!(b.contains("\"outcome\":\"REJECTED\""), "body: {b}");
        assert!(b.contains("synthetic-reject"), "body: {b}");
    }

    #[tokio::test]
    async fn intents_enabled_rejects_unsupported_payload_mapping() {
        let state = enabled_state(fake_forwarder(CommandScript::Accept));
        let addr = spawn_server(state).await;
        // Drop symbol from the payload: the mapper must fail closed (422), never forward.
        let mut payload: serde_json::Value = serde_json::from_str(&bieq_payload_json()).unwrap();
        payload.as_object_mut().unwrap().remove("symbol");
        let req = encoded_request_with_payload("s3cr3t", payload).await;
        let (s, b) = raw_request(addr, &req).await;
        assert_eq!(s, 422, "body: {b}");
        assert!(b.contains("\"accepted\":false"), "body: {b}");
        assert!(b.contains("symbol required"), "body: {b}");
    }

    #[tokio::test]
    async fn intents_enabled_cancel_action_cancels_placed_order() {
        // Two scripts: one per bridge round-trip (place, then cancel) — the fake
        // consumes scripts FIFO, one per send_command.
        let mut fake = FakeBridge::new();
        fake.script(CommandScript::Accept);
        fake.script(CommandScript::Accept);
        let forwarder = Arc::new(tokio::sync::Mutex::new(
            Box::new(fake) as Box<dyn BridgeClient + Send>
        ));
        let state = enabled_state(forwarder);
        let addr = spawn_server(state).await;
        // Place first: the fake mints BRK-0001 into its order book.
        let (sp, bp) = raw_request(addr, &encoded_bieq_request("s3cr3t").await).await;
        assert_eq!(sp, 202, "place body: {bp}");
        assert!(
            bp.contains("\"broker_order_id\":\"BRK-0001\""),
            "body: {bp}"
        );
        // Cancel it via the action discriminator: same forwarder state, 202 + CANCELED.
        let payload = serde_json::json!({
            "action": "cancel",
            "instruction_id": "T9-SB-0001",
            "broker_order_id": "BRK-0001",
        });
        let (sc, bc) =
            raw_request(addr, &encoded_request_with_payload("s3cr3t", payload).await).await;
        assert_eq!(sc, 202, "cancel body: {bc}");
        assert!(bc.contains("\"action\":\"cancel\""), "body: {bc}");
        assert!(
            bc.contains("\"broker_order_id\":\"BRK-0001\""),
            "body: {bc}"
        );
        // The fake carries the terminal CANCELED status on the async report; the
        // sync ack may omit order_status (null here) — the lifecycle event leg
        // normalizes that to CANCELED (see events.rs default_state).
    }

    #[tokio::test]
    async fn intents_enabled_rejects_unknown_action_with_422() {
        let state = enabled_state(fake_forwarder(CommandScript::Accept));
        let addr = spawn_server(state).await;
        let mut payload: serde_json::Value = serde_json::from_str(&bieq_payload_json()).unwrap();
        payload["action"] = serde_json::json!("amend");
        let req = encoded_request_with_payload("s3cr3t", payload).await;
        let (s, b) = raw_request(addr, &req).await;
        assert_eq!(s, 422, "body: {b}");
        assert!(b.contains("unsupported action"), "body: {b}");
    }

    #[tokio::test]
    async fn intents_enabled_emits_event_and_reports_accepted() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        // Fake gateway /v1/events intake.
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let gw_addr = listener.local_addr().unwrap();
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<String>();
        let gateway = tokio::spawn(async move {
            let (mut s, _) = listener.accept().await.unwrap();
            let mut buf = Vec::new();
            let mut chunk = [0u8; 4096];
            let n = s.read(&mut chunk).await.unwrap();
            buf.extend_from_slice(&chunk[..n]);
            tx.send(String::from_utf8_lossy(&buf).to_string()).unwrap();
            s.write_all(
                b"HTTP/1.1 202 Accepted\r\nContent-Length: 2\r\nConnection: close\r\n\r\nok",
            )
            .await
            .unwrap();
            drop(tx);
        });

        let state = enabled_state(fake_forwarder(CommandScript::Accept))
            .with_gateway_endpoint(format!("http://{gw_addr}"));
        let addr = spawn_server(state).await;
        let req = encoded_bieq_request("s3cr3t").await;
        let (s, b) = raw_request(addr, &req).await;
        assert_eq!(s, 202, "body: {b}");
        assert!(b.contains("\"event_emission\":\"accepted\""), "body: {b}");
        let captured = rx.recv().await.expect("gateway received the event");
        assert!(
            captured.starts_with("POST /v1/events HTTP/1.1"),
            "captured: {captured}"
        );
        assert!(
            captured.contains("LIFECYCLE"),
            "captured contains event type"
        );
        assert!(
            captured.contains("\"brokerOrderId\":\"BRK-0001\""),
            "captured: {captured}"
        );
        gateway.await.unwrap();
    }

    #[tokio::test]
    async fn intents_enabled_without_gateway_endpoint_reports_disabled() {
        let state = enabled_state(fake_forwarder(CommandScript::Accept));
        let addr = spawn_server(state).await;
        let req = encoded_bieq_request("s3cr3t").await;
        let (s, b) = raw_request(addr, &req).await;
        assert_eq!(s, 202, "body: {b}");
        assert!(b.contains("\"event_emission\":\"disabled\""), "body: {b}");
    }
}
