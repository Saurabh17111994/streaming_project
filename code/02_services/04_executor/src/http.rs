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

use crate::gate::ExecState;
use crate::gateway_protocol;

/// Point-in-time health snapshot shared between the server and the shutdown path.
#[derive(Debug, Clone)]
pub struct ServerState {
    inner: Arc<Mutex<Snapshot>>,
}

#[derive(Debug)]
struct Snapshot {
    gate: ExecState,
    process_alive: bool,
    draining: bool,
    shared_secret: String,
    protocol_version: String,
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
            })),
        }
    }

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
            })),
        }
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
        })
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

fn route(state: &ServerState, method: &str, path: &str, body: &str) -> Vec<u8> {
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
            // Gate ENABLED + envelope accepted — paper/fake path would forward to bridge here.
            // For T4, acknowledge without executing (no LiveNode wiring yet).
            json(
                202,
                &serde_json::json!({ "accepted": true, "gate_state": snap.gate.as_str() }),
            )
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

    let resp = route(&state, &method, &path, &body);
    let _ = stream.write_all(&resp).await;
    let _ = stream.flush().await;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
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

    #[tokio::test]
    async fn intents_enabled_accepts_valid_envelope() {
        use crate::gateway_protocol::{encode_envelope, sha256_hex, Envelope};
        let payload = serde_json::json!({"instruction_id":"i2"});
        let payload_json = serde_json::to_string(&payload).unwrap();
        let hash = sha256_hex(payload_json.as_bytes());
        let env = Envelope {
            protocol_version: "execution-gateway.v1".into(),
            message_type: "EXECUTION_INTENT".into(),
            request_id: "req-2".into(),
            account_scope_id: "acc-1".into(),
            execution_partition_id: "part-1".into(),
            payload_hash: hash,
            gate_epoch: 1,
            fence_token: "fence-xyz".into(),
            deadline_epoch_ms: 9_999_999_999_999,
            payload: payload.clone(),
            authentication: String::new(),
        };
        let encoded = encode_envelope("s3cr3t", &env).unwrap();
        let state = ServerState::with_gateway_auth(
            ExecState::Enabled,
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
        assert_eq!(s, 202, "body: {b}");
        assert!(b.contains("\"accepted\":true"), "body: {b}");
    }
}
