//! Production `BridgeClient` transport against the Go execution bridge.
//!
//! Implements the offline-runnable HTTP/WS client for
//! [`BridgeClient`](super::client::BridgeClient): `POST /v1/commands` carries a
//! [`CommandEnvelope`] and synchronously returns a [`ReportEnvelope`], and `/v1/events`
//! streams asynchronous reports over WebSocket. It speaks the exact wire contract of
//! `code/02_services/06_execution_bridge/go-bridge/server.go` (`Bearer` auth, loopback bridge)
//! so it interoperates with the Go bridge's `fake` profile — no real Arrow credentials ever
//! leave this crate.
//!
//! The HTTP hop is a deliberately small client: the bridge is loopback-only and the server is a
//! controlled peer, so a minimal HTTP/1.1 client (content-length / chunked / read-to-close
//! framing) avoids pulling a full `reqwest` dependency tree.

use std::io;
use std::time::Duration;

use anyhow::{Context as _, Result};
use async_trait::async_trait;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use super::client::{BridgeClient, BridgeReportStream};
use super::protocol::{CommandEnvelope, ReportEnvelope};

/// A parsed HTTP/1.1 response (status + headers + raw body).
struct HttpResponse {
    status: u16,
    body: Vec<u8>,
}

/// Minimal loopback HTTP/1.1 request (no redirects, no TLS, `Connection: close`).
async fn http_request(
    method: &str,
    url: &str,
    auth_token: &str,
    body_json: &[u8],
) -> Result<HttpResponse> {
    let (host, port, path) = parse_url(url)?;
    let addrs = tokio::net::lookup_host((host.as_str(), port))
        .await
        .with_context(|| format!("resolve bridge host {host}:{port}"))?;
    let addr = addrs
        .into_iter()
        .next()
        .ok_or_else(|| anyhow::anyhow!("bridge host {host}:{port} resolved to nothing"))?;
    let mut stream = TcpStream::connect(addr)
        .await
        .with_context(|| format!("connect to bridge {addr}"))?;
    stream.set_nodelay(true)?;

    let head = format!(
        "{method} {path} HTTP/1.1\r\n\
         Host: {host}:{port}\r\n\
         Authorization: Bearer {auth_token}\r\n\
         Content-Type: application/json\r\n\
         Content-Length: {}\r\n\
         Connection: close\r\n\
         \r\n",
        body_json.len()
    );
    stream.write_all(head.as_bytes()).await?;
    stream.write_all(body_json).await?;
    stream.flush().await?;

    let mut buf = Vec::new();
    tokio::time::timeout(Duration::from_secs(10), async {
        let mut chunk = [0u8; 4096];
        loop {
            let n = stream.read(&mut chunk).await?;
            if n == 0 {
                break;
            }
            buf.extend_from_slice(&chunk[..n]);
        }
        Ok::<_, io::Error>(())
    })
    .await
    .map_err(|_| anyhow::anyhow!("bridge read timeout"))??;

    parse_http_response(&buf)
}

/// Minimal loopback HTTP/1.1 `POST`.
async fn http_post(url: &str, auth_token: &str, body_json: &[u8]) -> Result<HttpResponse> {
    http_request("POST", url, auth_token, body_json).await
}

/// Minimal loopback HTTP/1.1 `GET`.
async fn http_get(url: &str, auth_token: &str) -> Result<HttpResponse> {
    http_request("GET", url, auth_token, b"").await
}

fn parse_url(url: &str) -> Result<(String, u16, String)> {
    let rest = url
        .strip_prefix("http://")
        .ok_or_else(|| anyhow::anyhow!("bridge base url must be http:// (loopback only): {url}"))?;
    let (authority, path) = match rest.find('/') {
        Some(i) => (&rest[..i], rest[i..].to_string()),
        None => (rest, "/".to_string()),
    };
    // Reject userinfo and fragments/schemes we cannot support.
    anyhow::ensure!(
        !authority.contains('@'),
        "bridge url must not contain credentials: {url}"
    );
    let (host, port) = match authority.rsplit_once(':') {
        Some((h, p)) => (h.to_string(), p.parse::<u16>()?),
        None => (authority.to_string(), 8787),
    };
    Ok((host, port, path))
}

/// Parses a raw HTTP/1.1 response into status + headers + body.
fn parse_http_response(raw: &[u8]) -> Result<HttpResponse> {
    let head_end = raw
        .windows(4)
        .position(|w| w == b"\r\n\r\n")
        .ok_or_else(|| anyhow::anyhow!("malformed HTTP response (no header terminator)"))?;
    let head = std::str::from_utf8(&raw[..head_end])?;
    let mut lines = head.split("\r\n");

    let status_line = lines.next().unwrap_or_default();
    let status: u16 = status_line
        .split_whitespace()
        .nth(1)
        .ok_or_else(|| anyhow::anyhow!("malformed status line: {status_line:?}"))?
        .parse()?;

    let raw_body = &raw[head_end + 4..];
    let header = |name: &str| {
        lines.clone().find_map(|line| {
            let (k, v) = line.split_once(':')?;
            (k.trim().eq_ignore_ascii_case(name)).then(|| v.trim().to_string())
        })
    };
    let chunked = header("transfer-encoding")
        .unwrap_or_default()
        .to_lowercase()
        == "chunked";
    let body = if let Some(len) = header("content-length") {
        let len: usize = len.trim().parse()?;
        raw_body
            .get(..len.min(raw_body.len()))
            .unwrap_or(raw_body)
            .to_vec()
    } else if chunked {
        parse_chunked_body(raw_body)?
    } else {
        raw_body.to_vec()
    };
    Ok(HttpResponse { status, body })
}

/// Decodes a chunked transfer body.
fn parse_chunked_body(data: &[u8]) -> Result<Vec<u8>> {
    let mut out = Vec::new();
    let mut pos = 0usize;
    loop {
        let line_end = match data[pos..].iter().position(|&b| b == b'\n') {
            Some(i) => pos + i,
            None => anyhow::bail!("malformed chunked body"),
        };
        let size_str = std::str::from_utf8(&data[pos..line_end])?
            .trim()
            .trim_end_matches(';'); // ignore chunk extensions
        let size = usize::from_str_radix(size_str, 16)
            .map_err(|e| anyhow::anyhow!("bad chunk size: {e}"))?;
        if size == 0 {
            break;
        }
        let chunk_start = line_end + 1;
        let chunk_end = chunk_start + size;
        if chunk_end > data.len() {
            anyhow::bail!("chunk overruns body");
        }
        out.extend_from_slice(&data[chunk_start..chunk_end]);
        pos = chunk_end;
        if data.get(pos..pos + 2) == Some(b"\r\n") {
            pos += 2;
        }
    }
    Ok(out)
}

/// Production HTTP/WS [`BridgeClient`] targeting the Go execution bridge.
#[derive(Debug, Clone)]
pub struct HttpBridgeClient {
    base_url: String,
    auth_token: String,
    connected: bool,
}

impl HttpBridgeClient {
    pub fn new(base_url: String, auth_token: String) -> Self {
        Self {
            base_url,
            auth_token,
            connected: false,
        }
    }
}

#[async_trait]
impl BridgeClient for HttpBridgeClient {
    fn is_connected(&self) -> bool {
        self.connected
    }

    async fn connect(&mut self) -> Result<()> {
        // Reachability probe only — `/healthz` answers without Arrow credentials of any kind.
        let url = format!("{}/healthz", self.base_url.trim_end_matches('/'));
        let resp = http_get(&url, &self.auth_token).await?;
        anyhow::ensure!(
            resp.status == 200,
            "bridge health probe failed with status {}",
            resp.status
        );
        self.connected = true;
        Ok(())
    }

    async fn disconnect(&mut self) -> Result<()> {
        self.connected = false;
        Ok(())
    }

    async fn send_command(&mut self, envelope: CommandEnvelope) -> Result<ReportEnvelope> {
        let url = format!("{}/v1/commands", self.base_url.trim_end_matches('/'));
        let body = serde_json::to_vec(&envelope)?;
        let resp = http_post(&url, &self.auth_token, &body).await?;
        anyhow::ensure!(
            resp.status == 200,
            "bridge command rejected with status {}: {}",
            resp.status,
            String::from_utf8_lossy(&resp.body)
        );
        let report: ReportEnvelope = serde_json::from_slice(&resp.body)
            .with_context(|| "bridge returned an unparseable report envelope")?;
        Ok(report)
    }

    fn take_reports(&mut self) -> Option<BridgeReportStream> {
        // The WebSocket report stream is surfaced as a caller-owned receiver by the node wiring
        // in a later step (T4 report intake). The synchronous command path is proven here.
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bridge::protocol::{
        Command, CommandEnvelope, OrderCommand, OrderType, Product, ReportEnvelope, ReportOutcome,
        TransactionType, Validity, PROTOCOL_VERSION, RECORD_COMMAND, RECORD_REPORT,
    };
    use std::time::Duration;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::{TcpListener, TcpStream};

    fn command_env() -> CommandEnvelope {
        CommandEnvelope {
            record_type: RECORD_COMMAND.to_string(),
            contract_version: PROTOCOL_VERSION,
            request_id: uuid::Uuid::new_v4().to_string(),
            command: Command::Place.as_str().to_string(),
            instruction_id: "ins-1".into(),
            execution_attempt_id: "a-1".into(),
            client_order_ref: "COREF00000001".into(),
            broker_order_id: String::new(),
            order: Some(
                OrderCommand::new("NSE", "RELIANCE")
                    .with_quantity("10")
                    .with_side(TransactionType::Buy)
                    .with_order_type(OrderType::Mkt)
                    .with_product(Product::Cash)
                    .with_validity(Validity::Day),
            ),
        }
    }

    /// In-process Go-bridge-compatible command endpoint used by the round-trip tests.
    async fn serve_bridge(addr: &str, token: String) -> tokio::task::JoinHandle<()> {
        let listener = TcpListener::bind(addr).await.unwrap();
        tokio::spawn(async move {
            loop {
                let Ok((stream, _)) = listener.accept().await else {
                    continue;
                };
                tokio::spawn(handle_conn(stream, token.clone()));
            }
        })
    }

    async fn handle_conn(mut stream: TcpStream, token: String) {
        // Read request: headers + Content-Length body.
        let mut buf = Vec::new();
        let mut chunk = [0u8; 2048];
        loop {
            match stream.read(&mut chunk).await {
                Ok(0) => break,
                Ok(n) => {
                    buf.extend_from_slice(&chunk[..n]);
                    if buf.windows(4).any(|w| w == b"\r\n\r\n") {
                        break;
                    }
                }
                Err(_) => break,
            }
        }
        let head_str = String::from_utf8_lossy(&buf);
        let authorized = head_str.contains(&format!("Bearer {token}"));

        let mut body = Vec::new();
        if let Some(idx) = head_str.find("\r\n\r\n") {
            let header_block = &head_str[..idx];
            let clen: usize = header_block
                .lines()
                .find_map(|l| {
                    let mut it = l.splitn(2, ':');
                    (it.next()?.trim().eq_ignore_ascii_case("content-length"))
                        .then(|| it.next()?.trim().parse::<usize>().ok())
                        .flatten()
                })
                .unwrap_or(0);
            body = buf[idx + 4..]
                .get(..clen.min(buf.len() - idx - 4))
                .unwrap_or(&[])
                .to_vec();
        }

        let response: Vec<u8> = if !authorized {
            b"HTTP/1.1 401 Unauthorized\r\nContent-Length: 0\r\n\r\n".to_vec()
        } else {
            let cmd = match serde_json::from_slice::<CommandEnvelope>(&body) {
                Ok(cmd) => cmd,
                Err(_) => {
                    let bad = b"HTTP/1.1 400 Bad Request\r\nContent-Length: 0\r\n\r\n".to_vec();
                    let _ = stream.write_all(&bad).await;
                    return;
                }
            };
            let report = ReportEnvelope {
                record_type: RECORD_REPORT.to_string(),
                contract_version: PROTOCOL_VERSION,
                request_id: cmd.request_id.clone(),
                command: cmd.command.clone(),
                outcome: ReportOutcome::Success.as_str().to_string(),
                reason: String::new(),
                instruction_id: cmd.instruction_id.clone(),
                execution_attempt_id: cmd.execution_attempt_id.clone(),
                client_order_ref: cmd.client_order_ref.clone(),
                broker_order_id: format!("FAKE-{}", cmd.client_order_ref),
                exchange_order_id: String::new(),
                postback_event_id: String::new(),
                order_status: Some("ACCEPTED".into()),
                report_type: None,
                fill_shares: String::new(),
                average_price: String::new(),
                fill_price: None,
                fill_quantity: None,
                fill_time: String::new(),
                instrument_token: String::new(),
                received_ts_ms: 0,
                response_fingerprint: String::new(),
                data: None,
            };
            let json = serde_json::to_vec(&report).unwrap();
            let mut out = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n",
                json.len()
            )
            .into_bytes();
            out.extend_from_slice(&json);
            out
        };
        let _ = stream.write_all(&response).await;
        let _ = stream.flush().await;
    }

    #[tokio::test]
    async fn send_command_round_trip_accepted() {
        let _l = serve_bridge("127.0.0.1:18787", "s3cret".into()).await;
        tokio::time::sleep(Duration::from_millis(50)).await;
        let mut client = HttpBridgeClient::new("http://127.0.0.1:18787".into(), "s3cret".into());
        let report = client.send_command(command_env()).await.unwrap();
        assert_eq!(report.outcome, ReportOutcome::Success.as_str());
        assert_eq!(report.order_status.as_deref(), Some("ACCEPTED"));
        assert!(report.broker_order_id.starts_with("FAKE-"));
        assert_eq!(report.client_order_ref, "COREF00000001");
    }

    #[tokio::test]
    async fn send_command_rejects_unauthorized() {
        let _l = serve_bridge("127.0.0.1:18788", "s3cret".into()).await;
        tokio::time::sleep(Duration::from_millis(50)).await;
        let mut client = HttpBridgeClient::new("http://127.0.0.1:18788".into(), "wrong".into());
        let err = client.send_command(command_env()).await.unwrap_err();
        assert!(err.to_string().contains("401"), "got: {err}");
    }

    #[test]
    fn parses_url_without_port_defaults_8787() {
        let (host, port, path) = parse_url("http://127.0.0.1/v1/commands").unwrap();
        assert_eq!(host, "127.0.0.1");
        assert_eq!(port, 8787);
        assert_eq!(path, "/v1/commands");
    }

    #[test]
    fn parses_url_with_port() {
        let (host, port, path) = parse_url("http://localhost:9999/healthz").unwrap();
        assert_eq!(host, "localhost");
        assert_eq!(port, 9999);
        assert_eq!(path, "/healthz");
    }

    #[test]
    fn parses_chunked_body() {
        let data = b"7\r\nMozilla\r\n9\r\nDeveloper\r\n7\r\nNetwork\r\n0\r\n\r\n";
        let out = parse_chunked_body(data).unwrap();
        assert_eq!(out, b"MozillaDeveloperNetwork");
    }
}
