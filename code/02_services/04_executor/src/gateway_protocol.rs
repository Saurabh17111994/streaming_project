//! Private gateway-to-Nautilus authenticated envelope (T4).
//!
//! Faithful Rust port of `com.trading.execution.gateway.GatewayProtocol`:
//! - canonical string is `protocol_version\nmessage_type\nrequest_id\naccount_scope_id\n`
//!   `execution_partition_id\npayload_hash\ngate_epoch\nfence_token\ndeadline_epoch_ms\n`
//!   `payload_json` (payload is the compact JSON encoding of the `payload` node);
//! - authentication is `hex(hmacSha256(canonical, secret))`;
//! - payload hash is `hex(sha256(payload_json_bytes))`;
//! - verification checks version, required identity fields, deadline, HMAC, and
//!   payload hash — same order and semantics as the Java `verify` method.
//!
//! The service **never** logs the shared secret.

use hmac::{Hmac, Mac};
use sha2::{Digest, Sha256};

type HmacSha256 = Hmac<Sha256>;

/// Result of envelope verification, mirroring `GatewayProtocol.Verification`.
#[derive(Debug, Clone)]
pub struct Verification {
    pub accepted: bool,
    pub reason: String,
    pub envelope: Option<Envelope>,
}

#[derive(Debug, Clone)]
pub struct Envelope {
    pub protocol_version: String,
    pub message_type: String,
    pub request_id: String,
    pub account_scope_id: String,
    pub execution_partition_id: String,
    pub payload_hash: String,
    pub gate_epoch: i64,
    pub fence_token: String,
    pub deadline_epoch_ms: i64,
    pub payload: serde_json::Value,
    pub authentication: String,
}

/// Compute `hex(sha256(bytes))` — same as Java `GatewayProtocol.sha256`.
pub fn sha256_hex(bytes: &[u8]) -> String {
    let mut h = Sha256::new();
    h.update(bytes);
    hex::encode(h.finalize())
}

fn hmac_hex(secret: &str, value: &str) -> String {
    let mut mac =
        HmacSha256::new_from_slice(secret.as_bytes()).expect("HMAC accepts any key length");
    mac.update(value.as_bytes());
    hex::encode(mac.finalize().into_bytes())
}

fn canonical(e: &Envelope, payload_json: &str) -> String {
    [
        e.protocol_version.as_str(),
        e.message_type.as_str(),
        e.request_id.as_str(),
        e.account_scope_id.as_str(),
        e.execution_partition_id.as_str(),
        e.payload_hash.as_str(),
        &e.gate_epoch.to_string(),
        e.fence_token.as_str(),
        &e.deadline_epoch_ms.to_string(),
        payload_json,
    ]
    .join("\n")
}

fn text(v: &serde_json::Value, field: &str) -> String {
    v.get(field)
        .and_then(|x| x.as_str())
        .unwrap_or("")
        .to_string()
}

fn reject(reason: &str) -> Verification {
    Verification {
        accepted: false,
        reason: reason.to_string(),
        envelope: None,
    }
}

/// Encode an unsigned envelope: compute canonical + HMAC and return the full JSON string.
///
/// Mirrors `GatewayProtocol.encode` — the caller supplies all fields except `authentication`.
pub fn encode_envelope(secret: &str, e: &Envelope) -> Result<String, String> {
    if secret.is_empty() {
        return Err("secret required".to_string());
    }
    // payload_json must be the compact encoding the Java ObjectMapper would produce.
    // We use serde_json::to_string which is compact; Java's default is also compact
    // for the payload node (no pretty). For cross-language fidelity the test vectors
    // use the same compact form.
    let payload_json = serde_json::to_string(&e.payload).map_err(|e| e.to_string())?;
    let canon = canonical(e, &payload_json);
    let auth = hmac_hex(secret, &canon);
    let mut node = serde_json::Map::new();
    node.insert(
        "protocol_version".into(),
        serde_json::Value::String(e.protocol_version.clone()),
    );
    node.insert(
        "message_type".into(),
        serde_json::Value::String(e.message_type.clone()),
    );
    node.insert(
        "request_id".into(),
        serde_json::Value::String(e.request_id.clone()),
    );
    node.insert(
        "account_scope_id".into(),
        serde_json::Value::String(e.account_scope_id.clone()),
    );
    node.insert(
        "execution_partition_id".into(),
        serde_json::Value::String(e.execution_partition_id.clone()),
    );
    node.insert(
        "payload_hash".into(),
        serde_json::Value::String(e.payload_hash.clone()),
    );
    node.insert(
        "gate_epoch".into(),
        serde_json::Value::Number(e.gate_epoch.into()),
    );
    node.insert(
        "fence_token".into(),
        serde_json::Value::String(e.fence_token.clone()),
    );
    node.insert(
        "deadline_epoch_ms".into(),
        serde_json::Value::Number(e.deadline_epoch_ms.into()),
    );
    node.insert("payload".into(), e.payload.clone());
    node.insert("authentication".into(), serde_json::Value::String(auth));
    serde_json::to_string(&serde_json::Value::Object(node)).map_err(|e| e.to_string())
}

/// Verify a JSON envelope string — mirrors `GatewayProtocol.verify`.
pub fn verify(json: &str, secret: &str, expected_version: &str, now_ms: i64) -> Verification {
    if secret.is_empty() {
        return reject("authentication failed");
    }
    let v: serde_json::Value = match serde_json::from_str(json) {
        Ok(x) => x,
        Err(_) => return reject("malformed envelope"),
    };
    let payload = v.get("payload").cloned().unwrap_or(serde_json::Value::Null);
    let e = Envelope {
        protocol_version: text(&v, "protocol_version"),
        message_type: text(&v, "message_type"),
        request_id: text(&v, "request_id"),
        account_scope_id: text(&v, "account_scope_id"),
        execution_partition_id: text(&v, "execution_partition_id"),
        payload_hash: text(&v, "payload_hash"),
        gate_epoch: v
            .get("gate_epoch")
            .and_then(|x| x.as_i64())
            .unwrap_or(i64::MIN),
        fence_token: text(&v, "fence_token"),
        deadline_epoch_ms: v
            .get("deadline_epoch_ms")
            .and_then(|x| x.as_i64())
            .unwrap_or(i64::MIN),
        payload: payload.clone(),
        authentication: text(&v, "authentication"),
    };
    if e.protocol_version != expected_version {
        return reject("unsupported version");
    }
    if e.request_id.is_empty()
        || e.account_scope_id.is_empty()
        || e.execution_partition_id.is_empty()
        || e.payload_hash.is_empty()
        || e.fence_token.is_empty()
    {
        return reject("missing identity");
    }
    if e.deadline_epoch_ms < now_ms {
        return reject("deadline expired");
    }
    // Recompute canonical + HMAC using the same payload_json the sender used.
    // The sender's payload_json is the compact encoding of the payload node.
    let payload_json = match serde_json::to_string(&payload) {
        Ok(s) => s,
        Err(_) => return reject("malformed envelope"),
    };
    let canon = canonical(&e, &payload_json);
    let expected_auth = hmac_hex(secret, &canon);
    if expected_auth != e.authentication {
        return reject("authentication failed");
    }
    let payload_hash = sha256_hex(payload_json.as_bytes());
    if payload_hash != e.payload_hash {
        return reject("payload hash mismatch");
    }
    Verification {
        accepted: true,
        reason: "accepted".to_string(),
        envelope: Some(e),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn envelope(payload: serde_json::Value) -> Envelope {
        let payload_json = serde_json::to_string(&payload).unwrap();
        let hash = sha256_hex(payload_json.as_bytes());
        Envelope {
            protocol_version: "execution-gateway.v1".into(),
            message_type: "EXECUTION_INTENT".into(),
            request_id: "req-1".into(),
            account_scope_id: "acc-1".into(),
            execution_partition_id: "part-1".into(),
            payload_hash: hash,
            gate_epoch: 7,
            fence_token: "fence-abc".into(),
            deadline_epoch_ms: 9_999_999_999_999,
            payload,
            authentication: String::new(),
        }
    }

    #[test]
    fn round_trip_accepted() {
        let e = envelope(json!({"instruction_id":"i1"}));
        let encoded = encode_envelope("s3cr3t", &e).unwrap();
        let v = verify(&encoded, "s3cr3t", "execution-gateway.v1", 1_000_000);
        assert!(v.accepted, "reason: {}", v.reason);
    }

    #[test]
    fn rejects_wrong_secret() {
        let e = envelope(json!({"x":1}));
        let encoded = encode_envelope("s3cr3t", &e).unwrap();
        let v = verify(&encoded, "wrong", "execution-gateway.v1", 1_000_000);
        assert!(!v.accepted);
        assert_eq!(v.reason, "authentication failed");
    }

    #[test]
    fn rejects_expired_deadline() {
        let mut e = envelope(json!({"x":1}));
        e.deadline_epoch_ms = 1000;
        let encoded = encode_envelope("s3cr3t", &e).unwrap();
        let v = verify(&encoded, "s3cr3t", "execution-gateway.v1", 2_000);
        assert!(!v.accepted);
        assert_eq!(v.reason, "deadline expired");
    }

    #[test]
    fn rejects_wrong_version() {
        let e = envelope(json!({"x":1}));
        let encoded = encode_envelope("s3cr3t", &e).unwrap();
        let v = verify(&encoded, "s3cr3t", "other.v9", 1_000_000);
        assert!(!v.accepted);
        assert_eq!(v.reason, "unsupported version");
    }

    #[test]
    fn rejects_missing_identity() {
        let mut e = envelope(json!({"x":1}));
        e.request_id = String::new();
        let encoded = encode_envelope("s3cr3t", &e).unwrap();
        // Manually blank the field after encode to simulate missing.
        let mut v: serde_json::Value = serde_json::from_str(&encoded).unwrap();
        v["request_id"] = serde_json::Value::String(String::new());
        let s = serde_json::to_string(&v).unwrap();
        let vr = verify(&s, "s3cr3t", "execution-gateway.v1", 1_000_000);
        assert!(!vr.accepted);
        assert_eq!(vr.reason, "missing identity");
    }

    #[test]
    fn payload_hash_mismatch() {
        let e = envelope(json!({"x":1}));
        let mut encoded = encode_envelope("s3cr3t", &e).unwrap();
        // Tamper payload after encode without updating hash.
        let mut v: serde_json::Value = serde_json::from_str(&encoded).unwrap();
        v["payload"] = json!({"x":2});
        encoded = serde_json::to_string(&v).unwrap();
        let vr = verify(&encoded, "s3cr3t", "execution-gateway.v1", 1_000_000);
        assert!(
            !vr.accepted,
            "tampered payload should fail auth: {}",
            vr.reason
        );
        // Auth will fail first; but if we recompute auth for tampered payload, hash mismatches.
        // To test hash path, re-encode with correct auth for new payload but keep old hash.
        let mut e2 = envelope(json!({"x":2}));
        e2.payload_hash = e.payload_hash.clone(); // keep old hash
        let enc2 = encode_envelope("s3cr3t", &e2).unwrap();
        let vr2 = verify(&enc2, "s3cr3t", "execution-gateway.v1", 1_000_000);
        assert!(!vr2.accepted);
        assert_eq!(vr2.reason, "payload hash mismatch");
    }
    #[test]
    fn deadline_boundary_at_now_is_valid_not_expired() {
        // TIME-004 / TIME-005 skew boundary: verify() expires an envelope only
        // when `deadline_epoch_ms < now_ms` (strict). A deadline exactly equal to
        // now — i.e. zero residual latency, no skew, no backward drift — must be
        // accepted; otherwise a healthy order would be phantom-expired. This
        // pins "latency is never negative / UTC epoch is the monotonic reference".
        let mut e = envelope(json!({"x": 1}));
        e.deadline_epoch_ms = 1_000_000;
        let encoded = encode_envelope("s3cr3t", &e).unwrap();

        // now_ms == deadline -> NOT expired (strict <).
        let v = verify(&encoded, "s3cr3t", "execution-gateway.v1", 1_000_000);
        assert!(v.accepted, "deadline == now must be accepted, got: {}", v.reason);

        // now_ms one past the deadline -> expired.
        let v2 = verify(&encoded, "s3cr3t", "execution-gateway.v1", 1_000_001);
        assert!(!v2.accepted);
        assert_eq!(v2.reason, "deadline expired");
    }
    #[test]
    fn time008_envelope_serialize_deserialize_roundtrips_exactly() {
        // TIME-008 ser/deser: encode -> verify reconstructs the SAME envelope — fields,
        // hash and payload survive byte-for-byte (stable UTC/version encoding).
        let e = envelope(json!({"instruction_id":"i1","symbol":"wti","qty":10}));
        let encoded = encode_envelope("s3cr3t", &e).unwrap();
        let v = verify(&encoded, "s3cr3t", "execution-gateway.v1", 1_000_000);
        assert!(v.accepted, "reason: {}", v.reason);
        let got = v.envelope.expect("accepted envelope decodes");
        assert_eq!(got.protocol_version, e.protocol_version);
        assert_eq!(got.message_type, e.message_type);
        assert_eq!(got.request_id, e.request_id);
        assert_eq!(got.account_scope_id, e.account_scope_id);
        assert_eq!(got.execution_partition_id, e.execution_partition_id);
        assert_eq!(got.payload_hash, e.payload_hash);
        assert_eq!(got.gate_epoch, e.gate_epoch);
        assert_eq!(got.fence_token, e.fence_token);
        assert_eq!(got.deadline_epoch_ms, e.deadline_epoch_ms);
        assert_eq!(
            serde_json::to_string(&got.payload).unwrap(),
            serde_json::to_string(&e.payload).unwrap(),
            "payload must round-trip byte-identically (TIME-008)"
        );
    }

}
