//! Nautilus execution service — real bootstrap (WP-1) + hosted `LiveNode` run loop (Workstream B).
//!
//! Parses strict config, builds the fail-closed [`Runtime`] (gate boots `HALTED`, health never
//! implies `ENABLED`), starts the [`LiveNodeRuntime`] hosted run loop (bridge execution client
//! boots `HALTED`, no broker commands), serves `/healthz` + `/readyz`, and shuts down cleanly
//! on Ctrl-C / SIGTERM: stop request → run loop returns → draining (`/readyz` flips to 503).
//! No broker credentials are read here.

use std::net::SocketAddr;

use nautilus_execution_service::{
    bootstrap::Runtime,
    config::ServiceConfig,
    engine::{BridgeSelection, LiveNodeRuntime},
    http, telemetry,
};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    telemetry::init_logging("info")?;
    telemetry::METRICS.record_restart();

    let config = ServiceConfig::from_env()?;
    let addr: SocketAddr = config.listen_addr()?;
    // WP-2 remainder (2026-08-21): a configured BRIDGE_ENDPOINT selects the production
    // HttpBridgeClient transport; no endpoint keeps the offline FakeBridge default. Either
    // way the execution client boots HALTED — no broker command flows until an authorized
    // approval advances the gate.
    let selection = BridgeSelection::from_config(&config);
    let bridge_mode = selection.mode();
    let runtime = Runtime::init(config)?;
    let mut node = LiveNodeRuntime::build_with_bridge(selection)?;

    tracing::info!(
        "nautilus-execution-service boot: gate HALTED, bridge mode {bridge_mode}, LiveNode hosted run loop armed, health on {addr} (execution enabled: false)"
    );

    let state = runtime.server_state();
    let server = tokio::spawn(http::serve(addr, state));

    tokio::select! {
        _ = wait_for_shutdown_signal() => {
            tracing::info!("shutdown signal received; stopping LiveNode, draining (readyz -> 503)");
            node.request_shutdown();
        }
        result = node.run_forever() => {
            // The loop must not end on its own in normal operation (node stays HALTED).
            result?;
        }
    }
    runtime.begin_shutdown();
    server.abort();
    let _ = server.await;
    Ok(())
}

#[cfg(unix)]
async fn wait_for_shutdown_signal() -> anyhow::Result<()> {
    use tokio::signal::unix::{signal, SignalKind};
    let mut term = signal(SignalKind::terminate())?;
    tokio::select! {
        _ = tokio::signal::ctrl_c() => {}
        _ = term.recv() => {}
    }
    Ok(())
}

#[cfg(not(unix))]
async fn wait_for_shutdown_signal() -> anyhow::Result<()> {
    tokio::signal::ctrl_c().await?;
    Ok(())
}
