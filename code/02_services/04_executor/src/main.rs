//! Nautilus execution service — real bootstrap (WP-1).
//!
//! Parses strict config, builds the fail-closed [`Runtime`] (gate boots `HALTED`, health never
//! implies `ENABLED`), serves `/healthz` + `/readyz`, and shuts down gracefully on Ctrl-C /
//! SIGTERM (marks draining so `/readyz` flips to 503). No broker credentials are read here.

use std::net::SocketAddr;

use nautilus_execution_service::{bootstrap::Runtime, config::ServiceConfig, http, telemetry};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    telemetry::init_logging("info")?;
    telemetry::METRICS.record_restart();

    let config = ServiceConfig::from_env()?;
    let addr: SocketAddr = config.listen_addr()?;
    let runtime = Runtime::init(config)?;

    tracing::info!(
        "nautilus-execution-service boot: gate HALTED, health on {addr} (execution enabled: false)"
    );

    let state = runtime.server_state();
    let server = tokio::spawn(http::serve(addr, state));

    wait_for_shutdown_signal().await?;
    tracing::info!("shutdown signal received; draining (readyz -> 503)");
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
