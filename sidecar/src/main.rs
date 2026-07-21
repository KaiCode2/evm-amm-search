use std::{env, path::PathBuf, sync::Arc};

use anyhow::{Context, Result, anyhow, bail};
use evm_amm_route_sidecar::{SERVICE_VERSION, api, config::SidecarConfig, node::RoutingSupervisor};
use tokio::{net::TcpListener, task::LocalSet};
use tracing::{info, warn};
use tracing_subscriber::EnvFilter;

fn main() -> Result<()> {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .context("build Tokio runtime")?;
    let local = LocalSet::new();
    runtime.block_on(local.run_until(run()))
}

async fn run() -> Result<()> {
    let (path, check_only) = arguments()?;
    let config = Arc::new(SidecarConfig::load(&path)?);
    init_tracing(config.server.json_logs)?;
    if check_only {
        println!(
            "configuration valid: chain={} profile={:#x}",
            config.chain.expected_chain_id, config.profile_fingerprint
        );
        return Ok(());
    }
    if config.server.admin_bearer_token.is_none() {
        warn!(
            "admin bearer token is unset; prewarm and refresh endpoints rely on network isolation"
        );
    }

    let listen = config.server.listen.clone();
    let supervisor = RoutingSupervisor::bootstrap(Arc::clone(&config)).await?;
    let listener = TcpListener::bind(&listen)
        .await
        .with_context(|| format!("bind HTTP server to {listen}"))?;
    info!(%listen, "HTTP sidecar listening");
    let app = api::router(api::AppState::new(Arc::clone(&supervisor)));
    let result = axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await;
    supervisor.shutdown().await;
    result.context("serve routing sidecar")
}

fn arguments() -> Result<(PathBuf, bool)> {
    let mut arguments = env::args().skip(1);
    let mut path = env::var_os("AMM_ROUTE_CONFIG")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("/etc/amm-route-sidecar/config.toml"));
    let mut check_only = false;
    while let Some(argument) = arguments.next() {
        match argument.as_str() {
            "--config" => {
                path = arguments
                    .next()
                    .map(PathBuf::from)
                    .context("--config requires a path")?;
            }
            "--check-config" => check_only = true,
            "--version" | "-V" => {
                println!("evm-amm-route-sidecar {SERVICE_VERSION}");
                std::process::exit(0);
            }
            "--help" | "-h" => {
                println!(
                    "evm-amm-route-sidecar [--config PATH] [--check-config] [--version]\n\nAMM_ROUTE_CONFIG may also set the TOML path."
                );
                std::process::exit(0);
            }
            value => bail!("unknown argument {value}"),
        }
    }
    Ok((path, check_only))
}

fn init_tracing(json: bool) -> Result<()> {
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("evm_amm_route_sidecar=info,tower_http=info"));
    if json {
        tracing_subscriber::fmt()
            .with_env_filter(filter)
            .json()
            .try_init()
            .map_err(|error| anyhow!(error.to_string()))?;
    } else {
        tracing_subscriber::fmt()
            .with_env_filter(filter)
            .try_init()
            .map_err(|error| anyhow!(error.to_string()))?;
    }
    Ok(())
}

async fn shutdown_signal() {
    #[cfg(unix)]
    {
        let mut terminate =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
                .expect("install SIGTERM handler");
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {}
            _ = terminate.recv() => {}
        }
    }
    #[cfg(not(unix))]
    let _ = tokio::signal::ctrl_c().await;
    info!("shutdown signal received");
}
