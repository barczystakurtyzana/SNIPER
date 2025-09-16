/*!
Market Simulator for SNIPER Trading Bot

This is a separate Tokio program that simulates near-real market conditions by generating
virtual tokens with predefined profiles. It runs in parallel with the Bot (SNIPER/bot)
and communicates with the same local validator.

Usage:
    cargo run --bin market_simulator [-- [OPTIONS]]

Options:
    --config <PATH>       Configuration file path (default: config.toml)
    --duration <SECS>     Simulation duration in seconds (default: unlimited)
    --interval-min <MS>   Minimum interval between token generation (default: 500ms)
    --interval-max <MS>   Maximum interval between token generation (default: 5000ms)
    --help                Show this help message

Examples:
    cargo run --bin market_simulator
    cargo run --bin market_simulator -- --duration 3600
    cargo run --bin market_simulator -- --interval-min 1000 --interval-max 3000
*/

use std::env;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use tokio::time;
use tracing::{info, error};
use tracing_subscriber::EnvFilter;

mod token_generator;

use sniffer_bot_light::config::Config;
use sniffer_bot_light::wallet::WalletManager;
use sniffer_bot_light::rpc_manager::{RpcManager, RpcBroadcaster};
use token_generator::{TokenGenerator, SimulatorConfig};

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize logging
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .init();

    info!("Starting Market Simulator...");

    // Parse command line arguments
    let args: Vec<String> = env::args().collect();
    let (_config_path, duration, interval_min, interval_max) = parse_args(&args)?;

    // Load configuration
    let config = Config::load();

    // Setup RPC manager
    let rpc_manager = Arc::new(RpcManager::new_with_config(config.rpc_endpoints.clone(), config.clone()));
    let rpc: Arc<dyn RpcBroadcaster> = rpc_manager.clone();

    // Setup wallet - use random wallet if none configured
    let wallet = if let Some(keypair_path) = &config.keypair_path {
        match WalletManager::from_file(keypair_path) {
            Ok(wallet) => {
                info!("Loaded wallet from {}", keypair_path);
                Arc::new(wallet)
            }
            Err(e) => {
                error!("Failed to load wallet from {}: {}", keypair_path, e);
                info!("Using random wallet for simulation");
                Arc::new(WalletManager::new_random())
            }
        }
    } else {
        info!("No keypair configured, using random wallet for simulation");
        Arc::new(WalletManager::new_random())
    };

    info!("Simulator wallet pubkey: {}", wallet.pubkey());

    // Create simulator configuration
    let simulator_config = SimulatorConfig {
        interval_min: Duration::from_millis(interval_min),
        interval_max: Duration::from_millis(interval_max),
    };

    // Initialize token generator
    let token_generator = TokenGenerator::new(
        rpc.clone(),
        wallet.clone(),
        simulator_config,
    ).await?;

    info!("Token generator initialized, starting simulation...");

    // Start the token generation loop
    let generator_handle = tokio::spawn(async move {
        if let Err(e) = token_generator.run().await {
            error!("Token generator failed: {}", e);
        }
    });

    // Run for specified duration or indefinitely
    if let Some(duration_secs) = duration {
        info!("Running simulation for {} seconds", duration_secs);
        time::sleep(Duration::from_secs(duration_secs)).await;
        info!("Simulation duration completed, shutting down...");
    } else {
        info!("Running simulation indefinitely (Ctrl+C to stop)");
        // Wait for Ctrl+C
        tokio::signal::ctrl_c().await?;
        info!("Received shutdown signal, stopping simulation...");
    }

    // Clean shutdown
    generator_handle.abort();
    info!("Market Simulator stopped");

    Ok(())
}

fn parse_args(args: &[String]) -> Result<(String, Option<u64>, u64, u64)> {
    let mut config_path = "config.toml".to_string();
    let mut duration = None;
    let mut interval_min = 500;
    let mut interval_max = 5000;

    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "--config" => {
                if i + 1 >= args.len() {
                    anyhow::bail!("--config requires a path argument");
                }
                config_path = args[i + 1].clone();
                i += 2;
            }
            "--duration" => {
                if i + 1 >= args.len() {
                    anyhow::bail!("--duration requires a number argument");
                }
                duration = Some(args[i + 1].parse()
                    .with_context(|| "Invalid duration value")?);
                i += 2;
            }
            "--interval-min" => {
                if i + 1 >= args.len() {
                    anyhow::bail!("--interval-min requires a number argument");
                }
                interval_min = args[i + 1].parse()
                    .with_context(|| "Invalid interval-min value")?;
                i += 2;
            }
            "--interval-max" => {
                if i + 1 >= args.len() {
                    anyhow::bail!("--interval-max requires a number argument");
                }
                interval_max = args[i + 1].parse()
                    .with_context(|| "Invalid interval-max value")?;
                i += 2;
            }
            "--help" => {
                print_help();
                std::process::exit(0);
            }
            _ => {
                anyhow::bail!("Unknown argument: {}", args[i]);
            }
        }
    }

    if interval_min >= interval_max {
        anyhow::bail!("interval-min must be less than interval-max");
    }

    Ok((config_path, duration, interval_min, interval_max))
}

fn print_help() {
    println!("Market Simulator for SNIPER Trading Bot");
    println!();
    println!("USAGE:");
    println!("    cargo run --bin market_simulator [-- [OPTIONS]]");
    println!();
    println!("OPTIONS:");
    println!("    --config <PATH>       Configuration file path (default: config.toml)");
    println!("    --duration <SECS>     Simulation duration in seconds (default: unlimited)");
    println!("    --interval-min <MS>   Minimum interval between token generation (default: 500ms)");
    println!("    --interval-max <MS>   Maximum interval between token generation (default: 5000ms)");
    println!("    --help                Show this help message");
    println!();
    println!("EXAMPLES:");
    println!("    cargo run --bin market_simulator");
    println!("    cargo run --bin market_simulator -- --duration 3600");
    println!("    cargo run --bin market_simulator -- --interval-min 1000 --interval-max 3000");
}