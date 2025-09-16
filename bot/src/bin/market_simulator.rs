/*!
Market Simulator - Complete simulation environment for SNIPER trading bot testing

This binary orchestrates the complete Market Simulator environment including
both token generation and market making activities to create realistic
trading scenarios for bot testing.
*/

use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use tokio::time::sleep;
use tracing::{info, warn, error};

use sniffer_bot_light::market_maker::{MarketMaker, MarketMakerConfig};
use sniffer_bot_light::test_environment::{TestEnvironment, TestValidatorConfig};
use sniffer_bot_light::types::TokenProfile;

/// Configuration for market simulation
#[derive(Debug, Clone)]
pub struct SimulationConfig {
    /// Duration to run the simulation (in seconds)
    pub duration_secs: u64,
    /// Number of tokens to simulate
    pub token_count: usize,
    /// MarketMaker configuration
    pub market_maker: MarketMakerConfig,
    /// Test environment configuration
    pub test_env: TestValidatorConfig,
}

impl Default for SimulationConfig {
    fn default() -> Self {
        Self {
            duration_secs: 300, // 5 minutes
            token_count: 15,
            market_maker: MarketMakerConfig {
                loop_interval_ms: 1000,
                trader_wallet_count: 8,
                gem_min_duration_mins: 1,
                gem_max_duration_mins: 3,
                rug_min_sleep_mins: 1,
                rug_max_sleep_mins: 2,
                trash_transaction_count: 3,
            },
            test_env: TestValidatorConfig::default(),
        }
    }
}

/// Market simulator orchestrator
pub struct MarketSimulator {
    config: SimulationConfig,
    test_env: Option<TestEnvironment>,
    market_maker: Option<Arc<MarketMaker>>,
}

impl MarketSimulator {
    /// Create a new market simulator
    pub fn new(config: SimulationConfig) -> Self {
        Self {
            config,
            test_env: None,
            market_maker: None,
        }
    }

    /// Initialize the simulation environment
    pub async fn initialize(&mut self) -> Result<()> {
        info!("🔧 Initializing Market Simulator environment");

        // Initialize test environment
        let mut test_env = TestEnvironment::new(self.config.test_env.clone());
        
        // Note: In a real implementation, you would start the test validator
        // For this demo, we'll just initialize the MarketMaker component
        info!("📊 Setting up MarketMaker");
        test_env.init_market_maker(Some(self.config.market_maker.clone()))?;
        
        self.test_env = Some(test_env);
        
        // Create standalone MarketMaker for direct control
        let market_maker = Arc::new(MarketMaker::new(self.config.market_maker.clone())?);
        self.market_maker = Some(market_maker);

        info!("✅ Market Simulator environment initialized");
        Ok(())
    }

    /// Add test tokens to the simulation
    pub async fn setup_tokens(&self) -> Result<()> {
        info!("📈 Setting up {} test tokens", self.config.token_count);

        let market_maker = self.market_maker.as_ref()
            .ok_or_else(|| anyhow::anyhow!("MarketMaker not initialized"))?;

        // Calculate token distribution
        let gem_count = (self.config.token_count as f64 * 0.3) as usize; // 30% gems
        let rug_count = (self.config.token_count as f64 * 0.2) as usize; // 20% rug pulls
        let trash_count = self.config.token_count - gem_count - rug_count; // Rest are trash

        // Add gem tokens
        for i in 0..gem_count {
            let mint = solana_sdk::pubkey::Pubkey::new_unique();
            market_maker.add_token(mint, TokenProfile::Gem).await?;
            info!("💎 Added Gem token {}/{}: {}", i + 1, gem_count, mint);
        }

        // Add rug pull tokens
        for i in 0..rug_count {
            let mint = solana_sdk::pubkey::Pubkey::new_unique();
            market_maker.add_token(mint, TokenProfile::RugPull).await?;
            info!("💀 Added RugPull token {}/{}: {}", i + 1, rug_count, mint);
        }

        // Add trash tokens
        for i in 0..trash_count {
            let mint = solana_sdk::pubkey::Pubkey::new_unique();
            market_maker.add_token(mint, TokenProfile::Trash).await?;
            info!("🗑️ Added Trash token {}/{}: {}", i + 1, trash_count, mint);
        }

        let total_added = market_maker.get_token_count().await;
        info!("✅ Added {} tokens total (Gems: {}, Rugs: {}, Trash: {})", 
              total_added, gem_count, rug_count, trash_count);

        Ok(())
    }

    /// Start the market simulation
    pub async fn start_simulation(&self) -> Result<tokio::task::JoinHandle<Result<()>>> {
        info!("🚀 Starting market simulation");

        let market_maker = self.market_maker.as_ref()
            .ok_or_else(|| anyhow::anyhow!("MarketMaker not initialized"))?;

        let mm_clone = market_maker.clone();
        let handle = tokio::spawn(async move {
            mm_clone.start().await.context("MarketMaker execution failed")
        });

        info!("📊 Market simulation started");
        Ok(handle)
    }

    /// Stop the market simulation
    pub async fn stop_simulation(&self) -> Result<()> {
        info!("🛑 Stopping market simulation");

        if let Some(market_maker) = &self.market_maker {
            market_maker.stop().await;
        }

        if let Some(test_env) = &self.test_env {
            test_env.stop_market_maker().await?;
        }

        info!("✅ Market simulation stopped");
        Ok(())
    }

    /// Run the complete simulation
    pub async fn run(&mut self) -> Result<()> {
        info!("🎭 Starting Market Simulator");

        // Initialize environment
        self.initialize().await?;

        // Setup tokens
        self.setup_tokens().await?;

        // Start simulation
        let simulation_handle = self.start_simulation().await?;

        // Monitor simulation
        info!("⏳ Running simulation for {} seconds", self.config.duration_secs);
        let start_time = std::time::Instant::now();
        let mut last_report = start_time;

        while start_time.elapsed().as_secs() < self.config.duration_secs {
            // Report progress every 30 seconds
            if last_report.elapsed().as_secs() >= 30 {
                let elapsed = start_time.elapsed().as_secs();
                let remaining = self.config.duration_secs.saturating_sub(elapsed);
                
                if let Some(market_maker) = &self.market_maker {
                    let token_count = market_maker.get_token_count().await;
                    info!("📊 Simulation progress: {}s elapsed, {}s remaining, {} active tokens", 
                          elapsed, remaining, token_count);
                }
                
                last_report = std::time::Instant::now();
            }

            sleep(Duration::from_secs(5)).await;
        }

        // Stop simulation
        self.stop_simulation().await?;

        // Wait for simulation thread to complete
        if let Err(e) = simulation_handle.await {
            warn!("Simulation task join error: {}", e);
        }

        info!("🎉 Market simulation completed successfully");
        Ok(())
    }

    /// Get simulation statistics
    pub async fn get_stats(&self) -> Result<SimulationStats> {
        let token_count = if let Some(market_maker) = &self.market_maker {
            market_maker.get_token_count().await
        } else {
            0
        };

        Ok(SimulationStats {
            active_tokens: token_count,
            duration_secs: self.config.duration_secs,
            total_configured_tokens: self.config.token_count,
        })
    }
}

/// Simulation statistics
#[derive(Debug)]
pub struct SimulationStats {
    pub active_tokens: usize,
    pub duration_secs: u64,
    pub total_configured_tokens: usize,
}

/// CLI configuration for the market simulator
#[derive(Debug)]
struct CliConfig {
    duration_secs: u64,
    token_count: usize,
    trader_wallets: usize,
    loop_interval_ms: u64,
}

impl Default for CliConfig {
    fn default() -> Self {
        Self {
            duration_secs: 300, // 5 minutes
            token_count: 15,
            trader_wallets: 8,
            loop_interval_ms: 1000,
        }
    }
}

/// Parse command line arguments
fn parse_args() -> CliConfig {
    let args: Vec<String> = std::env::args().collect();
    let mut config = CliConfig::default();
    
    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "--duration" => {
                if i + 1 < args.len() {
                    config.duration_secs = args[i + 1].parse().unwrap_or(config.duration_secs);
                    i += 1;
                }
            }
            "--tokens" => {
                if i + 1 < args.len() {
                    config.token_count = args[i + 1].parse().unwrap_or(config.token_count);
                    i += 1;
                }
            }
            "--traders" => {
                if i + 1 < args.len() {
                    config.trader_wallets = args[i + 1].parse().unwrap_or(config.trader_wallets);
                    i += 1;
                }
            }
            "--interval" => {
                if i + 1 < args.len() {
                    config.loop_interval_ms = args[i + 1].parse().unwrap_or(config.loop_interval_ms);
                    i += 1;
                }
            }
            "--help" => {
                print_help();
                std::process::exit(0);
            }
            _ => {}
        }
        i += 1;
    }
    
    config
}

/// Print help message
fn print_help() {
    println!("Market Simulator - Complete trading environment simulation");
    println!();
    println!("Usage: market_simulator [OPTIONS]");
    println!();
    println!("Options:");
    println!("  --duration <SECS>   Simulation duration in seconds (default: 300)");
    println!("  --tokens <N>        Number of tokens to simulate (default: 15)");
    println!("  --traders <N>       Number of trader wallets (default: 8)");
    println!("  --interval <MS>     Loop interval in milliseconds (default: 1000)");
    println!("  --help              Show this help message");
    println!();
    println!("Example:");
    println!("  market_simulator --duration 600 --tokens 25 --traders 10");
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter("info")
        .init();

    let cli_config = parse_args();
    
    info!("🎭 Market Simulator Starting");
    info!("Configuration: {:?}", cli_config);
    
    // Create simulation configuration
    let config = SimulationConfig {
        duration_secs: cli_config.duration_secs,
        token_count: cli_config.token_count,
        market_maker: MarketMakerConfig {
            loop_interval_ms: cli_config.loop_interval_ms,
            trader_wallet_count: cli_config.trader_wallets,
            gem_min_duration_mins: 1,
            gem_max_duration_mins: 3,
            rug_min_sleep_mins: 1,
            rug_max_sleep_mins: 2,
            trash_transaction_count: 3,
        },
        test_env: TestValidatorConfig::default(),
    };
    
    // Create and run simulator
    let mut simulator = MarketSimulator::new(config);
    
    // Run the simulation
    match simulator.run().await {
        Ok(()) => {
            let stats = simulator.get_stats().await?;
            info!("📊 Final statistics: {:?}", stats);
            info!("✅ Market simulation completed successfully");
        }
        Err(e) => {
            error!("❌ Market simulation failed: {}", e);
            return Err(e);
        }
    }
    
    Ok(())
}