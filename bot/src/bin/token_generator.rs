/*!
Token Generator - Utility to generate test tokens for Market Simulator

This binary creates and manages test tokens for the MarketMaker environment.
It helps simulate realistic token creation scenarios for testing the SNIPER bot.
*/

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result};
use solana_client::rpc_client::RpcClient;
use solana_sdk::{
    commitment_config::CommitmentConfig,
    pubkey::Pubkey,
    signature::{Keypair, Signer},
};
use tokio::time::sleep;
use tracing::{info, warn, error, debug};

use sniffer_bot_light::types::TokenProfile;
use sniffer_bot_light::market_maker::{MarketMaker, MarketMakerConfig};

/// Configuration for token generation
#[derive(Debug, Clone)]
pub struct TokenGeneratorConfig {
    /// RPC endpoint for blockchain operations
    pub rpc_endpoint: String,
    /// Number of tokens to generate per batch
    pub batch_size: usize,
    /// Delay between token generations (in milliseconds)
    pub generation_delay_ms: u64,
    /// Total number of tokens to generate
    pub total_tokens: usize,
    /// Distribution of token profiles (Gem, RugPull, Trash percentages)
    pub profile_distribution: ProfileDistribution,
}

/// Distribution configuration for token profiles
#[derive(Debug, Clone)]
pub struct ProfileDistribution {
    pub gem_percentage: f64,
    pub rug_pull_percentage: f64,
    pub trash_percentage: f64,
}

impl Default for ProfileDistribution {
    fn default() -> Self {
        Self {
            gem_percentage: 0.3,      // 30% gems
            rug_pull_percentage: 0.2, // 20% rug pulls
            trash_percentage: 0.5,    // 50% trash
        }
    }
}

impl Default for TokenGeneratorConfig {
    fn default() -> Self {
        Self {
            rpc_endpoint: "https://api.devnet.solana.com".to_string(),
            batch_size: 5,
            generation_delay_ms: 2000,
            total_tokens: 20,
            profile_distribution: ProfileDistribution::default(),
        }
    }
}

/// Represents a generated token
#[derive(Debug, Clone)]
pub struct GeneratedToken {
    /// Token mint address
    pub mint: Pubkey,
    /// Token profile (determines behavior)
    pub profile: TokenProfile,
    /// Creator wallet address
    pub creator: Pubkey,
    /// Timestamp when this token was created
    pub created_at: u64,
    /// Token supply
    pub supply: u64,
    /// Token decimals
    pub decimals: u8,
    /// Metadata URI (optional)
    pub metadata_uri: Option<String>,
}

/// Token storage for managing generated tokens
#[derive(Debug)]
pub struct TokenStorage {
    tokens: HashMap<Pubkey, GeneratedToken>,
    profiles: HashMap<TokenProfile, Vec<Pubkey>>,
}

impl TokenStorage {
    pub fn new() -> Self {
        Self {
            tokens: HashMap::new(),
            profiles: HashMap::new(),
        }
    }

    pub fn add_token(&mut self, token: GeneratedToken) {
        let profile = token.profile;
        let mint = token.mint;
        
        self.tokens.insert(mint, token);
        self.profiles.entry(profile).or_insert_with(Vec::new).push(mint);
    }

    pub fn get_tokens_by_profile(&self, profile: TokenProfile) -> Vec<&GeneratedToken> {
        self.profiles
            .get(&profile)
            .map(|mints| mints.iter().filter_map(|mint| self.tokens.get(mint)).collect())
            .unwrap_or_default()
    }

    pub fn total_count(&self) -> usize {
        self.tokens.len()
    }
}

/// Main token generator struct
pub struct TokenGenerator {
    config: TokenGeneratorConfig,
    /// RPC client for blockchain operations
    rpc: Arc<RpcClient>,
    /// Generated token storage
    token_storage: TokenStorage,
    /// Market maker instance for simulation
    market_maker: Option<Arc<MarketMaker>>,
}

impl TokenGenerator {
    /// Create a new token generator
    pub fn new(config: TokenGeneratorConfig) -> Result<Self> {
        let rpc = Arc::new(RpcClient::new_with_commitment(
            config.rpc_endpoint.clone(),
            CommitmentConfig::confirmed(),
        ));

        Ok(Self {
            config,
            rpc,
            token_storage: TokenStorage::new(),
            market_maker: None,
        })
    }

    /// Get reference to token storage
    pub fn token_storage(&self) -> &TokenStorage {
        &self.token_storage
    }

    /// Set the market maker for token simulation
    pub fn set_market_maker(&mut self, market_maker: Arc<MarketMaker>) {
        self.market_maker = Some(market_maker);
    }

    /// Generate a batch of tokens
    pub async fn generate_batch(&mut self) -> Result<Vec<GeneratedToken>> {
        info!("Generating batch of {} tokens", self.config.batch_size);
        
        let mut generated_tokens = Vec::new();
        
        for i in 0..self.config.batch_size {
            let profile = self.determine_token_profile()?;
            let token = self.create_token(profile).await?;
            let mint = token.mint; // Get mint before moving token
            
            info!("Generated token {} with profile {:?}: {}", i + 1, profile, mint);
            generated_tokens.push(token.clone());
            self.token_storage.add_token(token);
            
            // Add to market maker if available
            if let Some(market_maker) = &self.market_maker {
                market_maker.add_token(mint, profile).await
                    .context("Failed to add token to MarketMaker")?;
            }
            
            // Sleep between generations
            if i < self.config.batch_size - 1 {
                sleep(Duration::from_millis(self.config.generation_delay_ms)).await;
            }
        }
        
        Ok(generated_tokens)
    }

    /// Determine token profile based on distribution
    fn determine_token_profile(&self) -> Result<TokenProfile> {
        let random = fastrand::f64();
        let dist = &self.config.profile_distribution;
        
        if random < dist.gem_percentage {
            Ok(TokenProfile::Gem)
        } else if random < dist.gem_percentage + dist.rug_pull_percentage {
            Ok(TokenProfile::RugPull)
        } else {
            Ok(TokenProfile::Trash)
        }
    }

    /// Create a single token (simulated)
    async fn create_token(&self, profile: TokenProfile) -> Result<GeneratedToken> {
        // Generate keypair for the token mint
        let mint_keypair = Keypair::new();
        let creator_keypair = Keypair::new();
        
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .context("Failed to get timestamp")?
            .as_secs();
        
        // Simulate token creation (in a real implementation, this would create actual SPL tokens)
        let token = GeneratedToken {
            mint: mint_keypair.pubkey(),
            profile,
            creator: creator_keypair.pubkey(),
            created_at: timestamp,
            supply: 1_000_000_000, // 1 billion tokens
            decimals: 9,
            metadata_uri: Some(format!("https://metadata.example.com/{}", mint_keypair.pubkey())),
        };
        
        debug!("Created token simulation: {:?}", token);
        Ok(token)
    }

    /// Generate all tokens according to configuration
    pub async fn generate_all(&mut self) -> Result<()> {
        info!("Starting token generation: {} total tokens", self.config.total_tokens);
        
        let batches = (self.config.total_tokens + self.config.batch_size - 1) / self.config.batch_size;
        
        for batch in 0..batches {
            let remaining = self.config.total_tokens - (batch * self.config.batch_size);
            let batch_size = remaining.min(self.config.batch_size);
            
            // Temporarily adjust batch size for last batch
            let original_batch_size = self.config.batch_size;
            self.config.batch_size = batch_size;
            
            info!("Generating batch {}/{} ({} tokens)", batch + 1, batches, batch_size);
            self.generate_batch().await?;
            
            // Restore original batch size
            self.config.batch_size = original_batch_size;
            
            // Sleep between batches
            if batch < batches - 1 {
                sleep(Duration::from_millis(self.config.generation_delay_ms * 2)).await;
            }
        }
        
        info!("Token generation completed. Total tokens: {}", self.token_storage.total_count());
        self.print_summary();
        
        Ok(())
    }

    /// Print generation summary
    fn print_summary(&self) {
        info!("=== Token Generation Summary ===");
        info!("Total tokens generated: {}", self.token_storage.total_count());
        
        for profile in [TokenProfile::Gem, TokenProfile::RugPull, TokenProfile::Trash] {
            let count = self.token_storage.get_tokens_by_profile(profile).len();
            info!("{:?} tokens: {}", profile, count);
        }
    }

    /// Start market simulation with generated tokens
    pub async fn start_simulation(&self) -> Result<()> {
        if let Some(market_maker) = &self.market_maker {
            info!("Starting market simulation with {} tokens", self.token_storage.total_count());
            market_maker.start().await?;
        } else {
            warn!("No MarketMaker configured, skipping simulation");
        }
        Ok(())
    }

    /// Stop market simulation
    pub async fn stop_simulation(&self) -> Result<()> {
        if let Some(market_maker) = &self.market_maker {
            market_maker.stop().await;
            info!("Market simulation stopped");
        }
        Ok(())
    }
}

/// CLI configuration for the token generator
#[derive(Debug)]
struct CliConfig {
    tokens: usize,
    batch_size: usize,
    delay_ms: u64,
    rpc_endpoint: String,
    simulate: bool,
}

impl Default for CliConfig {
    fn default() -> Self {
        Self {
            tokens: 20,
            batch_size: 5,
            delay_ms: 2000,
            rpc_endpoint: "https://api.devnet.solana.com".to_string(),
            simulate: true,
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
            "--tokens" => {
                if i + 1 < args.len() {
                    config.tokens = args[i + 1].parse().unwrap_or(config.tokens);
                    i += 1;
                }
            }
            "--batch-size" => {
                if i + 1 < args.len() {
                    config.batch_size = args[i + 1].parse().unwrap_or(config.batch_size);
                    i += 1;
                }
            }
            "--delay" => {
                if i + 1 < args.len() {
                    config.delay_ms = args[i + 1].parse().unwrap_or(config.delay_ms);
                    i += 1;
                }
            }
            "--rpc" => {
                if i + 1 < args.len() {
                    config.rpc_endpoint = args[i + 1].clone();
                    i += 1;
                }
            }
            "--no-simulate" => {
                config.simulate = false;
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
    println!("Token Generator - Create test tokens for Market Simulator");
    println!();
    println!("Usage: token_generator [OPTIONS]");
    println!();
    println!("Options:");
    println!("  --tokens <N>        Number of tokens to generate (default: 20)");
    println!("  --batch-size <N>    Tokens per batch (default: 5)");
    println!("  --delay <MS>        Delay between generations in ms (default: 2000)");
    println!("  --rpc <URL>         RPC endpoint (default: devnet)");
    println!("  --no-simulate       Don't start market simulation");
    println!("  --help              Show this help message");
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter("info")
        .init();

    let cli_config = parse_args();
    
    info!("🚀 Starting Token Generator");
    info!("Configuration: {:?}", cli_config);
    
    // Create generator configuration
    let config = TokenGeneratorConfig {
        rpc_endpoint: cli_config.rpc_endpoint,
        batch_size: cli_config.batch_size,
        generation_delay_ms: cli_config.delay_ms,
        total_tokens: cli_config.tokens,
        profile_distribution: ProfileDistribution::default(),
    };
    
    // Create token generator
    let mut generator = TokenGenerator::new(config)?;
    
    // Create and configure market maker if simulation is enabled
    if cli_config.simulate {
        let market_maker_config = MarketMakerConfig {
            loop_interval_ms: 1000,
            trader_wallet_count: 5,
            gem_min_duration_mins: 2,
            gem_max_duration_mins: 4,
            rug_min_sleep_mins: 1,
            rug_max_sleep_mins: 3,
            trash_transaction_count: 3,
        };
        
        let market_maker = Arc::new(MarketMaker::new(market_maker_config)?);
        generator.set_market_maker(market_maker.clone());
        
        // Generate all tokens
        generator.generate_all().await?;
        
        // Start simulation
        info!("🎭 Starting market simulation...");
        let simulation_handle = tokio::spawn(async move {
            if let Err(e) = market_maker.start().await {
                error!("Market simulation failed: {}", e);
            }
        });
        
        // Let simulation run for a while
        info!("⏳ Running simulation for 2 minutes...");
        sleep(Duration::from_secs(120)).await;
        
        // Stop simulation
        info!("🛑 Stopping simulation...");
        generator.stop_simulation().await?;
        
        // Wait for simulation to finish
        if let Err(e) = simulation_handle.await {
            warn!("Simulation task join error: {}", e);
        }
    } else {
        // Just generate tokens without simulation
        generator.generate_all().await?;
    }
    
    info!("✅ Token generation completed successfully");
    Ok(())
}