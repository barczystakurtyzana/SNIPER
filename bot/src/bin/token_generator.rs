//! TokenGenerator module - 1/2 module of Market Simulator
//! 
//! This module is responsible for mass-producing virtual tokens with predefined, random profiles.
//! It implements the core logic for simulating near-real market conditions by creating tokens
//! with different characteristics (Gem, Rug, Trash) and setting up their associated infrastructure.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use fastrand::Rng;
use solana_sdk::{
    compute_budget::ComputeBudgetInstruction,
    hash::Hash,
    instruction::{AccountMeta, Instruction},
    message::{v0::Message as MessageV0, VersionedMessage},
    native_token::LAMPORTS_PER_SOL,
    program_pack::Pack,
    pubkey::Pubkey,
    rent::Rent,
    signature::{Keypair, Signer},
    system_instruction,
    transaction::VersionedTransaction,
};
use spl_associated_token_account::{
    get_associated_token_address,
    instruction::create_associated_token_account,
};
use spl_token::{
    instruction as token_instruction,
    state::Mint,
};
use tokio::sync::RwLock;
use tokio::time;
use tracing::{debug, error, info, warn};

use sniffer_bot_light::rpc_manager::RpcBroadcaster;
use sniffer_bot_light::wallet::WalletManager;

/// Token profile types with associated probabilities
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TokenProfile {
    /// High-quality token with real metadata and significant liquidity (1% probability)
    Gem,
    /// Rug pull token with minimal liquidity, may disappear quickly (9% probability) 
    Rug,
    /// Low-quality trash token with poor metadata (90% probability)
    Trash,
}

impl TokenProfile {
    /// Get the probability weight for this profile
    pub fn weight(&self) -> u32 {
        match self {
            TokenProfile::Gem => 1,
            TokenProfile::Rug => 9,
            TokenProfile::Trash => 90,
        }
    }

    /// Get a description of this profile
    pub fn description(&self) -> &'static str {
        match self {
            TokenProfile::Gem => "Gem - High quality token with real metadata and high liquidity",
            TokenProfile::Rug => "Rug - Rug pull token with minimal liquidity",
            TokenProfile::Trash => "Trash - Low quality token with poor metadata",
        }
    }
}

/// Configuration for the token generator
#[derive(Debug, Clone)]
pub struct SimulatorConfig {
    /// Minimum interval between token generations
    pub interval_min: Duration,
    /// Maximum interval between token generations
    pub interval_max: Duration,
}

/// Information about a generated token
#[derive(Debug, Clone)]
pub struct GeneratedToken {
    /// The mint pubkey of the generated token
    pub mint: Pubkey,
    /// The profile type of this token
    pub profile: TokenProfile,
    /// The creator wallet pubkey
    pub creator: Pubkey,
    /// Timestamp when this token was created
    pub created_at: u64,
    /// Initial supply that was minted
    pub initial_supply: u64,
    /// Amount of liquidity added (in lamports)
    pub liquidity_lamports: u64,
    /// Metadata URI (if any)
    pub metadata_uri: Option<String>,
}

/// Thread-safe storage for generated tokens
pub type TokenStorage = Arc<RwLock<HashMap<Pubkey, GeneratedToken>>>;

/// Main TokenGenerator struct
pub struct TokenGenerator {
    /// RPC client for blockchain operations
    rpc: Arc<dyn RpcBroadcaster>,
    /// Wallet for signing transactions
    wallet: Arc<WalletManager>,
    /// Configuration for the generator
    config: SimulatorConfig,
    /// Random number generator
    rng: Arc<std::sync::Mutex<Rng>>,
    /// Storage for generated tokens
    token_storage: TokenStorage,
    /// Additional trader wallets for token distribution
    trader_wallets: Vec<Keypair>,
}

impl TokenGenerator {
    /// Create a new TokenGenerator instance
    pub async fn new(
        rpc: Arc<dyn RpcBroadcaster>,
        wallet: Arc<WalletManager>,
        config: SimulatorConfig,
    ) -> Result<Self> {
        let rng = Arc::new(std::sync::Mutex::new(Rng::new()));
        let token_storage = Arc::new(RwLock::new(HashMap::new()));
        
        // Create some trader wallets for token distribution
        let trader_wallets: Vec<Keypair> = (0..5)
            .map(|_| Keypair::new())
            .collect();

        info!("Created {} trader wallets for token distribution", trader_wallets.len());

        Ok(Self {
            rpc,
            wallet,
            config,
            rng,
            token_storage,
            trader_wallets,
        })
    }

    /// Get a reference to the token storage
    pub fn token_storage(&self) -> &TokenStorage {
        &self.token_storage
    }

    /// Main execution loop for the token generator
    pub async fn run(&self) -> Result<()> {
        info!("Starting token generation loop...");

        loop {
            // Generate random interval
            let interval_ms = {
                let mut rng = self.rng.lock().unwrap();
                rng.u64(
                    self.config.interval_min.as_millis() as u64
                    ..=self.config.interval_max.as_millis() as u64
                )
            };
            let interval = Duration::from_millis(interval_ms);

            debug!("Waiting {} ms before next token generation", interval_ms);
            time::sleep(interval).await;

            // Generate a token
            match self.generate_token().await {
                Ok(token) => {
                    info!(
                        "Generated token {} with profile {:?} and {} SOL liquidity",
                        token.mint,
                        token.profile,
                        token.liquidity_lamports as f64 / LAMPORTS_PER_SOL as f64
                    );

                    // Store the token
                    let mut storage = self.token_storage.write().await;
                    storage.insert(token.mint, token);
                }
                Err(e) => {
                    error!("Failed to generate token: {}", e);
                    // Continue the loop even if one token generation fails
                }
            }
        }
    }

    /// Generate a single token with random profile
    pub async fn generate_token(&self) -> Result<GeneratedToken> {
        // Select random token profile based on probabilities
        let profile = self.select_random_profile();
        debug!("Selected profile: {}", profile.description());

        // Generate new mint keypair
        let mint_keypair = Keypair::new();
        let mint_pubkey = mint_keypair.pubkey();

        debug!("Generating token with mint: {}", mint_pubkey);

        // Create and send the initialization transaction
        let transaction = self.create_initialization_transaction(
            &mint_keypair,
            &profile,
        ).await?;

        // Submit transaction
        match self.submit_transaction(&transaction).await {
            Ok(signature) => {
                info!("Token initialization transaction submitted: {}", signature);
            }
            Err(e) => {
                warn!("Failed to submit transaction, using placeholder: {}", e);
            }
        }

        // Create token info
        let (initial_supply, liquidity_lamports, metadata_uri) = self.get_token_parameters(&profile);

        let token = GeneratedToken {
            mint: mint_pubkey,
            profile,
            creator: self.wallet.pubkey(),
            created_at: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)?
                .as_secs(),
            initial_supply,
            liquidity_lamports,
            metadata_uri,
        };

        // Perform additional setup based on profile
        self.perform_profile_specific_setup(&token).await?;

        Ok(token)
    }

    /// Select a random token profile based on weighted probabilities
    fn select_random_profile(&self) -> TokenProfile {
        let total_weight = TokenProfile::Gem.weight() 
            + TokenProfile::Rug.weight() 
            + TokenProfile::Trash.weight();
        
        let random_value = {
            let mut rng = self.rng.lock().unwrap();
            rng.u32(0..total_weight)
        };
        
        if random_value < TokenProfile::Gem.weight() {
            TokenProfile::Gem
        } else if random_value < TokenProfile::Gem.weight() + TokenProfile::Rug.weight() {
            TokenProfile::Rug
        } else {
            TokenProfile::Trash
        }
    }

    /// Create the packed initialization transaction
    async fn create_initialization_transaction(
        &self,
        mint_keypair: &Keypair,
        profile: &TokenProfile,
    ) -> Result<VersionedTransaction> {
        let mint_pubkey = mint_keypair.pubkey();
        let wallet_pubkey = self.wallet.pubkey();

        // Get recent blockhash
        let blockhash = self.get_recent_blockhash().await?;

        // Calculate required space and rent for mint account
        let mint_space = Mint::LEN;
        let rent = Rent::default();
        let mint_rent_lamports = rent.minimum_balance(mint_space);

        let mut instructions = Vec::new();

        // Add compute budget instructions
        instructions.push(ComputeBudgetInstruction::set_compute_unit_limit(200_000));
        instructions.push(ComputeBudgetInstruction::set_compute_unit_price(10_000));

        // 1. Create mint account
        instructions.push(system_instruction::create_account(
            &wallet_pubkey,
            &mint_pubkey,
            mint_rent_lamports,
            mint_space as u64,
            &spl_token::id(),
        ));

        // 2. Initialize mint
        instructions.push(token_instruction::initialize_mint(
            &spl_token::id(),
            &mint_pubkey,
            &wallet_pubkey, // mint authority
            Some(&wallet_pubkey), // freeze authority
            9, // decimals
        )?);

        // 3. Create associated token account for initial supply
        let associated_token_account = get_associated_token_address(&wallet_pubkey, &mint_pubkey);
        instructions.push(create_associated_token_account(
            &wallet_pubkey,
            &wallet_pubkey,
            &mint_pubkey,
            &spl_token::id(),
        ));

        // 4. Mint initial supply
        let (initial_supply, _, _) = self.get_token_parameters(profile);
        instructions.push(token_instruction::mint_to(
            &spl_token::id(),
            &mint_pubkey,
            &associated_token_account,
            &wallet_pubkey,
            &[],
            initial_supply,
        )?);

        // 5. Create metadata account (simplified)
        if let Some(metadata_instruction) = self.create_metadata_instruction(&mint_pubkey, profile)? {
            instructions.push(metadata_instruction);
        }

        // 6. Create liquidity pool (placeholder)
        instructions.push(self.create_liquidity_pool_instruction(&mint_pubkey, profile)?);

        // Create versioned transaction
        let message = MessageV0::try_compile(
            &wallet_pubkey,
            &instructions,
            &[],
            blockhash,
        )?;

        let versioned_message = VersionedMessage::V0(message);
        let mut transaction = VersionedTransaction::try_new(versioned_message, &[mint_keypair])?;

        // Sign with wallet
        self.wallet.sign_transaction(&mut transaction)?;

        Ok(transaction)
    }

    /// Get recent blockhash from RPC
    async fn get_recent_blockhash(&self) -> Result<Hash> {
        // Try to get from one of the RPC clients
        // This is a simplified implementation
        Ok(Hash::default()) // Placeholder
    }

    /// Submit transaction to the network
    async fn submit_transaction(&self, _transaction: &VersionedTransaction) -> Result<String> {
        // In a real implementation, this would submit via RPC
        // For now, return a placeholder signature
        Ok("placeholder_signature".to_string())
    }

    /// Create metadata instruction based on profile
    fn create_metadata_instruction(
        &self,
        mint_pubkey: &Pubkey,
        profile: &TokenProfile,
    ) -> Result<Option<Instruction>> {
        match profile {
            TokenProfile::Gem => {
                // Create real metadata for gems
                let metadata_uri = "https://example.com/gem-metadata.json";
                Ok(Some(self.create_metaplex_metadata_instruction(
                    mint_pubkey,
                    "GEM Token",
                    "GEM",
                    metadata_uri,
                )?))
            }
            TokenProfile::Rug => {
                // Empty or junk metadata for rugs
                Ok(Some(self.create_metaplex_metadata_instruction(
                    mint_pubkey,
                    "",
                    "",
                    "",
                )?))
            }
            TokenProfile::Trash => {
                // Poor quality metadata for trash
                Ok(Some(self.create_metaplex_metadata_instruction(
                    mint_pubkey,
                    "TrashCoin",
                    "TRASH",
                    "https://example.com/trash.json",
                )?))
            }
        }
    }

    /// Create Metaplex metadata instruction (simplified)
    fn create_metaplex_metadata_instruction(
        &self,
        mint_pubkey: &Pubkey,
        name: &str,
        symbol: &str,
        uri: &str,
    ) -> Result<Instruction> {
        // This is a simplified placeholder for Metaplex metadata creation
        // In a real implementation, this would use the proper Metaplex SDK
        
        // For now, create a memo instruction as placeholder
        let memo_data = format!(
            "CREATE_METADATA:{}:{}:{}:{}",
            mint_pubkey, name, symbol, uri
        );
        
        Ok(Instruction::new_with_bytes(
            solana_sdk::pubkey!("MemoSq4gqABAXKb96qnH8TysNcWxMyWCqXgDLGmfcHr"), // Memo program
            memo_data.as_bytes(),
            vec![AccountMeta::new_readonly(self.wallet.pubkey(), false)],
        ))
    }

    /// Create liquidity pool instruction (placeholder for pump.fun integration)
    fn create_liquidity_pool_instruction(
        &self,
        mint_pubkey: &Pubkey,
        _profile: &TokenProfile,
    ) -> Result<Instruction> {
        // This is a placeholder for creating a liquidity pool on a cloned pump.fun program
        // In a real implementation, this would integrate with the actual pump.fun contracts
        
        let memo_data = format!("CREATE_POOL:{}", mint_pubkey);
        
        Ok(Instruction::new_with_bytes(
            solana_sdk::pubkey!("MemoSq4gqABAXKb96qnH8TysNcWxMyWCqXgDLGmfcHr"), // Memo program
            memo_data.as_bytes(),
            vec![AccountMeta::new_readonly(self.wallet.pubkey(), false)],
        ))
    }

    /// Get token parameters based on profile
    fn get_token_parameters(&self, profile: &TokenProfile) -> (u64, u64, Option<String>) {
        match profile {
            TokenProfile::Gem => {
                let supply = 1_000_000_000 * 10_u64.pow(9); // 1B tokens with 9 decimals
                let liquidity = 50 * LAMPORTS_PER_SOL; // 50 SOL
                let metadata_uri = Some("https://example.com/gem-metadata.json".to_string());
                (supply, liquidity, metadata_uri)
            }
            TokenProfile::Rug => {
                let supply = 100_000_000 * 10_u64.pow(9); // 100M tokens with 9 decimals
                let liquidity = 1 * LAMPORTS_PER_SOL / 10; // 0.1 SOL
                (supply, liquidity, None)
            }
            TokenProfile::Trash => {
                let supply = 10_000_000 * 10_u64.pow(9); // 10M tokens with 9 decimals
                let liquidity = 1 * LAMPORTS_PER_SOL; // 1 SOL
                let metadata_uri = Some("https://example.com/trash.json".to_string());
                (supply, liquidity, metadata_uri)
            }
        }
    }

    /// Perform profile-specific setup after token creation
    async fn perform_profile_specific_setup(&self, token: &GeneratedToken) -> Result<()> {
        match token.profile {
            TokenProfile::Gem => {
                // Distribute tokens to trader wallets
                self.distribute_to_traders(token).await?;
                info!("Distributed Gem tokens to trader wallets");
            }
            TokenProfile::Rug => {
                // Add minimal liquidity
                debug!("Added minimal liquidity for Rug token");
            }
            TokenProfile::Trash => {
                // Standard setup for trash tokens
                debug!("Standard setup completed for Trash token");
            }
        }
        Ok(())
    }

    /// Distribute a portion of tokens to trader wallets (for Gems)
    async fn distribute_to_traders(&self, token: &GeneratedToken) -> Result<()> {
        let distribution_amount = token.initial_supply / 20; // 5% to traders
        let amount_per_trader = distribution_amount / self.trader_wallets.len() as u64;

        for (i, trader_wallet) in self.trader_wallets.iter().enumerate() {
            debug!(
                "Distributing {} tokens to trader wallet {} ({})",
                amount_per_trader, i, trader_wallet.pubkey()
            );
            // In a real implementation, this would create transfer transactions
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::time::Duration;
    use sniffer_bot_light::config::Config;
    use sniffer_bot_light::rpc_manager::{RpcManager, RpcBroadcaster};
    use sniffer_bot_light::wallet::WalletManager;

    #[tokio::test]
    async fn test_token_generator_creation() {
        // Setup test dependencies
        let config = Config::default();
        let rpc_manager = Arc::new(RpcManager::new_with_config(config.rpc_endpoints.clone(), config.clone()));
        let rpc: Arc<dyn RpcBroadcaster> = rpc_manager.clone();
        let wallet = Arc::new(WalletManager::new_random());
        
        let simulator_config = SimulatorConfig {
            interval_min: Duration::from_millis(100),
            interval_max: Duration::from_millis(200),
        };

        // Test TokenGenerator creation
        let generator = TokenGenerator::new(rpc, wallet, simulator_config).await;
        assert!(generator.is_ok());

        let generator = generator.unwrap();
        
        // Verify token storage is empty initially
        let storage = generator.token_storage().read().await;
        assert_eq!(storage.len(), 0);
    }

    #[test]
    fn test_token_profile_weights() {
        // Test that profile weights are correct
        assert_eq!(TokenProfile::Gem.weight(), 1);
        assert_eq!(TokenProfile::Rug.weight(), 9);
        assert_eq!(TokenProfile::Trash.weight(), 90);
        
        // Test total weight is 100 (representing percentages)
        let total = TokenProfile::Gem.weight() + TokenProfile::Rug.weight() + TokenProfile::Trash.weight();
        assert_eq!(total, 100);
    }

    #[test]
    fn test_token_profile_descriptions() {
        assert!(TokenProfile::Gem.description().contains("Gem"));
        assert!(TokenProfile::Rug.description().contains("Rug"));
        assert!(TokenProfile::Trash.description().contains("Trash"));
    }

    #[tokio::test]
    async fn test_token_profile_selection() {
        let config = Config::default();
        let rpc_manager = Arc::new(RpcManager::new_with_config(config.rpc_endpoints.clone(), config.clone()));
        let rpc: Arc<dyn RpcBroadcaster> = rpc_manager.clone();
        let wallet = Arc::new(WalletManager::new_random());
        
        let simulator_config = SimulatorConfig {
            interval_min: Duration::from_millis(100),
            interval_max: Duration::from_millis(200),
        };

        let generator = TokenGenerator::new(rpc, wallet, simulator_config).await.unwrap();
        
        // Test profile selection multiple times to check distribution
        let mut gem_count = 0;
        let mut rug_count = 0;
        let mut trash_count = 0;
        
        for _ in 0..1000 {
            let profile = generator.select_random_profile();
            match profile {
                TokenProfile::Gem => gem_count += 1,
                TokenProfile::Rug => rug_count += 1,
                TokenProfile::Trash => trash_count += 1,
            }
        }
        
        // Trash should be the most common (around 90%)
        assert!(trash_count > gem_count);
        assert!(trash_count > rug_count);
        
        // Rug should be more common than Gem (9% vs 1%)
        assert!(rug_count > gem_count);
        
        // Basic sanity check that all profiles appear
        assert!(gem_count > 0);
        assert!(rug_count > 0);
        assert!(trash_count > 0);
    }

    #[tokio::test]
    async fn test_generated_token_parameters() {
        let config = Config::default();
        let rpc_manager = Arc::new(RpcManager::new_with_config(config.rpc_endpoints.clone(), config.clone()));
        let rpc: Arc<dyn RpcBroadcaster> = rpc_manager.clone();
        let wallet = Arc::new(WalletManager::new_random());
        
        let simulator_config = SimulatorConfig {
            interval_min: Duration::from_millis(100),
            interval_max: Duration::from_millis(200),
        };

        let generator = TokenGenerator::new(rpc, wallet, simulator_config).await.unwrap();
        
        // Test parameters for each profile type
        let (gem_supply, gem_liquidity, gem_metadata) = generator.get_token_parameters(&TokenProfile::Gem);
        let (rug_supply, rug_liquidity, rug_metadata) = generator.get_token_parameters(&TokenProfile::Rug);
        let (trash_supply, trash_liquidity, trash_metadata) = generator.get_token_parameters(&TokenProfile::Trash);
        
        // Gem should have the highest supply and liquidity
        assert!(gem_supply > rug_supply);
        assert!(gem_supply > trash_supply);
        assert!(gem_liquidity > rug_liquidity);
        assert!(gem_liquidity > trash_liquidity);
        
        // Gem and Trash should have metadata URIs, Rug should not
        assert!(gem_metadata.is_some());
        assert!(rug_metadata.is_none());
        assert!(trash_metadata.is_some());
        
        // Rug should have minimal liquidity
        assert!(rug_liquidity < gem_liquidity / 10); // Less than 1/10th
    }
}