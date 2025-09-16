/*!
MarketMaker Module - Second module [2/2] of Market Simulator environment

This module generates realistic on-chain activities for existing tokens based on their profiles.
It creates simulated trading activities to test the SNIPER bot's behavior in different market conditions.
*/

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{anyhow, Result};
use fastrand;
use solana_sdk::pubkey::Pubkey;
use tokio::time::interval;
use tracing::{debug, error, info, warn};

use crate::types::{TokenProfile, TokenState};
use crate::wallet::WalletManager;
use crate::tx_builder::TransactionBuilder;

/// Configuration for MarketMaker
#[derive(Debug, Clone)]
pub struct MarketMakerConfig {
    /// Interval between main loop iterations (default: 1 second)
    pub loop_interval_ms: u64,
    /// Number of trader wallets to simulate activities
    pub trader_wallet_count: usize,
    /// Minimum activity duration for Gem tokens (in minutes)
    pub gem_min_duration_mins: u64,
    /// Maximum activity duration for Gem tokens (in minutes)
    pub gem_max_duration_mins: u64,
    /// Minimum sleep duration for Rug Pull before executing (in minutes)
    pub rug_min_sleep_mins: u64,
    /// Maximum sleep duration for Rug Pull before executing (in minutes)
    pub rug_max_sleep_mins: u64,
    /// Number of random transactions for Trash tokens before removal
    pub trash_transaction_count: u32,
}

impl Default for MarketMakerConfig {
    fn default() -> Self {
        Self {
            loop_interval_ms: 1000, // 1 second
            trader_wallet_count: 10,
            gem_min_duration_mins: 2,
            gem_max_duration_mins: 5,
            rug_min_sleep_mins: 1,
            rug_max_sleep_mins: 3,
            trash_transaction_count: 3,
        }
    }
}

/// MarketMaker manages simulated trading activities for tokens
pub struct MarketMaker {
    config: MarketMakerConfig,
    live_tokens: Arc<tokio::sync::RwLock<HashMap<Pubkey, TokenState>>>,
    trader_wallets: Vec<Arc<WalletManager>>,
    creator_rug_wallet: Arc<WalletManager>,
    tx_builder: Option<Arc<TransactionBuilder>>,
    is_running: Arc<tokio::sync::RwLock<bool>>,
}

impl MarketMaker {
    /// Create a new MarketMaker instance
    pub fn new(config: MarketMakerConfig) -> Result<Self> {
        info!("🏭 Creating MarketMaker with {} trader wallets", config.trader_wallet_count);
        
        // Generate trader wallets
        let mut trader_wallets = Vec::new();
        for i in 0..config.trader_wallet_count {
            let wallet = WalletManager::new_random();
            debug!("Generated trader wallet {}: {}", i, wallet.pubkey());
            trader_wallets.push(Arc::new(wallet));
        }

        // Generate creator rug wallet
        let creator_rug_wallet = Arc::new(WalletManager::new_random());
        info!("Generated creator rug wallet: {}", creator_rug_wallet.pubkey());

        Ok(Self {
            config,
            live_tokens: Arc::new(tokio::sync::RwLock::new(HashMap::new())),
            trader_wallets,
            creator_rug_wallet,
            tx_builder: None,
            is_running: Arc::new(tokio::sync::RwLock::new(false)),
        })
    }

    /// Set the transaction builder for creating actual transactions
    pub fn set_transaction_builder(&mut self, tx_builder: Arc<TransactionBuilder>) {
        self.tx_builder = Some(tx_builder);
        info!("✅ Transaction builder configured for MarketMaker");
    }

    /// Add a new token to be managed by the MarketMaker
    pub async fn add_token(&self, mint: Pubkey, profile: TokenProfile) -> Result<()> {
        let token_state = TokenState {
            mint,
            profile,
            created_at: Instant::now(),
            activity_count: 0,
            is_active: true,
        };

        let mut tokens = self.live_tokens.write().await;
        tokens.insert(mint, token_state);
        
        info!("📈 Added token {} with profile {:?} to MarketMaker", mint, profile);
        Ok(())
    }

    /// Remove a token from active management
    pub async fn remove_token(&self, mint: &Pubkey) -> Result<()> {
        let mut tokens = self.live_tokens.write().await;
        if let Some(token_state) = tokens.remove(mint) {
            info!("🗑️ Removed token {} ({:?}) from MarketMaker", mint, token_state.profile);
        }
        Ok(())
    }

    /// Get the count of currently managed tokens
    pub async fn get_token_count(&self) -> usize {
        let tokens = self.live_tokens.read().await;
        tokens.len()
    }

    /// Start the MarketMaker main loop
    pub async fn start(&self) -> Result<()> {
        {
            let mut running = self.is_running.write().await;
            if *running {
                return Err(anyhow!("MarketMaker is already running"));
            }
            *running = true;
        }

        info!("🚀 Starting MarketMaker main loop");
        
        let mut ticker = interval(Duration::from_millis(self.config.loop_interval_ms));
        
        loop {
            // Check if we should stop
            {
                let running = self.is_running.read().await;
                if !*running {
                    info!("🛑 MarketMaker main loop stopped");
                    break;
                }
            }

            ticker.tick().await;
            
            // Process all active tokens
            if let Err(e) = self.process_tokens().await {
                error!("Error processing tokens: {}", e);
                // Continue running even if there's an error
            }
        }

        Ok(())
    }

    /// Stop the MarketMaker
    pub async fn stop(&self) {
        let mut running = self.is_running.write().await;
        *running = false;
        info!("🛑 MarketMaker stop requested");
    }

    /// Process all active tokens according to their profiles
    async fn process_tokens(&self) -> Result<()> {
        let tokens_snapshot = {
            let tokens = self.live_tokens.read().await;
            tokens.clone()
        };

        for (mint, token_state) in tokens_snapshot {
            if !token_state.is_active {
                continue;
            }

            // Spawn a dedicated task for each token
            let tokens_ref = self.live_tokens.clone();
            let config = self.config.clone();
            let trader_wallets = self.trader_wallets.clone();
            let creator_rug_wallet = self.creator_rug_wallet.clone();
            let tx_builder = self.tx_builder.clone();

            tokio::spawn(async move {
                if let Err(e) = Self::process_single_token(
                    mint,
                    token_state,
                    tokens_ref,
                    config,
                    trader_wallets,
                    creator_rug_wallet,
                    tx_builder,
                ).await {
                    error!("Error processing token {}: {}", mint, e);
                }
            });
        }

        Ok(())
    }

    /// Process a single token based on its profile
    async fn process_single_token(
        mint: Pubkey,
        mut token_state: TokenState,
        tokens_ref: Arc<tokio::sync::RwLock<HashMap<Pubkey, TokenState>>>,
        config: MarketMakerConfig,
        trader_wallets: Vec<Arc<WalletManager>>,
        creator_rug_wallet: Arc<WalletManager>,
        tx_builder: Option<Arc<TransactionBuilder>>,
    ) -> Result<()> {
        match token_state.profile {
            TokenProfile::Gem => {
                Self::handle_gem_token(
                    mint,
                    token_state,
                    tokens_ref,
                    config,
                    trader_wallets,
                    tx_builder,
                ).await
            }
            TokenProfile::RugPull => {
                Self::handle_rug_pull_token(
                    mint,
                    token_state,
                    tokens_ref,
                    config,
                    creator_rug_wallet,
                    tx_builder,
                ).await
            }
            TokenProfile::Trash => {
                Self::handle_trash_token(
                    mint,
                    token_state,
                    tokens_ref,
                    config,
                    trader_wallets,
                    tx_builder,
                ).await
            }
        }
    }

    /// Handle Gem token logic: Small swap transactions from random trader wallets
    async fn handle_gem_token(
        mint: Pubkey,
        mut token_state: TokenState,
        tokens_ref: Arc<tokio::sync::RwLock<HashMap<Pubkey, TokenState>>>,
        config: MarketMakerConfig,
        trader_wallets: Vec<Arc<WalletManager>>,
        _tx_builder: Option<Arc<TransactionBuilder>>,
    ) -> Result<()> {
        let duration_mins = fastrand::u64(config.gem_min_duration_mins..=config.gem_max_duration_mins);
        let elapsed_mins = token_state.created_at.elapsed().as_secs() / 60;

        if elapsed_mins >= duration_mins {
            // Gem activity period is over, remove from active tokens
            let mut tokens = tokens_ref.write().await;
            tokens.remove(&mint);
            info!("💎 Gem token {} activity completed after {} minutes", mint, elapsed_mins);
            return Ok(());
        }

        // Perform small swap transaction from random trader wallet
        let wallet_index = fastrand::usize(0..trader_wallets.len());
        let trader_wallet = &trader_wallets[wallet_index];
        
        debug!(
            "💎 Gem token {} - Simulating small swap from trader wallet {} (activity {})",
            mint, trader_wallet.pubkey(), token_state.activity_count + 1
        );

        // Update activity count
        token_state.activity_count += 1;
        {
            let mut tokens = tokens_ref.write().await;
            tokens.insert(mint, token_state);
        }

        // TODO: Create actual swap transaction when tx_builder is available
        // For now, just simulate the activity
        
        Ok(())
    }

    /// Handle Rug Pull token logic: Sleep then sell 100% from creator wallet
    async fn handle_rug_pull_token(
        mint: Pubkey,
        mut token_state: TokenState,
        tokens_ref: Arc<tokio::sync::RwLock<HashMap<Pubkey, TokenState>>>,
        config: MarketMakerConfig,
        creator_rug_wallet: Arc<WalletManager>,
        _tx_builder: Option<Arc<TransactionBuilder>>,
    ) -> Result<()> {
        let sleep_duration_mins = fastrand::u64(config.rug_min_sleep_mins..=config.rug_max_sleep_mins);
        let elapsed_mins = token_state.created_at.elapsed().as_secs() / 60;

        if elapsed_mins >= sleep_duration_mins {
            // Time to execute the rug pull
            warn!(
                "💀 Executing rug pull for token {} from creator wallet {} after {} minutes",
                mint, creator_rug_wallet.pubkey(), elapsed_mins
            );

            // TODO: Create sell transaction for 100% of tokens when tx_builder is available
            // For now, just simulate the rug pull

            // Remove token from active management
            let mut tokens = tokens_ref.write().await;
            tokens.remove(&mint);
            
            info!("💀 Rug pull completed for token {}", mint);
        } else {
            // Still waiting for rug pull timing
            debug!(
                "💀 Rug pull token {} waiting... {}/{} minutes elapsed",
                mint, elapsed_mins, sleep_duration_mins
            );
        }

        Ok(())
    }

    /// Handle Trash token logic: Few random transactions then remove
    async fn handle_trash_token(
        mint: Pubkey,
        mut token_state: TokenState,
        tokens_ref: Arc<tokio::sync::RwLock<HashMap<Pubkey, TokenState>>>,
        config: MarketMakerConfig,
        trader_wallets: Vec<Arc<WalletManager>>,
        _tx_builder: Option<Arc<TransactionBuilder>>,
    ) -> Result<()> {
        if token_state.activity_count >= config.trash_transaction_count {
            // Trash token has completed its activities, remove it
            let mut tokens = tokens_ref.write().await;
            tokens.remove(&mint);
            info!("🗑️ Trash token {} removed after {} transactions", mint, token_state.activity_count);
            return Ok(());
        }

        // Perform a random small transaction
        let wallet_index = fastrand::usize(0..trader_wallets.len());
        let trader_wallet = &trader_wallets[wallet_index];
        
        debug!(
            "🗑️ Trash token {} - Simulating random transaction from trader wallet {} (activity {}/{})",
            mint, trader_wallet.pubkey(), token_state.activity_count + 1, config.trash_transaction_count
        );

        // Update activity count
        token_state.activity_count += 1;
        {
            let mut tokens = tokens_ref.write().await;
            tokens.insert(mint, token_state);
        }

        // TODO: Create actual transaction when tx_builder is available
        // For now, just simulate the activity

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::time::{sleep, Duration};

    #[tokio::test]
    async fn test_market_maker_creation() {
        let config = MarketMakerConfig::default();
        let market_maker = MarketMaker::new(config).expect("Failed to create MarketMaker");
        
        // Check that trader wallets were created
        assert_eq!(market_maker.trader_wallets.len(), 10);
        
        // Check that initial token count is zero
        assert_eq!(market_maker.get_token_count().await, 0);
    }

    #[tokio::test]
    async fn test_add_remove_tokens() {
        let config = MarketMakerConfig::default();
        let market_maker = MarketMaker::new(config).expect("Failed to create MarketMaker");
        
        let mint = Pubkey::new_unique();
        
        // Add a token
        market_maker.add_token(mint, TokenProfile::Gem).await.expect("Failed to add token");
        assert_eq!(market_maker.get_token_count().await, 1);
        
        // Remove the token
        market_maker.remove_token(&mint).await.expect("Failed to remove token");
        assert_eq!(market_maker.get_token_count().await, 0);
    }

    #[tokio::test]
    async fn test_token_profiles() {
        let config = MarketMakerConfig {
            trash_transaction_count: 2,
            ..Default::default()
        };
        let market_maker = MarketMaker::new(config).expect("Failed to create MarketMaker");
        
        // Test different token profiles
        let gem_mint = Pubkey::new_unique();
        let rug_mint = Pubkey::new_unique();
        let trash_mint = Pubkey::new_unique();
        
        market_maker.add_token(gem_mint, TokenProfile::Gem).await.expect("Failed to add gem token");
        market_maker.add_token(rug_mint, TokenProfile::RugPull).await.expect("Failed to add rug token");
        market_maker.add_token(trash_mint, TokenProfile::Trash).await.expect("Failed to add trash token");
        
        assert_eq!(market_maker.get_token_count().await, 3);
        
        // Verify tokens were added with correct profiles
        let tokens = market_maker.live_tokens.read().await;
        assert_eq!(tokens.get(&gem_mint).unwrap().profile, TokenProfile::Gem);
        assert_eq!(tokens.get(&rug_mint).unwrap().profile, TokenProfile::RugPull);
        assert_eq!(tokens.get(&trash_mint).unwrap().profile, TokenProfile::Trash);
    }

    #[tokio::test]
    async fn test_market_maker_start_stop() {
        let config = MarketMakerConfig {
            loop_interval_ms: 100, // Fast loop for testing
            ..Default::default()
        };
        let market_maker = Arc::new(MarketMaker::new(config).expect("Failed to create MarketMaker"));
        
        // Start the market maker in a separate task
        let mm_clone = market_maker.clone();
        let handle = tokio::spawn(async move {
            mm_clone.start().await
        });
        
        // Let it run for a short time
        sleep(Duration::from_millis(300)).await;
        
        // Stop the market maker
        market_maker.stop().await;
        
        // Wait for the task to complete
        let result = handle.await.expect("Task panicked");
        assert!(result.is_ok());
    }
}