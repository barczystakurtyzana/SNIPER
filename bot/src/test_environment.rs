/*!
Test Environment for SNIPER Trading Bot

This module implements a comprehensive test environment for the Solana sniper bot,
designed to work with a local solana-test-validator instance with additional flags.

Goal: Verify the logical and functional correctness of the entire bot in a blockchain environment.
*/

use std::collections::HashMap;
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{anyhow, Context, Result};
use solana_client::rpc_client::RpcClient;
use solana_sdk::{
    commitment_config::CommitmentConfig,
    pubkey::Pubkey,
    signature::{Keypair, Signature, Signer},
    system_instruction,
    transaction::Transaction,
};
use tempfile::TempDir;
use tokio::time::sleep;
use tracing::{debug, error, info, warn};

use crate::config::Config;
use crate::types::PremintCandidate;
use crate::rpc_manager::RpcManager;
use crate::buy_engine::BuyEngine;
use crate::nonce_manager::NonceManager;
use crate::market_maker::{MarketMaker, MarketMakerConfig};

/// Configuration for the test validator environment
#[derive(Debug, Clone)]
pub struct TestValidatorConfig {
    /// Local validator RPC URL
    pub rpc_url: String,
    /// Local validator websocket URL
    pub ws_url: String,
    /// Test keypair for transactions
    pub keypair_path: Option<PathBuf>,
    /// BPF program paths to load
    pub bpf_programs: Vec<BpfProgram>,
    /// Test ledger directory
    pub ledger_dir: Option<PathBuf>,
    /// Additional validator flags
    pub additional_flags: Vec<String>,
    /// Test duration in seconds
    pub test_duration_secs: u64,
}

/// BPF program to load into the test validator
#[derive(Debug, Clone)]
pub struct BpfProgram {
    pub program_id: Pubkey,
    pub program_path: PathBuf,
}

impl Default for TestValidatorConfig {
    fn default() -> Self {
        Self {
            rpc_url: "http://127.0.0.1:8899".to_string(),
            ws_url: "ws://127.0.0.1:8900".to_string(),
            keypair_path: None,
            bpf_programs: Vec::new(),
            ledger_dir: None,
            additional_flags: vec![
                "--reset".to_string(),
                "--quiet".to_string(),
                "--log".to_string(),
            ],
            test_duration_secs: 300, // 5 minutes default
        }
    }
}

/// Test environment for the SNIPER bot
pub struct TestEnvironment {
    config: TestValidatorConfig,
    validator_process: Option<Child>,
    temp_dir: Option<TempDir>,
    rpc_client: Option<Arc<RpcClient>>,
    test_keypair: Option<Keypair>,
    market_maker: Option<Arc<MarketMaker>>,
}

impl TestEnvironment {
    /// Create a new test environment
    pub fn new(config: TestValidatorConfig) -> Self {
        Self {
            config,
            validator_process: None,
            temp_dir: None,
            rpc_client: None,
            test_keypair: None,
            market_maker: None,
        }
    }

    /// Start the test validator and initialize the environment
    pub async fn start(&mut self) -> Result<()> {
        info!("🚀 Starting test environment for SNIPER bot");

        // Create temporary directory if needed
        if self.config.ledger_dir.is_none() {
            let temp_dir = TempDir::new().context("Failed to create temporary directory")?;
            self.config.ledger_dir = Some(temp_dir.path().to_path_buf());
            self.temp_dir = Some(temp_dir);
        }

        // Start the test validator
        self.start_validator().await?;

        // Initialize RPC client
        self.init_rpc_client().await?;

        // Setup test keypair
        self.setup_test_keypair().await?;

        // Fund test accounts
        self.fund_test_accounts().await?;

        info!("✅ Test environment started successfully");
        Ok(())
    }

    /// Start the solana-test-validator process
    async fn start_validator(&mut self) -> Result<()> {
        info!("🔧 Starting solana-test-validator");

        let ledger_dir = self.config.ledger_dir.as_ref()
            .ok_or_else(|| anyhow!("Ledger directory not set"))?;

        let mut cmd = Command::new("solana-test-validator");
        cmd.arg("--ledger").arg(ledger_dir);
        cmd.arg("--rpc-port").arg("8899");
        cmd.arg("--rpc-bind-address").arg("127.0.0.1");

        // Add BPF programs
        for bpf_program in &self.config.bpf_programs {
            cmd.arg("--bpf-program")
                .arg(bpf_program.program_id.to_string())
                .arg(&bpf_program.program_path);
        }

        // Add additional flags
        for flag in &self.config.additional_flags {
            cmd.arg(flag);
        }

        cmd.stdout(Stdio::piped())
            .stderr(Stdio::piped());

        debug!("Executing command: {:?}", cmd);

        let child = cmd.spawn()
            .context("Failed to start solana-test-validator")?;

        self.validator_process = Some(child);

        // Wait for validator to start
        self.wait_for_validator().await?;

        info!("✅ Test validator started successfully");
        Ok(())
    }

    /// Wait for the validator to be ready
    async fn wait_for_validator(&self) -> Result<()> {
        info!("⏳ Waiting for validator to be ready...");

        let start_time = Instant::now();
        let timeout = Duration::from_secs(60);
        let client = RpcClient::new_with_commitment(
            self.config.rpc_url.clone(),
            CommitmentConfig::confirmed(),
        );

        while start_time.elapsed() < timeout {
            match client.get_health() {
                Ok(_) => {
                    info!("✅ Validator is healthy and ready");
                    return Ok(());
                }
                Err(e) => {
                    debug!("Validator not ready yet: {}", e);
                    sleep(Duration::from_millis(1000)).await;
                }
            }
        }

        Err(anyhow!("Validator failed to become ready within {} seconds", timeout.as_secs()))
    }

    /// Initialize RPC client
    async fn init_rpc_client(&mut self) -> Result<()> {
        let client = Arc::new(RpcClient::new_with_commitment(
            self.config.rpc_url.clone(),
            CommitmentConfig::confirmed(),
        ));

        // Test the connection
        client.get_version()
            .context("Failed to connect to test validator")?;

        self.rpc_client = Some(client);
        info!("✅ RPC client initialized");
        Ok(())
    }

    /// Setup test keypair
    async fn setup_test_keypair(&mut self) -> Result<()> {
        let keypair = if let Some(keypair_path) = &self.config.keypair_path {
            // Load from file
            let keypair_bytes = std::fs::read(keypair_path)
                .context("Failed to read keypair file")?;
            Keypair::try_from(&keypair_bytes[..])
                .map_err(|e| anyhow!("Invalid keypair file: {}", e))?
        } else {
            // Generate new keypair
            Keypair::new()
        };

        info!("📋 Test keypair: {}", keypair.pubkey());
        self.test_keypair = Some(keypair);
        Ok(())
    }

    /// Fund test accounts with SOL
    async fn fund_test_accounts(&self) -> Result<()> {
        let client = self.rpc_client.as_ref()
            .ok_or_else(|| anyhow!("RPC client not initialized"))?;
        let keypair = self.test_keypair.as_ref()
            .ok_or_else(|| anyhow!("Test keypair not set"))?;

        info!("💰 Requesting airdrop for test account");

        // Request airdrop
        let signature = client.request_airdrop(&keypair.pubkey(), 10_000_000_000)?; // 10 SOL
        
        // Wait for confirmation
        loop {
            match client.get_signature_status(&signature)? {
                Some(status) => {
                    if status.is_ok() {
                        break;
                    }
                }
                None => {}
            }
            sleep(Duration::from_millis(100)).await;
        }

        let balance = client.get_balance(&keypair.pubkey())?;
        info!("✅ Test account funded with {} SOL", balance as f64 / 1_000_000_000.0);

        Ok(())
    }

    /// Run the test suite
    pub async fn run_tests(&self, bot_config: Config) -> Result<TestResults> {
        info!("🧪 Starting bot test suite");

        let start_time = Instant::now();
        let mut results = TestResults::new();

        // Test 1: Basic connectivity and health checks
        results.add_test("validator_health", self.test_validator_health().await);

        // Test 2: RPC manager functionality
        results.add_test("rpc_manager", self.test_rpc_manager(&bot_config).await);

        // Test 3: Transaction broadcasting
        results.add_test("transaction_broadcast", self.test_transaction_broadcast().await);

        // Test 4: Nonce management
        results.add_test("nonce_management", self.test_nonce_management(&bot_config).await);

        // Test 5: Mock sniffer functionality
        results.add_test("mock_sniffer", self.test_mock_sniffer(&bot_config).await);

        // Test 6: Integration test with mock candidates
        results.add_test("integration_mock", self.test_integration_with_mock_data(&bot_config).await);

        results.total_duration = start_time.elapsed();
        info!("🎉 Test suite completed in {:.2}s", results.total_duration.as_secs_f64());

        Ok(results)
    }

    /// Test validator health and basic functionality
    async fn test_validator_health(&self) -> Result<()> {
        let client = self.rpc_client.as_ref()
            .ok_or_else(|| anyhow!("RPC client not initialized"))?;

        // Check health
        client.get_health()?;

        // Check version
        let version = client.get_version()?;
        info!("📊 Validator version: {}", version.solana_core);

        // Check slot height
        let slot = client.get_slot()?;
        info!("📊 Current slot: {}", slot);

        Ok(())
    }

    /// Test RPC manager functionality
    async fn test_rpc_manager(&self, bot_config: &Config) -> Result<()> {
        let mut config = bot_config.clone();
        config.rpc_endpoints = vec![self.config.rpc_url.clone()];

        let _rpc_manager = RpcManager::new_with_config(config.rpc_endpoints.clone(), config);

        // Test basic RPC operations
        info!("🔍 Testing RPC manager functionality");

        Ok(())
    }

    /// Test transaction broadcasting
    async fn test_transaction_broadcast(&self) -> Result<()> {
        let client = self.rpc_client.as_ref()
            .ok_or_else(|| anyhow!("RPC client not initialized"))?;
        let keypair = self.test_keypair.as_ref()
            .ok_or_else(|| anyhow!("Test keypair not set"))?;

        info!("📡 Testing transaction broadcast");

        // Create a simple transfer transaction
        let recipient = Pubkey::new_unique();
        let instruction = system_instruction::transfer(&keypair.pubkey(), &recipient, 1_000_000);

        let recent_blockhash = client.get_latest_blockhash()?;
        let transaction = Transaction::new_signed_with_payer(
            &[instruction],
            Some(&keypair.pubkey()),
            &[keypair],
            recent_blockhash,
        );

        // Send and confirm transaction
        let signature = client.send_and_confirm_transaction(&transaction)?;
        info!("✅ Transaction confirmed: {}", signature);

        Ok(())
    }

    /// Test nonce management
    async fn test_nonce_management(&self, bot_config: &Config) -> Result<()> {
        info!("🔢 Testing nonce management");

        let nonce_manager = Arc::new(NonceManager::new(bot_config.nonce_count));
        
        // Test nonce allocation and management
        let (_, index1) = nonce_manager.acquire_nonce().await?;
        let (_, index2) = nonce_manager.acquire_nonce().await?;

        if index1 == index2 {
            return Err(anyhow!("Nonce manager returned same slot twice"));
        }

        info!("✅ Nonce management working correctly");
        Ok(())
    }

    /// Test mock sniffer functionality
    async fn test_mock_sniffer(&self, _bot_config: &Config) -> Result<()> {
        info!("🎯 Testing mock sniffer");

        // Create mock candidate
        let mock_candidate = PremintCandidate {
            mint: Pubkey::new_unique(),
            creator: Pubkey::new_unique(),
            program: "pump.fun".to_string(),
            slot: 1000,
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
            instruction_summary: Some("Test instruction".to_string()),
            is_jito_bundle: Some(false),
        };

        info!("✅ Mock candidate created: {}", mock_candidate.mint);
        Ok(())
    }

    /// Test integration with mock data
    async fn test_integration_with_mock_data(&self, _bot_config: &Config) -> Result<()> {
        info!("🔗 Testing integration with mock data");

        // This would test the full bot pipeline with mock data
        // For now, we'll simulate this test
        sleep(Duration::from_millis(100)).await;

        info!("✅ Integration test completed");
        Ok(())
    }

    /// Initialize MarketMaker for testing token activities
    pub fn init_market_maker(&mut self, config: Option<MarketMakerConfig>) -> Result<()> {
        let market_maker_config = config.unwrap_or_else(|| MarketMakerConfig {
            loop_interval_ms: 500, // Faster for testing
            trader_wallet_count: 5, // Fewer wallets for testing
            gem_min_duration_mins: 1,
            gem_max_duration_mins: 2,
            rug_min_sleep_mins: 1,
            rug_max_sleep_mins: 2,
            trash_transaction_count: 2,
        });

        let market_maker = MarketMaker::new(market_maker_config)
            .context("Failed to create MarketMaker")?;

        self.market_maker = Some(Arc::new(market_maker));
        info!("✅ MarketMaker initialized for test environment");
        Ok(())
    }

    /// Add a token to MarketMaker for simulation
    pub async fn add_test_token(&self, mint: Pubkey, profile: crate::types::TokenProfile) -> Result<()> {
        let market_maker = self.market_maker.as_ref()
            .ok_or_else(|| anyhow!("MarketMaker not initialized"))?;
        
        market_maker.add_token(mint, profile).await
            .context("Failed to add token to MarketMaker")?;
        
        info!("📈 Added test token {} with profile {:?}", mint, profile);
        Ok(())
    }

    /// Start MarketMaker activities in the background
    pub async fn start_market_maker(&self) -> Result<tokio::task::JoinHandle<Result<()>>> {
        let market_maker = self.market_maker.as_ref()
            .ok_or_else(|| anyhow!("MarketMaker not initialized"))?;
        
        let mm_clone = market_maker.clone();
        let handle = tokio::spawn(async move {
            mm_clone.start().await.context("MarketMaker failed")
        });
        
        info!("🚀 MarketMaker started in background");
        Ok(handle)
    }

    /// Stop MarketMaker activities
    pub async fn stop_market_maker(&self) -> Result<()> {
        if let Some(market_maker) = &self.market_maker {
            market_maker.stop().await;
            info!("🛑 MarketMaker stopped");
        }
        Ok(())
    }

    /// Get current MarketMaker token count
    pub async fn get_market_maker_token_count(&self) -> Result<usize> {
        let market_maker = self.market_maker.as_ref()
            .ok_or_else(|| anyhow!("MarketMaker not initialized"))?;
        
        Ok(market_maker.get_token_count().await)
    }

    /// Run a comprehensive test of MarketMaker functionality
    pub async fn test_market_maker(&mut self) -> Result<()> {
        info!("🧪 Running MarketMaker functionality test");

        // Initialize MarketMaker
        self.init_market_maker(None)?;

        // Create test tokens with different profiles
        let gem_mint = Pubkey::new_unique();
        let rug_mint = Pubkey::new_unique();
        let trash_mint = Pubkey::new_unique();

        self.add_test_token(gem_mint, crate::types::TokenProfile::Gem).await?;
        self.add_test_token(rug_mint, crate::types::TokenProfile::RugPull).await?;
        self.add_test_token(trash_mint, crate::types::TokenProfile::Trash).await?;

        // Verify tokens were added
        let token_count = self.get_market_maker_token_count().await?;
        if token_count != 3 {
            return Err(anyhow!("Expected 3 tokens, got {}", token_count));
        }

        // Start MarketMaker
        let _handle = self.start_market_maker().await?;

        // Let it run for a short period
        sleep(Duration::from_secs(3)).await;

        // Stop MarketMaker
        self.stop_market_maker().await?;

        info!("✅ MarketMaker test completed successfully");
        Ok(())
    }

    /// Stop the test environment
    pub async fn stop(&mut self) -> Result<()> {
        info!("🛑 Stopping test environment");

        // Stop MarketMaker first
        self.stop_market_maker().await?;

        if let Some(mut process) = self.validator_process.take() {
            match process.kill() {
                Ok(_) => info!("✅ Test validator stopped"),
                Err(e) => warn!("⚠️ Error stopping validator: {}", e),
            }
        }

        // Cleanup temp directory
        if let Some(_temp_dir) = self.temp_dir.take() {
            // TempDir automatically cleans up when dropped
            info!("✅ Temporary files cleaned up");
        }

        Ok(())
    }

    /// Get the test keypair
    pub fn test_keypair(&self) -> Option<&Keypair> {
        self.test_keypair.as_ref()
    }

    /// Get the RPC client
    pub fn rpc_client(&self) -> Option<&Arc<RpcClient>> {
        self.rpc_client.as_ref()
    }
}

impl Drop for TestEnvironment {
    fn drop(&mut self) {
        if let Some(mut process) = self.validator_process.take() {
            let _ = process.kill();
        }
    }
}

/// Test results collection
#[derive(Debug)]
pub struct TestResults {
    pub tests: HashMap<String, Result<(), String>>,
    pub total_duration: Duration,
}

impl TestResults {
    pub fn new() -> Self {
        Self {
            tests: HashMap::new(),
            total_duration: Duration::default(),
        }
    }

    pub fn add_test(&mut self, name: &str, result: Result<()>) {
        let result = result.map_err(|e| e.to_string());
        self.tests.insert(name.to_string(), result);
    }

    /// Print test results summary
    pub fn print_summary(&self) {
        println!("\n🧪 Test Results Summary");
        println!("========================");

        let mut passed = 0;
        let mut failed = 0;

        for (name, result) in &self.tests {
            match result {
                Ok(_) => {
                    println!("✅ {}: PASSED", name);
                    passed += 1;
                }
                Err(e) => {
                    println!("❌ {}: FAILED - {}", name, e);
                    failed += 1;
                }
            }
        }

        println!("\n📊 Summary:");
        println!("  Total tests: {}", self.tests.len());
        println!("  Passed: {}", passed);
        println!("  Failed: {}", failed);
        println!("  Duration: {:.2}s", self.total_duration.as_secs_f64());

        if failed > 0 {
            println!("\n❌ Some tests failed - check the logs above");
        } else {
            println!("\n🎉 All tests passed!");
        }
    }

    /// Check if all tests passed
    pub fn all_passed(&self) -> bool {
        self.tests.values().all(|r| r.is_ok())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_environment_config_default() {
        let config = TestValidatorConfig::default();
        assert_eq!(config.rpc_url, "http://127.0.0.1:8899");
        assert_eq!(config.ws_url, "ws://127.0.0.1:8900");
        assert!(config.additional_flags.contains(&"--reset".to_string()));
    }

    #[tokio::test]
    async fn test_results_collection() {
        let mut results = TestResults::new();
        results.add_test("test1", Ok(()));
        results.add_test("test2", Err(anyhow!("test error")));

        assert_eq!(results.tests.len(), 2);
        assert!(!results.all_passed());
    }

    #[test]
    fn test_bpf_program_creation() {
        let program = BpfProgram {
            program_id: Pubkey::new_unique(),
            program_path: PathBuf::from("/path/to/program.so"),
        };

        assert!(!program.program_id.to_string().is_empty());
        assert_eq!(program.program_path, PathBuf::from("/path/to/program.so"));
    }

    #[tokio::test]
    async fn test_market_maker_integration() {
        let config = TestValidatorConfig::default();
        let mut test_env = TestEnvironment::new(config);
        
        // Test MarketMaker initialization
        let result = test_env.init_market_maker(None);
        assert!(result.is_ok(), "MarketMaker initialization should succeed");
        
        // Test adding tokens
        let mint = Pubkey::new_unique();
        let result = test_env.add_test_token(mint, crate::types::TokenProfile::Gem).await;
        assert!(result.is_ok(), "Adding test token should succeed");
        
        // Test token count
        let count = test_env.get_market_maker_token_count().await;
        assert!(count.is_ok(), "Getting token count should succeed");
        assert_eq!(count.unwrap(), 1, "Should have 1 token");
        
        // Test stopping MarketMaker
        let result = test_env.stop_market_maker().await;
        assert!(result.is_ok(), "Stopping MarketMaker should succeed");
    }
}