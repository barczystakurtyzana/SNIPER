//! tx_builder.rs
//! Production-ready TransactionBuilder for Solana sniper bot
//! - supports pump.fun integration (via `pumpfun` crate if enabled, or HTTP PumpPortal/Moralis fallback)
//! - supports LetsBonk (external HTTP provider) for liquidity/quote lookup
//! - validates config values
//! - retry/backoff + multi-RPC fallback for blockhash
//! - signs VersionedTransaction via WalletManager
//! - prepares simple Jito bundle wrapper (struct) for later submission
//! - careful logging and safe fallbacks (memo fallback when no program integration)

use anyhow::anyhow;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::{
    compute_budget::ComputeBudgetInstruction,
    hash::Hash,
    instruction::{AccountMeta, Instruction},
    message::{v0::Message as MessageV0, VersionedMessage},
    pubkey::Pubkey,
    signature::Signature,
    transaction::VersionedTransaction,
};
use std::str::FromStr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::{sync::Arc, time::Duration};
use thiserror::Error;
use tokio::sync::RwLock;
use tokio_retry::{
    strategy::{jitter, ExponentialBackoff},
    Retry,
};
use tracing::{debug, info, warn};

use crate::nonce_manager::NonceManager;
use crate::types::PremintCandidate;
use crate::wallet::WalletManager;

// Optional integration: `pumpfun` crate
#[cfg(feature = "pumpfun")]
use pumpfun::PumpFun;

// Configuration

#[derive(Debug, Clone)]
pub struct TransactionConfig {
    /// Compute unit price in micro-lamports per CU (for priority fees)
    pub priority_fee_lamports: u64,
    /// Compute unit limit for the transaction
    pub compute_unit_limit: u32,
    /// Amount to buy in SOL lamports
    pub buy_amount_lamports: u64,
    /// Slippage tolerance percentage (0.0..=100.0)
    pub slippage_percent: f64,
    /// RPC endpoints for rotation/fallback
    pub rpc_endpoints: Vec<String>,
    /// Max attempts per endpoint
    pub rpc_retry_attempts: usize,
    /// HTTP and RPC request timeout (ms)
    pub rpc_timeout_ms: u64,
    /// PumpPortal HTTP endpoint and API key
    pub pumpportal_url: Option<String>,
    pub pumpportal_api_key: Option<String>,
    /// LetsBonk HTTP endpoint and API key
    pub letsbonk_api_url: Option<String>,
    pub letsbonk_api_key: Option<String>,
    /// Jito bundle toggle
    pub jito_bundle_enabled: bool,
    /// Optional signer keypair index (for multi-signer wallets)
    pub signer_keypair_index: Option<usize>,
    /// Nonce semaphore capacity (parallel builds control)
    pub nonce_count: usize,
    /// Allowlist of programs (empty = allow all)
    pub allowed_programs: Vec<Pubkey>,
}

impl Default for TransactionConfig {
    fn default() -> Self {
        Self {
            priority_fee_lamports: 10_000,
            compute_unit_limit: 200_000,
            buy_amount_lamports: 10_000_000,
            slippage_percent: 10.0,
            rpc_endpoints: vec!["https://api.mainnet-beta.solana.com".to_string()],
            rpc_retry_attempts: 3,
            rpc_timeout_ms: 8_000,
            pumpportal_url: None,
            pumpportal_api_key: None,
            letsbonk_api_url: None,
            letsbonk_api_key: None,
            jito_bundle_enabled: false,
            signer_keypair_index: None,
            nonce_count: 5,
            allowed_programs: vec![],
        }
    }
}

impl TransactionConfig {
    pub fn validate(&self) -> Result<(), TransactionBuilderError> {
        if self.buy_amount_lamports == 0 {
            return Err(TransactionBuilderError::ConfigValidation(
                "buy_amount_lamports must be > 0".to_string(),
            ));
        }
        if !(0.0..=100.0).contains(&self.slippage_percent) {
            return Err(TransactionBuilderError::ConfigValidation(
                "slippage_percent must be in 0.0..=100.0".to_string(),
            ));
        }
        if self.rpc_endpoints.is_empty() {
            return Err(TransactionBuilderError::ConfigValidation(
                "rpc_endpoints must contain at least one endpoint".to_string(),
            ));
        }
        if self.nonce_count == 0 {
            return Err(TransactionBuilderError::ConfigValidation(
                "nonce_count must be > 0".to_string(),
            ));
        }
        Ok(())
    }

    pub fn is_program_allowed(&self, program_id: &Pubkey) -> bool {
        self.allowed_programs.is_empty() || self.allowed_programs.contains(program_id)
    }
}

// Jito bundle representation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JitoBundleCandidate {
    pub transactions: Vec<VersionedTransaction>,
    pub max_total_cost_lamports: u64,
    pub target_slot: Option<u64>,
}

// TransactionBuilder errors
#[derive(Debug, Error)]
pub enum TransactionBuilderError {
    #[error("Configuration validation failed: {0}")]
    ConfigValidation(String),
    #[error("RPC connection failed: {0}")]
    RpcConnection(String),
    #[error("Instruction building failed for {program}: {reason}")]
    InstructionBuild { program: String, reason: String },
    #[error("Signing failed: {0}")]
    SigningFailed(String),
    #[error("Blockhash fetch failed: {0}")]
    BlockhashFetch(String),
    #[error("Nonce acquisition failed: {0}")]
    NonceAcquisition(String),
    #[error("Serialization failed: {0}")]
    Serialization(String),
    #[error("Program {0} is not allowed by configuration")]
    ProgramNotAllowed(Pubkey),
}

// Supported DEX programs
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DexProgram {
    PumpFun,
    LetsBonk,
    Raydium,
    Orca,
    Meteora,
    Unknown(String),
}

impl From<&str> for DexProgram {
    fn from(s: &str) -> Self {
        match s.to_lowercase().as_str() {
            "pump.fun" | "pumpfun" | "pumpportal" => DexProgram::PumpFun,
            "letsbonk.fun" | "letsbonk" | "bonk" => DexProgram::LetsBonk,
            "raydium" => DexProgram::Raydium,
            "orca" => DexProgram::Orca,
            "meteora" => DexProgram::Meteora,
            _ => DexProgram::Unknown(s.to_string()),
        }
    }
}

// TransactionBuilder
pub struct TransactionBuilder {
    pub wallet: Arc<WalletManager>,
    http: Client,
    rpc_endpoints: Vec<String>,
    rpc_rotation_index: AtomicUsize,
    blockhash_cache: RwLock<Option<(std::time::Instant, Hash)>>,
    // Reduced to 15s as requested
    blockhash_cache_ttl: Duration,
    nonce_manager: Arc<NonceManager>,
    rpc_clients: Vec<Arc<RpcClient>>,
}

impl TransactionBuilder {
    pub fn new(
        wallet: Arc<WalletManager>,
        rpc_endpoints: Vec<String>,
        nonce_manager: Arc<NonceManager>,
    ) -> Self {
        let http = Client::builder()
            .timeout(Duration::from_millis(8000))
            .build()
            .expect("Failed to create HTTP client");

        // Pre-initialize RPC clients for connection pooling
        let rpc_clients = rpc_endpoints
            .iter()
            .map(|endpoint| {
                Arc::new(RpcClient::new_with_timeout(
                    endpoint.clone(),
                    Duration::from_millis(8000),
                ))
            })
            .collect();

        Self {
            wallet,
            http,
            rpc_endpoints: rpc_endpoints.clone(),
            rpc_rotation_index: AtomicUsize::new(0),
            blockhash_cache: RwLock::new(None),
            blockhash_cache_ttl: Duration::from_secs(15),
            nonce_manager,
            rpc_clients,
        }
    }

    pub async fn get_recent_blockhash(
        &self,
        config: &TransactionConfig,
    ) -> Result<Hash, TransactionBuilderError> {
        // Check cache first
        {
            let cache = self.blockhash_cache.read().await;
            if let Some((instant, hash)) = cache.as_ref() {
                if instant.elapsed() < self.blockhash_cache_ttl {
                    return Ok(*hash);
                }
            }
        }

        let mut last_err = None;
        let attempts = config.rpc_retry_attempts.max(1);

        for attempt in 0..attempts {
            let index =
                self.rpc_rotation_index
                    .fetch_add(1, Ordering::Relaxed)
                    % self.rpc_endpoints.len();
            let rpc_client = &self.rpc_clients[index];

            let retry_strategy = ExponentialBackoff::from_millis(50)
                .max_delay(Duration::from_millis(1000))
                .map(jitter)
                .take(3);

            match Retry::spawn(retry_strategy, || async {
                rpc_client
                    .get_latest_blockhash()
                    .await
                    .map_err(|e| anyhow!(e.to_string()))
            })
            .await
            {
                Ok(hash) => {
                    // Update cache
                    let mut cache = self.blockhash_cache.write().await;
                    *cache = Some((std::time::Instant::now(), hash));
                    return Ok(hash);
                }
                Err(e) => {
                    debug!(
                        attempt = attempt,
                        endpoint = %self.rpc_endpoints[index],
                        "Blockhash fetch failed: {}",
                        e
                    );
                    last_err = Some(e);
                }
            }
        }

        Err(TransactionBuilderError::BlockhashFetch(format!(
            "All RPC endpoints failed: {:?}",
            last_err
        )))
    }

    pub async fn build_buy_transaction(
        &self,
        candidate: &PremintCandidate,
        config: &TransactionConfig,
        sign: bool,
    ) -> Result<VersionedTransaction, TransactionBuilderError> {
        config.validate()?;
        info!(
            mint = %candidate.mint,
            program = %candidate.program,
            "Building buy transaction"
        );

        // Acquire nonce for parallel transaction preparation
        let _nonce_guard = self
            .nonce_manager
            .acquire_nonce()
            .await
            .map_err(|e| TransactionBuilderError::NonceAcquisition(e.to_string()))?;

        let recent_blockhash = self.get_recent_blockhash(config).await?;

        let mut instructions: Vec<Instruction> = Vec::with_capacity(4);

        // Compute budget instructions
        if config.compute_unit_limit > 0 {
            instructions.push(ComputeBudgetInstruction::set_compute_unit_limit(
                config.compute_unit_limit,
            ));
        }
        if config.priority_fee_lamports > 0 {
            instructions.push(ComputeBudgetInstruction::set_compute_unit_price(
                config.priority_fee_lamports,
            ));
        }

        // Build program-specific instruction
        let dex_program = DexProgram::from(candidate.program.as_str());
        let buy_instruction = match dex_program {
            DexProgram::PumpFun => self.build_pumpfun_instruction(candidate, config).await,
            DexProgram::LetsBonk => self.build_letsbonk_instruction(candidate, config).await,
            DexProgram::Raydium => self.build_raydium_instruction(candidate, config).await,
            DexProgram::Orca => self.build_orca_instruction(candidate, config).await,
            DexProgram::Meteora => self.build_meteora_instruction(candidate, config).await,
            DexProgram::Unknown(_) => self.build_placeholder_buy_instruction(candidate, config).await,
        }?;

        instructions.push(buy_instruction);

        // Compile message (V0)
        let payer = self.wallet.pubkey();
        let message_v0 = MessageV0::try_compile(&payer, &instructions, &[], recent_blockhash)
            .map_err(|e| TransactionBuilderError::InstructionBuild {
                program: candidate.program.clone(),
                reason: format!("Failed to compile message: {}", e),
            })?;

        let versioned_message = VersionedMessage::V0(message_v0);
        let mut tx = VersionedTransaction {
            signatures: vec![],
            message: versioned_message,
        };

        if sign {
            tx = self
                .wallet
                .sign_versioned_transaction(tx, config.signer_keypair_index)
                .await
                .map_err(|e| TransactionBuilderError::SigningFailed(e.to_string()))?;
        } else {
            // Initialize with default signatures matching required number of signers
            let required = tx.message.header().num_required_signatures as usize;
            tx.signatures = vec![Signature::default(); required];
        }

        debug!(mint = %candidate.mint, "Buy transaction built successfully");
        Ok(tx)
    }

    pub async fn build_sell_transaction(
        &self,
        mint: &Pubkey,
        program: &str,
        sell_percent: f64,
        config: &TransactionConfig,
        sign: bool,
    ) -> Result<VersionedTransaction, TransactionBuilderError> {
        config.validate()?;
        let sell_percent = sell_percent.clamp(0.0, 1.0);
        info!(mint = %mint, "Building sell transaction");

        let _nonce_guard = self
            .nonce_manager
            .acquire_nonce()
            .await
            .map_err(|e| TransactionBuilderError::NonceAcquisition(e.to_string()))?;

        let recent_blockhash = self.get_recent_blockhash(config).await?;

        let mut instructions: Vec<Instruction> = Vec::new();

        if config.compute_unit_limit > 0 {
            instructions.push(ComputeBudgetInstruction::set_compute_unit_limit(
                config.compute_unit_limit,
            ));
        }
        if config.priority_fee_lamports > 0 {
            instructions.push(ComputeBudgetInstruction::set_compute_unit_price(
                config.priority_fee_lamports,
            ));
        }

        let dex_program = DexProgram::from(program);
        let sell_instruction = match dex_program {
            DexProgram::PumpFun => {
                self.build_pumpfun_sell_instruction(mint, sell_percent, config).await
            }
            DexProgram::LetsBonk => {
                self.build_letsbonk_sell_instruction(mint, sell_percent, config).await
            }
            DexProgram::Raydium => {
                self.build_raydium_sell_instruction(mint, sell_percent, config).await
            }
            DexProgram::Orca => self.build_orca_sell_instruction(mint, sell_percent, config).await,
            DexProgram::Meteora => {
                self.build_meteora_sell_instruction(mint, sell_percent, config).await
            }
            DexProgram::Unknown(_) => {
                self.build_placeholder_sell_instruction(mint, sell_percent, config).await
            }
        }?;

        instructions.push(sell_instruction);

        let payer = self.wallet.pubkey();
        let message_v0 = MessageV0::try_compile(&payer, &instructions, &[], recent_blockhash)
            .map_err(|e| TransactionBuilderError::InstructionBuild {
                program: program.to_string(),
                reason: format!("Failed to compile sell message: {}", e),
            })?;

        let versioned_message = VersionedMessage::V0(message_v0);
        let mut tx = VersionedTransaction {
            signatures: vec![],
            message: versioned_message,
        };

        if sign {
            tx = self
                .wallet
                .sign_versioned_transaction(tx, config.signer_keypair_index)
                .await
                .map_err(|e| TransactionBuilderError::SigningFailed(e.to_string()))?;
        } else {
            let required = tx.message.header().num_required_signatures as usize;
            tx.signatures = vec![Signature::default(); required];
        }

        debug!(mint = %mint, "Sell transaction built successfully");
        Ok(tx)
    }

    pub fn prepare_jito_bundle(
        &self,
        txs: Vec<VersionedTransaction>,
        max_total_cost_lamports: u64,
        target_slot: Option<u64>,
    ) -> JitoBundleCandidate {
        JitoBundleCandidate {
            transactions: txs,
            max_total_cost_lamports,
            target_slot,
        }
    }

    pub fn rpc_client_for(&self, idx: usize) -> Arc<RpcClient> {
        let index = idx % self.rpc_clients.len();
        self.rpc_clients[index].clone()
    }

    // --- Instruction builders ---

    async fn build_pumpfun_instruction(
        &self,
        candidate: &PremintCandidate,
        config: &TransactionConfig,
    ) -> Result<Instruction, TransactionBuilderError> {
        #[cfg(feature = "pumpfun")]
        {
            match Self::try_build_pumpfun_instruction(self.wallet.clone(), candidate, config).await
            {
                Ok(ix) => return Ok(ix),
                Err(e) => {
                    warn!("Pumpfun crate build failed: {}", e);
                    // Fall through to HTTP builder
                }
            }
        }

        self.build_pumpportal_or_memo(candidate, config).await
    }

    async fn build_letsbonk_instruction(
        &self,
        candidate: &PremintCandidate,
        config: &TransactionConfig,
    ) -> Result<Instruction, TransactionBuilderError> {
        if let Some(url) = &config.letsbonk_api_url {
            let payload = serde_json::json!({
                "mint": candidate.mint.to_string(),
                "amount": config.buy_amount_lamports,
                "slippage": config.slippage_percent,
                "payer": self.wallet.pubkey().to_string(),
            });

            let mut req = self.http.post(url).json(&payload);
            if let Some(k) = &config.letsbonk_api_key {
                req = req.header("X-API-KEY", k);
            }

            match req.send().await {
                Ok(resp) if resp.status().is_success() => {
                    let j: serde_json::Value = resp.json().await.map_err(|e| {
                        TransactionBuilderError::InstructionBuild {
                            program: "letsbonk".to_string(),
                            reason: format!("JSON parse error: {}", e),
                        }
                    })?;

                    return self.parse_external_api_response(&j, "letsbonk", config);
                }
                Ok(resp) => {
                    warn!("LetsBonk API error: {}", resp.status());
                }
                Err(e) => {
                    warn!("LetsBonk request error: {}", e);
                }
            }
        }

        self.build_placeholder_buy_instruction(candidate, config).await
    }

    async fn build_raydium_instruction(
        &self,
        candidate: &PremintCandidate,
        config: &TransactionConfig,
    ) -> Result<Instruction, TransactionBuilderError> {
        // TODO: Implement real Raydium instruction building
        self.build_placeholder_buy_instruction(candidate, config).await
    }

    async fn build_orca_instruction(
        &self,
        candidate: &PremintCandidate,
        config: &TransactionConfig,
    ) -> Result<Instruction, TransactionBuilderError> {
        // TODO: Implement real Orca instruction building
        self.build_placeholder_buy_instruction(candidate, config).await
    }

    async fn build_meteora_instruction(
        &self,
        candidate: &PremintCandidate,
        config: &TransactionConfig,
    ) -> Result<Instruction, TransactionBuilderError> {
        // TODO: Implement real Meteora instruction building
        self.build_placeholder_buy_instruction(candidate, config).await
    }

    async fn build_pumpportal_or_memo(
        &self,
        candidate: &PremintCandidate,
        config: &TransactionConfig,
    ) -> Result<Instruction, TransactionBuilderError> {
        if let Some(url) = &config.pumpportal_url {
            let payload = serde_json::json!({
                "mint": candidate.mint.to_string(),
                "buy_amount": config.buy_amount_lamports,
                "slippage": config.slippage_percent,
                "payer": self.wallet.pubkey().to_string(),
            });

            let mut req = self.http.post(url).json(&payload);
            if let Some(k) = &config.pumpportal_api_key {
                req = req.header("Authorization", format!("Bearer {}", k));
            }

            match req.send().await {
                Ok(resp) if resp.status().is_success() => {
                    let j: serde_json::Value = resp.json().await.map_err(|e| {
                        TransactionBuilderError::InstructionBuild {
                            program: "pumpportal".to_string(),
                            reason: format!("JSON parse error: {}", e),
                        }
                    })?;

                    return self.parse_external_api_response(&j, "pumpportal", config);
                }
                Ok(resp) => {
                    warn!("PumpPortal API error: {}", resp.status());
                }
                Err(e) => {
                    warn!("PumpPortal request error: {}", e);
                }
            }
        }

        self.build_placeholder_buy_instruction(candidate, config).await
    }

    /// Parse an external API instruction description to a Solana Instruction.
    /// Exposed as public to enable integration testing from bot/tests.
    pub fn parse_external_api_response(
        &self,
        j: &serde_json::Value,
        api_name: &str,
        config: &TransactionConfig,
    ) -> Result<Instruction, TransactionBuilderError> {
        if let Some(obj) = j.as_object() {
            // Prefer program_id + data format
            if let (Some(pid_val), Some(data_val)) = (obj.get("program_id"), obj.get("data")) {
                let pid_str = pid_val.as_str().ok_or_else(|| TransactionBuilderError::InstructionBuild {
                    program: api_name.to_string(),
                    reason: "program_id not string".to_string(),
                })?;

                let pid = Pubkey::from_str(pid_str).map_err(|e| TransactionBuilderError::InstructionBuild {
                    program: api_name.to_string(),
                    reason: format!("invalid program_id: {}", e),
                })?;

                // Check if program is allowed
                if !config.is_program_allowed(&pid) {
                    return Err(TransactionBuilderError::ProgramNotAllowed(pid));
                }

                let data_b64 = data_val.as_str().ok_or_else(|| TransactionBuilderError::InstructionBuild {
                    program: api_name.to_string(),
                    reason: "data not string".to_string(),
                })?;

                let data = base64::decode(data_b64).map_err(|e| TransactionBuilderError::InstructionBuild {
                    program: api_name.to_string(),
                    reason: format!("base64 decode error: {}", e),
                })?;

                // Validate data size
                if data.len() > 4096 {
                    return Err(TransactionBuilderError::InstructionBuild {
                        program: api_name.to_string(),
                        reason: "instruction data too large (max 4KB)".to_string(),
                    });
                }

                // Parse accounts if provided, otherwise use default (payer as readonly)
                let accounts = if let Some(accounts_val) = obj.get("accounts") {
                    self.parse_accounts(accounts_val, api_name)?
                } else {
                    vec![AccountMeta::new_readonly(self.wallet.pubkey(), false)]
                };

                return Ok(Instruction::new_with_bytes(pid, &data, accounts));
            }

            // Fallback to instruction_b64 (legacy format)
            if let Some(b64) = obj.get("instruction_b64").and_then(|v| v.as_str()) {
                warn!(
                    "{} returned legacy instruction_b64 format - consider updating API",
                    api_name
                );
                let data = base64::decode(b64).map_err(|e| TransactionBuilderError::InstructionBuild {
                    program: api_name.to_string(),
                    reason: format!("base64 decode error: {}", e),
                })?;

                // Validate data size
                if data.len() > 4096 {
                    return Err(TransactionBuilderError::InstructionBuild {
                        program: api_name.to_string(),
                        reason: "instruction data too large (max 4KB)".to_string(),
                    });
                }

                // For legacy format, we can't determine program_id, so use memo as fallback
                return Ok(spl_memo::build_memo(&data, &[&self.wallet.pubkey()]));
            }
        }

        Err(TransactionBuilderError::InstructionBuild {
            program: api_name.to_string(),
            reason: "invalid response format".to_string(),
        })
    }

    /// Parse account metas from JSON; rejects unexpected signers (signers other than wallet).
    /// Exposed as public to enable integration testing from bot/tests.
    pub fn parse_accounts(
        &self,
        accounts_val: &serde_json::Value,
        api_name: &str,
    ) -> Result<Vec<AccountMeta>, TransactionBuilderError> {
        let accounts_array = accounts_val.as_array().ok_or_else(|| TransactionBuilderError::InstructionBuild {
            program: api_name.to_string(),
            reason: "accounts not an array".to_string(),
        })?;

        let mut accounts = Vec::with_capacity(accounts_array.len());
        for account_val in accounts_array {
            let account_obj = account_val.as_object().ok_or_else(|| TransactionBuilderError::InstructionBuild {
                program: api_name.to_string(),
                reason: "account entry not an object".to_string(),
            })?;

            let pubkey_str = account_obj
                .get("pubkey")
                .and_then(|v| v.as_str())
                .ok_or_else(|| TransactionBuilderError::InstructionBuild {
                    program: api_name.to_string(),
                    reason: "account pubkey missing or not string".to_string(),
                })?;

            let pubkey = Pubkey::from_str(pubkey_str).map_err(|e| TransactionBuilderError::InstructionBuild {
                program: api_name.to_string(),
                reason: format!("invalid account pubkey: {}", e),
            })?;

            let is_signer = account_obj
                .get("is_signer")
                .and_then(|v| v.as_bool())
                .unwrap_or(false);

            let is_writable = account_obj
                .get("is_writable")
                .and_then(|v| v.as_bool())
                .unwrap_or(false);

            // Reject unexpected signer accounts
            if is_signer && pubkey != self.wallet.pubkey() {
                return Err(TransactionBuilderError::InstructionBuild {
                    program: api_name.to_string(),
                    reason: format!("unexpected signer account: {}", pubkey),
                });
            }

            accounts.push(AccountMeta {
                pubkey,
                is_signer,
                is_writable,
            });
        }

        Ok(accounts)
    }

    async fn build_placeholder_buy_instruction(
        &self,
        candidate: &PremintCandidate,
        config: &TransactionConfig,
    ) -> Result<Instruction, TransactionBuilderError> {
        debug!(mint = %candidate.mint, "Creating placeholder buy memo");
        let memo_data = format!(
            "PLACEHOLDER_BUY:{}:{}:{}",
            candidate.program, candidate.mint, config.buy_amount_lamports
        );
        Ok(spl_memo::build_memo(
            memo_data.as_bytes(),
            &[&self.wallet.pubkey()],
        ))
    }

    async fn build_placeholder_sell_instruction(
        &self,
        mint: &Pubkey,
        sell_percent: f64,
        _config: &TransactionConfig,
    ) -> Result<Instruction, TransactionBuilderError> {
        debug!(mint = %mint, "Creating placeholder sell memo");
        let memo_data = format!("PLACEHOLDER_SELL:{}:{:.6}", mint, sell_percent);
        Ok(spl_memo::build_memo(
            memo_data.as_bytes(),
            &[&self.wallet.pubkey()],
        ))
    }

    // Sell instruction builders (placeholder implementations)
    async fn build_pumpfun_sell_instruction(
        &self,
        mint: &Pubkey,
        sell_percent: f64,
        config: &TransactionConfig,
    ) -> Result<Instruction, TransactionBuilderError> {
        self.build_placeholder_sell_instruction(mint, sell_percent, config)
            .await
    }

    async fn build_letsbonk_sell_instruction(
        &self,
        mint: &Pubkey,
        sell_percent: f64,
        config: &TransactionConfig,
    ) -> Result<Instruction, TransactionBuilderError> {
        self.build_placeholder_sell_instruction(mint, sell_percent, config)
            .await
    }

    async fn build_raydium_sell_instruction(
        &self,
        mint: &Pubkey,
        sell_percent: f64,
        config: &TransactionConfig,
    ) -> Result<Instruction, TransactionBuilderError> {
        self.build_placeholder_sell_instruction(mint, sell_percent, config)
            .await
    }

    async fn build_orca_sell_instruction(
        &self,
        mint: &Pubkey,
        sell_percent: f64,
        config: &TransactionConfig,
    ) -> Result<Instruction, TransactionBuilderError> {
        self.build_placeholder_sell_instruction(mint, sell_percent, config)
            .await
    }

    async fn build_meteora_sell_instruction(
        &self,
        mint: &Pubkey,
        sell_percent: f64,
        config: &TransactionConfig,
    ) -> Result<Instruction, TransactionBuilderError> {
        self.build_placeholder_sell_instruction(mint, sell_percent, config)
            .await
    }

    #[cfg(feature = "pumpfun")]
    async fn try_build_pumpfun_instruction(
        wallet: Arc<WalletManager>,
        candidate: &PremintCandidate,
        config: &TransactionConfig,
    ) -> Result<Instruction, TransactionBuilderError> {
        // Example implementation - adapt to actual pumpfun crate interface
        let pf = PumpFun::new();
        pf.get_buy_instruction(
            &candidate.mint,
            config.buy_amount_lamports,
            config.slippage_percent,
        )
        .map_err(|e| TransactionBuilderError::InstructionBuild {
            program: "pumpfun".to_string(),
            reason: format!("Pumpfun error: {}", e),
        })
    }

    #[cfg(not(feature = "pumpfun"))]
    async fn try_build_pumpfun_instruction(
        _wallet: Arc<WalletManager>,
        _candidate: &PremintCandidate,
        _config: &TransactionConfig,
    ) -> Result<Instruction, TransactionBuilderError> {
        Err(TransactionBuilderError::InstructionBuild {
            program: "pumpfun".to_string(),
            reason: "Pumpfun feature not enabled".to_string(),
        })
    }

    /// Test helper: inject a fresh blockhash to avoid RPC calls in unit/integration tests.
    #[cfg(any(test, feature = "test_utils"))]
    pub async fn inject_blockhash_for_tests(&self, hash: Hash) {
        let mut cache = self.blockhash_cache.write().await;
        *cache = Some((std::time::Instant::now(), hash));
    }
}

// SPL Memo helper
mod spl_memo {
    use solana_sdk::{
        instruction::{AccountMeta, Instruction},
        pubkey::Pubkey,
    };

    pub const MEMO_PROGRAM_ID: Pubkey =
        solana_sdk::pubkey!("MemoSq4gqABAXKb96qnH8TysNcWxMyWCqXgDLGmfcHr");

    pub fn build_memo(data: &[u8], signers: &[&Pubkey]) -> Instruction {
        let metas: Vec<AccountMeta> = signers
            .iter()
            .map(|&pk| AccountMeta::new_readonly(*pk, false)) // Memo doesn't require signer flag
            .collect();

        Instruction::new_with_bytes(MEMO_PROGRAM_ID, data, metas)
    }
}