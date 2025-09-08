//! Transaction builder for real Solana token operations.

use anyhow::{anyhow, Result};
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::{
    compute_budget::ComputeBudgetInstruction,
    instruction::Instruction,
    message::{v0::Message as MessageV0, VersionedMessage},
    pubkey::Pubkey,
    transaction::VersionedTransaction,
};
use tracing::{debug, info};

use crate::types::PremintCandidate;
use crate::wallet::WalletManager;

/// Transaction builder for creating real Solana token transactions
pub struct TransactionBuilder {
    wallet: WalletManager,
    rpc_client: RpcClient,
}

/// Configuration for transaction building
#[derive(Debug, Clone)]
pub struct TransactionConfig {
    /// Priority fee in lamports (0 for no priority fee)
    pub priority_fee_lamports: u64,
    /// Compute unit limit (0 for default)
    pub compute_unit_limit: u32,
    /// Amount to buy in SOL lamports
    pub buy_amount_lamports: u64,
    /// Slippage tolerance as percentage (e.g., 5.0 for 5%)
    pub slippage_percent: f64,
}

impl Default for TransactionConfig {
    fn default() -> Self {
        Self {
            priority_fee_lamports: 10_000, // 0.00001 SOL priority fee
            compute_unit_limit: 200_000,   // Standard compute limit
            buy_amount_lamports: 10_000_000, // 0.01 SOL buy amount
            slippage_percent: 10.0,        // 10% slippage tolerance
        }
    }
}

impl TransactionBuilder {
    /// Create a new transaction builder
    pub fn new(wallet: WalletManager, rpc_endpoint: &str) -> Self {
        let rpc_client = RpcClient::new(rpc_endpoint.to_string());
        Self { wallet, rpc_client }
    }

    /// Build a buy transaction for a token
    pub async fn build_buy_transaction(
        &self,
        candidate: &PremintCandidate,
        config: &TransactionConfig,
        blockhash: Option<solana_sdk::hash::Hash>,
    ) -> Result<VersionedTransaction> {
        info!(mint=%candidate.mint, program=%candidate.program, "Building BUY transaction");

        // Get recent blockhash if not provided
        let recent_blockhash = match blockhash {
            Some(hash) => hash,
            None => self.rpc_client.get_latest_blockhash().await
                .map_err(|e| anyhow!("Failed to get latest blockhash: {}", e))?,
        };

        let mut instructions = Vec::new();

        // Add priority fee instruction if specified
        if config.priority_fee_lamports > 0 {
            let priority_ix = ComputeBudgetInstruction::set_compute_unit_price(config.priority_fee_lamports);
            instructions.push(priority_ix);
        }

        // Add compute unit limit instruction if specified
        if config.compute_unit_limit > 0 {
            let compute_ix = ComputeBudgetInstruction::set_compute_unit_limit(config.compute_unit_limit);
            instructions.push(compute_ix);
        }

        // Build the actual token purchase instruction based on program
        let buy_instruction = match candidate.program.as_str() {
            "pump.fun" => self.build_pump_fun_buy_instruction(candidate, config).await?,
            program => {
                // For unknown programs, create a safe placeholder that won't fail
                info!("Unknown program '{}', creating placeholder transaction", program);
                self.build_placeholder_buy_instruction(candidate, config).await?
            }
        };

        instructions.push(buy_instruction);

        // Create versioned message
        let message = MessageV0::try_compile(
            &self.wallet.pubkey(),
            &instructions,
            &[],
            recent_blockhash,
        ).map_err(|e| anyhow!("Failed to compile message: {}", e))?;

        let versioned_message = VersionedMessage::V0(message);
        let tx = VersionedTransaction {
            signatures: vec![solana_sdk::signature::Signature::default()],
            message: versioned_message,
        };

        debug!(mint=%candidate.mint, "Transaction built (unsigned)");
        Ok(tx)
    }

    /// Build a sell transaction for a token
    pub async fn build_sell_transaction(
        &self,
        mint: &Pubkey,
        sell_percent: f64,
        config: &TransactionConfig,
        blockhash: Option<solana_sdk::hash::Hash>,
    ) -> Result<VersionedTransaction> {
        info!(mint=%mint, sell_percent=sell_percent, "Building SELL transaction");

        let sell_percent = sell_percent.clamp(0.0, 1.0);

        // Get recent blockhash if not provided
        let recent_blockhash = match blockhash {
            Some(hash) => hash,
            None => self.rpc_client.get_latest_blockhash().await
                .map_err(|e| anyhow!("Failed to get latest blockhash: {}", e))?,
        };

        let mut instructions = Vec::new();

        // Add priority fee instruction if specified
        if config.priority_fee_lamports > 0 {
            let priority_ix = ComputeBudgetInstruction::set_compute_unit_price(config.priority_fee_lamports);
            instructions.push(priority_ix);
        }

        // Add compute unit limit instruction if specified
        if config.compute_unit_limit > 0 {
            let compute_ix = ComputeBudgetInstruction::set_compute_unit_limit(config.compute_unit_limit);
            instructions.push(compute_ix);
        }

        // Build the actual token sell instruction
        let sell_instruction = self.build_sell_instruction(mint, sell_percent, config).await?;
        instructions.push(sell_instruction);

        // Create versioned message
        let message = MessageV0::try_compile(
            &self.wallet.pubkey(),
            &instructions,
            &[],
            recent_blockhash,
        ).map_err(|e| anyhow!("Failed to compile message: {}", e))?;

        let versioned_message = VersionedMessage::V0(message);
        let tx = VersionedTransaction {
            signatures: vec![solana_sdk::signature::Signature::default()],
            message: versioned_message,
        };

        debug!(mint=%mint, "Sell transaction built (unsigned)");
        Ok(tx)
    }

    /// Build pump.fun buy instruction (placeholder for now - needs actual pump.fun integration)
    async fn build_pump_fun_buy_instruction(
        &self,
        candidate: &PremintCandidate,
        config: &TransactionConfig,
    ) -> Result<Instruction> {
        // TODO: Implement actual pump.fun program interaction
        // This is a placeholder that mimics the structure but doesn't interact with real pump.fun
        
        debug!(mint=%candidate.mint, "Creating pump.fun buy instruction (placeholder)");
        
        // For now, create a memo instruction that would be safe to execute
        // In real implementation, this would create the proper pump.fun buy instruction
        let memo_data = format!(
            "PUMP_BUY:{}:{}:{}",
            candidate.mint,
            config.buy_amount_lamports,
            config.slippage_percent
        );
        
        // Create a memo instruction as a safe placeholder
        Ok(spl_memo::build_memo(memo_data.as_bytes(), &[&self.wallet.pubkey()]))
    }

    /// Build placeholder buy instruction for unknown programs
    async fn build_placeholder_buy_instruction(
        &self,
        candidate: &PremintCandidate,
        config: &TransactionConfig,
    ) -> Result<Instruction> {
        debug!(mint=%candidate.mint, program=%candidate.program, "Creating placeholder buy instruction");
        
        let memo_data = format!(
            "PLACEHOLDER_BUY:{}:{}:{}:{}",
            candidate.program,
            candidate.mint,
            config.buy_amount_lamports,
            config.slippage_percent
        );
        
        Ok(spl_memo::build_memo(memo_data.as_bytes(), &[&self.wallet.pubkey()]))
    }

    /// Build sell instruction (placeholder for now)
    async fn build_sell_instruction(
        &self,
        mint: &Pubkey,
        sell_percent: f64,
        config: &TransactionConfig,
    ) -> Result<Instruction> {
        debug!(mint=%mint, sell_percent=sell_percent, "Creating sell instruction (placeholder)");
        
        let memo_data = format!(
            "SELL:{}:{}:{}",
            mint,
            sell_percent,
            config.priority_fee_lamports
        );
        
        Ok(spl_memo::build_memo(memo_data.as_bytes(), &[&self.wallet.pubkey()]))
    }

    /// Get the RPC client for external use
    pub fn rpc_client(&self) -> &RpcClient {
        &self.rpc_client
    }

    /// Get wallet public key
    pub fn wallet_pubkey(&self) -> Pubkey {
        self.wallet.pubkey()
    }
}

// Helper function to create memo instructions
mod spl_memo {
    use solana_sdk::{instruction::Instruction, pubkey::Pubkey};

    /// SPL Memo program ID
    pub const MEMO_PROGRAM_ID: Pubkey = solana_sdk::pubkey!("MemoSq4gqABAXKb96qnH8TysNcWxMyWCqXgDLGmfcHr");

    pub fn build_memo(data: &[u8], signers: &[&Pubkey]) -> Instruction {
        Instruction::new_with_bytes(MEMO_PROGRAM_ID, data, signers.iter().map(|&pk| solana_sdk::instruction::AccountMeta::new_readonly(*pk, true)).collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::PremintCandidate;

    #[tokio::test]
    async fn test_transaction_builder_creation() {
        let wallet = WalletManager::new_random();
        let builder = TransactionBuilder::new(wallet, "https://api.devnet.solana.com");
        
        assert!(!builder.wallet_pubkey().to_string().is_empty());
    }

    #[tokio::test] 
    async fn test_build_placeholder_transaction() {
        let wallet = WalletManager::new_random();
        let builder = TransactionBuilder::new(wallet, "https://api.devnet.solana.com");
        
        let candidate = PremintCandidate {
            mint: Pubkey::new_unique(),
            program: "test.program".to_string(),
            creator: Pubkey::new_unique(),
            slot: 12345,
            timestamp: 1234567890,
        };
        
        let config = TransactionConfig::default();
        
        // This will fail without network access, but we can test the structure
        let result = builder.build_buy_transaction(&candidate, &config, Some(solana_sdk::hash::Hash::default())).await;
        
        // The transaction should be built successfully with a provided blockhash
        assert!(result.is_ok());
        
        let tx = result.unwrap();
        assert!(!tx.signatures.is_empty());
    }
}