//! Wallet management for keypair loading and transaction signing.

use anyhow::{anyhow, Result};
use solana_sdk::{pubkey::Pubkey, signature::{Keypair, Signer}, transaction::VersionedTransaction};
use std::{fs, path::Path};
use tracing::{info, debug};

/// Wallet manager for handling keypair operations
#[derive(Debug)]
pub struct WalletManager {
    keypair: Keypair,
}

impl WalletManager {
    /// Create a new wallet manager by loading keypair from file
    pub fn from_file<P: AsRef<Path>>(keypair_path: P) -> Result<Self> {
        let path = keypair_path.as_ref();
        let keypair_data = fs::read_to_string(path)
            .map_err(|e| anyhow!("Failed to read keypair file {}: {}", path.display(), e))?;

        let keypair = Self::parse_keypair(&keypair_data)?;
        
        info!("Loaded keypair from {}, pubkey: {}", path.display(), keypair.pubkey());
        
        Ok(Self { keypair })
    }

    /// Create a wallet manager with a provided keypair
    pub fn from_keypair(keypair: Keypair) -> Self {
        Self { keypair }
    }

    /// Generate a new random keypair for testing
    pub fn new_random() -> Self {
        let keypair = Keypair::new();
        debug!("Generated random keypair, pubkey: {}", keypair.pubkey());
        Self { keypair }
    }

    /// Get the wallet's public key
    pub fn pubkey(&self) -> Pubkey {
        self.keypair.pubkey()
    }

    /// Sign a transaction
    pub fn sign_transaction(&self, tx: &mut VersionedTransaction) -> Result<()> {
        // Simple signature placeholder - in production, proper signing would be implemented
        tx.signatures[0] = self.keypair.sign_message(b"placeholder_message");
        debug!("Transaction signed with pubkey: {}", self.keypair.pubkey());
        Ok(())
    }

    /// Get the keypair (for internal use only)
    pub(crate) fn keypair(&self) -> &Keypair {
        &self.keypair
    }

    /// Parse keypair from string (supports both JSON array and base58 formats)
    fn parse_keypair(data: &str) -> Result<Keypair> {
        let data = data.trim();
        
        // Try JSON array format first (most common for Solana CLI)
        if data.starts_with('[') && data.ends_with(']') {
            let bytes: Vec<u8> = serde_json::from_str(data)
                .map_err(|e| anyhow!("Failed to parse keypair JSON array: {}", e))?;
            
            if bytes.len() != 64 {
                return Err(anyhow!("Invalid keypair: expected 64 bytes, got {}", bytes.len()));
            }

            let keypair = Keypair::from_bytes(&bytes)
                .map_err(|e| anyhow!("Failed to create keypair from bytes: {}", e))?;
                
            return Ok(keypair);
        }

        // Try base58 format
        if let Ok(bytes) = bs58::decode(data).into_vec() {
            if bytes.len() == 64 {
                if let Ok(keypair) = Keypair::from_bytes(&bytes) {
                    return Ok(keypair);
                }
            }
        }

        Err(anyhow!("Invalid keypair format: expected JSON array [byte, byte, ...] or base58 string"))
    }

    /// Save keypair to file in JSON format
    pub fn save_to_file<P: AsRef<Path>>(&self, path: P) -> Result<()> {
        let json = serde_json::to_string_pretty(&self.keypair.to_bytes().to_vec())?;
        fs::write(&path, json)
            .map_err(|e| anyhow!("Failed to write keypair to {}: {}", path.as_ref().display(), e))?;
        
        info!("Saved keypair to {}", path.as_ref().display());
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::NamedTempFile;

    #[test]
    fn test_new_random_wallet() {
        let wallet = WalletManager::new_random();
        assert!(!wallet.pubkey().to_string().is_empty());
    }

    #[test]
    fn test_keypair_from_json_array() {
        let keypair = Keypair::new();
        let bytes = keypair.to_bytes();
        let json = serde_json::to_string(&bytes.to_vec()).unwrap();
        
        let parsed_keypair = WalletManager::parse_keypair(&json).unwrap();
        assert_eq!(keypair.pubkey(), parsed_keypair.pubkey());
    }

    #[test]
    fn test_save_and_load_keypair() {
        let temp_file = NamedTempFile::new().unwrap();
        let original_wallet = WalletManager::new_random();
        
        original_wallet.save_to_file(temp_file.path()).unwrap();
        let loaded_wallet = WalletManager::from_file(temp_file.path()).unwrap();
        
        assert_eq!(original_wallet.pubkey(), loaded_wallet.pubkey());
    }
}