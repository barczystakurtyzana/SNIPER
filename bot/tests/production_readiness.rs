use sniffer_bot_light::tx_builder::{TransactionBuilder, TransactionConfig};
use sniffer_bot_light::wallet::WalletManager;
use sniffer_bot_light::types::PremintCandidate;
use solana_sdk::pubkey::Pubkey;
use solana_sdk::hash::Hash;

#[tokio::test]
async fn test_complete_transaction_workflow() {
    // Create a random wallet for testing
    let wallet = WalletManager::new_random();
    println!("Test wallet pubkey: {}", wallet.pubkey());
    
    // Create transaction builder
    let builder = TransactionBuilder::new(wallet, "https://api.devnet.solana.com");
    
    // Create test candidate
    let candidate = PremintCandidate {
        mint: Pubkey::new_unique(),
        creator: Pubkey::new_unique(),
        program: "pump.fun".to_string(),
        slot: 12345,
        timestamp: 1699999999,
    };
    
    // Create transaction config with conservative settings
    let config = TransactionConfig {
        priority_fee_lamports: 5000, // 0.000005 SOL priority fee
        compute_unit_limit: 100_000,
        buy_amount_lamports: 1_000_000, // 0.001 SOL
        slippage_percent: 5.0,
    };
    
    // Test buy transaction creation
    let buy_result = builder.build_buy_transaction(
        &candidate,
        &config,
        Some(Hash::default()) // Use default hash to avoid network call
    ).await;
    
    match buy_result {
        Ok(tx) => {
            println!("✅ Buy transaction created successfully");
            println!("   Signatures: {}", tx.signatures.len());
            println!("   Message type: {:?}", tx.message);
            
            // Verify transaction structure
            assert_eq!(tx.signatures.len(), 1, "Should have exactly one signature slot");
            
            // Transaction should have multiple instructions (priority fee + compute limit + actual transaction)
            match &tx.message {
                solana_sdk::message::VersionedMessage::V0(msg) => {
                    println!("   Instructions: {}", msg.instructions.len());
                    assert!(msg.instructions.len() >= 2, "Should have at least priority fee + memo instructions");
                }
                _ => panic!("Expected V0 message format"),
            }
        }
        Err(e) => {
            panic!("Failed to create buy transaction: {}", e);
        }
    }
    
    // Test sell transaction creation
    let sell_result = builder.build_sell_transaction(
        &candidate.mint,
        0.5, // Sell 50%
        &config,
        Some(Hash::default())
    ).await;
    
    match sell_result {
        Ok(tx) => {
            println!("✅ Sell transaction created successfully");
            println!("   Signatures: {}", tx.signatures.len());
            assert_eq!(tx.signatures.len(), 1, "Should have exactly one signature slot");
        }
        Err(e) => {
            panic!("Failed to create sell transaction: {}", e);
        }
    }
    
    println!("✅ Complete transaction workflow test passed!");
}

#[test]
fn test_transaction_config_defaults() {
    let config = TransactionConfig::default();
    
    // Verify reasonable defaults
    assert!(config.priority_fee_lamports > 0, "Should have non-zero priority fee");
    assert!(config.compute_unit_limit > 0, "Should have compute unit limit");
    assert!(config.buy_amount_lamports > 0, "Should have positive buy amount");
    assert!(config.slippage_percent > 0.0 && config.slippage_percent < 100.0, "Should have reasonable slippage");
    
    println!("✅ Transaction config defaults are reasonable");
    println!("   Priority fee: {} lamports", config.priority_fee_lamports);
    println!("   Compute limit: {} units", config.compute_unit_limit);
    println!("   Buy amount: {} lamports", config.buy_amount_lamports);
    println!("   Slippage: {}%", config.slippage_percent);
}

#[tokio::test]
async fn test_transaction_builder_with_different_programs() {
    let wallet = WalletManager::new_random();
    let builder = TransactionBuilder::new(wallet, "https://api.devnet.solana.com");
    let config = TransactionConfig::default();
    
    // Test different program types
    let programs = vec!["pump.fun", "bonk.fun", "unknown.program"];
    
    for program in programs {
        let candidate = PremintCandidate {
            mint: Pubkey::new_unique(),
            creator: Pubkey::new_unique(),
            program: program.to_string(),
            slot: 12345,
            timestamp: 1699999999,
        };
        
        let result = builder.build_buy_transaction(
            &candidate,
            &config,
            Some(Hash::default())
        ).await;
        
        match result {
            Ok(tx) => {
                println!("✅ Transaction created for program: {}", program);
                assert_eq!(tx.signatures.len(), 1, "Should have signature slot");
            }
            Err(e) => {
                panic!("Failed to create transaction for program {}: {}", program, e);
            }
        }
    }
    
    println!("✅ Transaction builder handles different programs correctly");
}