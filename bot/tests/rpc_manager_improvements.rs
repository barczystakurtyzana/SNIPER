use sniffer_bot_light::config::{BroadcastMode, Config};
use sniffer_bot_light::rpc_manager::{RpcBroadcaster, RpcManager};
use sniffer_bot_light::types::create_versioned_transaction;
use std::sync::Arc;
use solana_sdk::{
    hash::Hash,
    pubkey::Pubkey,
    system_instruction,
};

#[tokio::test]
async fn test_broadcast_mode_configuration() {
    let endpoints = vec![
        "https://api.devnet.solana.com".to_string(),
        "https://api.mainnet-beta.solana.com".to_string(),
    ];

    // Test each broadcast mode
    for mode in [BroadcastMode::Pairwise, BroadcastMode::Replicate, BroadcastMode::RoundRobin, BroadcastMode::FullFanout] {
        let mut config = Config::default();
        config.broadcast_mode = mode;
        config.rpc_timeout_sec = 2; // Short timeout for testing
        config.early_cancel_threshold = 1;

        let manager = RpcManager::new_with_config(endpoints.clone(), config);
        
        // Verify the manager has the correct configuration
        assert_eq!(manager.config.broadcast_mode, mode);
        assert_eq!(manager.config.rpc_timeout_sec, 2);
        assert_eq!(manager.config.early_cancel_threshold, 1);
        assert_eq!(manager.endpoints.len(), 2);
    }
}

#[tokio::test]
async fn test_adaptive_endpoint_ranking() {
    let endpoints = vec![
        "https://api.devnet.solana.com".to_string(),
        "https://api.mainnet-beta.solana.com".to_string(),
        "https://solana-api.projectserum.com".to_string(),
    ];

    let mut config = Config::default();
    config.broadcast_mode = BroadcastMode::Pairwise;
    config.rpc_timeout_sec = 1; // Very short timeout to trigger errors
    
    let manager = RpcManager::new_with_config(endpoints, config);
    
    // Create a simple test transaction that will likely fail/timeout
    let from = Pubkey::new_unique();
    let to = Pubkey::new_unique();
    let instruction = system_instruction::transfer(&from, &to, 1_000_000);
    
    let test_tx = create_versioned_transaction(vec![instruction], &from, Hash::default(), 0);
    let txs = vec![test_tx];

    // This should fail but test the ranking logic
    let result = manager.send_on_many_rpc(txs).await;
    
    // We expect this to fail, but it should have attempted ranking
    assert!(result.is_err());
    
    // The ranked endpoints should have been computed
    let ranked = manager.get_ranked_endpoints().await;
    assert_eq!(ranked.len(), 3);
    
    // All indices should be valid
    for &idx in &ranked {
        assert!(idx < manager.endpoints.len());
    }
}

#[tokio::test] 
async fn test_early_cancellation_threshold() {
    let endpoints = vec![
        "https://invalid-endpoint-that-should-fail.com".to_string(),
        "https://another-invalid-endpoint.com".to_string(),
    ];
    
    let mut config = Config::default();
    config.early_cancel_threshold = 1; // Cancel after 1 fatal error
    config.rpc_timeout_sec = 1; // Short timeout
    
    let manager = RpcManager::new_with_config(endpoints, config);
    
    // Create test transaction
    let from = Pubkey::new_unique();
    let to = Pubkey::new_unique();
    let instruction = system_instruction::transfer(&from, &to, 1_000_000);
    let test_tx = create_versioned_transaction(vec![instruction], &from, Hash::default(), 0);
    
    let start_time = std::time::Instant::now();
    let result = manager.send_on_many_rpc(vec![test_tx]).await;
    let elapsed = start_time.elapsed();
    
    // Should fail and should be relatively quick due to early cancellation
    assert!(result.is_err());
    // Should not take much longer than the timeout + some overhead
    assert!(elapsed < std::time::Duration::from_secs(5));
}

#[tokio::test]
async fn test_commitment_consistency() {
    let endpoints = vec!["https://api.devnet.solana.com".to_string()];
    let config = Config::default();
    let manager = RpcManager::new_with_config(endpoints, config);
    
    // This test primarily validates that we don't have commitment mismatches
    // in the configuration. The actual RPC calls will likely fail in CI, 
    // but the important part is the configuration consistency.
    
    let from = Pubkey::new_unique();
    let to = Pubkey::new_unique();
    let instruction = system_instruction::transfer(&from, &to, 1_000_000);
    let test_tx = create_versioned_transaction(vec![instruction], &from, Hash::default(), 0);
    
    // Test that we can at least try to make the call without configuration errors
    let result = manager.send_on_many_rpc(vec![test_tx]).await;
    
    // We expect this to fail with an RPC error, but not a configuration error
    if let Err(e) = result {
        let error_str = e.to_string();
        // Should not contain commitment-related configuration errors
        assert!(!error_str.contains("commitment configuration"));
        assert!(!error_str.contains("preflight commitment"));
    }
}

#[tokio::test] 
async fn test_client_caching() {
    let endpoints = vec![
        "https://api.devnet.solana.com".to_string(),
        "https://api.mainnet-beta.solana.com".to_string(),
    ];
    
    let config = Config::default();
    let manager = RpcManager::new_with_config(endpoints.clone(), config);
    
    // Get client for first endpoint twice
    let client1 = manager.get_client(&endpoints[0]).await;
    let client2 = manager.get_client(&endpoints[0]).await;
    
    // Should be the same Arc instance (cached)
    assert!(Arc::ptr_eq(&client1, &client2));
    
    // Different endpoint should be different client
    let client3 = manager.get_client(&endpoints[1]).await;
    assert!(!Arc::ptr_eq(&client1, &client3));
}