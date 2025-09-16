/*!
Test suite for the test environment module
*/

use std::path::PathBuf;
use std::time::Duration;

use anyhow::Result;
use solana_sdk::pubkey::Pubkey;
use tempfile::TempDir;

use sniffer_bot_light::test_environment::{TestEnvironment, TestValidatorConfig, BpfProgram, TestResults};
use sniffer_bot_light::config::Config;

#[tokio::test]
async fn test_validator_config_creation() {
    let config = TestValidatorConfig::default();
    
    assert_eq!(config.rpc_url, "http://127.0.0.1:8899");
    assert_eq!(config.ws_url, "ws://127.0.0.1:8900");
    assert_eq!(config.test_duration_secs, 300);
    assert!(config.additional_flags.contains(&"--reset".to_string()));
    assert!(config.additional_flags.contains(&"--quiet".to_string()));
}

#[tokio::test]
async fn test_bpf_program_creation() {
    let program_id = Pubkey::new_unique();
    let program_path = PathBuf::from("/tmp/test_program.so");
    
    let bpf_program = BpfProgram {
        program_id,
        program_path: program_path.clone(),
    };
    
    assert_eq!(bpf_program.program_id, program_id);
    assert_eq!(bpf_program.program_path, program_path);
}

#[tokio::test]
async fn test_environment_initialization() {
    let config = TestValidatorConfig::default();
    let test_env = TestEnvironment::new(config);
    
    // Test that environment is created without starting
    assert!(test_env.test_keypair().is_none());
    assert!(test_env.rpc_client().is_none());
}

#[tokio::test]
async fn test_config_with_custom_settings() {
    let temp_dir = TempDir::new().unwrap();
    let program_id = Pubkey::new_unique();
    
    let mut config = TestValidatorConfig::default();
    config.rpc_url = "http://localhost:9999".to_string();
    config.ws_url = "ws://localhost:9998".to_string();
    config.test_duration_secs = 600;
    config.ledger_dir = Some(temp_dir.path().to_path_buf());
    config.bpf_programs.push(BpfProgram {
        program_id,
        program_path: PathBuf::from("/tmp/program.so"),
    });
    
    assert_eq!(config.rpc_url, "http://localhost:9999");
    assert_eq!(config.ws_url, "ws://localhost:9998");
    assert_eq!(config.test_duration_secs, 600);
    assert!(config.ledger_dir.is_some());
    assert_eq!(config.bpf_programs.len(), 1);
    assert_eq!(config.bpf_programs[0].program_id, program_id);
}

#[tokio::test]
async fn test_results_collection() {
    let mut results = TestResults::new();
    
    // Test adding successful and failed tests
    results.add_test("test_success", Ok(()));
    results.add_test("test_failure", Err(anyhow::anyhow!("Test failed")));
    results.add_test("test_another_success", Ok(()));
    
    assert_eq!(results.tests.len(), 3);
    assert!(!results.all_passed()); // Should fail because one test failed
    
    // Check individual test results
    assert!(results.tests.get("test_success").unwrap().is_ok());
    assert!(results.tests.get("test_failure").unwrap().is_err());
    assert!(results.tests.get("test_another_success").unwrap().is_ok());
}

#[tokio::test]
async fn test_results_all_passed() {
    let mut results = TestResults::new();
    
    // Add only successful tests
    results.add_test("test1", Ok(()));
    results.add_test("test2", Ok(()));
    results.add_test("test3", Ok(()));
    
    assert!(results.all_passed());
}

#[tokio::test]
async fn test_config_validation() {
    let bot_config = Config::default();
    
    // This should not panic
    let config = TestValidatorConfig::default();
    let _test_env = TestEnvironment::new(config);
    
    // Validate that bot config has required fields
    assert!(!bot_config.rpc_endpoints.is_empty());
    assert!(bot_config.nonce_count > 0);
}

#[test]
fn test_test_environment_drop() {
    // Test that the test environment can be dropped safely
    let config = TestValidatorConfig::default();
    let test_env = TestEnvironment::new(config);
    
    // This should not panic when dropped
    drop(test_env);
}

#[tokio::test]
async fn test_keypair_loading_from_path() {
    // Test loading keypair from a path (would require actual file in real scenario)
    let mut config = TestValidatorConfig::default();
    config.keypair_path = Some(PathBuf::from("/tmp/nonexistent-keypair.json"));
    
    let test_env = TestEnvironment::new(config);
    
    // Environment should be created even with invalid keypair path
    assert!(test_env.test_keypair().is_none());
}

#[tokio::test]
async fn test_timeout_configuration() {
    let config = TestValidatorConfig {
        test_duration_secs: 1, // Very short duration for testing
        ..Default::default()
    };
    
    assert_eq!(config.test_duration_secs, 1);
}

// Mock test that verifies the structure without requiring actual validator
#[tokio::test]
async fn test_mock_validator_setup() -> Result<()> {
    let config = TestValidatorConfig::default();
    let mut test_env = TestEnvironment::new(config);
    
    // This test verifies that the test environment structure is correct
    // without actually starting a validator (which would require solana-test-validator installed)
    
    // Test that we can create the environment
    assert!(test_env.test_keypair().is_none());
    assert!(test_env.rpc_client().is_none());
    
    // Test that cleanup works
    test_env.stop().await?;
    
    Ok(())
}