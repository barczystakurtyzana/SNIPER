/*!
Test Environment Runner for SNIPER Trading Bot

This is a separate program that runs the test environment for the Solana sniper bot
using a local solana-test-validator instance.

Usage:
    cargo run --bin test_runner [-- [OPTIONS]]

Options:
    --duration <SECS>     Test duration in seconds (default: 300)
    --ledger-dir <PATH>   Custom ledger directory (default: temp dir)
    --keypair <PATH>      Test keypair file path
    --bpf-program <ID> <PATH>  Load BPF program with given ID and SO file
    --help                Show this help message

Examples:
    cargo run --bin test_runner
    cargo run --bin test_runner -- --duration 60
    cargo run --bin test_runner -- --keypair ./test-keypair.json
    cargo run --bin test_runner -- --bpf-program TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA ./token.so
*/

use std::env;
use std::path::PathBuf;
use std::process;

use anyhow::{Context, Result};
use solana_sdk::pubkey::Pubkey;
use std::str::FromStr;
use tracing::{info, error};
use tracing_subscriber::EnvFilter;

use sniffer_bot_light::config::Config;
use sniffer_bot_light::test_environment::{TestEnvironment, TestValidatorConfig, BpfProgram};

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize logging
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env().add_directive("sniffer_bot_light=info".parse()?))
        .with_target(false)
        .init();

    info!("🚀 Starting SNIPER Bot Test Environment");

    // Parse command line arguments
    let config = parse_args()?;
    
    // Load bot configuration
    let bot_config = Config::load();
    
    // Create and start test environment
    let mut test_env = TestEnvironment::new(config);
    
    if let Err(e) = run_test_environment(&mut test_env, bot_config).await {
        error!("❌ Test environment failed: {}", e);
        process::exit(1);
    }

    info!("🎉 Test environment completed successfully");
    Ok(())
}

async fn run_test_environment(test_env: &mut TestEnvironment, bot_config: Config) -> Result<()> {
    // Start the test environment
    test_env.start().await
        .context("Failed to start test environment")?;

    // Run the test suite
    let results = test_env.run_tests(bot_config).await
        .context("Failed to run test suite")?;

    // Print results
    results.print_summary();

    // Stop the environment
    test_env.stop().await
        .context("Failed to stop test environment")?;

    // Exit with error code if tests failed
    if !results.all_passed() {
        return Err(anyhow::anyhow!("Some tests failed"));
    }

    Ok(())
}

fn parse_args() -> Result<TestValidatorConfig> {
    let args: Vec<String> = env::args().collect();
    let mut config = TestValidatorConfig::default();
    let mut i = 1;

    while i < args.len() {
        match args[i].as_str() {
            "--help" | "-h" => {
                print_help();
                process::exit(0);
            }
            "--duration" => {
                if i + 1 >= args.len() {
                    return Err(anyhow::anyhow!("--duration requires a value"));
                }
                config.test_duration_secs = args[i + 1].parse()
                    .context("Invalid duration value")?;
                i += 2;
            }
            "--ledger-dir" => {
                if i + 1 >= args.len() {
                    return Err(anyhow::anyhow!("--ledger-dir requires a path"));
                }
                config.ledger_dir = Some(PathBuf::from(&args[i + 1]));
                i += 2;
            }
            "--keypair" => {
                if i + 1 >= args.len() {
                    return Err(anyhow::anyhow!("--keypair requires a path"));
                }
                config.keypair_path = Some(PathBuf::from(&args[i + 1]));
                i += 2;
            }
            "--bpf-program" => {
                if i + 2 >= args.len() {
                    return Err(anyhow::anyhow!("--bpf-program requires program ID and path"));
                }
                let program_id = Pubkey::from_str(&args[i + 1])
                    .context("Invalid program ID")?;
                let program_path = PathBuf::from(&args[i + 2]);
                
                config.bpf_programs.push(BpfProgram {
                    program_id,
                    program_path,
                });
                i += 3;
            }
            "--rpc-url" => {
                if i + 1 >= args.len() {
                    return Err(anyhow::anyhow!("--rpc-url requires a URL"));
                }
                config.rpc_url = args[i + 1].clone();
                i += 2;
            }
            "--ws-url" => {
                if i + 1 >= args.len() {
                    return Err(anyhow::anyhow!("--ws-url requires a URL"));
                }
                config.ws_url = args[i + 1].clone();
                i += 2;
            }
            arg if arg.starts_with("--") => {
                return Err(anyhow::anyhow!("Unknown option: {}", arg));
            }
            _ => {
                return Err(anyhow::anyhow!("Unexpected argument: {}", args[i]));
            }
        }
    }

    Ok(config)
}

fn print_help() {
    println!(r#"
Test Environment Runner for SNIPER Trading Bot

USAGE:
    cargo run --bin test_runner [OPTIONS]

OPTIONS:
    --duration <SECS>           Test duration in seconds (default: 300)
    --ledger-dir <PATH>         Custom ledger directory (default: temp dir)
    --keypair <PATH>            Test keypair file path
    --rpc-url <URL>             RPC URL for test validator (default: http://127.0.0.1:8899)
    --ws-url <URL>              WebSocket URL for test validator (default: ws://127.0.0.1:8900)
    --bpf-program <ID> <PATH>   Load BPF program with given ID and SO file
    --help, -h                  Show this help message

EXAMPLES:
    cargo run --bin test_runner
    cargo run --bin test_runner -- --duration 60
    cargo run --bin test_runner -- --keypair ./test-keypair.json
    cargo run --bin test_runner -- --bpf-program TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA ./token.so

DESCRIPTION:
    This program creates a comprehensive test environment for the SNIPER trading bot
    using a local solana-test-validator instance. It verifies the logical and functional
    correctness of the entire bot in a blockchain environment.

    The test suite includes:
    - Validator health checks
    - RPC manager functionality
    - Transaction broadcasting
    - Nonce management
    - Mock sniffer functionality
    - Integration testing

REQUIREMENTS:
    - solana-test-validator must be installed and available in PATH
    - Sufficient disk space for temporary ledger directory
    - Network ports 8899 and 8900 must be available

EXIT CODES:
    0    All tests passed
    1    Some tests failed or environment setup failed
"#);
}