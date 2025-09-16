# Test Environment for SNIPER Trading Bot

This document describes the test environment component created for the SNIPER trading bot, designed to work with a local `solana-test-validator` instance.

## Overview

The test environment provides a comprehensive testing framework that:

1. **Creates a controlled blockchain environment** using `solana-test-validator`
2. **Validates bot functionality** without affecting live networks
3. **Tests all major bot components** including RPC management, transaction broadcasting, and nonce management
4. **Provides automated test reporting** with detailed results

## Components

### TestEnvironment

The main test environment class that manages:
- Local validator instance lifecycle
- Test account funding and management
- RPC client initialization
- Comprehensive test suite execution

### TestValidatorConfig

Configuration for the test validator with options for:
- Custom RPC and WebSocket URLs
- BPF program loading
- Additional validator flags
- Test duration and ledger directory

### TestResults

Results collection and reporting system that tracks:
- Individual test outcomes
- Overall test duration
- Pass/fail statistics
- Detailed error reporting

## Usage

### Command Line Test Runner

The test environment includes a dedicated binary for running tests:

```bash
# Basic usage
cargo run --bin test_runner

# With custom duration
cargo run --bin test_runner -- --duration 60

# With custom keypair
cargo run --bin test_runner -- --keypair ./test-keypair.json

# With BPF program
cargo run --bin test_runner -- --bpf-program TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA ./token.so

# Help
cargo run --bin test_runner -- --help
```

### Programmatic Usage

```rust
use sniffer_bot_light::test_environment::{TestEnvironment, TestValidatorConfig};
use sniffer_bot_light::config::Config;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Create test configuration
    let test_config = TestValidatorConfig::default();
    
    // Create test environment
    let mut test_env = TestEnvironment::new(test_config);
    
    // Start the environment
    test_env.start().await?;
    
    // Load bot configuration
    let bot_config = Config::load();
    
    // Run tests
    let results = test_env.run_tests(bot_config).await?;
    
    // Print results
    results.print_summary();
    
    // Stop environment
    test_env.stop().await?;
    
    Ok(())
}
```

## Test Suite

The test environment runs the following tests:

### 1. Validator Health Checks
- Verifies validator is running and responding
- Checks RPC connectivity
- Validates slot progression

### 2. RPC Manager Testing
- Tests multiple endpoint handling
- Validates connection pooling
- Checks timeout and retry logic

### 3. Transaction Broadcasting
- Tests transaction creation and submission
- Validates signature confirmation
- Checks transaction status tracking

### 4. Nonce Management
- Tests nonce allocation and management
- Validates unique nonce distribution
- Checks nonce slot availability

### 5. Mock Sniffer Testing
- Creates mock trading candidates
- Tests candidate validation
- Verifies data structure handling

### 6. Integration Testing
- End-to-end bot component testing
- Mock data pipeline validation
- Configuration consistency checks

## Configuration Options

### TestValidatorConfig Fields

- `rpc_url`: Local validator RPC URL (default: `http://127.0.0.1:8899`)
- `ws_url`: Local validator WebSocket URL (default: `ws://127.0.0.1:8900`)
- `keypair_path`: Path to test keypair file (optional)
- `bpf_programs`: List of BPF programs to load
- `ledger_dir`: Custom ledger directory (optional, uses temp dir by default)
- `additional_flags`: Extra flags for the validator
- `test_duration_secs`: Maximum test duration (default: 300 seconds)

### BPF Program Loading

```rust
use sniffer_bot_light::test_environment::{BpfProgram, TestValidatorConfig};
use solana_sdk::pubkey::Pubkey;
use std::path::PathBuf;

let mut config = TestValidatorConfig::default();
config.bpf_programs.push(BpfProgram {
    program_id: Pubkey::from_str("TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA")?,
    program_path: PathBuf::from("./programs/token.so"),
});
```

## Requirements

### System Dependencies

1. **solana-test-validator**: Must be installed and available in PATH
2. **Disk Space**: Sufficient space for temporary ledger directories
3. **Network Ports**: Ports 8899 (RPC) and 8900 (WS) must be available
4. **Rust Toolchain**: Compatible with Solana SDK version 2.3

### Installation

```bash
# Install Solana CLI tools (includes test validator)
sh -c "$(curl -sSfL https://release.solana.com/v1.18.0/install)"

# Add to PATH
export PATH="$HOME/.local/share/solana/install/active_release/bin:$PATH"

# Verify installation
solana-test-validator --version
```

## Environment Variables

The test environment respects the following environment variables:

- `RUST_LOG`: Controls logging level (default: `info`)
- `SNIFFER_MODE`: Bot operation mode (should be `mock` for testing)

## Error Handling

The test environment provides comprehensive error handling for:

- **Validator startup failures**: Automatic cleanup and clear error messages
- **Network connectivity issues**: Timeout handling with retries
- **Configuration errors**: Validation with descriptive error messages
- **Test failures**: Detailed error reporting with context

## Performance Considerations

- **Temporary Files**: Automatically cleaned up after test completion
- **Memory Usage**: Validator uses local storage, minimal memory overhead
- **Network**: All traffic is local, no external network dependencies
- **CPU Usage**: Moderate during test execution, minimal when idle

## Security

- **Isolated Environment**: Tests run in complete isolation from live networks
- **Temporary Keys**: Test keypairs are ephemeral and safe to use
- **No Real Funds**: All transactions use test lamports with no real value
- **Local Only**: No external network communication required

## Troubleshooting

### Common Issues

1. **Port Already in Use**:
   ```
   Error: Port 8899 already in use
   ```
   Solution: Stop existing validator or change ports in config

2. **Missing solana-test-validator**:
   ```
   Error: solana-test-validator not found
   ```
   Solution: Install Solana CLI tools and ensure PATH is correct

3. **Insufficient Disk Space**:
   ```
   Error: No space left on device
   ```
   Solution: Clean up temp files or specify custom ledger directory

4. **Permission Issues**:
   ```
   Error: Permission denied
   ```
   Solution: Ensure write permissions for ledger directory

### Debug Mode

Enable debug logging for more detailed output:

```bash
RUST_LOG=debug cargo run --bin test_runner
```

## Integration with CI/CD

The test environment is designed for integration with automated testing:

```yaml
# Example GitHub Actions workflow
name: Test Environment
on: [push, pull_request]

jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      - name: Install Solana
        run: |
          sh -c "$(curl -sSfL https://release.solana.com/stable/install)"
          echo "$HOME/.local/share/solana/install/active_release/bin" >> $GITHUB_PATH
      - name: Run Tests
        run: cargo run --bin test_runner -- --duration 60
```

## Future Enhancements

Planned improvements include:

1. **Parallel Test Execution**: Running multiple test validators simultaneously
2. **Advanced Scenario Testing**: Complex multi-transaction scenarios
3. **Performance Benchmarking**: Automated performance regression testing
4. **Network Simulation**: Simulating network conditions and failures
5. **Integration Testing**: Testing against real DEX programs

## Contributing

When adding new tests to the environment:

1. Follow the existing test structure in `TestEnvironment::run_tests()`
2. Add comprehensive error handling and logging
3. Ensure tests are deterministic and don't depend on external state
4. Update documentation for any new configuration options
5. Add unit tests for new functionality

## License

This test environment is part of the SNIPER trading bot project and follows the same license terms.