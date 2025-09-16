# Market Simulator - TokenGenerator Module

The Market Simulator is a separate Tokio program that runs in parallel with the SNIPER bot to simulate near-real market conditions by generating virtual tokens with predefined profiles.

## TokenGenerator Module (1/2)

This is the first of two modules for the Market Simulator. The TokenGenerator is responsible for mass-producing virtual tokens with predefined, random profiles.

### Features

- **Weighted Token Generation**: Creates tokens with different profiles based on probabilities:
  - **Gem** (1%): High-quality tokens with real metadata and significant liquidity (50 SOL)
  - **Rug** (9%): Rug pull tokens with minimal liquidity (0.1 SOL), empty metadata
  - **Trash** (90%): Low-quality tokens with poor metadata and moderate liquidity (1 SOL)

- **Random Intervals**: Generates tokens at random intervals (default: 500ms - 5000ms)

- **Complete Token Setup**: Each token creation includes:
  - Mint account creation and initialization
  - Metaplex metadata account creation
  - Initial supply minting
  - Liquidity pool creation (placeholder for pump.fun integration)
  - Token distribution to trader wallets (for Gems only)

- **Thread-Safe Storage**: Stores generated tokens in a shared map using Arc<RwLock<HashMap>>

### Usage

#### Basic Usage
```bash
# Run indefinitely with default settings
cargo run --bin market_simulator

# Run for 1 hour
cargo run --bin market_simulator -- --duration 3600

# Custom token generation intervals (1-3 seconds)
cargo run --bin market_simulator -- --interval-min 1000 --interval-max 3000
```

#### Command Line Options
- `--config <PATH>`: Configuration file path (default: config.toml)
- `--duration <SECS>`: Simulation duration in seconds (default: unlimited)
- `--interval-min <MS>`: Minimum interval between token generation (default: 500ms)
- `--interval-max <MS>`: Maximum interval between token generation (default: 5000ms)
- `--help`: Show help message

#### Example with Logging
```bash
# Run with info-level logging for 30 seconds
RUST_LOG=info cargo run --bin market_simulator -- --duration 30
```

### Configuration

The Market Simulator uses the same configuration system as the main bot. If no keypair is configured, it will generate a random wallet for simulation purposes.

### Token Profiles

#### Gem Tokens (1% probability)
- **Supply**: 1 billion tokens (1,000,000,000)
- **Liquidity**: 50 SOL
- **Metadata**: Real metadata with image links
- **Distribution**: 5% of supply distributed to trader wallets
- **Description**: High-quality tokens that represent valuable opportunities

#### Rug Tokens (9% probability)
- **Supply**: 100 million tokens (100,000,000)
- **Liquidity**: 0.1 SOL (minimal)
- **Metadata**: Empty or junk data
- **Distribution**: No distribution to traders
- **Description**: Rug pull tokens that may disappear quickly

#### Trash Tokens (90% probability)
- **Supply**: 10 million tokens (10,000,000)
- **Liquidity**: 1 SOL
- **Metadata**: Poor quality metadata
- **Distribution**: No distribution to traders
- **Description**: Low-quality tokens with limited value

### Testing

Run the test suite to verify functionality:

```bash
# Run all tests for the market simulator
cargo test --bin market_simulator

# Run specific tests
cargo test --bin market_simulator test_token_profile_weights
```

### Integration with Bot

The Market Simulator is designed to work alongside the main SNIPER bot. Both programs can:
- Share the same configuration file
- Connect to the same local validator
- Use compatible wallet and RPC infrastructure

### Future Enhancements

The current implementation uses placeholder transactions for safe testing. Future enhancements could include:
- Real blockchain integration with actual transaction submission
- Enhanced Metaplex metadata support with IPFS integration
- Real pump.fun program integration for liquidity pools
- Advanced token distribution mechanisms
- Market dynamics simulation (price movements, volume, etc.)

### Architecture

The TokenGenerator follows a modular design:
- `TokenProfile` enum defines the three token types
- `SimulatorConfig` manages timing configuration
- `GeneratedToken` struct stores token information
- `TokenGenerator` struct orchestrates the token creation process
- Thread-safe storage using Arc and RwLock for concurrent access

### Error Handling

The simulator includes comprehensive error handling:
- Graceful degradation when transactions fail
- Logging of all operations for debugging
- Continuation of operation even if individual token creation fails
- Clean shutdown on termination signals