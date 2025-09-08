# Production Deployment Guide

## Prerequisites

1. **Solana CLI installed**:
   ```bash
   sh -c "$(curl -sSfL https://release.solana.com/v1.18.14/install)"
   ```

2. **Keypair setup**:
   ```bash
   # Generate a new keypair (for testing on devnet/testnet)
   solana-keygen new --outfile ~/sniper-keypair.json
   
   # Or use existing keypair
   # Make sure it has SOL balance for transaction fees
   ```

3. **System requirements**:
   - 4+ GB RAM
   - 2+ CPU cores  
   - Stable internet connection
   - Linux/macOS (Windows via WSL)

## Configuration

1. **Copy and edit production config**:
   ```bash
   cp config.prod.toml config.toml
   ```

2. **Set keypair path** in `config.toml`:
   ```toml
   keypair_path = "/home/user/sniper-keypair.json"
   ```

3. **Configure RPC endpoints** (recommended to use paid RPC for production):
   ```toml
   rpc_endpoints = [
       "https://your-paid-rpc-endpoint.com",  # Primary
       "https://api.mainnet-beta.solana.com"  # Backup
   ]
   ```

## Running

### Development/Testing
```bash
# Test with mock mode (no real transactions)
SNIFFER_MODE=mock cargo run

# Test with real mode but devnet
SNIFFER_MODE=real cargo run  # Make sure keypair is funded on correct network
```

### Production
```bash
# Build optimized release
cargo build --release

# Run production bot
./target/release/sniffer_bot_light

# With environment override
SNIFFER_MODE=real ./target/release/sniffer_bot_light
```

## Monitoring

Key metrics to monitor:
- Transaction success rate (should be >90%)
- Latency (sniffer to buy attempt should be <200ms)
- RPC errors and timeouts
- Nonce pool health

## Security Considerations

1. **Keypair Security**:
   - Use hardware wallet or HSM for large amounts
   - Keep keypair file permissions restrictive (600)
   - Never commit keypair to git

2. **Network Security**:
   - Use VPN or dedicated server
   - Monitor for unusual activity
   - Set reasonable transaction limits

3. **Operational Security**:
   - Regular backups of configuration
   - Test on devnet/testnet first
   - Monitor logs for errors

## Troubleshooting

### Common Issues

1. **"Failed to load wallet"**:
   - Check keypair file path and permissions
   - Verify keypair file format (JSON array)

2. **"RPC timeout errors"**:
   - Use faster/paid RPC endpoints
   - Reduce parallel transaction count
   - Check network connectivity

3. **"Transaction failures"**:
   - Ensure sufficient SOL balance for fees
   - Check nonce account status
   - Verify program addresses

### Performance Tuning

For high-frequency trading:
- Use premium RPC with low latency
- Deploy close to Solana validators geographically
- Optimize nonce_count (5-8 recommended)
- Use priority fees for transaction acceleration

## Legal Disclaimer

This bot is for educational purposes. Users are responsible for:
- Complying with local laws and regulations
- Understanding MEV and frontrunning implications  
- Managing financial risks appropriately
- Following Solana network terms of service