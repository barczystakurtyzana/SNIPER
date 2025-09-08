#!/bin/bash

# Solana Sniffer Bot Production Deployment Script

set -e

echo "🚀 Deploying Solana Sniffer Bot..."

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Check if running as root
if [ "$EUID" -eq 0 ]; then
    echo -e "${RED}⚠️  Don't run this as root for security reasons${NC}"
    exit 1
fi

# Check dependencies
echo "📋 Checking dependencies..."

if ! command -v cargo &> /dev/null; then
    echo -e "${RED}❌ Rust/Cargo not found. Please install Rust first.${NC}"
    exit 1
fi

if ! command -v solana &> /dev/null; then
    echo -e "${YELLOW}⚠️  Solana CLI not found. Install it for keypair management.${NC}"
fi

# Build the project
echo "🔨 Building release version..."
cargo build --release

# Check if config exists
if [ ! -f "config.toml" ]; then
    echo -e "${YELLOW}⚠️  No config.toml found. Creating from template...${NC}"
    cp config.prod.toml config.toml
    echo -e "${YELLOW}📝 Please edit config.toml to set your keypair path and RPC endpoints${NC}"
fi

# Check keypair configuration
if grep -q "# keypair_path" config.toml; then
    echo -e "${YELLOW}⚠️  Keypair path not configured in config.toml${NC}"
    echo -e "${YELLOW}   The bot will run with placeholder transactions only${NC}"
fi

# Create systemd service file (optional)
SERVICE_NAME="solana-sniffer-bot"
SERVICE_FILE="/etc/systemd/system/${SERVICE_NAME}.service"

read -p "🔧 Create systemd service for auto-start? (y/N): " -n 1 -r
echo
if [[ $REPLY =~ ^[Yy]$ ]]; then
    if [ ! -w "/etc/systemd/system" ]; then
        echo -e "${RED}❌ Need sudo access to create systemd service${NC}"
        echo "You can create the service manually later"
    else
        echo "Creating systemd service..."
        sudo tee "$SERVICE_FILE" > /dev/null <<EOF
[Unit]
Description=Solana Sniffer Bot
After=network.target

[Service]
Type=simple
User=$USER
WorkingDirectory=$(pwd)
ExecStart=$(pwd)/target/release/sniffer_bot_light
Restart=on-failure
RestartSec=5
Environment=RUST_LOG=info

[Install]
WantedBy=multi-user.target
EOF
        
        sudo systemctl daemon-reload
        sudo systemctl enable "$SERVICE_NAME"
        echo -e "${GREEN}✅ Systemd service created and enabled${NC}"
        echo -e "   Start with: sudo systemctl start $SERVICE_NAME"
        echo -e "   Check logs with: journalctl -fu $SERVICE_NAME"
    fi
fi

# Final checks and recommendations
echo
echo -e "${GREEN}🎉 Deployment completed!${NC}"
echo
echo "📋 Pre-flight checklist:"
echo "  □ Configure keypair_path in config.toml"
echo "  □ Verify keypair has SOL balance for transaction fees"
echo "  □ Test with SNIFFER_MODE=mock first"
echo "  □ Configure premium RPC endpoints for production"
echo "  □ Review PRODUCTION.md for security guidelines"
echo
echo "🚀 To start the bot:"
echo "  ./target/release/sniffer_bot_light"
echo
echo "🔧 To test first:"
echo "  SNIFFER_MODE=mock ./target/release/sniffer_bot_light"
echo
echo -e "${YELLOW}⚠️  Remember: This bot performs real financial transactions in production mode!${NC}"