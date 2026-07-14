#!/bin/bash

# Memento Local Node Installer (Linux / macOS)
# Respects the ImagineOS PM2 mandate

set -e

echo "🧠 Building Memento Sovereign Memory Node..."

# 1. Build the Rust binary in release mode
cargo build --release

# 2. Setup local installation paths
INSTALL_DIR="$HOME/.local/bin"
mkdir -p "$INSTALL_DIR"

echo "📦 Installing binary to $INSTALL_DIR/memento"
cp target/release/memento "$INSTALL_DIR/memento"

# 3. Setup PM2 (ImagineOS Standard)
echo "🚀 Configuring PM2 Process Manager..."
if ! command -v pm2 &> /dev/null
then
    echo "❌ PM2 could not be found. Please install it with 'npm install -g pm2'"
    exit 1
fi

# Stop it if it's already running
pm2 stop memento-node 2>/dev/null || true

# Start or Restart the Memento Node
pm2 start ecosystem.config.cjs
pm2 save

echo "✅ Memento Local Node installation complete!"
echo "🌐 View your Local Dashboard at: http://localhost:3306"
echo "🗄️ Checking Memento Status:"
pm2 status memento-node
