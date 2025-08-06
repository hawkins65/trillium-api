#!/bin/bash

# Source path initialization
source "$(dirname "$0")/000_init_paths.sh" || {
    echo "❌ Failed to source path initialization script" >&2
    exit 1
}

# Source common logging
source $TRILLIUM_SCRIPTS_BASH/999_common_log.sh

# Initialize logging  
init_logging

log_info "🌐 Starting mainnet stake percentage analysis"

# Configuration - matches original script behavior
WORK_DIR="/home/smilax/trillium_api/data/stake_accounts"
DISCORD_WEBHOOK="https://discord.com/api/webhooks/1397269122441805835/_Qh0rY24s5a4QPqgOjWVTB2MxYgJWAZRf4ytCc6SU6t8TQCIpitTdVYQ233e-4Z-cJQk"

# Ensure work directory exists
mkdir -p "$WORK_DIR"
cd "$WORK_DIR"

# 1) Activate Python environment (like original)
if [ -f "/home/smilax/.python_env/bin/activate" ]; then
    source /home/smilax/.python_env/bin/activate
    log_info "✅ Python environment activated"
else
    log_info "⚠️ Python environment not found, using system Python"
fi

# 2) Run the Python stake analysis (matches original approach)
log_info "🐍 Running Python stake percentage analysis for mainnet"

if ! python3 $TRILLIUM_SCRIPTS_PYTHON/stake-percentage.py mainnet; then
    log_error "❌ Python stake analysis failed"
    
    # Send failure notification to Discord
    current_epoch=$(/home/smilax/agave/bin/solana epoch --url https://api.mainnet-beta.solana.com 2>/dev/null || echo "unknown")
    
    message="❌ **Mainnet Stake Analysis Failed**\n\nThe stake percentage analysis script encountered an error.\nEpoch: $current_epoch\n\nPlease check the logs for details."
    
    payload=$(cat <<EOF
{
    "embeds": [{
        "title": "🚨 Mainnet Stake Analysis Error",
        "description": "$message",
        "color": 16711680,
        "timestamp": "$(date -u +%Y-%m-%dT%H:%M:%S.000Z)",
        "footer": {
            "text": "Mainnet Stake Monitor - Epoch $current_epoch"
        }
    }]
}
EOF
    )
    
    curl -H "Content-Type: application/json" \
         -d "$payload" \
         "$DISCORD_WEBHOOK" > /dev/null 2>&1
    
    exit 1
fi

log_info "✅ Python stake analysis completed successfully"

# 3) Copy results to web (matches original)
if [ -f "mainnet-validators-stake-rank.txt" ]; then
    log_info "📁 Copying mainnet stake results to web"
    if ! bash $TRILLIUM_SCRIPTS_BASH/copy-pages-to-web.sh mainnet-validators-stake-rank.txt; then
        log_error "❌ Failed to copy results to web"
        
        # Send failure notification to Discord
        current_epoch=$(/home/smilax/agave/bin/solana epoch --url https://api.mainnet-beta.solana.com 2>/dev/null || echo "unknown")
        
        message="❌ **Mainnet Stake Results Copy Failed**\n\nFailed to copy stake analysis results to web server.\nEpoch: $current_epoch\n\nPlease check the logs for details."
        
        payload=$(cat <<EOF
{
    "embeds": [{
        "title": "🚨 Mainnet File Copy Error",
        "description": "$message",
        "color": 16711680,
        "timestamp": "$(date -u +%Y-%m-%dT%H:%M:%S.000Z)",
        "footer": {
            "text": "Mainnet Stake Monitor - Epoch $current_epoch"
        }
    }]
}
EOF
        )
        
        curl -H "Content-Type: application/json" \
             -d "$payload" \
             "$DISCORD_WEBHOOK" > /dev/null 2>&1
    else
        log_info "✅ Results copied to web successfully"
    fi
else
    log_error "❌ Expected output file mainnet-validators-stake-rank.txt not found"
    
    # Send failure notification to Discord
    current_epoch=$(/home/smilax/agave/bin/solana epoch --url https://api.mainnet-beta.solana.com 2>/dev/null || echo "unknown")
    
    message="❌ **Mainnet Stake Output Missing**\n\nExpected output file mainnet-validators-stake-rank.txt not found.\nEpoch: $current_epoch\n\nThe analysis may have failed silently."
    
    payload=$(cat <<EOF
{
    "embeds": [{
        "title": "🚨 Mainnet Output File Missing",
        "description": "$message",
        "color": 16711680,
        "timestamp": "$(date -u +%Y-%m-%dT%H:%M:%S.000Z)",
        "footer": {
            "text": "Mainnet Stake Monitor - Epoch $current_epoch"
        }
    }]
}
EOF
    )
    
    curl -H "Content-Type: application/json" \
         -d "$payload" \
         "$DISCORD_WEBHOOK" > /dev/null 2>&1
fi

# Successfully completed - no notification needed

log_info "🎉 Mainnet stake percentage analysis completed"

# Cleanup logging
cleanup_logging