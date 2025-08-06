#!/bin/bash

# Source path initialization
source "$(dirname "$0")/000_init_paths.sh" || {
    echo "❌ Failed to source path initialization script" >&2
    exit 1
}

# Source common logging
source $TRILLIUM_SCRIPTS_BASH/999_common_log.sh

# RPC configuration
DEFAULT_RPC_URL="https://wiser-young-star.solana-mainnet.quiknode.pro/887452d66f8b645b8824eab20011dbd3c315d84f/"
FALLBACK_RPC_URL="https://solana-mainnet.g.alchemy.com/v2/97zE6cvElUYwOp_zVSqMXt5H7dYSYxtW"

RPC_TIMEOUT=10
RPC_MAX_RETRIES=3


# Initialize logging
init_logging

log_info "💰 Starting epoch stake account details retrieval"

# Ensure data directory exists
mkdir -p /home/smilax/trillium_api/data/stake_accounts

# Change to data directory
cd /home/smilax/trillium_api/data/stake_accounts

# Get current epoch
current_epoch=$(/home/smilax/agave/bin/solana epoch --url https://api.mainnet-beta.solana.com)
log_info "📊 Current epoch: $current_epoch"

# Calculate epoch progress (0-10%, 11-20%, etc.)
# This runs every hour, so we need to determine which 10% bucket we're in
hour=$(date +%H)
bucket=$((hour % 10))
bucket_start=$((bucket * 10))
bucket_end=$(((bucket + 1) * 10 - 1))

log_info "🕐 Hour: $hour, Processing bucket: ${bucket_start}-${bucket_end}%"

# Create filename based on epoch and bucket
filename="epoch${current_epoch}_stake_${bucket_start}-${bucket_end}pct.json"

log_info "📥 Fetching stake account details for epoch $current_epoch (${bucket_start}-${bucket_end}%)"

# Fetch stake accounts with fallback
fetch_success=false

# Try DEFAULT_RPC_URL first
log_info "🔗 Attempting to fetch with primary RPC URL..."
if /home/smilax/agave/bin/solana stakes --url $DEFAULT_RPC_URL --output json > "$filename" 2>/dev/null; then
    # Verify file is valid JSON
    if jq empty "$filename" 2>/dev/null; then
        log_info "✅ Successfully fetched from primary RPC URL"
        fetch_success=true
    else
        log_warn "⚠️  Primary RPC returned invalid JSON"
        rm -f "$filename"
    fi
else
    log_warn "⚠️  Failed to fetch from primary RPC URL"
fi

# If primary failed, try fallback
if [ "$fetch_success" = false ]; then
    log_info "🔄 Attempting to fetch with fallback RPC URL..."
    if /home/smilax/agave/bin/solana stakes --url $FALLBACK_RPC_URL --output json > "$filename" 2>/dev/null; then
        # Verify file is valid JSON
        if jq empty "$filename" 2>/dev/null; then
            log_info "✅ Successfully fetched from fallback RPC URL"
            fetch_success=true
        else
            log_error "❌ Fallback RPC returned invalid JSON"
            rm -f "$filename"
        fi
    else
        log_error "❌ Failed to fetch from fallback RPC URL"
    fi
fi

# Check if we successfully fetched data
if [ "$fetch_success" = true ]; then
    log_info "✅ Successfully saved stake account details to $filename"
    
    # Get stake account count
    stake_count=$(jq '.stakeAccounts | length' "$filename" 2>/dev/null || echo "unknown")
    log_info "📊 Retrieved $stake_count stake accounts"
else
    log_error "❌ Failed to fetch stake account details from all RPC URLs"
    exit 1
fi

# Clean up old stake account files (keep last 5 epochs)
log_info "🧹 Cleaning up old stake account files"
find . -name "epoch*_stake_*.json" -type f | sort -V | head -n -50 | xargs -r rm -f

log_info "🎉 Epoch stake account details retrieval completed"

# Cleanup logging
cleanup_logging