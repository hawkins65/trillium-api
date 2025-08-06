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

log_info "🏆 Starting leader schedule retrieval"

# Ensure data directory exists
mkdir -p /home/smilax/trillium_api/data/leader_schedules

# Change to data directory
cd /home/smilax/trillium_api/data/leader_schedules

# Get current epoch
current_epoch=$(/home/smilax/agave/bin/solana epoch --url https://api.mainnet-beta.solana.com)
next_epoch=$((current_epoch + 1))

log_info "📊 Current epoch: $current_epoch"
log_info "📊 Next epoch: $next_epoch"

# Function to fetch leader schedule
fetch_leader_schedule() {
    local epoch=$1
    local filename="epoch${epoch}-leaderschedule.json"
    
    log_info "📥 Fetching leader schedule for epoch $epoch"
    
    if /home/smilax/agave/bin/solana leader-schedule --epoch $epoch --url https://api.mainnet-beta.solana.com --output json > "$filename"; then
        log_info "✅ Successfully saved leader schedule for epoch $epoch to $filename"
        
        # Verify file is valid JSON
        if jq empty "$filename" 2>/dev/null; then
            log_info "✅ Leader schedule JSON is valid for epoch $epoch"
        else
            log_error "❌ Invalid JSON in leader schedule for epoch $epoch"
            rm -f "$filename"
            return 1
        fi
    else
        log_error "❌ Failed to fetch leader schedule for epoch $epoch"
        return 1
    fi
}

# Fetch current epoch leader schedule
if fetch_leader_schedule $current_epoch; then
    log_info "✅ Current epoch leader schedule retrieved successfully"
else
    log_error "❌ Failed to retrieve current epoch leader schedule"
fi

# Fetch next epoch leader schedule
if fetch_leader_schedule $next_epoch; then
    log_info "✅ Next epoch leader schedule retrieved successfully"
else
    log_info "ℹ️ Next epoch leader schedule not yet available"
fi

# Clean up old leader schedules (keep last 10 epochs)
log_info "🧹 Cleaning up old leader schedules"
ls -1 epoch*-leaderschedule.json 2>/dev/null | sort -V | head -n -10 | xargs -r rm -f

log_info "🎉 Leader schedule retrieval completed"

# Cleanup logging
cleanup_logging