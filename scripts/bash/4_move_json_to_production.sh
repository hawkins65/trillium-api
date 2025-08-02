#!/bin/bash

# Source the common logging functions
source /home/smilax/api/999_common_log.sh
# Initialize enhanced logging
init_logging

# Get the basename of the script for Discord notifications
script_name=$(basename "$0")

VERBOSE=1

log "INFO" "🚀 Starting JSON files move to production process"

# Function to handle errors
handle_error() {
    local command="$1"
    log "ERROR" "❌ Command failed: $command"
    exit 1
}

# Function to run a command with logging and error handling
run_command() {
    local command="$1"
    if [ "$VERBOSE" -eq 1 ]; then
        log "INFO" "⚡ Running: $command"
    fi
    eval "$command" || handle_error "$command"
}

log "INFO" "📁 Moving website JSON files to production directory"

# Move website JSON files
if ls ./json/epoch*.json 1> /dev/null 2>&1; then
    log "INFO" "📊 Found epoch*.json files to move"
    run_command "mv -v ./json/epoch*.json /home/smilax/block-production/leaderboard/production/validator_rewards/static/json"
else
    log "INFO" "ℹ️ No './json/epoch*.json' files found to move"
fi

if [ -e "./json/last_ten_epoch_aggregate_data.json" ]; then
    log "INFO" "📈 Moving last ten epoch aggregate data file"
    run_command "mv -v ./json/last_ten_epoch_aggregate_data.json /home/smilax/block-production/leaderboard/production/validator_rewards/static/json"
else
    log "INFO" "ℹ️ File not found: ./json/last_ten_epoch_aggregate_data.json"
fi

if ls ./json/ten_epoch_*.json 1> /dev/null 2>&1; then
    log "INFO" "📊 Found ten_epoch_*.json files to move"
    run_command "mv -v ./json/ten_epoch_*.json /home/smilax/block-production/leaderboard/production/validator_rewards/static/json"
else
    log "INFO" "ℹ️ No './json/ten_epoch_*.json' files found to move"
fi

if [ -e "./json/recency_weighted_average_validator_rewards.json" ]; then
    log "INFO" "⚖️ Moving recency weighted average validator rewards file"
    run_command "mv -v ./json/recency_weighted_average_validator_rewards.json /home/smilax/block-production/leaderboard/production/validator_rewards/static/json"
else
    log "INFO" "ℹ️ File not found: ./json/recency_weighted_average_validator_rewards.json"
fi

log "INFO" "🗳️ Processing vote latency files"

for file in vote_latency*.{json,txt}; do
    if [ -f "$file" ]; then
        log "INFO" "📋 Processing vote latency file: $file"
        if ./copy-pages-to-web.sh "$file"; then
            if rm "$file"; then
                log "INFO" "✅ Successfully processed and removed: $file"
            else
                log "ERROR" "❌ Failed to remove $file"
            fi
        else
            log "ERROR" "❌ Failed to process: $file"
        fi
    fi
done

log "INFO" "☁️ Purging Cloudflare cache"
if bash /home/smilax/api/cloudflare-purge-cache.sh; then
    log "INFO" "✅ Successfully purged Cloudflare cache"
else
    log "ERROR" "❌ Failed to purge Cloudflare cache"
fi

log "INFO" "🎉 JSON files move to production completed successfully"
