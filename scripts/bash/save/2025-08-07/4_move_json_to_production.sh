#!/bin/bash

# Source path initialization
source "$(dirname "$0")/000_init_paths.sh" || {
    echo "❌ Failed to source path initialization script" >&2
    exit 1
}

# Source the common logging functions
source $TRILLIUM_SCRIPTS_BASH/999_common_log.sh
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

# Define the leaderboard JSON directory
LEADERBOARD_JSON_DIR="${TRILLIUM_DATA}/leaderboard/json"

# Move website JSON files from leaderboard output directory
if ls ${LEADERBOARD_JSON_DIR}/epoch*.json 1> /dev/null 2>&1; then
    log "INFO" "📊 Found epoch*.json files to move from leaderboard output"
    run_command "mv -v ${LEADERBOARD_JSON_DIR}/epoch*.json /home/smilax/block-production/leaderboard/production/validator_rewards/static/json"
else
    log "INFO" "ℹ️ No '${LEADERBOARD_JSON_DIR}/epoch*.json' files found to move"
fi

if [ -e "${LEADERBOARD_JSON_DIR}/last_ten_epoch_aggregate_data.json" ]; then
    log "INFO" "📈 Moving last ten epoch aggregate data file from leaderboard output"
    run_command "mv -v ${LEADERBOARD_JSON_DIR}/last_ten_epoch_aggregate_data.json /home/smilax/block-production/leaderboard/production/validator_rewards/static/json"
else
    log "INFO" "ℹ️ File not found: ${LEADERBOARD_JSON_DIR}/last_ten_epoch_aggregate_data.json"
fi

if ls ${LEADERBOARD_JSON_DIR}/ten_epoch_*.json 1> /dev/null 2>&1; then
    log "INFO" "📊 Found ten_epoch_*.json files to move from leaderboard output"
    run_command "mv -v ${LEADERBOARD_JSON_DIR}/ten_epoch_*.json /home/smilax/block-production/leaderboard/production/validator_rewards/static/json"
else
    log "INFO" "ℹ️ No '${LEADERBOARD_JSON_DIR}/ten_epoch_*.json' files found to move"
fi

if [ -e "${LEADERBOARD_JSON_DIR}/recency_weighted_average_validator_rewards.json" ]; then
    log "INFO" "⚖️ Moving recency weighted average validator rewards file from leaderboard output"
    run_command "mv -v ${LEADERBOARD_JSON_DIR}/recency_weighted_average_validator_rewards.json /home/smilax/block-production/leaderboard/production/validator_rewards/static/json"
else
    log "INFO" "ℹ️ File not found: ${LEADERBOARD_JSON_DIR}/recency_weighted_average_validator_rewards.json"
fi

# Also check the original data/json directory for any remaining files
if ls ${TRILLIUM_DATA_JSON}/epoch*.json 1> /dev/null 2>&1; then
    log "INFO" "📊 Found epoch*.json files in original location"
    run_command "mv -v ${TRILLIUM_DATA_JSON}/epoch*.json /home/smilax/block-production/leaderboard/production/validator_rewards/static/json"
fi

if [ -e "${TRILLIUM_DATA_JSON}/last_ten_epoch_aggregate_data.json" ]; then
    log "INFO" "📈 Moving last ten epoch aggregate data file from original location"
    run_command "mv -v ${TRILLIUM_DATA_JSON}/last_ten_epoch_aggregate_data.json /home/smilax/block-production/leaderboard/production/validator_rewards/static/json"
fi

if ls ${TRILLIUM_DATA_JSON}/ten_epoch_*.json 1> /dev/null 2>&1; then
    log "INFO" "📊 Found ten_epoch_*.json files in original location"
    run_command "mv -v ${TRILLIUM_DATA_JSON}/ten_epoch_*.json /home/smilax/block-production/leaderboard/production/validator_rewards/static/json"
fi

if [ -e "${TRILLIUM_DATA_JSON}/recency_weighted_average_validator_rewards.json" ]; then
    log "INFO" "⚖️ Moving recency weighted average validator rewards file from original location"
    run_command "mv -v ${TRILLIUM_DATA_JSON}/recency_weighted_average_validator_rewards.json /home/smilax/block-production/leaderboard/production/validator_rewards/static/json"
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
if bash /home/smilax/trillium_api/scripts/bash/cloudflare-purge-cache.sh; then
    log "INFO" "✅ Successfully purged Cloudflare cache"
else
    log "ERROR" "❌ Failed to purge Cloudflare cache"
fi

log "INFO" "🎉 JSON files move to production completed successfully"
