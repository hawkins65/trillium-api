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

# activate python environment with a full path
. /home/smilax/.python_env/bin/activate

# Set PYTHONPATH to include the project's python scripts directory
export PYTHONPATH="${TRILLIUM_BASE}"

# Function to execute command with error handling
execute_with_logging() {
    local command=$1
    local description=$2
    local emoji=$3
    
    log "INFO" "$emoji Running $description: $command"
    
    if eval "$command"; then
        log "INFO" "✅ Successfully completed $description"
        return 0
    else
        local exit_code=$?
        local error_msg="❌ Failed to execute $description (exit code: $exit_code)"
        log "ERROR" "$error_msg"
        bash "$DISCORD_NOTIFY_SCRIPT" error "$script_name" "$description" "$command" "$exit_code" "$epoch_number"
        return $exit_code
    fi
}

# Script start
log "INFO" "🚀 Starting build leaderboard process"

# Create a temporary file for responses
response_file=$(mktemp)

# Ensure the temporary file is deleted after execution
trap 'rm -f "$response_file"' EXIT

# Get the basename of the script for Discord notifications
script_name=$(basename "$0")

# Check if an epoch number is provided as the parameter
if [ -n "$1" ]; then
    epoch_number="$1"
    log "INFO" "📊 Using epoch number from parameter: $epoch_number"
else
    read -p "Enter the epoch number: " epoch_number
    log "INFO" "📊 Using epoch number from user input: $epoch_number"
fi

log "INFO" "🔄 Processing epoch $epoch_number"

echo " "
execute_with_logging "python3 -m scripts.python.solana_leaderboard.build_leaderboard $epoch_number $epoch_number" "Solana leaderboard build" "🔨"

log "INFO" "🎉 Build leaderboard process completed successfully for epoch $epoch_number"

cleanup_logging
