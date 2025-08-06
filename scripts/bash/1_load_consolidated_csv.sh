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

log "INFO" "🚀 Starting consolidated CSV loading process"

# Check if an epoch number is provided as a parameter
if [ -n "$1" ]; then
    epoch_number="$1"
    log "INFO" "📊 Using epoch number from parameter: $epoch_number"
else
    read -p "Enter the epoch number: " epoch_number
    log "INFO" "📊 Using epoch number from user input: $epoch_number"
fi

log "INFO" "🔄 Executing Python script to load consolidated CSV for epoch $epoch_number"

# Execute the Python script with the epoch number
if echo $epoch_number | python3 ../python/91_load_consolidated_csv.py; then
    log "INFO" "✅ Successfully completed consolidated CSV loading for epoch $epoch_number"
else
    exit_code=$?
    log "ERROR" "❌ Failed to load consolidated CSV for epoch $epoch_number (exit code: $exit_code)"
    
    # Send error notification using centralized script
    bash "$DISCORD_NOTIFY_SCRIPT" error "$script_name" "Consolidated CSV loading" "echo $epoch_number | python3 ../python/91_load_consolidated_csv.py" "$exit_code" "$epoch_number"
    
    exit $exit_code
fi

log "INFO" "🎉 Consolidated CSV loading process completed successfully for epoch $epoch_number"
