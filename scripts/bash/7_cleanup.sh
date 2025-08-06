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

log "INFO" "🚀 Starting cleanup operations"

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

log "INFO" "🗂️ Removing old log files (older than 6 hours) from various directories"

# Remove log files older than 6 hours with logging and error checking
directories=(
    "~/trillium_api"
    "~/trillium_api/major_minor_version"
    "~/trillium_api/get_epoch_stake_account_details"
    "~/trillium_api/stake-percentage"
    "~/trillium_api/geolite2"
)

for dir in "${directories[@]}"; do
    # Handle .log files for most directories, .csv for geolite2
    if [ "$dir" = "~/trillium_api/geolite2" ]; then
        pattern="*.csv"
        file_type="CSV files"
    else
        pattern="*.log"
        file_type="log files"
    fi

    log "INFO" "🔍 Checking for old $file_type in directory: $dir"

    if find "$dir" -maxdepth 1 -type f -name "$pattern" -mmin +360 2> /dev/null | grep -q .; then
        log "INFO" "🗑️ Found old $file_type to remove in $dir"
        run_command "find $dir -maxdepth 1 -type f -name \"$pattern\" -mmin +360 -ls -exec rm -v {} \;"
    else
        log "INFO" "ℹ️ No files older than 6 hours matching pattern: $dir/$pattern"
    fi
done

log "INFO" "🧹 Removing various temporary and processing files"

# Remove files with logging and error checking
file_patterns=(
    "validator_history_data_*.csv"
    "slot_duration_*.png"
    "delete_*.sql"
    "recreate_validator_stats_to_inspect.sql"
    "update_epoch_aggregate_data.sql"
    "update_validator_stats.sql"
    "script_log_2024-*.log"
    "validator-info.json"
    "epoch_*_processing.log"
    "epoch*.csv"
    "vote*.txt"
    "92_validator_history_data_*.csv"
    "jito-steward-state-all-validators-epoch*.txt"
    "epoch*._validator_counts_charts-jito.html"
)

for pattern in "${file_patterns[@]}"; do
    log "INFO" "🔍 Checking for files matching pattern: $pattern"
    
    if ls $pattern 1> /dev/null 2>&1; then
        log "INFO" "🗑️ Found files matching $pattern - removing them"
        run_command "rm -v $pattern"
    else
        log "INFO" "ℹ️ No files matching pattern: $pattern"
    fi
done

log "INFO" "🎉 Cleanup operations completed successfully"
