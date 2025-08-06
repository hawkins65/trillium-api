#!/bin/bash

source 999_common_log.sh
# Initialize enhanced logging
init_logging

VERBOSE=1
LOGFILE="$HOME/log/$(basename "$0" .sh).log"

# Function to handle errors
handle_error() {
    local command="$1"
    log_message "ERROR" "Command failed: $command"
    exit 1
}

# Function to run a command with logging and error handling
run_command() {
    local command="$1"
    if [ "$VERBOSE" -eq 1 ]; then
        log_message "INFO" "Running: $command"
    fi
    eval "$command" || handle_error "$command"
}

log_message "INFO" "Script started."

# Remove log files older than 6 hours with logging and error checking
for dir in \
    "~/api" \
    "~/api/major_minor_version" \
    "~/api/get_epoch_stake_account_details" \
    "~/api/stake-percentage" \
    "~/api/geolite2"; do

    # Handle .log files for most directories, .csv for geolite2
    if [ "$dir" = "~/api/geolite2" ]; then
        pattern="*.csv"
    else
        pattern="*.log"
    fi

    if find "$dir" -maxdepth 1 -type f -name "$pattern" -mmin +360 2> /dev/null | grep -q .; then
        run_command "find $dir -maxdepth 1 -type f -name \"$pattern\" -mmin +360 -ls -exec rm -v {} \;"
    else
        log_message "INFO" "No files older than 6 hours matching pattern: $dir/$pattern"
    fi
done

# Remove files with logging and error checking
for pattern in \
    "validator_history_data_*.csv" \
    "slot_duration_*.png" \
    "delete_*.sql" \
    "recreate_validator_stats_to_inspect.sql" \
    "update_epoch_aggregate_data.sql" \
    "update_validator_stats.sql" \
    "script_log_2024-*.log" \
    "validator-info.json" \
    "epoch_*_processing.log" \
    "epoch*.csv" \
    "vote*.txt" \
    "92_validator_history_data_*.csv" \
    "jito-steward-state-all-validators-epoch*.txt" \
    "epoch*._validator_counts_charts-jito.html"; do

    if ls $pattern 1> /dev/null 2>&1; then
        run_command "rm -v $pattern"
    else
        log_message "INFO" "No files matching pattern: $pattern"
    fi
done

log_message "INFO" "Script completed successfully."
