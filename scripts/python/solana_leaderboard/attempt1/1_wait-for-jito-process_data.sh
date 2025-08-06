#!/bin/bash

source 999_common_log.sh
# Initialize enhanced logging
init_logging

# Enable strict mode for safer scripting
set -euo pipefail

# Check if epoch number is provided
if [ -z "${1:-}" ]; then
    log_message "ERROR" "Epoch number not provided."
    exit 1
fi

epoch_number="$1"
session_name="get_slots$epoch_number"
script_name=$(basename "$0")

# Function to execute a command and check for errors
execute_and_check() {
    local cmd="$1"
    log_message "INFO" "Executing: $cmd"
    eval "$cmd"
    local exit_code=$?

    if [[ $exit_code -ne 0 ]]; then
        log_message "ERROR" "Command failed with exit code $exit_code: $cmd"
        # Exit the script to avoid continuing on error
        exit $exit_code
    fi

    return $exit_code
}

log_message "INFO" "In $script_name Starting tasks in tmux session '$session_name' for epoch $epoch_number"

# Check for the flag file from 1_no-wait-for-jito-process_data.sh
flag_file="$HOME/log/1_no_wait_full_process_${epoch_number}.flag"
skip_flag=""
if [ -f "$flag_file" ]; then
    log_message "INFO" "Flag file $flag_file found, setting skip flag for reduced processing"
    skip_flag="--skip-previous"
fi

# Run scripts with the skip flag if applicable
execute_and_check "bash 90_wait-for-jito-kobe-epoch-data.sh $epoch_number"
execute_and_check "bash 2_update_validator_aggregate_info.sh $epoch_number $skip_flag"
execute_and_check "python3 92-jito-steward-data-collection.py $epoch_number"
execute_and_check "bash 3_build_leaderboard_json.sh $epoch_number"
# jrh not using this yet and files are HUGE
# execute_and_check "python3 93_solana_stakes_export.py"
execute_and_check "python3 93_skip_analysis.py $epoch_number"
execute_and_check "bash 4_move_json_to_production.sh"
execute_and_check "bash 5_cp_images.sh"
execute_and_check "bash 6_update_discord_channel_trillium-api-data.sh $epoch_number"
execute_and_check "bash 7_cleanup.sh"

execute_and_check "bash copy_tar_gdrive_disk3.sh $epoch_number"

log_message "INFO" "in $script_name All steps completed successfully for epoch $epoch_number"

# Kill the current tmux session
log_message "INFO" "In $script_name Killing tmux session '$session_name'"
tmux kill-session -t "$session_name"

exit 0
