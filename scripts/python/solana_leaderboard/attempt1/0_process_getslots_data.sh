#!/bin/bash

source 999_common_log.sh
# Initialize enhanced logging
init_logging

# Enable strict mode for safer scripting
set -euo pipefail

# Get the basename of the script (without the extension)
script_name=$(basename "$0")

# Ensure the ~/api directory exists
mkdir -p "$HOME/api"

# Function to execute a command and check for errors
execute_and_check() {
    local cmd="$1"
    log_message "INFO" "Executing: $cmd"
    eval "$cmd"
    local exit_code=$?

    if [[ $exit_code -ne 0 ]]; then
        log_message "ERROR" "Command failed with exit code $exit_code: $cmd"
        log_message "INFO" "An error occurred. Press Y or Enter to continue, or anything else to exit."
        read -r choice
        if [[ "$choice" != "Y" && "$choice" != "y" && -n "$choice" ]]; then
            log_message "INFO" "Exiting script due to user choice."
            exit 1
        fi
    fi

    return $exit_code
}

# Check if an epoch number is provided as a parameter
if [ -n "${1:-}" ]; then
    epoch_number="$1"
else
    read -p "Enter the epoch number: " epoch_number
fi

log_message "INFO" "Starting to process epoch $epoch_number with $script_name"

# Run each script and check for errors
execute_and_check "bash 90_xshin_load_data.sh $epoch_number"
execute_and_check "python3 90_stakewiz_validators.py"
execute_and_check "bash 90_get_block_data.sh $epoch_number"
execute_and_check "bash 90_update_discord_channel_trillium-api-data.sh $epoch_number"
execute_and_check "bash 90_untar_epoch.sh $epoch_number"
execute_and_check "bash 1_load_consolidated_csv.sh $epoch_number"

log_message "INFO" "running 1_no-wait-for-jito-process_data.sh for $epoch_number"
bash 1_no-wait-for-jito-process_data.sh $epoch_number
 
log_message "INFO" "running 1_wait-for-jito-process_data.sh for $epoch_number in a tmux session *jito_process_${epoch_number}*"
tmux new-session -d -s "jito_process_${epoch_number}" "bash 1_wait-for-jito-process_data.sh $epoch_number"


# Construct the base filename
FILE="epoch${epoch_number}.tar.zst"
log_message "INFO" "Copying $FILE to /mnt/disk3/apiserver/epochs/..."
cp "$FILE" /mnt/disk3/apiserver/epochs/

exit 0
