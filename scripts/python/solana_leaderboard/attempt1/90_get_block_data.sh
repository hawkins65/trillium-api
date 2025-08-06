#!/bin/bash

source 999_common_log.sh
# Initialize enhanced logging
init_logging

# Parse command line arguments
if [[ "$#" -ne 1 ]]; then
    log_message "ERROR" "Usage: $0 <epoch-number>"
    exit 1
fi

epoch_number="$1"

# You can now use $epoch_number as needed
log_message "INFO" "Epoch number: $epoch_number"

# Save the current directory so we can come back at the end
ORIGINAL_DIR=$(pwd)

# Switch to the directory to save slot data
target_dir="/home/smilax/block-production/get_slots/epoch$epoch_number"
log_message "INFO" "Switching to $target_dir"

if ! cd "$target_dir"; then
    log_message "ERROR" "Failed to change directory to $target_dir"
    exit 1
fi

# Function to execute a command and check for errors
execute_and_check() {
    local cmd="$1"
    log_message "INFO" "Executing: $cmd"
    eval "$cmd"
    local exit_code=$?

    if [[ $exit_code -ne 0 ]]; then
        log_message "ERROR" "Command failed with exit code $exit_code: $cmd"
        if [[ $cmd == *"get_epoch_data_csv.py"* && $exit_code -eq 99 ]]; then
            log_message "WARNING" "Warning: get_epoch_data_csv.py returned error code 99. Continuing..."
            return 0
        else
            log_message "INFO" "An error occurred. Press Y or Enter to continue, or anything else to exit."
            read -r choice
            if [[ "$choice" != "Y" && "$choice" != "y" && -n "$choice" ]]; then
                log_message "INFO" "Exiting script due to user choice."
                exit 1
            fi
        fi
    fi
    return $exit_code
}

# Start the collection of block data with epoch number and number of concurrent threads
execute_and_check "bash ./rpc_get_block_data.sh $epoch_number"

# Process tar files
cd /home/smilax/block-production/get_slots || { log_message "ERROR" "Failed to change to parent directory"; exit 1; }
execute_and_check "bash tar_files.sh $epoch_number"
execute_and_check "bash copy_tar.sh $epoch_number"

# Return to the original directory
cd "$ORIGINAL_DIR" || {
    log_message "ERROR" "Failed to return to the original directory $ORIGINAL_DIR"
    exit 1
}

log_message "INFO" "Script completed successfully."
