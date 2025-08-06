#!/bin/bash
source 999_common_log.sh
# Initialize enhanced logging
init_logging
set -euo pipefail  # Using stricter mode like the second script

# Get the basename of the script (without the extension)
script_name=$(basename "$0")

# Ensure the ~/api directory exists
mkdir -p "$HOME/api"

epoch_number=$1

# If no parameter passed, prompt the user for an epoch number
if [ -z "$epoch_number" ]; then
    read -p "Please enter an epoch number: " epoch_number
fi

log_message "INFO" "Starting script $script_name with initial epoch $epoch_number"

while true; do
    log_message "INFO" "Processing epoch number: $epoch_number"

    # Create the directory for the current epoch
    log_message "INFO" "Creating directory for epoch $epoch_number"
    mkdir -p "/home/smilax/block-production/get_slots/epoch$epoch_number"

    # Copy necessary files into the epoch directory
    log_message "INFO" "Copying necessary files to epoch$epoch_number directory"
    cp "/home/smilax/block-production/get_slots/get_epoch_data_csv.py" "/home/smilax/block-production/get_slots/epoch$epoch_number"
    cp "/home/smilax/block-production/get_slots/slot_data.py" "/home/smilax/block-production/get_slots/epoch$epoch_number"
    cp "/home/smilax/block-production/get_slots/vote_data.py" "/home/smilax/block-production/get_slots/epoch$epoch_number"
    cp "/home/smilax/block-production/get_slots/vote_latency.py" "/home/smilax/block-production/get_slots/epoch$epoch$epoch_number"
    cp "/home/smilax/block-production/get_slots/get_shin_voting.sh" "/home/smilax/block-production/get_slots/epoch$epoch_number"
    cp "/home/smilax/block-production/get_slots/rpc_get_block_data.sh" "/home/smilax/block-production/get_slots/epoch$epoch_number"
    cp "/home/smilax/block-production/get_slots/cleanup_rundir.sh" "/home/smilax/block-production/get_slots/epoch$epoch_number"

    # Execute the processing script with the current epoch number
    log_message "INFO" "Executing processing script for epoch $epoch_number"
    bash "/home/smilax/block-production/api/0_process_getslots_data.sh" "$epoch_number"

    # Remove the epoch directory after processing
    # jrh -- need more error checking 
    #log_message "INFO" "Removing epoch$epoch_number directory"
    #rm -r "/home/smilax/block-production/get_slots/epoch$epoch_number"

    # Increment the epoch number for the next iteration
    epoch_number=$((epoch_number + 1))
    log_message "INFO" "Moving to next epoch: $epoch_number"
done