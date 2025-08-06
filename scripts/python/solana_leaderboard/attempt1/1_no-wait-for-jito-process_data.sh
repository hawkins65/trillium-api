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

# URL to check JSON data using Jito1 validator as the basis
log_message "INFO" "Checking to see if we already have Jito MEV"
URL="https://kobe.mainnet.jito.network/api/v1/validators/J1to1yufRnoWn81KYg1XkTWzmKjnYSnmE2VY8DGUJ9Qv"
response=$(curl -s "$URL")
first_object=$(echo "$response" | jq '.[0]')
epoch=$(echo "$first_object" | jq '.epoch')
mev_rewards=$(echo "$first_object" | jq '.mev_rewards')

if [[ "$epoch" -eq "$epoch_number" && "$mev_rewards" != "null" ]]; then
    log_message "INFO" "Already have Jito Staknet History CLI data for epoch $epoch with MEV = $mev_rewards"
    log_message "INFO" "No need to run $script_name Exiting -- already have Jito MEV"
    exit 0
fi

log_message "INFO" "NO Stakenet data YET ... Starting to process epoch $epoch_number with $script_name"

# Run each script and check for errors
execute_and_check "bash 2_update_validator_aggregate_info.sh $epoch_number"
execute_and_check "bash 3_build_leaderboard_json.sh $epoch_number"
# jrh not using this yet and files are HUGE
# execute_and_check "python3 93_solana_stakes_export.py"
execute_and_check "python3 93_skip_analysis.py $epoch_number"
execute_and_check "bash 4_move_json_to_production.sh"
execute_and_check "bash 5_cp_images.sh"
execute_and_check "bash 61_update_discord_channel_trillium-api-data.sh $epoch_number"
execute_and_check "bash 7_cleanup.sh"

log_message "INFO" "All steps completed successfully for epoch $epoch_number in $script_name"

# Create a flag file to indicate full processing occurred
flag_file="$HOME/log/1_no_wait_full_process_${epoch_number}.flag"

cat << EOF > "$flag_file"
This file ($flag_file) was created on $(date '+%Y-%m-%d %H:%M:%S') by $script_name.
It indicates that the script completed full processing for epoch $epoch_number because mev_rewards was null.
When this file exists, 1_wait-for-jito-process_data.sh will instruct 2_update_validator_aggregate_info.sh
to skip certain steps (e.g., vote latency, inflation rewards, APY calculations) to avoid redundant processing.
EOF

log_message "INFO" "Created flag file $flag_file to indicate full processing"

exit 0
