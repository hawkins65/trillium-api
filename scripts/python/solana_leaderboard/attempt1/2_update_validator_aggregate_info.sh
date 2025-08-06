#!/bin/bash

source 999_common_log.sh
# Initialize enhanced logging
init_logging

# Check if an epoch number is provided as the first parameter
if [ -n "$1" ]; then
    epoch_number="$1"
else
    read -p "Enter the epoch number: " epoch_number
fi

# Create a persistent response file with the format 2_response_file.$epoch_number
response_file="2_response_file.$epoch_number"

# Check for the --skip-previous flag passed as the second parameter
# The --skip-previous flag indicates that 1_no-wait-for-jito-process_data.sh completed full processing
# for this epoch (i.e., mev_rewards was null), meaning certain steps like vote latency, inflation rewards,
# APY calculations, and block time updates were already handled. When this flag is present, we skip
# those redundant steps to avoid duplicating work and reduce processing time.
skip_previous=false
if [ "${2:-}" = "--skip-previous" ]; then
    skip_previous=true
fi

if [ "$skip_previous" = false ]; then
    # Full processing mode: No prior full processing detected from 1_no-wait-for-jito-process_data.sh
    # We run all steps, including vote latency, inflation rewards, and related calculations.
    
    # Load the vote latency table for this epoch
    python3 92_vx-call.py $epoch_number

    # Write responses to the temporary file for full processing
    # These responses configure 92_update_validator_aggregate_info.py to retrieve all data
    {
        # Retrieve icons (y = yes)
        printf "y\n"
        # ONLY process icons (n = no)
        printf "n\n"
        # Starting epoch number
        printf "%s\n" "$epoch_number"
        # Ending epoch number
        printf "%s\n" "$epoch_number"
        # Retrieve aggregate info (y = yes)
        printf "y\n"
        # Retrieve stakenet info (y = yes)
        printf "y\n"
        # Process leader schedule info (y = yes)
        printf "y\n"
    } > "$response_file"
else
    # Reduced processing mode: --skip-previous flag is present, meaning 1_no-wait-for-jito-process_data.sh
    # already processed vote latency, inflation rewards, APY, and block time data for this epoch.
    # We skip those steps here and configure 92_update_validator_aggregate_info.py to retrieve only
    # the remaining necessary data, avoiding redundant processing.

    # Write responses to the temporary file for reduced processing
    # These responses limit the scope of 92_update_validator_aggregate_info.py
    {
        # Retrieve icons (n = no, already processed)
        printf "n\n"
        # Starting epoch number
        printf "%s\n" "$epoch_number"
        # Ending epoch number
        printf "%s\n" "$epoch_number"
        # Retrieve aggregate info (y = yes, still needed)
        printf "y\n"
        # Retrieve stakenet info (y = yes, still needed)
        printf "y\n"
        # Process leader schedule info (n = no, already processed)
        # Process leader schedule info (y = yes, let's duplicate due to some issues 2025-05-04 jrh)
        printf "y\n"
    } > "$response_file"
fi

# Pipe the responses to the Python script to update validator aggregate info
# The script uses these inputs to determine what data to fetch and process
cat "$response_file" | python3 92_update_validator_aggregate_info.py

# If needed, Send Discord message that we are continuing with Stakenet data instead of Kobe data
# Define the file pattern
FILE_PATTERN="/home/smilax/log/92_update_validator_aggregate_info_log_*"
# Find the latest file matching the pattern
LATEST_FILE=$(ls -t $FILE_PATTERN | head -n 1)
# Check if a file was found
if [ -z "$LATEST_FILE" ]; then
    log_message "ERROR" "No files found matching the pattern: $FILE_PATTERN"
    exit 1
fi
# Search for the error message in the latest file
if grep -q "ERROR - Using fetch_validator_history data for epoch" "$LATEST_FILE"; then
    log_message "INFO" "Error message found in $LATEST_FILE. Running 92_missing_kobe_api_data.sh..."
    bash 92_missing_kobe_api_data.sh "$epoch_number"
else
    log_message "INFO" "Kobe data was used for Epoch $epoch_number"
fi

# Update epoch aggregate data with vote latency information
bash 92_vote_latency_update_ead.sh $epoch_number

# Calculate the average slot time for this epoch
python3 92_block_time_calculation.py $epoch_number

# Get inflation rewards for validators and epoch aggregate data
python3 92_update_vs_inflation_reward.py $epoch_number
python3 92_update_ead_inflation_reward.py $epoch_number
# Calculate APY values for this epoch
python3 92_calculate_apy.py $epoch_number

# let's run the very efficient program to get geo ip info
python3 92_ip_api.py $epoch_number

# Run post-processing SQL updates to finalize the data
bash 92_run_sql_updates.sh

# Note: python3 92-jito-steward-data-collection.py is intentionally omitted here
# It runs exclusively in 1_wait-for-jito-process_data.sh to ensure Jito steward data is collected
# only once, after waiting for Jito-related dependencies.

