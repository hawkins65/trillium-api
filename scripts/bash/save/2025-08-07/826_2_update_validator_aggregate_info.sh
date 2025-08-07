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

# Get the basename of the script for Discord notifications
script_name=$(basename "$0")

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
log "INFO" "🚀 Starting validator aggregate info update process"

# Check if an epoch number is provided as the first parameter
if [ -n "$1" ]; then
    epoch_number="$1"
    log "INFO" "📊 Using epoch number from parameter: $epoch_number"
else
    read -p "Enter the epoch number: " epoch_number
    log "INFO" "📊 Using epoch number from user input: $epoch_number"
fi

# Create a persistent response file with the format 2_response_file.<epoch_number>
response_file="2_response_file.$epoch_number"

# Check for the --skip-previous flag passed as the second parameter
skip_previous=false
if [ "${2:-}" = "--skip-previous" ]; then
    skip_previous=true
    log "INFO" "⚡ Skip previous mode enabled - reduced processing"
else
    log "INFO" "🔄 Full processing mode enabled"
fi

if [ "$skip_previous" = false ]; then
    # Full processing mode: No prior full processing detected from 1_no-wait-for-jito-process_data.sh
    log "INFO" "📈 Configuring for full processing mode"
    
    # Load the vote latency table for this epoch
    #execute_with_logging "python3 $TRILLIUM_SCRIPTS_PYTHON/92_vx-call.py $epoch_number" "vote latency table loading" "📊"

    # Write responses to the temporary file for full processing
    {
        # Retrieve icons (y = yes)
        printf "n\n"
        # Starting epoch number
        printf "%s\n" "$epoch_number"
        # Ending epoch number
        printf "%s\n" "$epoch_number"
        # Retrieve aggregate info (y = yes)
        printf "n\n"
        # Retrieve stakenet info (y = yes)
        printf "y\n"
        # Process leader schedule info (y = yes)
        printf "n\n"
    } > "$response_file"
    
    log "INFO" "📝 Created full processing response file"
else
    # Reduced processing mode
    log "INFO" "⚡ Configuring for reduced processing mode (--skip-previous)"

    # Write responses to the temporary file for reduced processing
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
        # Process leader schedule info (y = yes)
        printf "y\n"
    } > "$response_file"
    
    log "INFO" "📝 Created reduced processing response file"
fi

# Pipe the responses to the Python script to update validator aggregate info
log "INFO" "🔄 Starting validator aggregate info update"
if cat "$response_file" | python3 $TRILLIUM_SCRIPTS_PYTHON/92_update_validator_aggregate_info.py; then
    log "INFO" "✅ Successfully completed validator aggregate info update"
else
    exit_code=$?
    log "ERROR" "❌ Failed to update validator aggregate info (exit code: $exit_code)"
    bash "$DISCORD_NOTIFY_SCRIPT" error "$script_name" "Validator aggregate info update" "cat \"$response_file\" | python3 $TRILLIUM_SCRIPTS_PYTHON/92_update_validator_aggregate_info.py" "$exit_code" "$epoch_number"
    exit $exit_code
fi

# Check for Kobe API data availability
log "INFO" "🔍 Checking for Kobe API data availability"
FILE_PATTERN="/home/smilax/log/92_update_validator_aggregate_info_log_*"
LATEST_FILE=$(ls -t $FILE_PATTERN | head -n 1)

if [ -z "$LATEST_FILE" ]; then
    log "ERROR" "❌ No log files found matching pattern: $FILE_PATTERN"
    bash "$DISCORD_NOTIFY_SCRIPT" error "$script_name" "Log file check" "ls -t $FILE_PATTERN | head -n 1" "1" "$epoch_number" "No log files found matching pattern: $FILE_PATTERN"
    exit 1
fi

if grep -q "ERROR - Using fetch_validator_history data for epoch" "$LATEST_FILE"; then
    log "INFO" "⚠️ Kobe API data not available, using Stakenet data instead"
    execute_with_logging "bash 999_public_discord_notify.sh missing_kobe_data \"$epoch_number\"" "missing Kobe API data notification" "🔄"
else
    log "INFO" "✅ Kobe data was successfully used for Epoch $epoch_number"
fi

# Execute remaining steps with logging and error handling
#execute_with_logging "bash 92_vote_latency_update_ead.sh $epoch_number" "vote latency epoch aggregate update" "⏱️"

#execute_with_logging "python3 $TRILLIUM_SCRIPTS_PYTHON/92_block_time_calculation.py $epoch_number" "average slot time calculation" "⏰"

#execute_with_logging "python3 $TRILLIUM_SCRIPTS_PYTHON/92_update_vs_inflation_reward.py $epoch_number" "validator inflation rewards update" "💰"

#execute_with_logging "python3 $TRILLIUM_SCRIPTS_PYTHON/92_update_ead_inflation_reward.py $epoch_number" "epoch aggregate inflation rewards update" "💵"

#execute_with_logging "python3 $TRILLIUM_SCRIPTS_PYTHON/92_calculate_apy.py $epoch_number" "APY calculations" "📈"

#execute_with_logging "python3 $TRILLIUM_SCRIPTS_PYTHON/92_ip_api.py $epoch_number" "geographic IP information gathering" "🌍"

#execute_with_logging "bash 92_run_sql_updates.sh" "SQL post-processing updates" "🗄️"

#execute_with_logging "bash 92_slot_duration.sh $epoch_number" "slot duration analysis" "⏱️"

#execute_with_logging "python3 $TRILLIUM_SCRIPTS_PYTHON/92_solana_block_laggards.py $epoch_number" "slot duration laggards analysis" "⏱️"

log "INFO" "🎉 Validator aggregate info update process completed successfully for epoch $epoch_number"

# Send success notification to Discord using centralized script
processing_mode="Full processing"
if [ "$skip_previous" = true ]; then
    processing_mode="Reduced processing (--skip-previous)"
fi

components_processed="   • Vote latency data loading
   • Validator aggregate info updates
   • Stakenet info processing
   • Leader schedule processing
   • Vote latency epoch aggregate updates
   • Block time calculations
   • Inflation rewards processing
   • APY calculations
   • Geographic IP information
   • SQL post-processing updates
   • Slot duration analysis"

additional_notes="Processing Mode: $processing_mode"

bash "$DISCORD_NOTIFY_SCRIPT" success "$script_name" "$epoch_number" "Validator Aggregate Info Update Completed Successfully" "$components_processed" "$additional_notes"
cleanup_logging
