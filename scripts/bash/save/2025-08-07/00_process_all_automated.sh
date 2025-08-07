#!/bin/bash

# Source path initialization
source "$(dirname "$0")/000_init_paths.sh" || {
    echo "❌ Failed to source path initialization script" >&2
    exit 1
}
set -euo pipefail  # Using stricter mode like the second script

# Set LOG_BASE_DIR before sourcing logging functions
export LOG_BASE_DIR="$HOME/log"
export LOG_LEVEL="INFO"

# Source the common logging functions
source $TRILLIUM_SCRIPTS_BASH/999_common_log.sh
# Initialize enhanced logging
init_logging

# Get the basename of the script (without the extension)
script_name=$(basename "$0")
log_file="$HOME/log/get_slots/${script_name%.*}.log"

# Ensure the log directory exists
mkdir -p "$HOME/log/get_slots"

# Function to execute command with error handling
execute_with_logging() {
    local command=$1
    local description=$2
    local emoji=$3
    local epoch_number=$4
    
    log "INFO" "$emoji $description"
    
    if eval "$command"; then
        log "INFO" "✅ Successfully completed $description"
        return 0
    else
        local exit_code=$?
        local error_msg="❌ Failed to execute $description (exit code: $exit_code)"
        log "ERROR" "$error_msg"
        bash "$DISCORD_NOTIFY_SCRIPT" error "$script_name" "$description" "$command" "$exit_code" "$epoch_number" "Automated processing loop interrupted"
        exit $exit_code
    fi
}

# Get initial epoch number
epoch_number=$1

# If no parameter passed, prompt the user for an epoch number
if [ -z "$epoch_number" ]; then
    read -p "Please enter an epoch number: " epoch_number
    log "INFO" "📊 Using epoch number from user input: $epoch_number"
else
    log "INFO" "📊 Using epoch number from parameter: $epoch_number"
fi

log "INFO" "🚀 Starting automated processing script with initial epoch $epoch_number"

# Send startup notification to Discord
startup_details="🔄 **Starting automated epoch processing loop**
📊 **Initial Epoch:** $epoch_number
♾️ **Mode:** Continuous processing (infinite loop)
📁 **Working Directory:** $TRILLIUM_DATA_EPOCHS/

⚙️ **Process Flow:**
   1. Create epoch directory
   2. Copy required files
   3. Execute processing script
   4. Move to next epoch
   5. Repeat indefinitely"

bash "$DISCORD_NOTIFY_SCRIPT" startup "$script_name" "$epoch_number" "Process All Automated Script Started" "$startup_details"

while true; do
    log "INFO" "🔄 Processing epoch number: $epoch_number"

    # Create the directory for the current epoch
    epoch_dir="$TRILLIUM_DATA_EPOCHS/epoch$epoch_number"
    execute_with_logging "mkdir -p \"$epoch_dir\"" "Creating directory for epoch $epoch_number" "📁" "$epoch_number"

    # Copy necessary files into the epoch directory
    log "INFO" "📋 Copying necessary files to epoch$epoch_number directory"
    
    files_to_copy=(
        "get_epoch_data_csv.py"
        "slot_data.py"
        "vote_data.py"
        "vote_latency.py"
        "get_shin_voting.sh"
        "rpc_get_block_data.sh"
        "cleanup_rundir.sh"
    )
    
    for file in "${files_to_copy[@]}"; do
        source_file="/home/smilax/trillium_api/scripts/get_slots/$file"
        execute_with_logging "cp \"$source_file\" \"$epoch_dir\"" "Copying $file" "📄" "$epoch_number"
    done

    # Execute the processing script with the current epoch number
    execute_with_logging "bash \"/home/smilax/trillium_api/scripts/bash/0_process_getslots_data.sh\" \"$epoch_number\"" "Executing processing script for epoch $epoch_number" "⚙️" "$epoch_number"

    log "INFO" "🎉 Successfully completed processing for epoch $epoch_number"
    
    # Send epoch completion notification to Discord
    components_processed="   • Directory creation for epoch data
   • Required files copied to epoch directory
   • Epoch data processing executed
   • Moving to next epoch: $((epoch_number + 1))"

    additional_notes="Continuing automated processing loop..."

    bash "$DISCORD_NOTIFY_SCRIPT" success "$script_name" "$epoch_number" "Epoch Processing Completed Successfully" "$components_processed" "$additional_notes"
    cleanup_logging

    # Remove the epoch directory after processing (currently commented out for safety)
    # log "INFO" "🗑️ Removing epoch$epoch_number directory"
    # execute_with_logging "rm -r \"$epoch_dir\"" "Removing epoch directory" "🗑️" "$epoch_number"

    # Increment the epoch number for the next iteration
    epoch_number=$((epoch_number + 1))
    log "INFO" "➡️ Moving to next epoch: $epoch_number"
done
