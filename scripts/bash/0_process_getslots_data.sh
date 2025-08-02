#!/bin/bash

# Enable strict mode for safer scripting
set -euo pipefail

# Source the common logging functions
source /home/smilax/api/999_common_log.sh
# Initialize enhanced logging
init_logging

# Get the basename of the script (without the extension)
script_name=$(basename "$0")
log_file="$HOME/log/${script_name%.*}.log"

# Ensure the ~/api directory exists
mkdir -p "$HOME/api"

# Enhanced function to execute a command and check for errors with Discord alerts
execute_and_check() {
    local cmd="$1"
    local description="${2:-$cmd}"
    local emoji="${3:-⚙️}"
    
    log "INFO" "$emoji Executing: $description"
    
    if eval "$cmd"; then
        local exit_code=$?
        log "INFO" "✅ Successfully completed: $description"
        return $exit_code
    else
        local exit_code=$?
        local error_msg="❌ Command failed with exit code $exit_code: $description"
        log "ERROR" "$error_msg"
        
        # Send Discord alert using centralized script
        bash 999_discord_notify.sh error "$script_name" "$description" "$cmd" "$exit_code" "$epoch_number"
        
        echo "An error occurred. Press Y or Enter to continue, or anything else to exit."
        read -r choice
        if [[ "$choice" != "Y" && "$choice" != "y" && -n "$choice" ]]; then
            log "ERROR" "❌ Exiting script due to user choice after error"
            bash 999_discord_notify.sh custom "$script_name" "Script Terminated" "Script terminated by user after error in: $description\nEpoch: $epoch_number\nUser chose to exit rather than continue" "🛑"
            exit 1
        else
            log "INFO" "⚠️ Continuing script execution despite error (user choice)"
        fi
        return $exit_code
    fi
}

# Script start
log "INFO" "🚀 Starting get slots data processing"

# Check if an epoch number is provided as a parameter
if [ -n "${1:-}" ]; then
    epoch_number="$1"
    log "INFO" "📊 Using epoch number from parameter: $epoch_number"
else
    read -p "Enter the epoch number: " epoch_number
    log "INFO" "📊 Using epoch number from user input: $epoch_number"
fi

log "INFO" "🔄 Starting to process epoch $epoch_number with $script_name"

# Run each script and check for errors with enhanced logging
execute_and_check "bash 90_xshin_load_data.sh $epoch_number" "Xshin data loading" "📊"

execute_and_check "python3 90_stakewiz_validators.py" "Stakewiz validators processing" "👥"

execute_and_check "bash 90_get_block_data.sh $epoch_number" "Block data retrieval" "🧱"

execute_and_check "bash 999_public_discord_notify.sh collection_ended $epoch_number" "Public Discord channel update (collection ended)" "💬"

execute_and_check "bash 90_untar_epoch.sh $epoch_number" "Epoch data extraction" "📦"

execute_and_check "bash 1_load_consolidated_csv.sh $epoch_number" "Consolidated CSV loading" "📈"

# Run the no-wait Jito processing
log "INFO" "🚀 Running no-wait Jito processing for epoch $epoch_number"
if bash 1_no-wait-for-jito-process_data.sh $epoch_number; then
    log "INFO" "✅ Successfully completed no-wait Jito processing"
else
    exit_code=$?
    log "ERROR" "❌ Failed no-wait Jito processing (exit code: $exit_code)"
    bash 999_discord_notify.sh error "$script_name" "No-wait Jito processing" "bash 1_no-wait-for-jito-process_data.sh $epoch_number" "$exit_code" "$epoch_number"
    exit $exit_code
fi

# Start the wait-for-jito processing in a tmux session
tmux_session="jito_process_${epoch_number}"
log "INFO" "🖥️ Starting Jito wait processing in tmux session: $tmux_session"
if tmux new-session -d -s "$tmux_session" "bash 1_wait-for-jito-process_data.sh $epoch_number"; then
    log "INFO" "✅ Successfully started tmux session: $tmux_session"
else
    exit_code=$?
    log "ERROR" "❌ Failed to start tmux session (exit code: $exit_code)"
    bash 999_discord_notify.sh error "$script_name" "Tmux session creation" "tmux new-session -d -s \"$tmux_session\" \"bash 1_wait-for-jito-process_data.sh $epoch_number\"" "$exit_code" "$epoch_number"
    exit $exit_code
fi

# Copy the epoch file to the API server
FILE="epoch${epoch_number}.tar.zst"
destination="/home/smilax/epochs/"
log "INFO" "📁 Copying $FILE to $destination"
if cp "$FILE" "$destination"; then
    log "INFO" "✅ Successfully copied $FILE to API server"
else
    exit_code=$?
    log "ERROR" "❌ Failed to copy $FILE to API server (exit code: $exit_code)"
    bash 999_discord_notify.sh error "$script_name" "File copy to API server" "cp \"$FILE\" \"$destination\"" "$exit_code" "$epoch_number" "File: $FILE"
    exit $exit_code
fi

log "INFO" "🎉 Get slots data processing completed successfully for epoch $epoch_number"

# Send success notification to Discord using centralized script
components_processed="   • Xshin data loading
   • Stakewiz validators processing
   • Block data retrieval
   • Discord channel updates
   • Epoch data extraction
   • Consolidated CSV loading
   • No-wait Jito processing
   • Tmux session for Jito wait processing
   • Epoch archive copying to API server"

additional_notes="Jito wait processing continues in tmux session: *jito_process_${epoch_number}*"

bash 999_discord_notify.sh success "$script_name" "$epoch_number" "Process Get Slots Data Completed Successfully" "$components_processed" "$additional_notes"
cleanup_logging

exit 0
