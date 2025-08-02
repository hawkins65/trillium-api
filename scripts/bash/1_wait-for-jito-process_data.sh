#!/bin/bash

# Enable strict mode for safer scripting
set -euo pipefail

# Source the common logging functions
source /home/smilax/api/999_common_log.sh
# Initialize enhanced logging
init_logging

# Check if epoch number is provided
if [ -z "${1:-}" ]; then
    log "ERROR" "❌ Epoch number not provided"
    echo "Error: Epoch number not provided."
    exit 1
fi

epoch_number="$1"
session_name="get_slots$epoch_number"
script_name=$(basename "$0")

log "INFO" "🚀 Starting wait-for-jito processing for epoch $epoch_number"
log "INFO" "📺 Using tmux session: $session_name"

# Function to execute a command and check for errors
execute_and_check() {
    local cmd="$1"
    log "INFO" "⚡ Executing: $cmd"
    
    if eval "$cmd"; then
        log "INFO" "✅ Successfully completed: $cmd"
        return 0
    else
        local exit_code=$?
        log "ERROR" "❌ Command failed with exit code $exit_code: $cmd"
        
        # Send error notification using centralized script
        bash 999_discord_notify.sh error "$script_name" "Command execution failed" "$cmd" "$exit_code" "$epoch_number"
        
        # Exit the script to avoid continuing on error
        exit $exit_code
    fi
}

log "INFO" "🔍 Checking for flag file from previous no-wait processing"

# Check for the flag file from 1_no-wait-for-jito-process_data.sh
flag_file="$HOME/log/1_no_wait_full_process_${epoch_number}.flag"
skip_flag=""
if [ -f "$flag_file" ]; then
    log "INFO" "🏷️ Flag file found: $flag_file"
    log "INFO" "⚡ Setting skip flag for reduced processing mode"
    skip_flag="--skip-previous"
else
    log "INFO" "📋 No flag file found - will run full processing"
fi

# Run scripts with the skip flag if applicable
log "INFO" "⏳ Waiting for Jito Kobe epoch data..."
execute_and_check "bash 90_wait-for-jito-kobe-epoch-data.sh $epoch_number"

processing_mode="Full processing"
if [ -n "$skip_flag" ]; then
    processing_mode="Reduced processing (--skip-previous)"
fi
log "INFO" "🔄 Starting validator aggregate info update ($processing_mode)..."
execute_and_check "bash 2_update_validator_aggregate_info.sh $epoch_number $skip_flag"

log "INFO" "📊 Collecting Jito steward data..."
execute_and_check "python3 92-jito-steward-data-collection.py $epoch_number"

log "INFO" "🏗️ Building leaderboard JSON..."
execute_and_check "bash 3_build_leaderboard_json.sh $epoch_number"

# jrh not using this yet and files are HUGE
# execute_and_check "python3 93_solana_stakes_export.py"

log "INFO" "📈 Running skip analysis..."
execute_and_check "python3 93_skip_analysis.py $epoch_number"

log "INFO" "📁 Moving JSON files to production..."
execute_and_check "bash 4_move_json_to_production.sh"

log "INFO" "🖼️ Copying images..."
execute_and_check "bash 5_cp_images.sh"

log "INFO" "📢 Sending completion notification..."
execute_and_check "bash 999_public_discord_notify.sh complete $epoch_number"

log "INFO" "🧹 Running cleanup operations..."
execute_and_check "bash 7_cleanup.sh"

log "INFO" "💾 Copying archive to Google Drive..."
execute_and_check "bash 999_copy_tar_gdrive_disk3.sh $epoch_number"

log "INFO" "✅ All steps completed successfully for epoch $epoch_number"

# Send success notification using centralized script
components_processed="   • Jito Kobe epoch data retrieval
   • Validator aggregate info updates
   • Jito steward data collection
   • Leaderboard JSON building
   • Skip analysis processing
   • JSON files moved to production
   • Images copied
   • Discord channel updates
   • Cleanup operations
   • Archive copying to Google Drive"

bash 999_discord_notify.sh success "$script_name" "$epoch_number" "Jito Wait Processing Completed Successfully" "$components_processed"
cleanup_logging

# Kill the current tmux session
log "INFO" "🔌 Terminating tmux session: $session_name"
if tmux kill-session -t "$session_name" 2>/dev/null; then
    log "INFO" "✅ Successfully killed tmux session: $session_name"
else
    log "WARN" "⚠️ Failed to kill tmux session or session didn't exist: $session_name"
fi

log "INFO" "🎉 Wait-for-jito processing completed successfully for epoch $epoch_number"

exit 0
