#!/bin/bash

# Enable strict mode for safer scripting
set -euo pipefail

# Source the common logging functions
source /home/smilax/api/999_common_log.sh
# Initialize enhanced logging
init_logging

# Get the basename of the script (without the extension)
script_name=$(basename "$0")

log "INFO" "🚀 Starting no-wait Jito processing for data pipeline"

# Ensure the ~/api directory exists
mkdir -p "$HOME/api"

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
        
        echo "An error occurred. Press Y or Enter to continue, or anything else to exit."
        read -r choice
        if [[ "$choice" != "Y" && "$choice" != "y" && -n "$choice" ]]; then
            log "INFO" "🛑 Script terminated by user choice after error"
            bash 999_discord_notify.sh custom "$script_name" "Script Terminated" "Script terminated by user after error in command: $cmd\nEpoch: $epoch_number\nUser chose to exit rather than continue" "🛑"
            exit 1
        fi
        log "INFO" "⚠️ User chose to continue after error"
        return $exit_code
    fi
}

# Check if an epoch number is provided as a parameter
if [ -n "${1:-}" ]; then
    epoch_number="$1"
    log "INFO" "📊 Using epoch number from parameter: $epoch_number"
else
    read -p "Enter the epoch number: " epoch_number
    log "INFO" "📊 Using epoch number from user input: $epoch_number"
fi

log "INFO" "🔍 Checking for existing Jito MEV data for epoch $epoch_number"

# URL to check JSON data using Jito1 validator as the basis
URL="https://kobe.mainnet.jito.network/api/v1/validators/J1to1yufRnoWn81KYg1XkTWzmKjnYSnmE2VY8DGUJ9Qv"
log "INFO" "📡 Fetching data from: $URL"

response=$(curl -s "$URL")
first_object=$(echo "$response" | jq '.[0]')
epoch=$(echo "$first_object" | jq '.epoch')
mev_rewards=$(echo "$first_object" | jq '.mev_rewards')

log "INFO" "🔍 Found epoch: $epoch, MEV rewards: $mev_rewards"

if [[ "$epoch" -eq "$epoch_number" && "$mev_rewards" != "null" ]]; then
    log "INFO" "✅ Already have Jito Staknet History CLI data for epoch $epoch with MEV = $mev_rewards"
    log "INFO" "⏭️ No need to run processing - data already available, exiting"
    
    # Send notification that processing was skipped
    bash 999_discord_notify.sh custom "$script_name" "Processing Skipped" "Already have Jito Staknet History CLI data for epoch $epoch_number with MEV = $mev_rewards\nNo processing needed - data already available" "⏭️"
    exit 0
fi

log "INFO" "⚠️ No Stakenet data yet - starting to process epoch $epoch_number"

# Run each script and check for errors
log "INFO" "🔄 Starting validator aggregate info update..."
execute_and_check "bash 2_update_validator_aggregate_info.sh $epoch_number"

log "INFO" "🏗️ Building leaderboard JSON..."
execute_and_check "bash 3_build_leaderboard_json.sh $epoch_number"

# jrh not using this yet and files are HUGE
# execute_and_check "python3 93_solana_stakes_export.py"

log "INFO" "📊 Running skip analysis..."
execute_and_check "python3 93_skip_analysis.py $epoch_number"

log "INFO" "📁 Moving JSON files to production..."
execute_and_check "bash 4_move_json_to_production.sh"

log "INFO" "🖼️ Copying images..."
execute_and_check "bash 5_cp_images.sh"

log "INFO" "📢 Sending 'mostly updated' notification..."
execute_and_check "bash 999_public_discord_notify.sh mostly_updated $epoch_number"

log "INFO" "🧹 Running cleanup operations..."
execute_and_check "bash 7_cleanup.sh"

log "INFO" "✅ All steps completed successfully for epoch $epoch_number"

# Send success notification using centralized script
components_processed="   • Validator aggregate info updates
   • Leaderboard JSON building
   • Skip analysis processing
   • JSON files moved to production
   • Images copied
   • Discord channel updates
   • Cleanup operations"

additional_notes="Full processing completed because mev_rewards was null. Flag file created for wait script to skip redundant processing."

bash 999_discord_notify.sh success "$script_name" "$epoch_number" "No-Wait Jito Processing Completed Successfully" "$components_processed" "$additional_notes"
cleanup_logging

# Create a flag file to indicate full processing occurred
flag_file="$HOME/log/1_no_wait_full_process_${epoch_number}.flag"

log "INFO" "🏷️ Creating flag file: $flag_file"

cat << EOF > "$flag_file"
This file ($flag_file) was created on $(date '+%Y-%m-%d %H:%M:%S') by $script_name.
It indicates that the script completed full processing for epoch $epoch_number because mev_rewards was null.
When this file exists, 1_wait-for-jito-process_data.sh will instruct 2_update_validator_aggregate_info.sh
to skip certain steps (e.g., vote latency, inflation rewards, APY calculations) to avoid redundant processing.
EOF

log "INFO" "✅ Created flag file to indicate full processing completed"

log "INFO" "🎉 No-wait Jito processing completed successfully for epoch $epoch_number"

exit 0
