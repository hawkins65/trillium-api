#!/bin/bash

# Source the common logging functions
source /home/smilax/api/999_common_log.sh
# Initialize enhanced logging
init_logging

# Get the basename of the script for Discord notifications
script_name=$(basename "$0")

log "INFO" "🚀 Starting Xshin data loading process"

# Prompt for epoch if not provided
if [ -z "$1" ]; then
    read -p "Enter epoch number: " epoch
    log "INFO" "📊 Using epoch number from user input: $epoch"
else
    epoch=$1
    log "INFO" "📊 Using epoch number from parameter: $epoch"
fi

log "INFO" "📡 Running node script to fetch all validators data"

# Run node script and rename output
if node 90_xshin.js all all; then
    log "INFO" "✅ Successfully fetched all validators data"
else
    exit_code=$?
    log "ERROR" "❌ Failed to fetch all validators data (exit code: $exit_code)"
    
    # Send error notification using centralized script
    bash 999_discord_notify.sh error "$script_name" "All validators data fetch" "node 90_xshin.js all all" "$exit_code" "$epoch"
    
    exit $exit_code
fi

log "INFO" "📁 Renaming all validators output file for epoch $epoch"
if mv all_all_validators.json 90_xshin_all_validators_${epoch}.json; then
    log "INFO" "✅ Successfully renamed to 90_xshin_all_validators_${epoch}.json"
else
    exit_code=$?
    log "ERROR" "❌ Failed to rename all validators file (exit code: $exit_code)"
    
    # Send error notification using centralized script
    bash 999_discord_notify.sh error "$script_name" "File rename operation" "mv all_all_validators.json 90_xshin_all_validators_${epoch}.json" "$exit_code" "$epoch"
    
    exit $exit_code
fi

log "INFO" "🏆 Running node script to fetch award winners data"

# Run node script for award winners
if node 90_xshin.js award; then
    log "INFO" "✅ Successfully fetched award winners data"
else
    exit_code=$?
    log "ERROR" "❌ Failed to fetch award winners data (exit code: $exit_code)"
    
    # Send error notification using centralized script
    bash 999_discord_notify.sh error "$script_name" "Award winners data fetch" "node 90_xshin.js award" "$exit_code" "$epoch"
    
    exit $exit_code
fi

log "INFO" "📁 Renaming award winners output file for epoch $epoch"
if mv all_award_winners.json 90_xshin_all_award_winners_${epoch}.json; then
    log "INFO" "✅ Successfully renamed to 90_xshin_all_award_winners_${epoch}.json"
else
    exit_code=$?
    log "ERROR" "❌ Failed to rename award winners file (exit code: $exit_code)"
    
    # Send error notification using centralized script
    bash 999_discord_notify.sh error "$script_name" "File rename operation" "mv all_award_winners.json 90_xshin_all_award_winners_${epoch}.json" "$exit_code" "$epoch"
    
    exit $exit_code
fi

log "INFO" "🐍 Running Python script to load Xshin data for epoch $epoch"

# Run Python script to load data
if python3 90_xshin_load_data.py ${epoch}; then
    log "INFO" "✅ Successfully completed Python data loading for epoch $epoch"
else
    exit_code=$?
    log "ERROR" "❌ Failed to load Xshin data via Python script (exit code: $exit_code)"
    
    # Send error notification using centralized script
    bash 999_discord_notify.sh error "$script_name" "Python data loading" "python3 90_xshin_load_data.py ${epoch}" "$exit_code" "$epoch"
    
    exit $exit_code
fi

log "INFO" "🎉 Xshin data loading process completed successfully for epoch $epoch"

# Send success notification using centralized script
components_processed="   • All validators data fetch (Node.js)
   • Award winners data fetch (Node.js)
   • File renaming operations
   • Python data loading and processing"

bash 999_discord_notify.sh success "$script_name" "$epoch" "Xshin Data Loading Completed Successfully" "$components_processed"
cleanup_logging
