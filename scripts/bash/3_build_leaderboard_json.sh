#!/bin/bash

# Source the common logging functions
source /home/smilax/api/999_common_log.sh
# Initialize enhanced logging
init_logging

# activate python environment with a full path
. /home/smilax/.python_env/bin/activate

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
        bash 999_discord_notify.sh error "$script_name" "$description" "$command" "$exit_code" "$epoch_number"
        return $exit_code
    fi
}

# Script start
log "INFO" "🚀 Starting build leaderboard process"

# Create a temporary file for responses
response_file=$(mktemp)

# Ensure the temporary file is deleted after execution
trap 'rm -f "$response_file"' EXIT

# Get the basename of the script for Discord notifications
script_name=$(basename "$0")

# Check if an epoch number is provided as the parameter
if [ -n "$1" ]; then
    epoch_number="$1"
    log "INFO" "📊 Using epoch number from parameter: $epoch_number"
else
    read -p "Enter the epoch number: " epoch_number
    log "INFO" "📊 Using epoch number from user input: $epoch_number"
fi

log "INFO" "🔄 Processing epoch $epoch_number"

# Execute each step with logging and error handling
echo " "
execute_with_logging "python3 93_skip_analysis.py $epoch_number" "skip analysis" "🔍"

echo " "
execute_with_logging "python3 93_vote_latency_json.py $epoch_number" "vote latency JSON generation" "📈"

echo " "
execute_with_logging "bash 93_vote_latency.sh $epoch_number" "vote latency processing" "⏱️"

echo " "
execute_with_logging "bash 93_build_leaderboard_json-jito-by_count.sh $epoch_number" "Jito leaderboard JSON build" "🏆"

echo " "
execute_with_logging "python3 -m solana_leaderboard.build_leaderboard $epoch_number $epoch_number" "Solana leaderboard build" "🔨"

echo " "
execute_with_logging "python3 93_plot_slot_duration_histogram.py $epoch_number" "slot duration histogram plotting" "📊"
execute_with_logging "bash copy-pages-to-web.sh slot_duration_histogram_epoch${epoch_number}.html" "copying histogram to web" "🌐"

log "INFO" "🎉 Build leaderboard process completed successfully for epoch $epoch_number"

# Send success notification to Discord using centralized script
components_processed="   • Skip analysis
   • Vote latency JSON generation
   • Vote latency processing
   • Jito leaderboard JSON build
   • Solana leaderboard build
   • Slot duration histogram plotting
   • Slot duration statistics"

bash 999_discord_notify.sh success "$script_name" "$epoch_number" "Build Leaderboard Completed Successfully" "$components_processed"
cleanup_logging
