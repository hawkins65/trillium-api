#!/bin/bash

# Source path initialization
source "$(dirname "$0")/000_init_paths.sh" || {
    echo "❌ Failed to source path initialization script" >&2
    exit 1
}

# Set up output directories for leaderboard files
export TRILLIUM_LEADERBOARD_DIR="${TRILLIUM_DATA}/leaderboard"
export TRILLIUM_LEADERBOARD_JSON="${TRILLIUM_LEADERBOARD_DIR}/json"
export TRILLIUM_LEADERBOARD_CSV="${TRILLIUM_LEADERBOARD_DIR}/csv"
export TRILLIUM_LEADERBOARD_HTML="${TRILLIUM_LEADERBOARD_DIR}/html"
export TRILLIUM_LEADERBOARD_LOGS="${TRILLIUM_DATA}/logs"

# Create directories if they don't exist
mkdir -p "${TRILLIUM_LEADERBOARD_JSON}"
mkdir -p "${TRILLIUM_LEADERBOARD_CSV}"
mkdir -p "${TRILLIUM_LEADERBOARD_HTML}"
mkdir -p "${TRILLIUM_LEADERBOARD_LOGS}"

# Change to leaderboard directory to generate files there
cd "${TRILLIUM_LEADERBOARD_DIR}" || {
    echo "❌ Failed to change to leaderboard directory" >&2
    exit 1
}

# Source the common logging functions
source $TRILLIUM_SCRIPTS_BASH/999_common_log.sh
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
        bash "$DISCORD_NOTIFY_SCRIPT" error "$script_name" "$description" "$command" "$exit_code" "$epoch_number"
        return $exit_code
    fi
}

# Script start
log "INFO" "🚀 Starting build leaderboard process"

# Create response file in the logs directory
response_file="${TRILLIUM_LEADERBOARD_LOGS}/2_response_file.$epoch_number"

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

# Create response file in the logs directory with epoch number
response_file="${TRILLIUM_LEADERBOARD_LOGS}/2_response_file.$epoch_number"

log "INFO" "🔄 Processing epoch $epoch_number"

# Execute each step with logging and error handling
echo " "
execute_with_logging "python3 $TRILLIUM_SCRIPTS_PYTHON/93_skip_analysis.py $epoch_number" "skip analysis" "🔍"

echo " "
execute_with_logging "python3 $TRILLIUM_SCRIPTS_PYTHON/93_vote_latency_json.py $epoch_number" "vote latency JSON generation" "📈"

echo " "
execute_with_logging "bash $TRILLIUM_SCRIPTS_BASH/93_vote_latency.sh $epoch_number" "vote latency processing" "⏱️"

echo " "
execute_with_logging "bash $TRILLIUM_SCRIPTS_BASH/93_build_leaderboard_json-jito-by_count.sh $epoch_number" "Jito leaderboard JSON build" "🏆"

echo " "
execute_with_logging "python3 $TRILLIUM_SCRIPTS_PYTHON/solana_leaderboard/build_leaderboard.py $epoch_number $epoch_number" "Solana leaderboard build" "🔨"

echo " "
execute_with_logging "python3 $TRILLIUM_SCRIPTS_PYTHON/93_plot_slot_duration_histogram.py $epoch_number" "slot duration histogram plotting" "📊"
execute_with_logging "bash $TRILLIUM_SCRIPTS_BASH/copy-pages-to-web.sh ${TRILLIUM_LEADERBOARD_HTML}/slot_duration_histogram_epoch${epoch_number}.html" "copying histogram to web" "🌐"

log "INFO" "🎉 Build leaderboard process completed successfully for epoch $epoch_number"

# Log file locations for user reference
log "INFO" "📁 Output files have been organized:"
log "INFO" "   JSON files: ${TRILLIUM_LEADERBOARD_JSON}"
log "INFO" "   CSV files: ${TRILLIUM_LEADERBOARD_CSV}"
log "INFO" "   HTML files: ${TRILLIUM_LEADERBOARD_HTML}"
log "INFO" "   Log files: ${TRILLIUM_LEADERBOARD_LOGS}"

# Send success notification to Discord using centralized script
components_processed="   • Skip analysis
   • Vote latency JSON generation
   • Vote latency processing
   • Jito leaderboard JSON build
   • Solana leaderboard build
   • Slot duration histogram plotting
   • Slot duration statistics"

bash "$DISCORD_NOTIFY_SCRIPT" success "$script_name" "$epoch_number" "Build Leaderboard Completed Successfully" "$components_processed"
cleanup_logging
