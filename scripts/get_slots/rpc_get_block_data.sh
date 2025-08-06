#!/bin/bash

# Source path initialization
source "$(dirname "$0")/../bash/000_init_paths.sh" || {
    echo "❌ Failed to source path initialization script" >&2
    exit 1
}

# Logging is already initialized by 000_init_paths.sh

# Save the original command-line arguments
original_args="$@"
script_name=$(basename "$0")

log "INFO" "🚀 Starting RPC get block data script with parameters: $0 $original_args"
sleep 5

# Parse command line arguments
if [[ "$#" -ne 1 ]]; then
    log "ERROR" "❌ Invalid number of arguments provided"
    echo "Usage: $0 <epoch-number>"
    bash "$DISCORD_NOTIFY_SCRIPT" error "$script_name" "Invalid arguments" "Usage: $0 <epoch-number>" "" "" "Provided: $# arguments"
    exit 1
fi

epoch_number="$1"
log "INFO" "📊 Processing epoch number: $epoch_number"

# Switch to the directory to save slot data
target_dir="$(resolve_data_path "epoch$epoch_number" "epochs")"
log "INFO" "📁 Switching to directory: $target_dir"

if ! safe_cd "$target_dir" "epoch data directory"; then
    log "ERROR" "❌ Failed to change directory to $target_dir"
    bash "$DISCORD_NOTIFY_SCRIPT" error "$script_name" "Directory change failed" "cd \"$target_dir\"" "1" "$epoch_number"
    exit 1
fi

log "INFO" "✅ Successfully changed to target directory"

# Get shinobi vote latency and consensus voting data
log "INFO" "🗳️ Getting Shinobi vote latency and consensus voting data"
if bash "$(resolve_script_path 'get_shin_voting.sh' 'getslots')"; then
    log "INFO" "✅ Successfully completed Shinobi voting data collection"
else
    exit_code=$?
    log "ERROR" "❌ Failed to get Shinobi voting data (exit code: $exit_code)"
    bash "$DISCORD_NOTIFY_SCRIPT" error "$script_name" "Shinobi voting data collection" "bash get_shin_voting.sh" "$exit_code" "$epoch_number"
fi

# Get epoch data in csv files
log "INFO" "📈 Running epoch data CSV generation"
python_script="$(resolve_script_path 'get_epoch_data_csv.py' 'getslots')"

# Add a marker to identify where errors occur
echo "=== BEFORE PYTHON EXECUTION ==="
python3 "$python_script" "$epoch_number"
exit_status=$?
echo "=== AFTER PYTHON EXECUTION ==="

log "DEBUG" "Python script completed with exit status: $exit_status"

# Check if the Python script exited cleanly or slots didn't change
if [ $exit_status -eq 99 ]; then
    log "INFO" "ℹ️ No new slots to process. Exiting script gracefully."
    exit 0
elif [ $exit_status -ne 0 ]; then
    log "ERROR" "❌ Python script did not exit cleanly (exit code: $exit_status)"
    
    # Send Discord alert for Python script failure
    bash "$DISCORD_NOTIFY_SCRIPT" error "$script_name" "Epoch data CSV generation" "python3 get_epoch_data_csv.py \"$epoch_number\"" "$exit_status" "$epoch_number"
    
    # Send PagerDuty alert for Python script failure
    if [[ -x "$(resolve_script_path '999_pagerduty.sh' 'bash')" ]]; then
        log "INFO" "📟 Sending PagerDuty alert for Python script failure"
        "$(resolve_script_path '999_pagerduty.sh' 'bash')" \
            --severity error \
            --source "$(hostname)" \
            --details "{\"exit_status\": $exit_status, \"script\": \"$(basename "$0")\", \"timestamp\": \"$(date -u --iso-8601=seconds)\"}" \
            "Python script failed with exit status $exit_status"
    else
        log "WARN" "⚠️ PagerDuty script not found at $(resolve_script_path '999_pagerduty.sh' 'bash')"
    fi
    
    # ENHANCED: Always continue automatically for timeout/processing errors (exit code 1)
    # Let the wrapper script handle retries rather than prompting user
    if [ $exit_status -eq 1 ]; then
        log "INFO" "🔄 Exit code 1 detected - allowing wrapper script to handle retry"
        exit 1  # Exit with code 1 to trigger wrapper retry mechanism
    else
        # For other errors, continue processing but log the issue
        log "WARN" "⚠️ Python script error (code $exit_status) - continuing with remaining operations"
    fi
else
    log "INFO" "✅ Successfully completed epoch data CSV generation"
fi

# Find the highest run directory
log "INFO" "📂 Finding highest run directory number"
max_run=0
for dir in run*; do
    if [[ -d $dir ]]; then
        run_num=$(echo $dir | sed 's/run//')
        if ((run_num > max_run)); then
            max_run=$run_num
        fi
    fi
done

# Create the next run directory
next_run=$((max_run + 1))
next_run_dir="run$next_run"
log "INFO" "📁 Creating next run directory: $next_run_dir"

if mkdir "$next_run_dir"; then
    log "INFO" "✅ Successfully created directory: $next_run_dir"
else
    exit_code=$?
    log "ERROR" "❌ Failed to create directory: $next_run_dir (exit code: $exit_code)"
    bash "$DISCORD_NOTIFY_SCRIPT" error "$script_name" "Directory creation" "mkdir \"$next_run_dir\"" "$exit_code" "$epoch_number"
    exit $exit_code
fi

# Move the specified files to the new directory
log "INFO" "📦 Moving CSV files and error logs to $next_run_dir"
if mv *.csv solana_rpc_errors.log "$next_run_dir" 2>/dev/null; then
    log "INFO" "✅ Successfully moved files to $next_run_dir"
else
    log "WARN" "⚠️ Some files may not have been moved (this may be normal if files don't exist)"
fi

# ENHANCED: Automatically determine re-run based on completion status
log "INFO" "🔍 Checking if script should re-run automatically"

# Check if there are still missing slots by looking at the last slots file
last_slots_file="last_slots_to_process.txt"
should_rerun=false

if [ -f "$last_slots_file" ]; then
    remaining_slots=$(cat "$last_slots_file" 2>/dev/null || echo "0")
    if [ "$remaining_slots" -gt 0 ]; then
        should_rerun=true
        log "INFO" "📊 Found $remaining_slots remaining slots - will re-run"
    else
        log "INFO" "✅ No remaining slots ($remaining_slots) - epoch processing complete"
    fi
else
    log "WARN" "⚠️ Last slots file not found - defaulting to re-run"
    should_rerun=true
fi

if [ "$should_rerun" = true ]; then
    log "INFO" "🔄 Automatically re-running script for remaining work: $0 $original_args"
    bash "$DISCORD_NOTIFY_SCRIPT" restart "$script_name" "$epoch_number" "$original_args"
    # Use the full script path for re-execution
    script_path="/home/smilax/trillium_api/scripts/get_slots/rpc_get_block_data.sh"
    log "DEBUG" "About to exec: $script_path $original_args"
    exec "$script_path" $original_args
else
    log "INFO" "🎉 Epoch processing completed - finishing script"
    
    log "INFO" "🎉 RPC get block data script execution completed for epoch $epoch_number"
    
    # Send success notification to Discord using centralized script
    components_processed="   • Directory navigation to epoch folder
   • Shinobi vote latency and consensus data
   • Epoch data CSV generation
   • Run directory management
   • File organization and cleanup
   • Automatic completion detection"

    bash "$DISCORD_NOTIFY_SCRIPT" success "$script_name" "$epoch_number" "RPC Get Block Data Completed Successfully" "$components_processed"
fi

# cleanup_logging is handled by the common logging script
