#!/bin/bash

# Source the common logging functions
source /home/smilax/api/999_common_log.sh

# Save the original command-line arguments
original_args="$@"
script_name=$(basename "$0")

log "INFO" "🚀 Starting RPC get block data script with parameters: $0 $original_args"
sleep 5

# Parse command line arguments
if [[ "$#" -ne 1 ]]; then
    log "ERROR" "❌ Invalid number of arguments provided"
    echo "Usage: $0 <epoch-number>"
    bash 999_discord_notify.sh error "$script_name" "Invalid arguments" "Usage: $0 <epoch-number>" "" "" "Provided: $# arguments"
    exit 1
fi

epoch_number="$1"
log "INFO" "📊 Processing epoch number: $epoch_number"

# Switch to the directory to save slot data
target_dir="/home/smilax/block-production/get_slots/epoch$epoch_number"
log "INFO" "📁 Switching to directory: $target_dir"

if ! cd "$target_dir"; then
    log "ERROR" "❌ Failed to change directory to $target_dir"
    bash 999_discord_notify.sh error "$script_name" "Directory change failed" "cd \"$target_dir\"" "1" "$epoch_number"
    exit 1
fi

log "INFO" "✅ Successfully changed to target directory"

# Get shinobi vote latency and consensus voting data
log "INFO" "🗳️ Getting Shinobi vote latency and consensus voting data"
if bash get_shin_voting.sh; then
    log "INFO" "✅ Successfully completed Shinobi voting data collection"
else
    exit_code=$?
    log "ERROR" "❌ Failed to get Shinobi voting data (exit code: $exit_code)"
    bash 999_discord_notify.sh error "$script_name" "Shinobi voting data collection" "bash get_shin_voting.sh" "$exit_code" "$epoch_number"
fi

# Get epoch data in csv files
log "INFO" "📈 Running epoch data CSV generation"
python3 get_epoch_data_csv.py "$epoch_number"
exit_status=$?

# Check if the Python script exited cleanly or slots didn't change
if [ $exit_status -eq 99 ]; then
    log "INFO" "ℹ️ No new slots to process. Exiting script gracefully."
    exit 0
elif [ $exit_status -ne 0 ]; then
    log "ERROR" "❌ Python script did not exit cleanly (exit code: $exit_status)"
    
    # Send Discord alert for Python script failure
    bash 999_discord_notify.sh error "$script_name" "Epoch data CSV generation" "python3 get_epoch_data_csv.py \"$epoch_number\"" "$exit_status" "$epoch_number"
    
    # Send PagerDuty alert for Python script failure
    if [[ -x "$HOME/api/999_pagerduty.sh" ]]; then
        log "INFO" "📟 Sending PagerDuty alert for Python script failure"
        "$HOME/api/999_pagerduty.sh" \
            --severity error \
            --source "$(hostname)" \
            --details "{\"exit_status\": $exit_status, \"script\": \"$(basename "$0")\", \"timestamp\": \"$(date -u --iso-8601=seconds)\"}" \
            "Python script failed with exit status $exit_status"
    else
        log "WARN" "⚠️ PagerDuty script not found at $HOME/api/999_pagerduty.sh"
    fi
    
    read -p "Do you want to quit the script or continue? (q/c): " choice
    if [[ $choice =~ ^[Qq]$ ]]; then
        log "INFO" "🛑 Script execution aborted by user choice"
        bash 999_discord_notify.sh custom "$script_name" "Script Aborted" "Script aborted by user after Python error\nExit Code: $exit_status\nEpoch: $epoch_number\nUser Choice: Quit" "🛑"
        exit 1
    else
        log "INFO" "⚠️ Continuing script execution despite Python error (user choice)"
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
    bash 999_discord_notify.sh error "$script_name" "Directory creation" "mkdir \"$next_run_dir\"" "$exit_code" "$epoch_number"
    exit $exit_code
fi

# Move the specified files to the new directory
log "INFO" "📦 Moving CSV files and error logs to $next_run_dir"
if mv *.csv solana_rpc_errors.log "$next_run_dir" 2>/dev/null; then
    log "INFO" "✅ Successfully moved files to $next_run_dir"
else
    log "WARN" "⚠️ Some files may not have been moved (this may be normal if files don't exist)"
fi

# Prompt the user to re-run the script
log "INFO" "❓ Prompting user for script re-run decision"
read -t 5 -p "Do you want to re-run the script? (y/n): " choice
if [ -z "$choice" ]; then
    choice="y"
    log "INFO" "⏰ No input received within timeout, defaulting to 'y' (re-run)"
else
    log "INFO" "👤 User input received: $choice"
fi

if [[ $choice =~ ^[Yy]$ ]]; then
    log "INFO" "🔄 Re-running the script with original parameters: $0 $original_args"
    bash 999_discord_notify.sh restart "$script_name" "$epoch_number" "$original_args"
    exec "$0" $original_args
else
    log "INFO" "💬 Updating Discord channel and completing script"
    if bash update_discord_channel.sh $epoch_number; then
        log "INFO" "✅ Successfully updated Discord channel"
    else
        log "WARN" "⚠️ Discord channel update may have failed"
    fi
    
    log "INFO" "🎉 RPC get block data script execution completed for epoch $epoch_number"
    
    # Send success notification to Discord using centralized script
    components_processed="   • Directory navigation to epoch folder
   • Shinobi vote latency and consensus data
   • Epoch data CSV generation
   • Run directory management
   • File organization and cleanup
   • Discord channel updates"

    bash 999_discord_notify.sh success "$script_name" "$epoch_number" "RPC Get Block Data Completed Successfully" "$components_processed"
fi
