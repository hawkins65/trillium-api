#!/bin/bash

# Source the common logging functions
source /home/smilax/api/999_common_log.sh
# Initialize enhanced logging
init_logging

# Get the basename of the script for Discord notifications
script_name=$(basename "$0")

log "INFO" "🚀 Starting block data collection process"

# Parse command line arguments
if [[ "$#" -ne 1 ]]; then
    log "ERROR" "❌ Invalid number of arguments provided"
    echo "Usage: $0 <epoch-number>"
    
    # Send error notification using centralized script
    bash 999_discord_notify.sh error "$script_name" "Invalid arguments" "Usage: $0 <epoch-number>" "1" ""
    
    exit 1
fi

epoch_number="$1"
log "INFO" "📊 Processing epoch number: $epoch_number"

# Save the current directory so we can come back at the end
ORIGINAL_DIR=$(pwd)
log "INFO" "💾 Original directory saved: $ORIGINAL_DIR"

# Switch to the directory to save slot data
target_dir="/home/smilax/block-production/get_slots/epoch$epoch_number"
log "INFO" "📁 Switching to target directory: $target_dir"

if ! cd "$target_dir"; then
    log "ERROR" "❌ Failed to change directory to $target_dir"
    echo "Error: Failed to change directory to $target_dir"
    
    # Send error notification using centralized script
    bash 999_discord_notify.sh error "$script_name" "Directory change failed" "cd $target_dir" "1" "$epoch_number"
    
    exit 1
fi

log "INFO" "✅ Successfully changed to target directory"

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
        
        if [[ $cmd == *"get_epoch_data_csv.py"* && $exit_code -eq 99 ]]; then
            log "WARN" "⚠️ get_epoch_data_csv.py returned error code 99. Continuing..."
            echo "Warning: get_epoch_data_csv.py returned error code 99. Continuing..."
            return 0
        else
            # Send error notification using centralized script
            bash 999_discord_notify.sh error "$script_name" "Command execution failed" "$cmd" "$exit_code" "$epoch_number"
            
            echo "An error occurred. Press Y or Enter to continue, or anything else to exit."
            read -r choice
            if [[ "$choice" != "Y" && "$choice" != "y" && -n "$choice" ]]; then
                log "INFO" "🛑 Script terminated by user choice after error"
                echo "Exiting script due to user choice."
                exit 1
            fi
            log "INFO" "⚠️ User chose to continue after error"
        fi
        return $exit_code
    fi
}

# Start the collection of block data with epoch number and number of concurrent threads
log "INFO" "🔄 Starting block data collection for epoch $epoch_number"
execute_and_check "bash ./rpc_get_block_data.sh $epoch_number"

# Process tar files
log "INFO" "📦 Processing tar files - changing to parent directory"
if cd /home/smilax/block-production/get_slots; then
    log "INFO" "✅ Successfully changed to parent directory"
else
    log "ERROR" "❌ Failed to change to parent directory"
    echo "Error: Failed to change to parent directory"
    
    # Send error notification using centralized script
    bash 999_discord_notify.sh error "$script_name" "Directory change failed" "cd /home/smilax/block-production/get_slots" "1" "$epoch_number"
    
    exit 1
fi

log "INFO" "🗜️ Creating tar files for epoch $epoch_number"
execute_and_check "bash tar_files.sh $epoch_number"

log "INFO" "📋 Copying tar files for epoch $epoch_number"
execute_and_check "bash copy_tar.sh $epoch_number"

# Return to the original directory
log "INFO" "🔙 Returning to original directory: $ORIGINAL_DIR"
if cd "$ORIGINAL_DIR"; then
    log "INFO" "✅ Successfully returned to original directory"
else
    log "ERROR" "❌ Failed to return to the original directory $ORIGINAL_DIR"
    echo "Error: Failed to return to the original directory $ORIGINAL_DIR"
    
    # Send error notification using centralized script
    bash 999_discord_notify.sh error "$script_name" "Directory change failed" "cd $ORIGINAL_DIR" "1" "$epoch_number"
    
    exit 1
fi

log "INFO" "🎉 Block data collection process completed successfully for epoch $epoch_number"
echo "Script completed successfully."

# Send success notification using centralized script
components_processed="   • Block data collection via RPC
   • Tar file creation and processing
   • File copying operations
   • Directory management and cleanup"

bash 999_discord_notify.sh success "$script_name" "$epoch_number" "Block Data Collection Completed Successfully" "$components_processed"
cleanup_logging
