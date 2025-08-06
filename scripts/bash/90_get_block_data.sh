#!/bin/bash

# Source path initialization
source "$(dirname "$0")/000_init_paths.sh" || {
    echo "❌ Failed to source path initialization script" >&2
    exit 1
}

# Common logging is already sourced by init_paths if available
# Initialize enhanced logging
init_logging

# Get the basename of the script for Discord notifications
script_name=$(basename "$0")

log "INFO" "🚀 Starting block data collection process"

# Parse command line arguments
log "DEBUG" "Script called with $# arguments: '$*'"
if [[ "$#" -lt 1 ]]; then
    log "ERROR" "❌ No epoch number provided"
    echo "Usage: $0 <epoch-number>"
    
    # Send error notification using centralized script
    bash "$DISCORD_NOTIFY_SCRIPT" error "$script_name" "Invalid arguments" "Usage: $0 <epoch-number>" "1" ""
    
    exit 1
fi

epoch_number="$1"
log "INFO" "📊 Processing epoch number: $epoch_number"

# Workaround: If we got extra arguments, log them but continue
if [[ "$#" -gt 1 ]]; then
    log "WARN" "⚠️ Received $# arguments but only using the first one: $epoch_number"
    log "DEBUG" "All arguments: $*"
fi

# Save the current directory so we can come back at the end
ORIGINAL_DIR=$(pwd)
log "INFO" "💾 Original directory saved: $ORIGINAL_DIR"

# Switch to the directory to save slot data
target_dir="$(resolve_data_path "epoch$epoch_number" "epochs")"
log "INFO" "📁 Switching to target directory: $target_dir"

# Create the epoch directory if it doesn't exist
if [[ ! -d "$target_dir" ]]; then
    log "INFO" "📂 Creating epoch directory: $target_dir"
    if ! mkdir -p "$target_dir"; then
        log "ERROR" "❌ Failed to create directory: $target_dir"
        echo "Error: Failed to create directory: $target_dir"
        
        # Send error notification using centralized script
        bash "$DISCORD_NOTIFY_SCRIPT" error "$script_name" "Directory creation failed" "mkdir -p $target_dir" "1" "$epoch_number"
        
        exit 1
    fi
    log "INFO" "✅ Successfully created epoch directory"
fi

if ! safe_cd "$target_dir" "epoch data directory"; then
    log "ERROR" "❌ Failed to change directory to $target_dir"
    echo "Error: Failed to change directory to $target_dir"
    
    # Send error notification using centralized script
    bash "$DISCORD_NOTIFY_SCRIPT" error "$script_name" "Directory change failed" "cd $target_dir" "1" "$epoch_number"
    
    exit 1
fi

log "INFO" "✅ Successfully changed to target directory"

# Function to execute a command and check for errors with automatic retry for exit code 1
execute_and_check() {
    local cmd="$1"
    local max_retries=3
    local retry_count=0
    
    while [ $retry_count -lt $max_retries ]; do
        log "INFO" "⚡ Executing (attempt $((retry_count + 1))/$max_retries): $cmd"
        
        if eval "$cmd"; then
            log "INFO" "✅ Successfully completed: $cmd"
            return 0
        else
            local exit_code=$?
            log "ERROR" "❌ Command failed with exit code $exit_code: $cmd (attempt $((retry_count + 1))/$max_retries)"
            
            if [[ $cmd == *"get_epoch_data_csv.py"* && $exit_code -eq 99 ]]; then
                log "WARN" "⚠️ get_epoch_data_csv.py returned error code 99. Continuing..."
                echo "Warning: get_epoch_data_csv.py returned error code 99. Continuing..."
                return 0
            elif [[ $exit_code -eq 1 && $retry_count -lt $((max_retries - 1)) ]]; then
                # Automatic retry for exit code 1 (timeout/processing issues)
                retry_count=$((retry_count + 1))
                local wait_time=$((retry_count * 30))  # Progressive backoff: 30s, 60s
                log "WARN" "🔄 Exit code 1 detected - automatically retrying in $wait_time seconds (attempt $retry_count/$max_retries)"
                bash "$DISCORD_NOTIFY_SCRIPT" warning "$script_name" "Automatic retry $retry_count" "Exit code 1 - retrying in ${wait_time}s" "$exit_code" "$epoch_number"
                sleep $wait_time
                continue
            else
                # Send error notification using centralized script
                if [[ $retry_count -ge $((max_retries - 1)) ]]; then
                    bash "$DISCORD_NOTIFY_SCRIPT" error "$script_name" "Command failed after $max_retries attempts" "$cmd" "$exit_code" "$epoch_number"
                    log "ERROR" "❌ Command failed after $max_retries attempts with exit code $exit_code"
                    echo "Error: Command failed after $max_retries attempts. Exiting."
                    exit 1
                else
                    bash "$DISCORD_NOTIFY_SCRIPT" error "$script_name" "Command execution failed" "$cmd" "$exit_code" "$epoch_number"
                    log "ERROR" "❌ Non-retriable error (exit code $exit_code). Exiting."
                    echo "Error: Non-retriable error occurred. Exiting."
                    exit $exit_code
                fi
            fi
        fi
    done
}

# Start the collection of block data with epoch number and number of concurrent threads
log "INFO" "🔄 Starting block data collection for epoch $epoch_number"
cmd="bash $(resolve_script_path 'rpc_get_block_data.sh' 'getslots') $epoch_number"
log "DEBUG" "About to execute command: $cmd"
execute_and_check "$cmd"

# Process tar files
log "INFO" "📦 Processing tar files - changing to parent directory"
if safe_cd "$TRILLIUM_DATA_EPOCHS" "epochs directory"; then
    log "INFO" "✅ Successfully changed to parent directory"
else
    log "ERROR" "❌ Failed to change to parent directory"
    echo "Error: Failed to change to parent directory"
    
    # Send error notification using centralized script
    bash "$DISCORD_NOTIFY_SCRIPT" error "$script_name" "Directory change failed" "cd $TRILLIUM_DATA_EPOCHS" "1" "$epoch_number"
    
    exit 1
fi

log "INFO" "🗜️ Creating tar files for epoch $epoch_number"
execute_and_check "bash $(resolve_script_path 'tar_files.sh' 'getslots') $epoch_number"

log "INFO" "📋 Copying tar files for epoch $epoch_number"
execute_and_check "bash $(resolve_script_path 'copy_tar.sh' 'getslots') $epoch_number"

# Return to the original directory
log "INFO" "🔙 Returning to original directory: $ORIGINAL_DIR"
if cd "$ORIGINAL_DIR"; then
    log "INFO" "✅ Successfully returned to original directory"
else
    log "ERROR" "❌ Failed to return to the original directory $ORIGINAL_DIR"
    echo "Error: Failed to return to the original directory $ORIGINAL_DIR"
    
    # Send error notification using centralized script
    bash "$DISCORD_NOTIFY_SCRIPT" error "$script_name" "Directory change failed" "cd $ORIGINAL_DIR" "1" "$epoch_number"
    
    exit 1
fi

log "INFO" "🎉 Block data collection process completed successfully for epoch $epoch_number"
echo "Script completed successfully."

# Send success notification using centralized script
components_processed="   • Block data collection via RPC
   • Tar file creation and processing
   • File copying operations
   • Directory management and cleanup"

bash "$DISCORD_NOTIFY_SCRIPT" success "$script_name" "$epoch_number" "Block Data Collection Completed Successfully" "$components_processed"
cleanup_logging
