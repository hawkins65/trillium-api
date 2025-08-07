#!/bin/bash

# Source path initialization
source "$(dirname "$0")/../bash/000_init_paths.sh" || {
    echo "❌ Failed to source path initialization script" >&2
    exit 1
}

# Source common logging (already sourced by init_paths but ensure it's available)
source "$COMMON_LOG_SCRIPT"

# Initialize logging
init_logging

# Function to check if the previous command executed successfully
check_error() {
    if [ $? -ne 0 ]; then
        log_error "❌ Command failed. Exiting script"
        exit 1
    fi
}

# Check if an epoch number is provided as a parameter
if [ -n "$1" ]; then
    epoch_number="$1"
else
    read -p "Enter the epoch number: " epoch_number
fi

# Construct the base filename and full path
FILE="epoch${epoch_number}.tar.zst"
# Use TRILLIUM_DATA_EPOCHS from paths.conf, fallback to hardcoded if not set
EPOCHS_DIR="${TRILLIUM_DATA_EPOCHS:-/home/smilax/trillium_api/data/epochs}"
FULL_PATH="${EPOCHS_DIR}/$FILE"

# Get the directory of this script
SCRIPT_DIR="$(dirname "$0")"

# Launch cloud storage copy in background
log_info "🚀 Starting background cloud storage copy for $FILE"
nohup "$SCRIPT_DIR/cloud_storage_copy.sh" "$FULL_PATH" > /home/smilax/log/cloud_copy_epoch${epoch_number}.log 2>&1 &
COPY_PID=$!
log_info "📋 Cloud storage copy launched in background (PID: $COPY_PID)"
log_info "📄 Cloud copy log: /home/smilax/log/cloud_copy_epoch${epoch_number}.log"

log_info "🎉 Pipeline continuing without waiting for cloud storage copy"

# Cleanup logging
cleanup_logging
