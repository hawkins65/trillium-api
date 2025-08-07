#!/bin/bash

# Script to handle cloud storage copying in the background
# This script is called from copy_tar.sh to copy tar.zst files to gdrive and idrive

# Source path initialization
source "$(dirname "$0")/../bash/000_init_paths.sh" || {
    echo "❌ Failed to source path initialization script" >&2
    exit 1
}

# Source common logging
source "$COMMON_LOG_SCRIPT"

# Initialize logging
init_logging

# Function to check if the previous command executed successfully
check_error() {
    if [ $? -ne 0 ]; then
        log_error "❌ Cloud storage copy failed for command"
        return 1
    fi
    return 0
}

# Check if a file path is provided as a parameter
if [ -z "$1" ]; then
    log_error "❌ No file path provided"
    exit 1
fi

FILE="$1"
BASENAME="$(basename "$FILE")"

# Check if the file exists
if [ ! -f "$FILE" ]; then
    log_error "❌ File not found: $FILE"
    exit 1
fi

log_info "🔄 Starting background cloud storage copy for $BASENAME from $FILE"

# Copy to Google Drive
log_info "📤 Copying $BASENAME to /mnt/gdrive/epochs/"
if cp "$FILE" /mnt/gdrive/epochs/; then
    log_info "✅ Successfully copied $BASENAME to /mnt/gdrive/epochs/"
else
    log_error "⚠️ Failed to copy $BASENAME to /mnt/gdrive/epochs/"
fi

# Copy to iDrive
log_info "📤 Copying $BASENAME to /mnt/idrive/epochs/"
if cp "$FILE" /mnt/idrive/epochs/; then
    log_info "✅ Successfully copied $BASENAME to /mnt/idrive/epochs/"
else
    log_error "⚠️ Failed to copy $BASENAME to /mnt/idrive/epochs/"
fi

log_info "🎉 Cloud storage copy process completed for $BASENAME"

# Cleanup logging
cleanup_logging