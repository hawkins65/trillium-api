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

# Check if an epoch number is provided as a parameter
if [ -n "$1" ]; then
    epoch_number="$1"
else
    read -p "Enter the epoch number: " epoch_number
fi

# Construct the base filename
base_filename="epoch${epoch_number}"

# Notify user and start compression with progress indicator
log_info "🗂️ Creating compressed archive with progress..."
tar -cf - ./"${base_filename}"/run*/ | zstd -9 --long -T0 -o "${base_filename}.tar.zst"
log_info "✅ Archive created: ${base_filename}.tar.zst"

# Cleanup logging
cleanup_logging
