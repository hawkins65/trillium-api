#!/bin/bash

# Source common logging
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/../bash/999_common_log.sh"

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
