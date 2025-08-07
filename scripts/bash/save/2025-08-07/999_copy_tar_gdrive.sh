#!/bin/bash

# Source path initialization
source "$(dirname "$0")/000_init_paths.sh" || {
    echo "❌ Failed to source path initialization script" >&2
    exit 1
}

# Source common logging
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/999_common_log.sh"

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

# Construct the base filename
FILE="$TRILLIUM_DATA_EPOCHS/epoch${epoch_number}.tar.zst"

log_info "📁 Copying $FILE to /mnt/gdrive/epochs/"
cp "$FILE" /mnt/gdrive/epochs/
check_error
log_info "✅ Step 3: $FILE copied to /mnt/gdrive/epochs/ successfully"

# Note: No longer copying to /home/smilax/epochs/ since pipeline works directly from get_slots directory
log_info "💡 Archive complete - pipeline now works directly from get_slots epoch directory"

log_info "🎉 All steps completed successfully"

# Cleanup logging
cleanup_logging
