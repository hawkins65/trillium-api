#!/bin/bash

# Source common logging
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/../bash/999_common_log.sh"

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
FILE="epoch${epoch_number}.tar.zst"

log_info "🚀 Starting archive: Copying $FILE to /mnt/gdrive/epochs/"
cp "$FILE" /mnt/gdrive/epochs/
check_error
log_info "✅ Archive complete: $FILE copied to /mnt/gdrive/epochs/ successfully"

log_info "🎉 All steps completed successfully"

# Cleanup logging
cleanup_logging
