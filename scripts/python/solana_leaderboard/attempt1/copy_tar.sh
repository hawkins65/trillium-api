#!/bin/bash

source common_log.sh

# Function to check if the previous command executed successfully
check_error() {
    if [ $? -ne 0 ]; then
        log_message "ERROR" "Command failed. Exiting script."
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

log_message "INFO" ""
log_message "INFO" "Starting Step 1: Copying $FILE to /home/smilax/block-production/api/..."
cp "$FILE" /home/smilax/block-production/api/
check_error
log_message "INFO" "Step 1: $FILE copied to /home/smilax/block-production/api/ successfully."
log_message "INFO" ""

log_message "INFO" "All steps completed successfully."
