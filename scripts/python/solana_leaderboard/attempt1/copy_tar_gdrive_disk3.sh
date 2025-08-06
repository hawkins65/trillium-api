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
FILE="/home/smilax/get_slots/epoch${epoch_number}.tar.zst"

log_message "INFO" "Copying $FILE to /mnt/gdrive/epochs/..."
cp "$FILE" /mnt/gdrive/epochs/
check_error
log_message "INFO" "Step 3: $FILE copied to /mnt/gdrive/epochs/ successfully."
log_message "INFO" ""

log_message "INFO" "Copying $FILE to /mnt/disk3/apiserver/epochs/..."
cp "$FILE" /mnt/disk3/apiserver/epochs/
check_error
log_message "INFO" "Step 4: $FILE copied to /mnt/disk2/apiserver/epochs/ successfully."
log_message "INFO" ""

log_message "INFO" "All steps completed successfully."
