#!/bin/bash

# Function to check if the previous command executed successfully
check_error() {
    if [ $? -ne 0 ]; then
        echo "Error: Command failed. Exiting script."
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

echo
echo "Starting Step 1: Copying $FILE to /home/smilax/block-production/api/..."
cp "$FILE" /home/smilax/block-production/api/
check_error
echo "Step 1: $FILE copied to /home/smilax/block-production/api/ successfully."
echo

echo "All steps completed successfully."
