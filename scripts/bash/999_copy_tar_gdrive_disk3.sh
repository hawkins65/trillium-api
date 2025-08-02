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
FILE="/home/smilax/get_slots/epoch${epoch_number}.tar.zst"

echo "Copying $FILE to /mnt/gdrive/epochs/..."
cp "$FILE" /mnt/gdrive/epochs/
check_error
echo "Step 3: $FILE copied to /mnt/gdrive/epochs/ successfully."
echo

echo "Copying $FILE to /home/smilax/epochs/..."
cp "$FILE" /home/smilax/epochs/
check_error
echo "Step 4: $FILE copied to /home/smilax/epochs/ successfully."
echo

echo "All steps completed successfully."
