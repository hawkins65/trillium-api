#!/bin/bash

source common_log.sh

# Check if epoch number is provided as an argument
if [ $# -lt 1 ]; then
    # If not provided, prompt the user for input
    read -p "Please enter the epoch number: " epoch_number
else
    # If provided, use the first argument
    epoch_number=$1
fi

# Define the message file name
message_file="trillium_message.txt"

# Create the message file with the specified content
cat > "$message_file" << EOF
🔔 Trillium API Update 🔔

Epoch: $epoch_number
Status: RPC Data Collection Complete
For more information, visit ~/block-production/get_slots/epoch$epoch_number
EOF

# Verify that the message file was created successfully
if [ ! -f "$message_file" ] || [ ! -r "$message_file" ]; then
    log_message "ERROR" "Failed to create message file '$message_file'."
    exit 1
fi

# Set environment variables for Discord customization
export DISCORD_USERNAME="CustomTrilliumBot"
export DISCORD_AVATAR_URL="https://trillium.so/pages/custom_trillium_logo.png"

# Check if trillium_alert.sh exists and is executable
if [ ! -f "trillium_alert.sh" ] || [ ! -x "trillium_alert.sh" ]; then
    log_message "ERROR" "'trillium_alert.sh' does not exist or is not executable."
    exit 1
fi

# Call trillium_alert.sh with the message file and epoch number
./trillium_alert.sh "$message_file" "$epoch_number"

# Display confirmation
log_message "INFO" "Message file '$message_file' created and trillium_alert.sh executed with epoch $epoch_number."