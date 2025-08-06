#!/bin/bash

source 999_common_log.sh
# Initialize enhanced logging
init_logging

# Discord webhook URL
DISCORD_WEBHOOK_URL="https://discord.com/api/webhooks/1267145835699769409/7b6MXomGQkOHcSi6_YLld4srbOu0CdZtC9PJqNgTbnM0KPgU8wg_SOMMMQkdBWkjAPom"

# Telegram bot token and chat ID
TELEGRAM_BOT_TOKEN="6487508986:AAHzmaPqTxl95g9S3CsQ6b0EQ5s20egM4yg"
TELEGRAM_CHAT_ID="6375790507"

# Discord webhook customization
DISCORD_USERNAME="Trillium API Bot"
DISCORD_AVATAR_URL="https://trillium.so/pages/white_trillium_logo.png"  # Replace with a valid URL to an image

# Check if epoch number is provided as an argument
if [ $# -eq 0 ]; then
    # If not provided, prompt the user for input
    read -p "Please enter the epoch number: " epoch_number
else
    # If provided, use the first argument
    epoch_number=$1
fi

# Get current date/time in UTC
CURRENT_DATETIME_UTC=$(date -u +"%A, %B %d, %Y at %I:%M:%S %p UTC")
# Get current date/time in US Central Time (CST/CDT)
CURRENT_DATETIME_CST=$(TZ="America/Chicago" date +"%A, %B %d, %Y at %I:%M:%S %p %Z")
EXTRA_INFO="Message generated on $CURRENT_DATETIME_UTC and $CURRENT_DATETIME_CST"

# Prepare the message
message="🏁✅ Trillium API Update ✅🏁

Epoch: $epoch_number
```
Status: Data updated - COMPLETE with Jito MEV
```
For more information, visit https://trillium.so
$EXTRA_INFO"

# Escape special characters for JSON
json_message=$(echo "$message" | sed 's/"/\\"/g' | sed ':a;N;$!ba;s/\n/\\n/g')

# Send the message to Discord with URL preview suppressed and custom username/avatar
discord_response=$(curl -s -H "Content-Type: application/json" \
     -X POST \
     -d "{\"content\":\"$json_message\", \"username\":\"$DISCORD_USERNAME\", \"avatar_url\":\"$DISCORD_AVATAR_URL\", \"flags\": 4}" \
     $DISCORD_WEBHOOK_URL)

if [ -z "$discord_response" ]; then
    log_message "INFO" "Message sent to Discord channel successfully."
else
    log_message "ERROR" "Error sending message to Discord: $discord_response"
fi

# Send the message to Telegram
# config files are here:  ~/block-production/api/subscriptions/telegram
telegram_response=$(curl -s -X POST "https://api.telegram.org/bot$TELEGRAM_BOT_TOKEN/sendMessage" \
     -d chat_id="$TELEGRAM_CHAT_ID" \
     -d text="$message" \
     -d disable_web_page_preview="true")

if [[ $telegram_response == *"\"ok\":true"* ]]; then
    log_message "INFO" "Message sent to Telegram bot successfully."
else
    log_message "ERROR" "Error sending message to Telegram: $telegram_response"
fi

# Display the message that was sent
log_message "INFO" "\nThe following message was sent to Discord and Telegram:"
log_message "INFO" "$message"
