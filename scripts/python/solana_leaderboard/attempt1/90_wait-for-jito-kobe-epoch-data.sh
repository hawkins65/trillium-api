#!/bin/bash

source 999_common_log.sh
# Initialize enhanced logging
init_logging

# Discord webhook URL
DISCORD_WEBHOOK_URL="https://discord.com/api/webhooks/1267145835699769409/7b6MXomGQkOHcSi6_YLld4srbOu0CdZtC9PJqNgTbnM0KPgU8wg_SOMMMQkdBWkjAPom"
DISCORD_USERNAME="Jito Kobe Bot"
DISCORD_AVATAR_URL="https://trillium.so/pages/white_trillium_logo.png"

# Telegram bot token and chat ID
TELEGRAM_BOT_TOKEN="6487508986:AAHzmaPqTxl95g9S3CsQ6b0EQ5s20egM4yg"
TELEGRAM_CHAT_ID="6375790507"

# Target epoch
TARGET_EPOCH=$1

# If TARGET_EPOCH is not passed as a parameter, prompt the user
if [ -z "$TARGET_EPOCH" ]; then
  read -p "Please enter the target epoch number: " TARGET_EPOCH
fi

# URL to check JSON data using Trillium
url="https://kobe.mainnet.jito.network/api/v1/validators/tri1cHBy47fPyhCvrCf6FnR7Mz6XdSoSBah2FsZVQeT"

while true; do
  # Fetch the JSON data from the URL
  response=$(curl -s "$url")
  
  # Search for the target epoch in the array
  target_object=$(echo "$response" | jq ".[] | select(.epoch == $TARGET_EPOCH)")
  
  # Print the target object for debugging
  log_message "DEBUG" "Target object for epoch $TARGET_EPOCH: $target_object"
  
  # Extract the epoch and mev_rewards values from the target object
  epoch=$(echo "$target_object" | jq '.epoch')
  mev_rewards=$(echo "$target_object" | jq '.mev_rewards')
  
  # Check if the epoch exists and mev_rewards is not null
  if [[ -n "$epoch" && "$epoch" -eq "$TARGET_EPOCH" && "$mev_rewards" != "null" ]]; then
    # Get current date/time in UTC
    CURRENT_DATETIME_UTC=$(date -u +"%A, %B %d, %Y at %I:%M:%S %p UTC")
    # Get current date/time in US Central Time (CST/CDT)
    CURRENT_DATETIME_CST=$(TZ="America/Chicago" date +"%A, %B %d, %Y at %I:%M:%S %p %Z")
    EXTRA_INFO="Message generated on $CURRENT_DATETIME_UTC and $CURRENT_DATETIME_CST"

    # Prepare the message
    message="ℹ️ Trillium API Update ℹ️

    Epoch: $TARGET_EPOCH
    ```
    Status: Jito Kobe Stakenet Data is available
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

    break

  else
    log_message "INFO" "Epoch $TARGET_EPOCH not found or mev_rewards is null. Checking again in 5 minutes..."
  fi
  
  # Wait for 1000 seconds before checking again
  sleep 1000
done