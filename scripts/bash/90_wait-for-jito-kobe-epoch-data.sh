#!/bin/bash

# Source the common logging functions
source /home/smilax/api/999_common_log.sh
# Initialize enhanced logging
init_logging

# Get the basename of the script for Discord notifications
script_name=$(basename "$0")

# Target epoch
TARGET_EPOCH=$1

# If TARGET_EPOCH is not passed as a parameter, prompt the user
if [ -z "$TARGET_EPOCH" ]; then
  read -p "Please enter the target epoch number: " TARGET_EPOCH
fi

log "INFO" "🎯 Starting Jito Kobe epoch data monitoring for epoch $TARGET_EPOCH"

# URL to check JSON data using Trillium
url="https://kobe.mainnet.jito.network/api/v1/validators/tri1cHBy47fPyhCvrCf6FnR7Mz6XdSoSBah2FsZVQeT"

log "INFO" "🔍 Monitoring URL: $url"

while true; do
  log "INFO" "📡 Fetching JSON data from Jito Kobe API..."
  
  # Fetch the JSON data from the URL
  response=$(curl -s "$url")
  
  if [ $? -ne 0 ]; then
    log "ERROR" "❌ Failed to fetch data from Jito Kobe API"
    log "INFO" "⏳ Retrying in 5 minutes..."
    sleep 300
    continue
  fi
  
  # Search for the target epoch in the array
  target_object=$(echo "$response" | jq ".[] | select(.epoch == $TARGET_EPOCH)")
  
  # Print the target object for debugging
  log "INFO" "🔍 Target object for epoch $TARGET_EPOCH: $target_object"
  
  # Extract the epoch and mev_rewards values from the target object
  epoch=$(echo "$target_object" | jq '.epoch')
  mev_rewards=$(echo "$target_object" | jq '.mev_rewards')
  
  # Check if the epoch exists and mev_rewards is not null
  if [[ -n "$epoch" && "$epoch" -eq "$TARGET_EPOCH" && "$mev_rewards" != "null" ]]; then
    log "INFO" "✅ Found epoch $TARGET_EPOCH with MEV rewards: $mev_rewards"
    log "INFO" "📢 Sending Jito Kobe available notification..."
    
    # Send Jito Kobe available notification using centralized public notification script
    if bash 999_public_discord_notify.sh jito_kobe_available "$TARGET_EPOCH"; then
      log "INFO" "✅ Successfully sent notification for epoch $TARGET_EPOCH"
    else
      log "ERROR" "❌ Failed to send notification for epoch $TARGET_EPOCH"
    fi
    
    log "INFO" "🎉 Monitoring completed successfully for epoch $TARGET_EPOCH"
    break
  else
    log "INFO" "⏳ Epoch $TARGET_EPOCH not found or mev_rewards is null. Checking again in 1000 seconds..."
  fi
  
  # Wait for 1000 seconds before checking again
  sleep 1000
done

log "INFO" "🏁 Script completed for epoch $TARGET_EPOCH"
