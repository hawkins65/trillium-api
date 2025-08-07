#!/bin/bash

# Source path initialization
source "$(dirname "$0")/000_init_paths.sh" || {
    echo "❌ Failed to source path initialization script" >&2
    exit 1
}

# Source common logging functions
source $HOME/trillium_api/scripts/bash/999_common_log.sh

# Configuration
CHECK_INTERVAL_MINUTES=5  # Interval to check slot progress
DISCORD_WEBHOOK="https://discord.com/api/webhooks/1288850414430715914/E6XttHmMxDv15jd5YI-9ThH9JmCgKncZNUDsdjfrvHh3HIR6vzaG1KcdAaNybHV7mHMK"
HEALTHY_AVATAR="https://trillium.so/images/slots-progressing.png"
UNHEALTHY_AVATAR="https://trillium.so/images/slots-not-progressing.png"
EPOCH_CHANGE_AVATAR="https://trillium.so/images/new-epoch.png"
CHECK_SLOTS_SCRIPT="$HOME/trillium_api/scripts/python/999_slots_progressing.py"
SLOTS_JSON_FILE="$HOME/trillium_api/data/slot_progressing/999_slots_progressing.json"

# RPC configuration
DEFAULT_RPC_URL="https://api.mainnet-beta.solana.com"
FALLBACK_RPC_URL="https://wiser-young-star.solana-mainnet.quiknode.pro/887452d66f8b645b8824eab20011dbd3c315d84f/"
RPC_URL="${SOLANA_RPC_URL:-$DEFAULT_RPC_URL}"
RPC_TIMEOUT=10
RPC_MAX_RETRIES=3

SLOTS_IN_EPOCH=432000  # Total slots per epoch
SLOTS_PER_HOUR=9000    # Approximate slots per hour (432000 / 48)
SKIP_FIRST_HOURS=2     # Skip first 2 hours of each epoch
SKIP_SLOTS_THRESHOLD=$((SLOTS_PER_HOUR * SKIP_FIRST_HOURS))  # 18000 slots

# Initialize variables
PREV_SLOTS_COUNT=0
LAST_DISCORD_TIME=0
CURRENT_EPOCH=0
PREV_EPOCH=0
PREV_CHECK_TIME=0  # Track time of previous check for rate calculation
CURRENT_RPC_URL="$RPC_URL"  # Track which RPC URL we're currently using

# Function to make RPC call with failover
make_rpc_call() {
    local payload="$1"
    local attempt=1
    local response=""
    local exit_code=1
    
    # Try primary RPC URL first
    while [ $attempt -le $RPC_MAX_RETRIES ]; do
        log "INFO" "Attempting RPC call to $CURRENT_RPC_URL (attempt $attempt/$RPC_MAX_RETRIES)"
        response=$(curl -s --max-time $RPC_TIMEOUT -X POST -H "Content-Type: application/json" -d "$payload" "$CURRENT_RPC_URL")
        exit_code=$?
        
        if [ $exit_code -eq 0 ] && [ -n "$response" ] && echo "$response" | jq -e '.result' >/dev/null 2>&1; then
            echo "$response"
            return 0
        fi
        
        log "WARNING" "RPC call failed (attempt $attempt/$RPC_MAX_RETRIES): exit_code=$exit_code, response=$response"
        ((attempt++))
        if [ $attempt -le $RPC_MAX_RETRIES ]; then
            sleep 2
        fi
    done
    
    # If primary failed, try fallback RPC URL (only if it's different)
    if [ "$CURRENT_RPC_URL" != "$FALLBACK_RPC_URL" ]; then
        log "INFO" "Primary RPC failed, switching to fallback: $FALLBACK_RPC_URL"
        CURRENT_RPC_URL="$FALLBACK_RPC_URL"
        attempt=1
        
        while [ $attempt -le $RPC_MAX_RETRIES ]; do
            log "INFO" "Attempting fallback RPC call to $CURRENT_RPC_URL (attempt $attempt/$RPC_MAX_RETRIES)"
            response=$(curl -s --max-time $RPC_TIMEOUT -X POST -H "Content-Type: application/json" -d "$payload" "$CURRENT_RPC_URL")
            exit_code=$?
            
            if [ $exit_code -eq 0 ] && [ -n "$response" ] && echo "$response" | jq -e '.result' >/dev/null 2>&1; then
                echo "$response"
                return 0
            fi
            
            log "WARNING" "Fallback RPC call failed (attempt $attempt/$RPC_MAX_RETRIES): exit_code=$exit_code, response=$response"
            ((attempt++))
            if [ $attempt -le $RPC_MAX_RETRIES ]; then
                sleep 2
            fi
        done
    fi
    
    # Both primary and fallback failed
    log "ERROR" "All RPC endpoints failed after $RPC_MAX_RETRIES attempts each"
    return 1
}

log "INFO" "Starting slots progress monitor at $(date -u --iso-8601=seconds)"
log "INFO" "Primary RPC URL: $RPC_URL"
log "INFO" "Fallback RPC URL: $FALLBACK_RPC_URL"
log "INFO" "Will skip monitoring first $SKIP_FIRST_HOURS hours ($SKIP_SLOTS_THRESHOLD slots) of each new epoch (assuming 48-hour epochs)"

while true; do
    START_TIME=$(date +%s)
    
    # Get current epoch
    PAYLOAD='{"jsonrpc":"2.0","id":1,"method":"getEpochInfo","params":[null]}'
    RESPONSE=$(make_rpc_call "$PAYLOAD")
    if [ $? -ne 0 ]; then
        log "ERROR" "Failed to retrieve epoch info from all RPC endpoints"
        if [[ -x "$HOME/trillium_api/999_pagerduty.sh" ]]; then
            "$HOME/trillium_api/999_pagerduty.sh" \
                --severity error \
                --source "$(hostname)" \
                --details "{\"script\": \"$(basename "$0")\", \"timestamp\": \"$(date -u --iso-8601=seconds)\", \"error\": \"Failed to retrieve epoch info from all RPC endpoints\", \"primary_rpc\": \"$RPC_URL\", \"fallback_rpc\": \"$FALLBACK_RPC_URL\"}" \
                "Failed to retrieve epoch info from all RPC endpoints"
        else
            log "WARNING" "PagerDuty script not found at $HOME/trillium_api/999_pagerduty.sh"
        fi
        sleep 60
        continue
    fi
    
    CURRENT_EPOCH=$(echo "$RESPONSE" | jq -r '.result.epoch')
    if [ -z "$CURRENT_EPOCH" ] || [ "$CURRENT_EPOCH" = "null" ]; then
        log "ERROR" "Invalid epoch info received: $RESPONSE"
        if [[ -x "$HOME/trillium_api/999_pagerduty.sh" ]]; then
            "$HOME/trillium_api/999_pagerduty.sh" \
                --severity error \
                --source "$(hostname)" \
                --details "{\"script\": \"$(basename "$0")\", \"timestamp\": \"$(date -u --iso-8601=seconds)\", \"response\": \"$RESPONSE\", \"rpc_url\": \"$CURRENT_RPC_URL\"}" \
                "Invalid epoch info received"
        else
            log "WARNING" "PagerDuty script not found at $HOME/trillium_api/999_pagerduty.sh"
        fi
        sleep 60
        continue
    fi
    log "INFO" "Current epoch: $CURRENT_EPOCH (via $CURRENT_RPC_URL)"

    # Check for epoch advancement
    if [ $PREV_EPOCH -ne 0 ] && [ $CURRENT_EPOCH -gt $PREV_EPOCH ]; then
        log "INFO" "Epoch advanced from $PREV_EPOCH to $CURRENT_EPOCH"
        curl -X POST -H 'Content-Type: application/json' \
             -d "{\"content\": \"Epoch advanced from $PREV_EPOCH to $CURRENT_EPOCH. Slot collection monitoring will start after first $SKIP_FIRST_HOURS hours ($SKIP_SLOTS_THRESHOLD slots).\", \"avatar_url\": \"$EPOCH_CHANGE_AVATAR\"}" \
             "$DISCORD_WEBHOOK"
        log "INFO" "Sent Discord epoch change notification"
        PREV_SLOTS_COUNT=0  # Reset slot count for new epoch
        PREV_CHECK_TIME=0   # Reset time tracking for new epoch
        # Reset to primary RPC URL for new epoch
        CURRENT_RPC_URL="$RPC_URL"
    fi
    PREV_EPOCH=$CURRENT_EPOCH

    # Check slot collection progress
    python3 "$CHECK_SLOTS_SCRIPT" "$CURRENT_EPOCH"
    EXIT_STATUS=$?
    if [ $EXIT_STATUS -eq 0 ]; then
        if [ -f "$SLOTS_JSON_FILE" ]; then
            SLOTS_COUNT=$(jq -r '.slots_count' "$SLOTS_JSON_FILE")
            JSON_EPOCH=$(jq -r '.epoch' "$SLOTS_JSON_FILE")
            if [ "$JSON_EPOCH" != "$CURRENT_EPOCH" ]; then
                log "WARNING" "JSON file epoch ($JSON_EPOCH) does not match current epoch ($CURRENT_EPOCH)"
                HEALTH_STATUS="unhealthy"
            elif [ -z "$SLOTS_COUNT" ] || [ "$SLOTS_COUNT" = "null" ]; then
                log "ERROR" "Invalid slot count in $SLOTS_JSON_FILE"
                HEALTH_STATUS="unhealthy"
                # Only send PagerDuty alert if we're past the skip threshold
                if [ $SLOTS_COUNT -gt $SKIP_SLOTS_THRESHOLD ]; then
                    if [[ -x "$HOME/trillium_api/999_pagerduty.sh" ]]; then
                        "$HOME/trillium_api/999_pagerduty.sh" \
                            --severity error \
                            --source "$(hostname)" \
                            --details "{\"script\": \"$(basename "$CHECK_SLOTS_SCRIPT")\", \"epoch\": \"$CURRENT_EPOCH\", \"timestamp\": \"$(date -u --iso-8601=seconds)\", \"error\": \"Invalid slot count in JSON file\"}" \
                            "Invalid slot count in JSON file for epoch $CURRENT_EPOCH"
                    else
                        log "WARNING" "PagerDuty script not found at $HOME/trillium_api/999_pagerduty.sh"
                    fi
                else
                    log "INFO" "Skipping PagerDuty alert for invalid slot count - still in first $SKIP_FIRST_HOURS hours of epoch"
                fi
            else
                PERCENT_COMPLETE=$(( (SLOTS_COUNT * 100) / SLOTS_IN_EPOCH ))
                
                # Calculate collection rate
                RATE_INFO=""
                if [ $PREV_SLOTS_COUNT -gt 0 ] && [ $PREV_CHECK_TIME -gt 0 ]; then
                    SLOTS_INCREASE=$((SLOTS_COUNT - PREV_SLOTS_COUNT))
                    TIME_ELAPSED=$((START_TIME - PREV_CHECK_TIME))
                    
                    if [ $TIME_ELAPSED -gt 0 ]; then
                        # Calculate rate in slots/second with 2 decimal places
                        RATE=$(echo "scale=2; $SLOTS_INCREASE / $TIME_ELAPSED" | bc -l)
                        RATE_INFO=" (${RATE} slots/s)"
                    fi
                fi

                log "INFO" "Current collected slots for epoch $CURRENT_EPOCH: $SLOTS_COUNT/$SLOTS_IN_EPOCH ($PERCENT_COMPLETE%)$RATE_INFO"

                # Skip monitoring if we're in the first 2 hours of the epoch
                if [ $SLOTS_COUNT -le $SKIP_SLOTS_THRESHOLD ]; then
                    log "INFO" "Skipping monitoring: Still in first $SKIP_FIRST_HOURS hours of epoch $CURRENT_EPOCH ($SLOTS_COUNT/$SKIP_SLOTS_THRESHOLD slots)"
                    HEALTH_STATUS="skipping"
                else
                    # Normal monitoring logic starts here (after first 2 hours)
                    
                    # Calculate the slot threshold for the first 4% of the epoch (keeping original logic for comparison)
                    FIRST_FOUR_PERCENT_THRESHOLD=$((SLOTS_IN_EPOCH * 4 / 100))

                    if [ $SLOTS_COUNT -gt $PREV_SLOTS_COUNT ]; then
                        HEALTH_STATUS="healthy"
                        # Calculate percentage increase
                        if [ $PREV_SLOTS_COUNT -gt 0 ]; then
                            INCREASE=$((SLOTS_COUNT - PREV_SLOTS_COUNT))
                            PERCENT_INCREASE=$(( (INCREASE * 100) / PREV_SLOTS_COUNT ))
                            log "INFO" "Healthy: Slot count increased from $PREV_SLOTS_COUNT to $SLOTS_COUNT (+$INCREASE slots$RATE_INFO, +$PERCENT_INCREASE%, $PERCENT_COMPLETE% complete)"
                        else
                            log "INFO" "Healthy: Slot count increased from $PREV_SLOTS_COUNT to $SLOTS_COUNT ($PERCENT_COMPLETE% complete)"
                        fi
                    else
                        HEALTH_STATUS="unhealthy"
                        log "WARNING" "Unhealthy: Slot count did not increase (previous: $PREV_SLOTS_COUNT, current: $SLOTS_COUNT, $PERCENT_COMPLETE% complete)"
                        if [[ -x "$HOME/trillium_api/999_pagerduty.sh" ]]; then
                            "$HOME/trillium_api/999_pagerduty.sh" \
                                --severity warning \
                                --source "$(hostname)" \
                                --details "{\"prev_slots\": $PREV_SLOTS_COUNT, \"current_slots\": $SLOTS_COUNT, \"percent_complete\": $PERCENT_COMPLETE, \"epoch\": \"$CURRENT_EPOCH\", \"timestamp\": \"$(date -u --iso-8601=seconds)\"}" \
                                "Slot collection not progressing for epoch $CURRENT_EPOCH"
                        else
                            log "WARNING" "PagerDuty script not found at $HOME/trillium_api/999_pagerduty.sh"
                        fi
                    fi
                fi

                # Update tracking variables (always update these)
                PREV_SLOTS_COUNT=$SLOTS_COUNT
                PREV_CHECK_TIME=$START_TIME

                # Check for epoch completion
                if [ $SLOTS_COUNT -ge $SLOTS_IN_EPOCH ]; then
                    log "INFO" "All slots ($SLOTS_COUNT/$SLOTS_IN_EPOCH, 100%) collected for epoch $CURRENT_EPOCH"
                    curl -X POST -H 'Content-Type: application/json' \
                         -d "{\"content\": \"Epoch $CURRENT_EPOCH: All slots ($SLOTS_COUNT/$SLOTS_IN_EPOCH, 100%) collected. Waiting for next epoch.\", \"avatar_url\": \"$HEALTHY_AVATAR\"}" \
                         "$DISCORD_WEBHOOK"
                    log "INFO" "Sent Discord epoch completion notification"
                fi
            fi
        else
            log "ERROR" "JSON file $SLOTS_JSON_FILE not found"
            HEALTH_STATUS="unhealthy"
            # For missing JSON file, we can't determine slot count, so check if we should skip based on epoch newness
            # We'll assume if the epoch directory doesn't exist, we're likely in the early phase
            log "INFO" "Skipping PagerDuty alert for missing JSON file - likely in first $SKIP_FIRST_HOURS hours of new epoch $CURRENT_EPOCH"
        fi
    else
        log "ERROR" "Failed to run $CHECK_SLOTS_SCRIPT for epoch $CURRENT_EPOCH"
        HEALTH_STATUS="unhealthy"
        # For script failures, we can't determine slot count, so check if we should skip based on epoch newness
        # We'll assume if the epoch directory doesn't exist, we're likely in the early phase
        log "INFO" "Skipping PagerDuty alert for Python script failure - likely in first $SKIP_FIRST_HOURS hours of new epoch $CURRENT_EPOCH"
    fi

    # Send Discord message at the top of every hour (but skip if we're in the first 2 hours)
    CURRENT_HOUR=$(date -u +%H)
    CURRENT_MINUTE=$(date -u +%M)
    if [ $CURRENT_MINUTE -lt $CHECK_INTERVAL_MINUTES ] && [ "$CURRENT_HOUR" != "$LAST_DISCORD_HOUR" ]; then
        if [ "$HEALTH_STATUS" = "skipping" ]; then
            MESSAGE="Epoch $CURRENT_EPOCH: Skipping monitoring for first $SKIP_FIRST_HOURS hours. Current slots: $SLOTS_COUNT/$SKIP_SLOTS_THRESHOLD (monitoring starts at $SKIP_SLOTS_THRESHOLD slots, ~48hr epochs)"
            AVATAR="$HEALTHY_AVATAR"
        elif [ "$HEALTH_STATUS" = "healthy" ]; then
            MESSAGE="Epoch $CURRENT_EPOCH: Slot collection is progressing. Current slots collected: $SLOTS_COUNT/$SLOTS_IN_EPOCH ($PERCENT_COMPLETE%)$RATE_INFO"
            AVATAR="$HEALTHY_AVATAR"
        else
            MESSAGE="Epoch $CURRENT_EPOCH: Slot collection is not progressing. Current slots collected: $SLOTS_COUNT/$SLOTS_IN_EPOCH ($PERCENT_COMPLETE%)"
            AVATAR="$UNHEALTHY_AVATAR"
        fi
        curl -X POST -H 'Content-Type: application/json' \
             -d "{\"content\": \"$MESSAGE\", \"avatar_url\": \"$AVATAR\"}" \
             "$DISCORD_WEBHOOK"
        LAST_DISCORD_HOUR=$CURRENT_HOUR
        log "INFO" "Sent Discord message at top of hour: $MESSAGE"
    fi

    # Wait until the next interval
    ELAPSED=$(( $(date +%s) - START_TIME ))
    SLEEP_TIME=$(( CHECK_INTERVAL_MINUTES * 60 - ELAPSED ))
    if [ $SLEEP_TIME -gt 0 ]; then
        log "INFO" "Waiting $SLEEP_TIME seconds until next check..."
        sleep $SLEEP_TIME
    fi
    log "INFO" "----------------------------------------"
done
