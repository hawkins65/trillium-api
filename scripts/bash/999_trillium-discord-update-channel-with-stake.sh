#!/bin/bash

# This script requires curl and jq to fetch and parse JSON data.
# Installation instructions for Ubuntu:
# sudo apt-get update && sudo apt-get install -y curl jq

# Define constant
SERVER_ID='1205527089856512100'

# Define validator identity
VALIDATOR_IDENTITY='Tri1F8B6YtjkBztGCwBNSLEZib1EAqMUEUM7dTT7ZG3'

# Define arrays for all metrics and their corresponding Discord channel IDs
METRICS=(
    'activated_stake'
    'avg_cu_per_block'
    'avg_mev_per_block'
    'avg_priority_fees_per_block'
    'avg_user_tx_per_block'
    'avg_vote_tx_per_block'
    'blocks_produced'
    'delegator_compound_total_apy'
    'epoch'
    'epoch_credits'
    'jito_steward_overall_rank'
    'max_vote_latency'
    'mean_vote_latency'
    'mev_to_stakers'
    'mev_to_validator'
    'rewards'
    'skip_rate'
    'slot_duration_is_lagging'
    'slot_duration_max'
    'slot_duration_mean'
    'slot_duration_min'
    'validator_compound_total_apy'
    'vote_cost'
    'vote_credits_rank'
)

CHANNEL_IDS=(
    '1389654931463475210'
    '1399205685685784679'
    '1399205851864240230'
    '1399205880519589909'
    '1399205899981164665'
    '1399205921535692860'
    '1399205948207267910'
    '1399205970915229696'
    '1399205999570587833'
    '1399206024103198773'
    '1399206044382789725'
    '1399206082362085509'
    '1399206115362865202'
    '1399206165153317028'
    '1399206188645744761'
    '1399206212515528714'
    '1399206245117726760'
    '1399206310225907752'
    '1399206282023538848'
    '1399206333882040344'
    '1399206354719215626'
    '1399206371982970900'
    '1399206393881296966'
    '1399206414060359801'
)

# Function to fetch validator data from the Trillium API
fetch_validator_data() {
    local validator_identity="$1"
    local api_response
    api_response=$(curl -s "https://api.trillium.so/validator_rewards/$validator_identity")
    
    if [ $? -ne 0 ] || [ -z "$api_response" ]; then
        echo "Error: Failed to fetch data from Trillium API for $validator_identity" >&2
        return 1
    fi
    
    # Get the first epoch record (most recent)
    local first_epoch_data
    first_epoch_data=$(echo "$api_response" | jq '.[0]')
    
    if [ "$first_epoch_data" = "null" ] || [ -z "$first_epoch_data" ]; then
        echo "Error: No epoch data found in API response" >&2
        return 1
    fi
    
    echo "$first_epoch_data"
}

# Function to extract specific metric value from JSON data
get_metric_value() {
    local json_data="$1"
    local metric_key="$2"
    local value
    
    value=$(echo "$json_data" | jq -r ".$metric_key")
    
    if [ "$value" = "null" ]; then
        echo "N/A"
    else
        echo "$value"
    fi
}

# Function to format a number with thousands separators (for integers)
format_with_commas() {
    local number="$1"
    # Check if it's a whole number
    if [[ "$number" =~ ^[0-9]+$ ]]; then
        LC_NUMERIC=en_US.UTF-8 printf "%'d" "$number"
    else
        echo "$number"
    fi
}

# Function to format metric name for display
format_metric_name() {
    local metric="$1"
    # Convert underscores to spaces and capitalize first letter of each word
    echo "$metric" | sed 's/_/ /g' | sed 's/\b\(.\)/\u\1/g'
}

# Function to update the Discord channel name with retry logic
update_channel_name() {
    local server_id="$1"
    local channel_id="$2"
    local metric_name="$3"
    local metric_value="$4"
    
    # Format the metric name for display
    local formatted_metric_name
    formatted_metric_name=$(format_metric_name "$metric_name")
    
    # Format numeric values with commas if they're integers
    local formatted_value
    if [[ "$metric_value" =~ ^[0-9]+$ ]]; then
        formatted_value=$(format_with_commas "$metric_value")
    else
        formatted_value="$metric_value"
    fi
    
    local channel_name="${formatted_metric_name}: ${formatted_value}"
    
    echo "Debug: Updating channel for $metric_name: $channel_name"
    
    # Retry logic for rate limiting
    local max_retries=3
    local retry_count=0
    
    while [ $retry_count -lt $max_retries ]; do
        if python trillium-discord-update-channel-with-stake.py \
            --server-id "$server_id" \
            --channel-id "$channel_id" \
            --channel-name "$channel_name"; then
            echo "Successfully updated channel $channel_id"
            return 0
        else
            retry_count=$((retry_count + 1))
            if [ $retry_count -lt $max_retries ]; then
                echo "Update failed for channel $channel_id, retrying in 5 seconds... (attempt $retry_count/$max_retries)"
                sleep 5
            else
                echo "Error: Failed to update channel $channel_id after $max_retries attempts"
                return 1
            fi
        fi
    done
}

# Main execution
if [ ${#METRICS[@]} -ne ${#CHANNEL_IDS[@]} ]; then
    echo "Error: The number of METRICS and CHANNEL_IDS must match" >&2
    echo "METRICS count: ${#METRICS[@]}, CHANNEL_IDS count: ${#CHANNEL_IDS[@]}" >&2
    exit 1
fi

# Fetch validator data once
echo "Fetching validator data for $VALIDATOR_IDENTITY..."
validator_data=$(fetch_validator_data "$VALIDATOR_IDENTITY")

if [ $? -ne 0 ]; then
    echo "Failed to fetch validator data. Exiting." >&2
    exit 1
fi

echo "Successfully fetched validator data. Updating channels..."

# Update channels for all metrics
for ((i=0; i<${#METRICS[@]}; i++)); do
    metric="${METRICS[$i]}"
    channel_id="${CHANNEL_IDS[$i]}"
    
    # Get the metric value from the JSON data
    metric_value=$(get_metric_value "$validator_data" "$metric")
    
    if [ "$metric_value" != "N/A" ]; then
        update_channel_name "$SERVER_ID" "$channel_id" "$metric" "$metric_value"
        # Add a longer delay to avoid rate limiting (Discord allows ~5 requests per 5 seconds for channel updates)
        echo "Waiting 2 seconds before next update..."
        sleep 2
    else
        echo "Warning: Metric '$metric' not found in data, skipping channel update" >&2
    fi
done

echo "Channel updates completed!"