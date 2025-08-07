#!/bin/bash

# Define paths for Solana CLI and validator-history-cli
SOLANA_CLI="/home/smilax/agave/bin/solana"
VALIDATOR_HISTORY_CLI="/home/smilax/stakenet/target/release/validator-history-cli"
DEFAULT_PUBKEY="tri1cHBy47fPyhCvrCf6FnR7Mz6XdSoSBah2FsZVQeT"
DEFAULT_PUBKEY_LENGTH=${#DEFAULT_PUBKEY}  # Compute length of DEFAULT_PUBKEY
TESTNET_RPC_URL="https://wiser-thrilling-reel.solana-testnet.quiknode.pro/d05bbe3aa7a9377d63a89a869a3fba1093555029/"
MAINNET_RPC_URL="https://wiser-young-star.solana-mainnet.quiknode.pro/887452d66f8b645b8824eab20011dbd3c315d84f/"
RPC_URL="$MAINNET_RPC_URL"

# Function to map alias to pubkey
map_pubkey() {
    case "$1" in
        trillium)
            echo "tri1cHBy47fPyhCvrCf6FnR7Mz6XdSoSBah2FsZVQeT"
            ;;
        ofv)
            echo "oRAnGeU5h8h2UkvbfnE5cjXnnAa4rBoaxmS4kbFymSe"
            ;;
        laine)
            echo "GE6atKoWiQ2pt3zL7N13pjNHjdLVys8LinG8qeJLcAiL"
            ;;
        cogent)
            echo "CogentC52e7kktFfWHwsqSmr8LiS1yAtfqhHcftCPcBJ"
            ;;
        ss)
            echo "punK4RDD3pFbcum79ACHatYPLLE1hr5UNnQVUGNfeyP"
            ;;
        pengu)
            echo "pENgUh4K9zNacyU3PXVE9KugW98XCqZsWpEvA8d8wzX"
            ;;
        *)
            echo ""
            ;;
    esac
}

# Function to map vote pubkey to identity pubkey using API
map_vote_to_identity() {
    local vote_pubkey="$1"
    local api_url="https://api.trillium.so/validator_rewards/$vote_pubkey"
    
    # Fetch JSON data using curl with timeout and retry
    local response=$(curl -s --max-time 10 --retry 2 --retry-delay 1 "$api_url")
    if [ $? -ne 0 ]; then
        echo "Error: Failed to fetch data from $api_url" >&2
        return 1
    fi
    
    # Check if response is empty
    if [ -z "$response" ]; then
        echo "Error: Empty response from $api_url" >&2
        return 1
    fi
    
    # Log raw API response for debugging
    #echo "API Response for $vote_pubkey: $response" >&2
    
    # Parse JSON to extract identity_pubkey
    local identity_pubkey=$(echo "$response" | jq -r '
        # Check if response is an object with an "error" field
        if type == "object" and has("error") then
            empty
        else
            # If input is an array, take the first element; if it is an object, use it directly
            (if type == "array" then .[0] else . end) | 
            .identity_pubkey // empty
        end
    ')
    
    # Check if jq parsing was successful
    if [ $? -ne 0 ]; then
        echo "Error: Failed to parse JSON response for pubkey $vote_pubkey" >&2
        return 1
    fi
    
    # Check if identity_pubkey is empty
    if [ -z "$identity_pubkey" ]; then
        echo "Error: No identity pubkey found for vote pubkey $vote_pubkey" >&2
        return 1
    fi
    
    echo "$identity_pubkey"
    return 0
}

# Determine the pubkey based on the first parameter
if [ $# -ge 1 ]; then
    INPUT_PARAM="$1"
    INPUT_LENGTH=${#INPUT_PARAM}
    
    if [ "$INPUT_LENGTH" -lt "$DEFAULT_PUBKEY_LENGTH" ]; then
        # Try to map the input as an alias
        MAPPED_PUBKEY=$(map_pubkey "$INPUT_PARAM")
        if [ -n "$MAPPED_PUBKEY" ]; then
            VOTE_PUBKEY="$MAPPED_PUBKEY"
        else
            echo "Error: Invalid alias '$INPUT_PARAM'. Valid aliases are: trillium, ofv, laine, cogent, ss, pengu" >&2
            exit 1
        fi
    else
        # Use the input as a custom pubkey
        VOTE_PUBKEY="$INPUT_PARAM"
    fi
else
    VOTE_PUBKEY="$DEFAULT_PUBKEY"
fi

# Check if epoch number is provided as the second argument
if [ $# -ge 2 ]; then
    CURRENT_EPOCH="$2"
else
    # Retrieve the current epoch using the defined Solana CLI path
    CURRENT_EPOCH=$("$SOLANA_CLI" epoch --url "$RPC_URL")
    # Check if the epoch was retrieved successfully
    if [ -z "$CURRENT_EPOCH" ]; then
        echo "Error: Failed to retrieve the current epoch." >&2
        exit 1
    fi
fi

# Map vote pubkey to validator identity using the API
VALIDATOR_IDENTITY=$(map_vote_to_identity "$VOTE_PUBKEY")
if [ $? -ne 0 ]; then
    echo "Error: Failed to map vote pubkey '$VOTE_PUBKEY' to validator identity." >&2
    exit 1
fi
echo "Validator identity for vote pubkey $VOTE_PUBKEY: $VALIDATOR_IDENTITY"

# Retrieve epoch details to get current slot
EPOCH_DETAILS=$("$SOLANA_CLI" --url "$RPC_URL" epoch-info)
if [ -z "$EPOCH_DETAILS" ]; then
    echo "Error: Failed to retrieve epoch details." >&2
    exit 1
fi

EPOCH_CURRENT_SLOT=$(echo "$EPOCH_DETAILS" | grep ^Slot: | awk '{ print $2 }')
if [ -z "$EPOCH_CURRENT_SLOT" ]; then
    echo "Error: Failed to parse current slot from epoch details." >&2
    exit 1
fi

# Check leader schedule for the validator
LEADER_SCHEDULE=$("$SOLANA_CLI" --url "$RPC_URL" leader-schedule --no-address-labels | grep "$VALIDATOR_IDENTITY" | awk '{ print $1 }')
if [ -z "$LEADER_SCHEDULE" ]; then
    echo "No leader slots found for validator $VALIDATOR_IDENTITY in epoch $CURRENT_EPOCH. Skipping MEV Commission check."
    exit 0
fi

# Check if any leader slot has occurred (slot <= current slot)
HAS_LEADER_SLOT=0
for SLOT in $LEADER_SCHEDULE; do
    if [ "$SLOT" -le "$EPOCH_CURRENT_SLOT" ]; then
        HAS_LEADER_SLOT=1
        break
    fi
done

if [ "$HAS_LEADER_SLOT" -eq 0 ]; then
    echo "No leader slots have occurred yet for validator $VALIDATOR_IDENTITY in epoch $CURRENT_EPOCH. Skipping MEV Commission check."
    exit 0
fi

# Run the validator-history-cli command and capture output
echo "$VALIDATOR_HISTORY_CLI --json-rpc-url $RPC_URL history --print-json --start-epoch $CURRENT_EPOCH $VOTE_PUBKEY"

VALIDATOR_OUTPUT=$("$VALIDATOR_HISTORY_CLI" --json-rpc-url "$RPC_URL" history --start-epoch "$CURRENT_EPOCH" "$VOTE_PUBKEY" 2>&1)

# Log the validator output for debugging
echo "Validator Output:"
echo "$VALIDATOR_OUTPUT"
echo "----------------------------------------"

# Check if MEV Commission is explicitly null or [NULL]
if echo "$VALIDATOR_OUTPUT" | grep -Eiq "MEV Commission:[[:space:]]*(null|\[NULL\])[[:space:]]*\|"; then
    # Define the message file name
    message_file="/home/smilax/api/trillium_message.txt"

    # Create the message file with the specified content
    cat > "$message_file" << EOF
🔔 Stakenet Validator History MEV Commission NULL 🔔
Status: Stakenet Validator History MEV Commission NULL for Epoch $CURRENT_EPOCH for validator $VOTE_PUBKEY
Validator Output:
$VALIDATOR_OUTPUT
For more information, visit $message_file
EOF

    # Verify that the message file was created successfully
    if [ ! -f "$message_file" ] || [ ! -r "$message_file" ]; then
        echo "Error: Failed to create message file '$message_file'." >&2
        exit 1
    fi

    # Display the message file content on the console
    echo "Message file content:"
    cat "$message_file"
    echo "----------------------------------------"

    # Set environment variables for Discord customization
    export DISCORD_USERNAME="StakenetBot"
    export DISCORD_AVATAR_URL="https://trillium.so/images/stakenet_null.png"

    # Check if trillium_alert.sh exists and is executable
    if [ ! -f "/home/smilax/api/trillium_alert.sh" ] || [ ! -x "/home/smilax/api/trillium_alert.sh" ]; then
        echo "Error: 'trillium_alert.sh' does not exist or is not executable." >&2
        exit 1
    fi

    # Call trillium_alert.sh with the message file and epoch number
    /home/smilax/api/trillium_alert.sh "$message_file" "$CURRENT_EPOCH"
    ALERT_STATUS=$?

    # Check if trillium_alert.sh executed successfully
    if [ $ALERT_STATUS -eq 0 ]; then
        echo "trillium_alert.sh executed successfully for epoch $CURRENT_EPOCH."
    else
        echo "Error: trillium_alert.sh failed with status $ALERT_STATUS." >&2
        exit 1
    fi

    # Display confirmation
    echo "Message file '$message_file' created and Discord alert sent for epoch $CURRENT_EPOCH."
else
    echo "Validator history output does not indicate MEV Commission is null. No alert triggered."
fi