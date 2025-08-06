#!/bin/bash

# Source path initialization
source "$(dirname "$0")/000_init_paths.sh" || {
    echo "❌ Failed to source path initialization script" >&2
    exit 1
}

# 999_public_discord_notify.sh - Public Discord API status notification script
# Usage: bash 999_public_discord_notify.sh <message_type> <epoch_number> [additional_parameters...]

set -euo pipefail

# Source the common logging functions if available
if [[ -f "common_log.sh" ]]; then
    source $TRILLIUM_SCRIPTS_BASH/999_common_log.sh
    # Initialize enhanced logging
    init_logging
else
    # Fallback logging function if common_log.sh is not available
    log() {
        local level="$1"
        local message="$2"
        echo "$(date '+%Y-%m-%d %H:%M:%S') [$level] $message"
    }
fi

# Public Discord webhook configuration (for end users)
PUBLIC_DISCORD_WEBHOOK_URL="https://discord.com/api/webhooks/1267145835699769409/7b6MXomGQkOHcSi6_YLld4srbOu0CdZtC9PJqNgTbnM0KPgU8wg_SOMMMQkdBWkjAPom"

# Telegram configuration
TELEGRAM_BOT_TOKEN="6487508986:AAHzmaPqTxl95g9S3CsQ6b0EQ5s20egM4yg"
TELEGRAM_CHAT_ID="6375790507"

# Default configuration
DEFAULT_USERNAME="Trillium API Bot"
DEFAULT_AVATAR_URL="https://trillium.so/pages/white_trillium_logo.png"

# Function to generate timestamps
generate_timestamps() {
    local current_datetime_utc=$(date -u +"%A, %B %d, %Y at %I:%M:%S %p UTC")
    local current_datetime_cst=$(TZ="America/Chicago" date +"%A, %B %d, %Y at %I:%M:%S %p %Z")
    echo "Message generated on $current_datetime_utc and $current_datetime_cst"
}

# Function to escape JSON strings
escape_json() {
    local input="$1"
    echo "$input" | sed 's/"/\\"/g' | sed ':a;N;$!ba;s/\n/\\n/g'
}

# Function to send Discord message
send_discord_message() {
    local message="$1"
    local username="${2:-$DEFAULT_USERNAME}"
    local avatar_url="${3:-$DEFAULT_AVATAR_URL}"
    
    # Escape the message for JSON
    local json_message=$(escape_json "$message")
    
    # Send to Discord
    local discord_response=$(curl -s -H "Content-Type: application/json" \
         -X POST \
         -d "{\"content\":\"$json_message\", \"username\":\"$username\", \"avatar_url\":\"$avatar_url\", \"flags\": 4}" \
         "$PUBLIC_DISCORD_WEBHOOK_URL")

    if [ -z "$discord_response" ]; then
        echo "Message sent to Discord channel successfully."
        return 0
    else
        echo "Error sending message to Discord: $discord_response"
        return 1
    fi
}

# Function to send Telegram message
send_telegram_message() {
    local message="$1"
    
    local telegram_response=$(curl -s -X POST "https://api.telegram.org/bot$TELEGRAM_BOT_TOKEN/sendMessage" \
         -d chat_id="$TELEGRAM_CHAT_ID" \
         -d text="$message" \
         -d disable_web_page_preview="true")

    if [[ $telegram_response == *"\"ok\":true"* ]]; then
        echo "Message sent to Telegram bot successfully."
        return 0
    else
        echo "Error sending message to Telegram: $telegram_response"
        return 1
    fi
}

# Function to send both Discord and Telegram
send_public_notification() {
    local message="$1"
    local username="${2:-$DEFAULT_USERNAME}"
    local avatar_url="${3:-$DEFAULT_AVATAR_URL}"
    
    echo -e "\nThe following message was sent to Discord and Telegram:"
    echo -e "$message"
    
    send_discord_message "$message" "$username" "$avatar_url"
    send_telegram_message "$message"
}

# Function for data collection ended status
send_data_collection_ended() {
    local epoch_number="$1"
    local extra_info=$(generate_timestamps)
    
    local message="🔚 Trillium API Update 🔚

Epoch: $epoch_number
\`\`\`
Status: Epoch Data Collection Ended, Continuing to Process the Epoch Data
\`\`\`

For more information, visit https://trillium.so

$extra_info"

    send_public_notification "$message"
}

# Function for data mostly updated (no Jito MEV yet)
send_data_mostly_updated() {
    local epoch_number="$1"
    local extra_info=$(generate_timestamps)
    
    local message="⏳ Trillium API Update ⏳

Epoch: $epoch_number
\`\`\`
Status: Data MOSTLY updated -- Jito MEV data NOT available yet - stay tuned
\`\`\`
For more information, visit https://trillium.so
$extra_info"

    send_public_notification "$message"
}

# Function for data complete with Jito MEV
send_data_complete() {
    local epoch_number="$1"
    local extra_info=$(generate_timestamps)
    
    local message="🏁✅ Trillium API Update ✅🏁

Epoch: $epoch_number
\`\`\`
Status: Data updated - COMPLETE with Jito MEV
\`\`\`
For more information, visit https://trillium.so
$extra_info"

    send_public_notification "$message"
}

# Function for Jito Kobe data available
send_jito_kobe_available() {
    local epoch_number="$1"
    local extra_info=$(generate_timestamps)
    
    local message="ℹ️ Trillium API Update ℹ️

Epoch: $epoch_number
\`\`\`
Status: Jito Kobe Stakenet Data is available
\`\`\`
For more information, visit https://trillium.so
$extra_info"

    send_public_notification "$message"
}

# Function for missing Kobe API data
send_missing_kobe_data() {
    local epoch_number="$1"
    local extra_info=$(generate_timestamps)
    
    local message="🔄 Trillium API Update 🔄

Epoch: $epoch_number
\`\`\`
Status: Jito Kobe API MEV data NOT available yet - using less accurate Jito Stakenet Validator History MEV values
\`\`\`
For more information, visit https://trillium.so
$extra_info"

    send_public_notification "$message"
}

# Function for custom public status update
send_custom_status() {
    local epoch_number="$1"
    local status_message="$2"
    local emoji="${3:-📊}"
    local extra_info=$(generate_timestamps)
    
    local message="$emoji Trillium API Update $emoji

Epoch: $epoch_number
\`\`\`
Status: $status_message
\`\`\`
For more information, visit https://trillium.so
$extra_info"

    send_public_notification "$message"
}

# Main script logic
main() {
    if [[ $# -lt 2 ]]; then
        echo "Usage: $0 <message_type> <epoch_number> [additional_parameters...]"
        echo ""
        echo "Message types:"
        echo "  collection_ended <epoch_number>"
        echo "  mostly_updated <epoch_number>"
        echo "  complete <epoch_number>"
        echo "  jito_kobe_available <epoch_number>"
        echo "  missing_kobe_data <epoch_number>"
        echo "  custom <epoch_number> <status_message> [emoji]"
        echo ""
        echo "Examples:"
        echo "  $0 collection_ended 12345"
        echo "  $0 mostly_updated 12345"
        echo "  $0 complete 12345"
        echo "  $0 jito_kobe_available 12345"
        echo "  $0 missing_kobe_data 12345"
        echo "  $0 custom 12345 'Processing in progress' '⚙️'"
        exit 1
    fi
    
    local message_type="$1"
    local epoch_number="$2"
    shift 2
    
    case "$message_type" in
        "collection_ended")
            send_data_collection_ended "$epoch_number"
            ;;
        "mostly_updated")
            send_data_mostly_updated "$epoch_number"
            ;;
        "complete")
            send_data_complete "$epoch_number"
            ;;
        "jito_kobe_available")
            send_jito_kobe_available "$epoch_number"
            ;;
        "missing_kobe_data")
            send_missing_kobe_data "$epoch_number"
            ;;
        "custom")
            if [[ $# -lt 1 ]]; then
                echo "Error: custom message type requires status message parameter"
                exit 1
            fi
            send_custom_status "$epoch_number" "$1" "${2:-}"
            ;;
        *)
            echo "Error: Unknown message type '$message_type'"
            echo "Valid types: collection_ended, mostly_updated, complete, jito_kobe_available, missing_kobe_data, custom"
            exit 1
            ;;
    esac
}

# Run main function if script is executed directly
if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
    main "$@"
fi
