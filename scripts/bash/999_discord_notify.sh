#!/bin/bash

# 999_discord_notify.sh - Centralized Discord notification script
# Usage: bash 999_discord_notify.sh <message_type> <script_name> [additional_parameters...]

set -euo pipefail

# Source the common logging functions if available
if [[ -f "/home/smilax/api/999_common_log.sh" ]]; then
    source /home/smilax/api/999_common_log.sh
    # Initialize enhanced logging
    init_logging 2>/dev/null || true
else
    # Fallback logging function if common_log.sh is not available
    log() {
        local level="$1"
        local message="$2"
        echo "$(date '+%Y-%m-%d %H:%M:%S') [$level] $message"
    }
    log_info() { log "INFO" "$1"; }
    log_error() { log "ERROR" "$1"; }
    log_debug() { log "DEBUG" "$1"; }
fi

# Discord webhook configuration
DISCORD_WEBHOOK_URL="https://discord.com/api/webhooks/1397269122441805835/_Qh0rY24s5a4QPqgOjWVTB2MxYgJWAZRf4ytCc6SU6t8TQCIpitTdVYQ233e-4Z-cJQk"

# Default configuration
DEFAULT_USERNAME="Trillium Notification"
DEFAULT_AVATAR_URL="https://trillium.so/images/trillium-default.png"
BASE_AVATAR_URL="https://trillium.so/images/"

# Function to get script-specific configuration with enhanced logic
get_script_config() {
    local script_name="$1"
    local config_type="$2"  # username or avatar_url

    # Extract script name without extension
    local script_basename="${script_name%.*}"

    if [[ "$config_type" == "username" ]]; then
        # Enhanced username logic based on script type
        case "$script_basename" in
            *jito*|*90_wait-for-jito*|*1_no-wait-for-jito*|*1_wait-for-jito*)
                echo "🚀 Jito Processing Bot"
                ;;
            *block*|*90_get_block_data*)
                echo "📦 Block Data Bot"
                ;;
            *stake*|*solana-stakes*)
                echo "💰 Stake Analysis Bot"
                ;;
            *validator*|*2_update_validator*)
                echo "🔍 Validator Data Bot"
                ;;
            *leaderboard*|*3_build_leaderboard*)
                echo "🏆 Leaderboard Bot"
                ;;
            *cleanup*|*7_cleanup*)
                echo "🧹 Cleanup Bot"
                ;;
            *compress*|*999_compress*|*999_orchestrate*)
                echo "🗜️ Compression Bot"
                ;;
            *sql*|*92_run_sql*|*92_slot_duration*)
                echo "🗄️ Database Bot"
                ;;
            *xshin*|*90_xshin*)
                echo "📊 Xshin Data Bot"
                ;;
            *vote*|*92_vote_latency*)
                echo "🗳️ Vote Analysis Bot"
                ;;
            *epoch*|*999_epoch*)
                echo "⏰ Epoch Monitor Bot"
                ;;
            *check*|*999_check*)
                echo "✅ Validation Bot"
                ;;
            999_*)
                echo "🖥️ System Monitor Bot"
                ;;
            *)
                # Convert script name to readable username (fallback)
                local username=$(echo "$script_basename" | sed 's/_/ /g' | sed 's/\b\w/\U&/g')
                echo "🤖 Trillium $username"
                ;;
        esac

    elif [[ "$config_type" == "avatar_url" ]]; then
        # Build the URL we expect
        local derived_url="${BASE_AVATAR_URL}${script_basename}.png"

        # Check via a HEAD request whether it exists (HTTP 200)
        if curl --silent --head --fail --max-time 5 "$derived_url" >/dev/null 2>&1; then
            log_debug "🖼️ Using script-specific avatar: $derived_url"
            echo "$derived_url"
        else
            log_debug "🖼️ No specific avatar found for $script_basename, using default"
            echo "$DEFAULT_AVATAR_URL"
        fi
    fi
}

# Function to generate timestamps
generate_timestamps() {
    local current_datetime_utc=$(date -u +"%A, %B %d, %Y at %I:%M:%S %p UTC")
    local current_datetime_cst=$(TZ="America/Chicago" date +"%A, %B %d, %Y at %I:%M:%S %p %Z")
    echo -e "\n🕐 Message generated on $current_datetime_utc and $current_datetime_cst"
}

# Function to escape JSON strings
escape_json() {
    local input="$1"
    echo "$input" | sed 's/"/\\"/g' | sed ':a;N;$!ba;s/\n/\\n/g'
}

# Function to send Discord message
send_discord_message() {
    local message="$1"
    local username="$2"
    local avatar_url="$3"
    
    log_debug "📤 Sending Discord message from $username"
    
    # Escape the message for JSON
    local json_message=$(escape_json "$message")
    
    # Send to Discord
    local discord_response=$(curl -s -H "Content-Type: application/json" \
         -X POST \
         -d "{\"content\":\"$json_message\", \"username\":\"$username\", \"avatar_url\":\"$avatar_url\", \"flags\": 4}" \
         "$DISCORD_WEBHOOK_URL")

    if [ -z "$discord_response" ]; then
        log_info "✅ Discord message sent successfully from $username"
        return 0
    else
        log_error "❌ Failed to send Discord message: $discord_response"
        return 1
    fi
}

# Function to send error alert (updated for new pattern)
send_error_alert() {
    local script_name="$1"
    local error_description="$2"
    local failed_command="$3"
    local exit_code="$4"
    local epoch_number="${5:-}"
    local additional_context="${6:-}"
    
    log_info "🚨 Preparing error alert for $script_name"
    
    local username=$(get_script_config "$script_name" "username")
    local avatar_url=$(get_script_config "$script_name" "avatar_url")
    
    local alert_message="🚨 **SCRIPT ERROR ALERT** 🚨

📊 **Script:** \`$script_name\`
❌ **Error:** $error_description
💥 **Exit Code:** $exit_code"

    # Add epoch information if provided
    if [[ -n "$epoch_number" && "$epoch_number" != "" ]]; then
        alert_message="$alert_message
🎯 **Epoch:** $epoch_number"
    fi

    alert_message="$alert_message

**Failed Command:**
\`\`\`
$failed_command
\`\`\`"

    # Add additional context if provided
    if [[ -n "$additional_context" ]]; then
        alert_message="$alert_message

**Additional Context:**
$additional_context"
    fi

    alert_message="$alert_message

🌐 **More info:** https://trillium.so$(generate_timestamps)"
    
    send_discord_message "$alert_message" "$username" "$avatar_url"
}

# Function to send success notification (updated for new pattern)
send_success_notification() {
    local script_name="$1"
    local epoch_number="$2"
    local success_title="$3"
    local components_processed="$4"
    local additional_notes="${5:-}"
    
    log_info "🎉 Preparing success notification for $script_name"
    
    local username=$(get_script_config "$script_name" "username")
    local avatar_url=$(get_script_config "$script_name" "avatar_url")
    
    local success_message="✅ **$success_title** ✅

📊 **Script:** \`$script_name\`"

    # Add epoch information if provided and not empty
    if [[ -n "$epoch_number" && "$epoch_number" != "" ]]; then
        success_message="$success_message
🎯 **Epoch:** $epoch_number"
    fi

    success_message="$success_message
🏁 **Status:** All processes completed successfully

**Components Processed:**
$components_processed"

    # Add additional notes if provided
    if [[ -n "$additional_notes" ]]; then
        success_message="$success_message

**Additional Information:**
$additional_notes"
    fi

    success_message="$success_message

🌐 **More info:** https://trillium.so$(generate_timestamps)"
    
    send_discord_message "$success_message" "$username" "$avatar_url"
}

# Function to send startup notification
send_startup_notification() {
    local script_name="$1"
    local epoch_number="$2"
    local startup_title="$3"
    local startup_details="$4"
    
    log_info "🚀 Preparing startup notification for $script_name"
    
    local username=$(get_script_config "$script_name" "username")
    local avatar_url=$(get_script_config "$script_name" "avatar_url")
    
    local startup_message="🚀 **$startup_title** 🚀

📊 **Script:** \`$script_name\`"

    # Add epoch information if provided and not empty
    if [[ -n "$epoch_number" && "$epoch_number" != "" ]]; then
        startup_message="$startup_message
🎯 **Epoch:** $epoch_number"
    fi

    startup_message="$startup_message

$startup_details

🎯 **Status:** Ready to begin processing...$(generate_timestamps)"
    
    send_discord_message "$startup_message" "$username" "$avatar_url"
}

# Function to send custom notification
send_custom_notification() {
    local script_name="$1"
    local title="$2"
    local message="$3"
    local emoji="${4:-🔔}"
    
    log_info "📢 Preparing custom notification for $script_name"
    
    local username=$(get_script_config "$script_name" "username")
    local avatar_url=$(get_script_config "$script_name" "avatar_url")
    
    local custom_message="$emoji **$title** $emoji

📊 **Script:** \`$script_name\`

$message$(generate_timestamps)"
    
    send_discord_message "$custom_message" "$username" "$avatar_url"
}

# Function to send restart notification
send_restart_notification() {
    local script_name="$1"  
    local epoch_number="$2"
    local original_args="$3"
    
    log_info "🔄 Preparing restart notification for $script_name"
    
    local username=$(get_script_config "$script_name" "username")
    local avatar_url=$(get_script_config "$script_name" "avatar_url")
    
    local restart_message="🔄 **$script_name Script Restarting** 🔄

📊 **Script:** \`$script_name\`"

    # Add epoch information if provided and not empty
    if [[ -n "$epoch_number" && "$epoch_number" != "" ]]; then
        restart_message="$restart_message
🎯 **Epoch:** $epoch_number"
    fi

    restart_message="$restart_message
🔄 **Action:** User chose to re-run the script
⚙️ **Status:** Restarting with original parameters
🎯 **Parameters:** $original_args

🚀 Restarting processing loop...$(generate_timestamps)"
    
    send_discord_message "$restart_message" "$username" "$avatar_url"
}

# Enhanced usage function
show_usage() {
    cat << EOF
Usage: $0 <message_type> <script_name> [additional_parameters...]

Message types:
  error <script_name> <error_description> <failed_command> <exit_code> [epoch_number] [additional_context]
  success <script_name> <epoch_number> <success_title> <components_processed> [additional_notes]
  startup <script_name> <epoch_number> <startup_title> <startup_details>
  custom <script_name> <title> <message> [emoji]
  restart <script_name> <epoch_number> <original_args>

Examples:
  $0 error 'myscript.sh' 'Data processing failed' 'python3 process.py' 1 '12345' 'Database connection failed'
  $0 success 'myscript.sh' '12345' 'Processing Complete' '• Data loaded\n• Files processed' 'All systems nominal'
  $0 startup 'myscript.sh' '12345' 'Script Started' 'Beginning automated processing...'
  $0 custom 'myscript.sh' 'Custom Alert' 'Something important happened' '⚠️'
  $0 restart 'myscript.sh' '12345' 'epoch=12345 --verbose'

Note: The script automatically detects script type and uses appropriate avatar/username.
Avatar files should be placed at: https://trillium.so/images/{script_name}.png
EOF
}

# Main script logic
main() {
    if [[ $# -lt 2 ]]; then
        show_usage
        exit 1
    fi
    
    local message_type="$1"
    local script_name="$2"
    shift 2
    
    # Initialize epoch_number to empty string to avoid undefined variable error
    local epoch_number=""
    
    # Extract epoch_number based on message type (it's typically the first parameter after script_name)
    case "$message_type" in
        "error"|"success"|"startup"|"restart")
            if [[ $# -ge 1 ]]; then
                epoch_number="$1"
            fi
            ;;
        *)
            # For other message types, epoch_number might not be provided
            ;;
    esac
    
    if [[ -n "$epoch_number" && "$epoch_number" != "" ]]; then
        log_debug "🎯 Processing $message_type notification for $script_name (epoch: $epoch_number)"
    else
        log_debug "🎯 Processing $message_type notification for $script_name"
    fi
    
    case "$message_type" in
        "error")
            if [[ $# -lt 3 ]]; then
                log_error "❌ Error: error message type requires at least 3 additional parameters"
                log_error "Usage: $0 error <script_name> <error_description> <failed_command> <exit_code> [epoch_number] [additional_context]"
                show_usage
                exit 1
            fi
            send_error_alert "$script_name" "$1" "$2" "$3" "${4:-}" "${5:-}"
            ;;
        "success")
            if [[ $# -lt 3 ]]; then
                log_error "❌ Error: success message type requires at least 3 additional parameters"
                log_error "Usage: $0 success <script_name> <epoch_number> <success_title> <components_processed> [additional_notes]"
                show_usage
                exit 1
            fi
            send_success_notification "$script_name" "$1" "$2" "$3" "${4:-}"
            ;;
        "startup")
            if [[ $# -lt 3 ]]; then
                log_error "❌ Error: startup message type requires at least 3 additional parameters"
                log_error "Usage: $0 startup <script_name> <epoch_number> <startup_title> <startup_details>"
                show_usage
                exit 1
            fi
            send_startup_notification "$script_name" "$1" "$2" "$3"
            ;;
        "custom")
            if [[ $# -lt 2 ]]; then
                log_error "❌ Error: custom message type requires at least 2 additional parameters"
                log_error "Usage: $0 custom <script_name> <title> <message> [emoji]"
                show_usage
                exit 1
            fi
            send_custom_notification "$script_name" "$1" "$2" "${3:-}"
            ;;
        "restart")
            if [[ $# -lt 2 ]]; then
                log_error "❌ Error: restart message type requires at least 2 additional parameters"
                log_error "Usage: $0 restart <script_name> <epoch_number> <original_args>"
                show_usage
                exit 1
            fi
            send_restart_notification "$script_name" "$1" "$2"
            ;;
        *)
            log_error "❌ Unknown message type '$message_type'"
            log_error "Valid types: error, success, startup, custom, restart"
            show_usage
            exit 1
            ;;
    esac
    
    log_info "✅ Notification processing completed for $message_type"
}

# Cleanup function
cleanup_discord_notify() {
    cleanup_logging
}

# Set up signal handling
trap cleanup_discord_notify EXIT

# Run main function if script is executed directly
if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
    main "$@"
fi
