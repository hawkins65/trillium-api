#!/bin/bash

source $HOME/api/999_common_log.sh

# Configuration
DEBUG="${DEBUG:-false}"
ALERT_THRESHOLD_SECONDS=30
MAX_CONCURRENT_CHECKS=5
ENABLE_PERFORMANCE_MONITORING=true
ENABLE_DETAILED_LOGGING=true

# Define paths for Solana CLI and constants
SOLANA_CLI="/home/smilax/.local/share/solana/install/active_release/bin/solana"
MAINNET_RPC_URL="https://wiser-young-star.solana-mainnet.quiknode.pro/887452d66f8b645b8824eab20011dbd3c315d84f/"
TESTNET_RPC_URL="https://wiser-thrilling-reel.solana-testnet.quiknode.pro/d05bbe3aa7a9377d63a89a869a3fba1093555029/"
MAINNET_GOSSIP_FILE="/home/smilax/api/gossip-mainnet.json"
TESTNET_GOSSIP_FILE="/home/smilax/api/gossip-testnet.json"
MAINNET_SFDP_API_URL="https://api.solana.org/api/epoch/required_versions?cluster=mainnet-beta"
TESTNET_SFDP_API_URL="https://api.solana.org/api/epoch/required_versions?cluster=testnet"
DISCORD_WEBHOOK="https://discord.com/api/webhooks/1386672713606627348/YmHyp_2_Mr263q-g0DgBfV7ef6845udRccX8pl_E4eNbAQFT1UT6Nqitkkc2Lu0QGhfN"
MESSAGE_FILE="/home/smilax/api/gossip_message.txt"

# Performance monitoring variables
SCRIPT_START_TIME=$(date +%s)
VALIDATOR_COUNT=0
SUCCESS_COUNT=0
ERROR_COUNT=0

# Arrays to store messages and data
declare -a GREEN_VALIDATORS
declare -a RED_MESSAGES
declare -a FAILED_VALIDATORS  # NEW: Track failed validators with details
declare -A CURRENT_REQUIREMENTS
declare -A FUTURE_EPOCH_INFO
declare -A AFFECTED_VALIDATORS
declare -A CURRENT_EPOCHS

# NEW: Variables for upcoming changes tracking
MAINNET_UPCOMING_CHANGES=""
TESTNET_UPCOMING_CHANGES=""

# Enhanced logging functions
setup_logging() {
    local log_dir="/home/smilax/api/logs"
    local timestamp=$(date +%Y%m%d_%H%M%S)
    
    mkdir -p "$log_dir"
    LOG_FILE="$log_dir/validator_monitor_${timestamp}.log"
    ERROR_LOG="$log_dir/validator_monitor_errors_${timestamp}.log"
    
    # Keep only last 30 days of logs
    find "$log_dir" -name "validator_monitor_*.log" -mtime +30 -delete 2>/dev/null
    
    log_info "Logging initialized: $LOG_FILE"
}

log_info() {
    local message="$1"
    local timestamp=$(date '+%Y-%m-%d %H:%M:%S')
    echo "[$timestamp] [INFO] $message" | tee -a "$LOG_FILE"
}

log_error() {
    local message="$1"
    local timestamp=$(date '+%Y-%m-%d %H:%M:%S')
    echo "[$timestamp] [ERROR] $message" | tee -a "$LOG_FILE" | tee -a "$ERROR_LOG"
}

log_warn() {
    local message="$1"
    local timestamp=$(date '+%Y-%m-%d %H:%M:%S')
    echo "[$timestamp] [WARN] $message" | tee -a "$LOG_FILE"
}

log_debug() {
    local message="$1"
    local timestamp=$(date '+%Y-%m-%d %H:%M:%S')
    [ "$DEBUG" = "true" ] && echo "[$timestamp] [DEBUG] $message" | tee -a "$LOG_FILE"
}

# NEW: Enhanced error tracking function
track_validator_error() {
    local alias="$1"
    local error_type="$2" 
    local error_message="$3"
    local network="$4"
    local pubkey="$5"
    
    local detailed_error="❌ **$alias ($network)** - $error_type: $error_message"
    [ -n "$pubkey" ] && detailed_error+="\n  📋 Pubkey: \`$pubkey\`"
    detailed_error+="\n  🕐 Time: $(date '+%H:%M:%S')"
    
    FAILED_VALIDATORS+=("$detailed_error")
    log_error "VALIDATOR_FAILURE: $alias ($network) - $error_type: $error_message"
}

# Enhanced error handling
handle_critical_error() {
    local error_msg="$1"
    local context="$2"
    local validator_alias="$3"  # Optional validator context
    
    log_error "CRITICAL: $error_msg in $context"
    
    local script_name=$(basename "$0")
    local alert_message="🚨 **CRITICAL ERROR** 🚨\n"
    alert_message+="**Time:** $(date '+%Y-%m-%d %H:%M:%S')\n"
    alert_message+="**Context:** $context\n"
    alert_message+="**Error:** $error_msg\n"
    [ -n "$validator_alias" ] && alert_message+="**Validator:** $validator_alias\n"
    alert_message+="**Host:** $(hostname)\n"
    alert_message+="**Script:** $script_name\n"
    
    send_discord_alert "critical" "$alert_message" "🚨"
}

# NEW: Enhanced Discord message chunking functions
send_single_discord_message() {
    local severity="$1"
    local message="$2"
    local emoji="$3"
    local max_retries="$4"
    local retry_delay="$5"
    
    local payload="{\"username\":\"VersionMonitorBot\",\"avatar_url\":\"https://trillium.so/images/monitor.png\",\"content\":\"$emoji $(echo "$message" | sed 's/"/\\"/g')\"}"
    
    for ((i=1; i<=max_retries; i++)); do
        local response=$(curl -s -w "%{http_code}" -H "Content-Type: application/json" -X POST -d "$payload" "$DISCORD_WEBHOOK")
        local http_code="${response: -3}"
        
        if [ "$http_code" = "200" ] || [ "$http_code" = "204" ]; then
            log_info "Discord alert sent successfully (attempt $i/$max_retries, severity: $severity, length: ${#message})"
            return 0
        else
            log_warn "Discord alert failed (attempt $i/$max_retries, HTTP: $http_code, severity: $severity, length: ${#message})"
            [ $i -lt $max_retries ] && sleep $retry_delay
        fi
    done
    
    log_error "Failed to send Discord alert after $max_retries attempts (severity: $severity, length: ${#message})"
    return 1
}

split_message_into_chunks() {
    local message="$1"
    local max_length="$2"
    local -n chunks_array="$3"
    
    # Clear the array
    chunks_array=()
    
    # Convert message to array of lines for easier processing
    local IFS=$'\n'
    local lines=($message)
    
    local current_chunk=""
    local current_length=0
    
    for line in "${lines[@]}"; do
        local line_length=${#line}
        
        # If adding this line would exceed the limit, start a new chunk
        if [ $((current_length + line_length + 1)) -gt $max_length ] && [ -n "$current_chunk" ]; then
            chunks_array+=("$current_chunk")
            current_chunk="$line"
            current_length=$line_length
        else
            # Add line to current chunk
            if [ -n "$current_chunk" ]; then
                current_chunk="$current_chunk\n$line"
                current_length=$((current_length + line_length + 1))
            else
                current_chunk="$line"
                current_length=$line_length
            fi
        fi
        
        # Handle extremely long single lines
        if [ $line_length -gt $max_length ]; then
            log_warn "Single line exceeds max length: ${line_length} chars"
            # Truncate the line with indication
            local truncated_line="${line:0:$((max_length-20))}...[TRUNCATED]"
            if [ -n "$current_chunk" ] && [ "$current_chunk" != "$line" ]; then
                current_chunk="${current_chunk%$line}$truncated_line"
            else
                current_chunk="$truncated_line"
            fi
            current_length=${#current_chunk}
        fi
    done
    
    # Add the last chunk if it has content
    if [ -n "$current_chunk" ]; then
        chunks_array+=("$current_chunk")
    fi
}

send_chunked_discord_message() {
    local severity="$1"
    local message="$2"
    local emoji="$3"
    local max_retries="$4"
    local retry_delay="$5"
    local max_length="$6"
    
    log_info "Message too long (${#message} chars), splitting into chunks"
    
    # Split message into logical chunks
    local chunks=()
    split_message_into_chunks "$message" "$max_length" chunks
    
    local total_chunks=${#chunks[@]}
    log_info "Split message into $total_chunks chunks"
    
    # Send each chunk
    for ((i=0; i<total_chunks; i++)); do
        local chunk_number=$((i+1))
        local chunk_message="${chunks[$i]}"
        
        # Add chunk indicator for multi-part messages
        if [ $total_chunks -gt 1 ]; then
            chunk_message="**[Part $chunk_number/$total_chunks]**\n$chunk_message"
        fi
        
        log_debug "Sending chunk $chunk_number/$total_chunks (${#chunk_message} chars)"
        
        if ! send_single_discord_message "$severity" "$chunk_message" "$emoji" "$max_retries" "$retry_delay"; then
            log_error "Failed to send chunk $chunk_number/$total_chunks"
            return 1
        fi
        
        # Small delay between chunks to avoid rate limiting
        [ $chunk_number -lt $total_chunks ] && sleep 1
    done
    
    log_info "Successfully sent all $total_chunks message chunks"
    return 0
}

# Enhanced Discord alerting with message chunking - UPDATED VERSION
send_discord_alert() {
    local severity="$1"  # info, warn, error, critical
    local message="$2"
    local emoji="$3"
    local max_retries=3
    local retry_delay=2
    local max_length=1900  # Leave buffer for Discord's 2000 char limit
    
    # Check if message needs to be split
    local message_length=${#message}
    if [ $message_length -le $max_length ]; then
        # Send single message
        send_single_discord_message "$severity" "$message" "$emoji" "$max_retries" "$retry_delay"
    else
        # Split message into chunks
        send_chunked_discord_message "$severity" "$message" "$emoji" "$max_retries" "$retry_delay" "$max_length"
    fi
}
# Performance monitoring
track_performance() {
    local operation="$1"
    local start_time="$2"
    local end_time=$(date +%s)
    local duration=$((end_time - start_time))
    
    log_debug "Performance: $operation took ${duration}s"
    
    # Alert on slow operations
    if [ $duration -gt $ALERT_THRESHOLD_SECONDS ]; then
        log_warn "Slow operation detected: $operation took ${duration}s"
    fi
}

# Health checks
check_dependencies() {
    local missing_deps=()
    
    command -v jq >/dev/null 2>&1 || missing_deps+=("jq")
    command -v curl >/dev/null 2>&1 || missing_deps+=("curl")
    [ -x "$SOLANA_CLI" ] || missing_deps+=("solana CLI")
    
    if [ ${#missing_deps[@]} -gt 0 ]; then
        handle_critical_error "Missing dependencies: ${missing_deps[*]}" "dependency_check"
        exit 1
    fi
    
    log_info "All dependencies verified"
}

check_network_connectivity() {
    local networks=("mainnet" "testnet")
    
    for network in "${networks[@]}"; do
        local rpc_url
        [ "$network" = "mainnet" ] && rpc_url="$MAINNET_RPC_URL" || rpc_url="$TESTNET_RPC_URL"
        
        if ! curl -s --max-time 5 --head "$rpc_url" >/dev/null 2>&1; then
            handle_critical_error "Cannot reach $network RPC: $rpc_url" "network_connectivity"
            exit 1
        fi
    done
    
    log_info "Network connectivity verified"
}

# Function to map alias to pubkey, logo, type (vote or identity), and network (mainnet or testnet)
map_pubkey() {
    case "$1" in
        trillium)
            echo "tri1cHBy47fPyhCvrCf6FnR7Mz6XdSoSBah2FsZVQeT|https://trillium.so/images/trillium.png|vote|mainnet"
            ;;
        trillium_standby)
            echo "tri1z9PER6SHRG9fByGMgriWSvX5Ne75cU47b3L8JJ5|https://trillium.so/images/trillium.png|identity|mainnet"
            ;;
        trillium_testnet)
            echo "TRi12sEaDkgoNSsEpep3YF8QPjqz4qM63mc1Z4tQCvD|https://trillium.so/images/trillium.png|identity|testnet"
            ;;
        ofv)
            echo "oRAnGeU5h8h2UkvbfnE5cjXnnAa4rBoaxmS4kbFymSe|https://trillium.so/images/ofv.jpeg|vote|mainnet"
            ;;
        ofv_standby)
            echo "BiLHQDGjDrG8LaGVJnLNxEko6A4vXSZnMxF27QW9ccfT|https://trillium.so/images/ofv.jpeg|identity|mainnet"
            ;;
        ofv_testnet)
            echo "HNFTsFJN6Hk2w73eR4Lf2tLfaCVp79vJsamVPkpZpDgq|https://trillium.so/images/ofv.jpeg|identity|testnet"
            ;;
        laine_standby1)
            echo "2bTuZ2W4yt6xiKdDaoY9wVFXv7bMW6zcmiJnGQpbNqWz|https://trillium.so/images/laine.png|identity|mainnet"
            ;;
        laine_standby2)
            echo "DhgsALAHgH4cS2aDqHjVDu9UzWN91jcMNyc5ew1wiG6h|https://trillium.so/images/laine.png|identity|mainnet"
            ;;
        laine)
            echo "LA1NEzryoih6CQW3gwQqJQffK2mKgnXcjSQZSRpM3wc|https://trillium.so/images/laine.png|identity|mainnet"
            ;;
        cogent)
            echo "CogentC52e7kktFfWHwsqSmr8LiS1yAtfqhHcftCPcBJ|https://trillium.so/images/cogent.jpeg|vote|mainnet"
            ;;
        cogent_standby)
            echo "co2ps7eHBzWV8iX6dar2hk7gWHufiJoGn3CZe5nZ9JY|https://trillium.so/images/cogent.jpeg|identity|mainnet"
            ;;
        ss)
            echo "punK4RDD3pFbcum79ACHatYPLLE1hr5UNnQVUGNfeyP|https://trillium.so/images/ss.png|vote|mainnet"
            ;;
        ss_standby)
            echo "9M9k1n1gASz5S4MJq7QFJ29qgpaabewG7rw8CPfLpLNs|https://trillium.so/images/ss.png|identity|mainnet"
            ;;
        ss_testnet)
            echo "SSTqBQxJHWVenXz95atxLzneaSYgf1QY9v6FHmb5LqH|https://trillium.so/images/ss.png|identity|testnet"
            ;;
        pengu)
            echo "pENgUh4K9zNacyU3PXVE9KugW98XCqZsWpEvA8d8wzX|https://trillium.so/images/pengu.jpeg|vote|mainnet"
            ;;
        pengu_standby)
            echo "4GarcRwXL2F9jdx52Wi8ukj5ZTsP8xWYtsqQwfgPTxMM|https://trillium.so/images/pengu.jpeg|identity|mainnet"
            ;;
        pengu_testnet)
            echo "FN1YjxJZAFjKsixZWfTc4bYaEQEXjJBLy1TFkWGV2aM9|https://trillium.so/images/pengu.jpeg|identity|testnet"
            ;;
        *)
            echo ""
            ;;
    esac
}

# List of all pubkeys to monitor, grouped by brand
PUBKEY_ALIASES=(
    "trillium" "trillium_standby" "trillium_testnet"
    "ofv" "ofv_standby" "ofv_testnet"
    "laine_standby1" "laine_standby2" "laine" 
    "cogent" "cogent_standby"
    "ss" "ss_standby" "ss_testnet"
    "pengu" "pengu_standby" "pengu_testnet"
)

# Function to map vote pubkey to identity pubkey using API
map_vote_to_identity() {
    local vote_pubkey="$1"
    local api_url="https://api.trillium.so/validator_rewards/$vote_pubkey"
    local start_time=$(date +%s)
    
    log_debug "Mapping vote pubkey to identity: $vote_pubkey"
    
    local response=$(curl -s --max-time 10 --retry 2 --retry-delay 1 "$api_url")
    if [ $? -ne 0 ]; then
        log_error "Failed to fetch data from $api_url"
        return 1
    fi
    
    if [ -z "$response" ]; then
        log_error "Empty response from $api_url"
        return 1
    fi
    
    local identity_pubkey=$(echo "$response" | jq -r '
        if type == "object" and has("error") then
            empty
        else
            (if type == "array" then .[0] else . end) | 
            .identity_pubkey // empty
        end
    ')
    
    if [ $? -ne 0 ]; then
        log_error "Failed to parse JSON response for pubkey $vote_pubkey"
        return 1
    fi
    
    if [ -z "$identity_pubkey" ]; then
        log_error "No identity pubkey found for vote pubkey $vote_pubkey"
        return 1
    fi
    
    track_performance "map_vote_to_identity" "$start_time"
    echo "$identity_pubkey"
    return 0
}

# Function to compare versions
compare_versions() {
    local ver1="$1"
    local ver2="$2"
    if [ -z "$ver1" ] || [ -z "$ver2" ]; then
        return 1
    fi
    local IFS=.
    local i ver1_parts=($ver1) ver2_parts=($ver2)
    for ((i=0; i<${#ver1_parts[@]}; i++)); do
        if [ -z "${ver2_parts[i]}" ]; then
            return 0
        fi
        if [ "${ver1_parts[i]}" -gt "${ver2_parts[i]}" ]; then
            return 0
        elif [ "${ver1_parts[i]}" -lt "${ver2_parts[i]}" ]; then
            return 1
        fi
    done
    return 0
}

# Enhanced version compliance check with improved emojis and formatting
check_version_compliance() {
    local version="$1"
    local min_version="$2"
    local max_version="$3"
    local version_type="$4"
    
    local min_compliant="true"
    if [ -n "$min_version" ] && ! compare_versions "$version" "$min_version"; then
        min_compliant="false"
    fi
    
    local max_compliant="true"
    if [ -n "$max_version" ] && compare_versions "$version" "$max_version"; then
        max_compliant="false"
    fi
    
    if [ "$min_compliant" = "true" ] && [ "$max_compliant" = "true" ]; then
        echo "✅ **COMPLIANT**: $version_type Version $version ▶️ Min: ${min_version:-N/A} ◀️ Max: ${max_version:-N/A}"
        return 0
    else
        local error_msg="❌ **NON-COMPLIANT**: $version_type Version $version"
        if [ "$min_compliant" = "false" ]; then
            error_msg="$error_msg 🔻 Below minimum ▶️ $min_version"
        fi
        if [ "$max_compliant" = "false" ]; then
            error_msg="$error_msg 🔺 Above maximum ◀️ $max_version"
        fi
        echo "$error_msg"
        return 1
    fi
}

# Enhanced message formatting
format_validator_message() {
    local alias="$1"
    local network="$2"
    local current_epoch="$3"
    local closest_epoch="$4"
    local validator_identity="$5"
    local version_type="$6"
    local validator_version="$7"
    local min_version="$8"
    local max_version="$9"
    local status="${10}"
    local future_info="${11}"
    
    local message="━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
    message+="🔍 **VALIDATOR MONITOR** - $alias ($network)\n"
    message+="━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
    message+="📊 **Current Epoch:** $current_epoch\n"
    message+="📋 **Reference Epoch:** $closest_epoch\n"
    message+="🔑 **Validator Identity:** \`$validator_identity\`\n"
    message+="⚙️ **Running Version:** $version_type $validator_version\n"
    message+="📏 **Required Range:** ▶️ ${min_version:-N/A} to ◀️ ${max_version:-N/A}\n"
    message+="🎯 **Status:** $status\n"
    
    if [ -n "$future_info" ]; then
        message+="\n$future_info"
    fi
    
    message+="━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
    echo "$message"
}

# Function to compare current vs future requirements
format_future_requirements_info() {
    local network="$1"
    local current_epoch="$2"
    local future_epoch="$3"
    local current_agave_min="$4"
    local current_agave_max="$5"
    local current_firedancer_min="$6"
    local current_firedancer_max="$7"
    local future_agave_min="$8"
    local future_agave_max="$9"
    local future_firedancer_min="${10}"
    local future_firedancer_max="${11}"
    
    local info_message=""
    
    if [ -n "$future_epoch" ] && [ "$future_epoch" != "null" ]; then
        # Check if requirements are changing
        local agave_min_changing="false"
        local agave_max_changing="false"
        local firedancer_min_changing="false"
        local firedancer_max_changing="false"
        
        [ "$current_agave_min" != "$future_agave_min" ] && agave_min_changing="true"
        [ "$current_agave_max" != "$future_agave_max" ] && agave_max_changing="true"
        [ "$current_firedancer_min" != "$future_firedancer_min" ] && firedancer_min_changing="true"
        [ "$current_firedancer_max" != "$future_firedancer_max" ] && firedancer_max_changing="true"
        
        if [ "$agave_min_changing" = "true" ] || [ "$agave_max_changing" = "true" ] || [ "$firedancer_min_changing" = "true" ] || [ "$firedancer_max_changing" = "true" ]; then
            info_message="⚠️ **REQUIREMENTS CHANGING in Epoch $future_epoch ($network)**\n"
            info_message+="⚙️ Agave: ▶️ Min: ${current_agave_min:-N/A} → ${future_agave_min:-N/A} | ◀️ Max: ${current_agave_max:-N/A} → ${future_agave_max:-N/A}\n"
            info_message+="🔥 Firedancer: ▶️ Min: ${current_firedancer_min:-N/A} → ${future_firedancer_min:-N/A} | ◀️ Max: ${current_firedancer_max:-N/A} → ${future_firedancer_max:-N/A}\n"
        else
            info_message="✅ **REQUIREMENTS UNCHANGED through Epoch $future_epoch ($network)**\n"
            info_message+="⚙️ Agave: ▶️ Min: ${current_agave_min:-N/A} | ◀️ Max: ${current_agave_max:-N/A}\n"
            info_message+="🔥 Firedancer: ▶️ Min: ${current_firedancer_min:-N/A} | ◀️ Max: ${current_firedancer_max:-N/A}\n"
        fi
    else
        info_message="📋 **NO FUTURE REQUIREMENTS SCHEDULED ($network)**\n"
        info_message+="Current requirements remain in effect\n"
    fi
    
    echo "$info_message"
}

# NEW: Function to analyze upcoming version changes
analyze_upcoming_changes() {
    local network="$1"
    local sfdp_response="$2"
    local current_epoch="$3"
    
    # Find the next epoch with actual changes (not inherited)
    local next_change_epoch=$(echo "$sfdp_response" | jq -r '
        .data | 
        map(select(.epoch > '"$current_epoch"' and .inherited_from_prev_epoch == false)) |
        sort_by(.epoch) | 
        first | 
        .epoch // empty
    ')
    
    if [ -n "$next_change_epoch" ] && [ "$next_change_epoch" != "null" ]; then
        # Get current requirements (closest epoch <= current)
        local current_data=$(echo "$sfdp_response" | jq -r '
            .data |
            map(select(.epoch <= '"$current_epoch"')) |
            sort_by(.epoch) | last
        ')
        
        # Get future requirements
        local future_data=$(echo "$sfdp_response" | jq -r '
            .data |
            map(select(.epoch == '"$next_change_epoch"')) |
            first
        ')
        
        local current_agave_min=$(echo "$current_data" | jq -r '.agave_min_version // "N/A"')
        local current_firedancer_min=$(echo "$current_data" | jq -r '.firedancer_min_version // "N/A"')
        local future_agave_min=$(echo "$future_data" | jq -r '.agave_min_version // "N/A"')
        local future_firedancer_min=$(echo "$future_data" | jq -r '.firedancer_min_version // "N/A"')
        
        # Check if there are actual changes
        if [ "$current_agave_min" != "$future_agave_min" ] || [ "$current_firedancer_min" != "$future_firedancer_min" ]; then
            local changes="📅 **Epoch $next_change_epoch:** "
            
            if [ "$current_agave_min" != "$future_agave_min" ]; then
                changes+="Agave: $current_agave_min → $future_agave_min "
            fi
            
            if [ "$current_firedancer_min" != "$future_firedancer_min" ]; then
                changes+="Firedancer: $current_firedancer_min → $future_firedancer_min"
            fi
            
            # Store in global variable based on network
            if [ "$network" = "mainnet" ]; then
                MAINNET_UPCOMING_CHANGES="$changes"
            else
                TESTNET_UPCOMING_CHANGES="$changes"
            fi
        fi
    fi
}

# NEW: Function to generate upcoming changes summary
generate_upcoming_changes_summary() {
    local changes_summary=""
    
    # Check mainnet upcoming changes
    if [ -n "$MAINNET_UPCOMING_CHANGES" ]; then
        changes_summary+="⚠️ **UPCOMING MAINNET CHANGES:**\n$MAINNET_UPCOMING_CHANGES"
    fi
    
    # Check testnet upcoming changes  
    if [ -n "$TESTNET_UPCOMING_CHANGES" ]; then
        if [ -n "$changes_summary" ]; then
            changes_summary+="\n"
        fi
        changes_summary+="⚠️ **UPCOMING TESTNET CHANGES:**\n$TESTNET_UPCOMING_CHANGES"
    fi
    
    echo "$changes_summary"
}

# NEW: Streamlined and enhanced summary message generation
generate_optimized_summary_message() {
    local script_runtime=$(($(date +%s) - SCRIPT_START_TIME))
    local success_rate=0
    [ $VALIDATOR_COUNT -gt 0 ] && success_rate=$(( SUCCESS_COUNT * 100 / VALIDATOR_COUNT ))
    
    local summary="📊 **VALIDATOR STATUS SUMMARY**\n"
    summary+="━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
    summary+="🌐 **Mainnet Epoch:** ${CURRENT_EPOCHS["mainnet"]} | **Testnet Epoch:** ${CURRENT_EPOCHS["testnet"]}\n"
    summary+="🕐 **Completed:** $(date -u '+%Y-%m-%d %H:%M:%S UTC')\n"
    summary+="📁 **Script:** $(realpath "$0")\n"
    
    # Add current requirements in condensed format
    for network in "${!CURRENT_REQUIREMENTS[@]}"; do
        summary+="\n📋 **$network Requirements:**\n"
        if [ "$network" = "mainnet" ]; then
            summary+="⚙️ Agave: ${MAINNET_AGAVE_MIN:-N/A}+ | 🔥 Firedancer: ${MAINNET_FIREDANCER_MIN:-N/A}+\n"
        else
            summary+="⚙️ Agave: ${TESTNET_AGAVE_MIN:-N/A}+ | 🔥 Firedancer: ${TESTNET_FIREDANCER_MIN:-N/A}+\n"
        fi
    done
    
    # Add upcoming version changes section
    local upcoming_changes=""
    upcoming_changes=$(generate_upcoming_changes_summary)
    if [ -n "$upcoming_changes" ]; then
        summary+="\n$upcoming_changes\n"
    fi
    
    # Add compliant validators count
    if [ ${#GREEN_VALIDATORS[@]} -gt 0 ]; then
        summary+="\n✅ **COMPLIANT:** ${#GREEN_VALIDATORS[@]}/${VALIDATOR_COUNT} (${success_rate}%)\n"
        
        # Group by network for compact display
        local mainnet_compliant=()
        local testnet_compliant=()
        
        for validator in "${GREEN_VALIDATORS[@]}"; do
            if [[ "$validator" == *"(mainnet)"* ]]; then
                mainnet_compliant+=("$(echo "$validator" | sed 's/ (mainnet):.*//g' | sed 's/✅ //g')")
            else
                testnet_compliant+=("$(echo "$validator" | sed 's/ (testnet):.*//g' | sed 's/✅ //g')")
            fi
        done
        
        if [ ${#mainnet_compliant[@]} -gt 0 ]; then
            summary+="🌐 **Mainnet:** $(IFS=','; echo "${mainnet_compliant[*]}")\n"
        fi
        if [ ${#testnet_compliant[@]} -gt 0 ]; then
            summary+="🧪 **Testnet:** $(IFS=','; echo "${testnet_compliant[*]}")\n"
        fi
    else
        summary+="\n❌ **NO COMPLIANT VALIDATORS**\n"
    fi
    
    # Add non-compliant count
    non_compliant_count=${#RED_MESSAGES[@]}
    if [ $non_compliant_count -gt 0 ]; then
        summary+="\n❌ **NON-COMPLIANT:** $non_compliant_count\n"
    fi
    
    # Add failed count with details
    failed_count=${#FAILED_VALIDATORS[@]}
    if [ $failed_count -gt 0 ]; then
        summary+="\n🚨 **FAILED CHECKS:** $failed_count\n"
        for failed_validator in "${FAILED_VALIDATORS[@]}"; do
            summary+="  $failed_validator\n"
        done
    fi
    
    # Add performance summary
    summary+="\n⏱️ **Performance:** ${script_runtime}s | ✅ $SUCCESS_COUNT | ❌ $ERROR_COUNT\n"
    summary+="━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
    
    echo "$summary"
}
# Initialize logging and perform health checks
setup_logging
log_info "Starting Solana Validator Monitor Script"
check_dependencies
check_network_connectivity

# Fetch current epoch for each network once before processing validators
log_info "Fetching current epochs for both networks"
epoch_start_time=$(date +%s)

CURRENT_EPOCHS["mainnet"]=$("$SOLANA_CLI" epoch --url "$MAINNET_RPC_URL")
if [ -z "${CURRENT_EPOCHS["mainnet"]}" ]; then
    handle_critical_error "Failed to retrieve the current epoch for mainnet" "epoch_fetch"
    exit 1
fi
log_info "CURRENT_EPOCH for mainnet: ${CURRENT_EPOCHS["mainnet"]}"

CURRENT_EPOCHS["testnet"]=$("$SOLANA_CLI" epoch --url "$TESTNET_RPC_URL")
if [ -z "${CURRENT_EPOCHS["testnet"]}" ]; then
    handle_critical_error "Failed to retrieve the current epoch for testnet" "epoch_fetch"
    exit 1
fi
log_info "CURRENT_EPOCH for testnet: ${CURRENT_EPOCHS["testnet"]}"

track_performance "epoch_fetch" "$epoch_start_time"

# Variables to track requirements per network
MAINNET_AGAVE_MIN=""
MAINNET_FIREDANCER_MIN=""
TESTNET_AGAVE_MIN=""
TESTNET_FIREDANCER_MIN=""

# Process each pubkey
log_info "Starting validator processing loop"
for ALIAS in "${PUBKEY_ALIASES[@]}"; do
    VALIDATOR_COUNT=$((VALIDATOR_COUNT + 1))
    validator_start_time=$(date +%s)
    
    log_debug "Processing validator: $ALIAS"
    
    PUBKEY_INFO=$(map_pubkey "$ALIAS")
    if [ -z "$PUBKEY_INFO" ]; then
        track_validator_error "$ALIAS" "MAPPING_FAILED" "Failed to map alias to pubkey" "unknown" ""
        ERROR_COUNT=$((ERROR_COUNT + 1))
        continue
    fi

    # Split pubkey, logo, type, and network
    PUBKEY=$(echo "$PUBKEY_INFO" | cut -d'|' -f1)
    VALIDATOR_LOGO=$(echo "$PUBKEY_INFO" | cut -d'|' -f2)
    PUBKEY_TYPE=$(echo "$PUBKEY_INFO" | cut -d'|' -f3)
    NETWORK=$(echo "$PUBKEY_INFO" | cut -d'|' -f4)

    log_debug "Processing $ALIAS: pubkey=$PUBKEY, type=$PUBKEY_TYPE, network=$NETWORK"

    # Set RPC URL, gossip file, and SFDP API URL based on network
    if [ "$NETWORK" = "mainnet" ]; then
        RPC_URL="$MAINNET_RPC_URL"
        GOSSIP_FILE="$MAINNET_GOSSIP_FILE"
        SFDP_API_URL="$MAINNET_SFDP_API_URL"
    elif [ "$NETWORK" = "testnet" ]; then
        RPC_URL="$TESTNET_RPC_URL"
        GOSSIP_FILE="$TESTNET_GOSSIP_FILE"
        SFDP_API_URL="$TESTNET_SFDP_API_URL"
    else
        track_validator_error "$ALIAS" "INVALID_NETWORK" "Invalid network: $NETWORK" "$NETWORK" "$PUBKEY"
        ERROR_COUNT=$((ERROR_COUNT + 1))
        continue
    fi

    # Use the pre-fetched epoch for the network
    CURRENT_EPOCH="${CURRENT_EPOCHS["$NETWORK"]}"
    if [ -z "$CURRENT_EPOCH" ]; then
        track_validator_error "$ALIAS" "EPOCH_MISSING" "No current epoch available for network" "$NETWORK" "$PUBKEY"
        ERROR_COUNT=$((ERROR_COUNT + 1))
        continue
    fi
    
    # Fetch required versions from SFDP API
    sfdp_start_time=$(date +%s)
    SFDP_RESPONSE=$(curl -s --max-time 10 --retry 2 --retry-delay 1 "$SFDP_API_URL")
    if [ $? -ne 0 ]; then
        track_validator_error "$ALIAS" "SFDP_API_FAILED" "Failed to fetch data from SFDP API" "$NETWORK" "$PUBKEY"
        ERROR_COUNT=$((ERROR_COUNT + 1))
        continue
    fi

    if [ -z "$SFDP_RESPONSE" ]; then
        track_validator_error "$ALIAS" "SFDP_EMPTY_RESPONSE" "Empty response from SFDP API" "$NETWORK" "$PUBKEY"
        ERROR_COUNT=$((ERROR_COUNT + 1))
        continue
    fi
    track_performance "sfdp_api_fetch" "$sfdp_start_time"

    # NEW: Analyze upcoming changes for this network (only do once per network)
    if [ "$NETWORK" = "mainnet" ] && [ -z "$MAINNET_UPCOMING_CHANGES" ]; then
        analyze_upcoming_changes "mainnet" "$SFDP_RESPONSE" "$CURRENT_EPOCH"
    elif [ "$NETWORK" = "testnet" ] && [ -z "$TESTNET_UPCOMING_CHANGES" ]; then
        analyze_upcoming_changes "testnet" "$SFDP_RESPONSE" "$CURRENT_EPOCH"
    fi

    # Find the closest epoch <= current epoch and check for future epoch
    CLOSEST_EPOCH_DATA=$(echo "$SFDP_RESPONSE" | jq -r '
        .data |
        map(select(.epoch <= '"$CURRENT_EPOCH"')) |
        sort_by(.epoch) | last
    ')
    FUTURE_EPOCH_DATA=$(echo "$SFDP_RESPONSE" | jq -r '
        .data |
        map(select(.epoch > '"$CURRENT_EPOCH"' and .inherited_from_prev_epoch == false)) |
        sort_by(.epoch) | first
    ')

    if [ -z "$CLOSEST_EPOCH_DATA" ] || [ "$CLOSEST_EPOCH_DATA" = "null" ]; then
        track_validator_error "$ALIAS" "NO_EPOCH_DATA" "No valid epoch data found for epoch <= $CURRENT_EPOCH" "$NETWORK" "$PUBKEY"
        ERROR_COUNT=$((ERROR_COUNT + 1))
        continue
    fi

    # Extract current version requirements
    CLOSEST_EPOCH=$(echo "$CLOSEST_EPOCH_DATA" | jq -r '.epoch // empty')
    AGAVE_MIN_VERSION=$(echo "$CLOSEST_EPOCH_DATA" | jq -r '.agave_min_version // empty')
    AGAVE_MAX_VERSION=$(echo "$CLOSEST_EPOCH_DATA" | jq -r '.agave_max_version // empty')
    FIREDANCER_MIN_VERSION=$(echo "$CLOSEST_EPOCH_DATA" | jq -r '.firedancer_min_version // empty')
    FIREDANCER_MAX_VERSION=$(echo "$CLOSEST_EPOCH_DATA" | jq -r '.firedancer_max_version // empty')

    # Store requirements per network for summary
    if [ "$NETWORK" = "mainnet" ]; then
        MAINNET_AGAVE_MIN="$AGAVE_MIN_VERSION"
        MAINNET_FIREDANCER_MIN="$FIREDANCER_MIN_VERSION"
    else
        TESTNET_AGAVE_MIN="$AGAVE_MIN_VERSION"
        TESTNET_FIREDANCER_MIN="$FIREDANCER_MIN_VERSION"
    fi

    # Store current requirements
    if [ -z "${CURRENT_REQUIREMENTS["$NETWORK"]}" ]; then
        CURRENT_REQUIREMENTS["$NETWORK"]="\n📋 **Current Requirements for $NETWORK (Epoch $CLOSEST_EPOCH)**\n"
        CURRENT_REQUIREMENTS["$NETWORK"]+="⚙️ Agave ▶️ Min: ${AGAVE_MIN_VERSION:-N/A} ◀️ Max: ${AGAVE_MAX_VERSION:-N/A}\n"
        CURRENT_REQUIREMENTS["$NETWORK"]+="🔥 Firedancer ▶️ Min: ${FIREDANCER_MIN_VERSION:-N/A} ◀️ Max: ${FIREDANCER_MAX_VERSION:-N/A}\n"
    fi

    # Extract future version requirements if applicable
    FUTURE_EPOCH=$(echo "$FUTURE_EPOCH_DATA" | jq -r '.epoch // empty')
    FUTURE_AGAVE_MIN_VERSION=$(echo "$FUTURE_EPOCH_DATA" | jq -r '.agave_min_version // empty')
    FUTURE_AGAVE_MAX_VERSION=$(echo "$FUTURE_EPOCH_DATA" | jq -r '.agave_max_version // empty')
    FUTURE_FIREDANCER_MIN_VERSION=$(echo "$FUTURE_EPOCH_DATA" | jq -r '.firedancer_min_version // empty')
    FUTURE_FIREDANCER_MAX_VERSION=$(echo "$FUTURE_EPOCH_DATA" | jq -r '.firedancer_max_version // empty')

    # Store future epoch info for the network if applicable
    if [ -n "$FUTURE_EPOCH" ] && [ "$FUTURE_EPOCH" != "null" ] && [ -z "${FUTURE_EPOCH_INFO["$NETWORK"]}" ]; then
        FUTURE_EPOCH_INFO["$NETWORK"]="\n⚠️ **Upcoming Requirements for Epoch $FUTURE_EPOCH ($NETWORK)**\n"
        FUTURE_EPOCH_INFO["$NETWORK"]+="⚙️ Future Agave ▶️ Min: ${FUTURE_AGAVE_MIN_VERSION:-N/A} ◀️ Max: ${FUTURE_AGAVE_MAX_VERSION:-N/A}\n"
        FUTURE_EPOCH_INFO["$NETWORK"]+="🔥 Future Firedancer ▶️ Min: ${FUTURE_FIREDANCER_MIN_VERSION:-N/A} ◀️ Max: ${FUTURE_FIREDANCER_MAX_VERSION:-N/A}\n"
    fi

    # Dump gossip data to file for the network
    gossip_start_time=$(date +%s)
    "$SOLANA_CLI" gossip --url "$RPC_URL" --output json > "$GOSSIP_FILE"
    if [ $? -ne 0 ]; then
        track_validator_error "$ALIAS" "GOSSIP_DUMP_FAILED" "Failed to dump gossip data" "$NETWORK" "$PUBKEY"
        ERROR_COUNT=$((ERROR_COUNT + 1))
        continue
    fi
    track_performance "gossip_dump" "$gossip_start_time"

    # Determine validator identity based on pubkey type
    if [ "$PUBKEY_TYPE" = "vote" ]; then
        VALIDATOR_IDENTITY=$(map_vote_to_identity "$PUBKEY")
        if [ $? -ne 0 ]; then
            track_validator_error "$ALIAS" "VOTE_TO_IDENTITY_FAILED" "Failed to map vote pubkey to validator identity" "$NETWORK" "$PUBKEY"
            ERROR_COUNT=$((ERROR_COUNT + 1))
            continue
        fi
    elif [ "$PUBKEY_TYPE" = "identity" ]; then
        VALIDATOR_IDENTITY="$PUBKEY"
    else
        track_validator_error "$ALIAS" "INVALID_PUBKEY_TYPE" "Invalid pubkey type: $PUBKEY_TYPE" "$NETWORK" "$PUBKEY"
        ERROR_COUNT=$((ERROR_COUNT + 1))
        continue
    fi
    log_debug "Validator identity for pubkey $PUBKEY ($ALIAS): $VALIDATOR_IDENTITY"

    # Check validator's current version from gossip
    VALIDATOR_VERSION=$(jq -r --arg id "$VALIDATOR_IDENTITY" '
        .[] | select(.identityPubkey == $id) | .version // empty
    ' "$GOSSIP_FILE")
    if [ -z "$VALIDATOR_VERSION" ]; then
        track_validator_error "$ALIAS" "VERSION_NOT_FOUND" "No version found in gossip data" "$NETWORK" "$PUBKEY"
        ERROR_COUNT=$((ERROR_COUNT + 1))
        continue
    fi

    # Determine version type based on major version
    MAJOR_VERSION=$(echo "$VALIDATOR_VERSION" | cut -d'.' -f1)
    if [ "$MAJOR_VERSION" = "2" ]; then
        VERSION_TYPE="Agave"
        VERSION_STATUS=$(check_version_compliance "$VALIDATOR_VERSION" "$AGAVE_MIN_VERSION" "$AGAVE_MAX_VERSION" "$VERSION_TYPE")
        STATUS_CODE=$?
        MIN_VERSION="$AGAVE_MIN_VERSION"
        MAX_VERSION="$AGAVE_MAX_VERSION"
    elif [ "$MAJOR_VERSION" = "0" ]; then
        VERSION_TYPE="Firedancer"
        VERSION_STATUS=$(check_version_compliance "$VALIDATOR_VERSION" "$FIREDANCER_MIN_VERSION" "$FIREDANCER_MAX_VERSION" "$VERSION_TYPE")
        STATUS_CODE=$?
        MIN_VERSION="$FIREDANCER_MIN_VERSION"
        MAX_VERSION="$FIREDANCER_MAX_VERSION"
    else
        VERSION_STATUS="❌ **NON-COMPLIANT**: Unknown version type for $VALIDATOR_VERSION"
        STATUS_CODE=1
        MIN_VERSION="N/A"
        MAX_VERSION="N/A"
        log_warn "Unknown version type for validator $ALIAS: $VALIDATOR_VERSION"
    fi

    # Check if validator is affected by future requirements
    if [ -n "$FUTURE_EPOCH" ] && [ "$FUTURE_EPOCH" != "null" ]; then
        if [ "$VERSION_TYPE" = "Agave" ] && [ -n "$FUTURE_AGAVE_MIN_VERSION" ] && ! compare_versions "$VALIDATOR_VERSION" "$FUTURE_AGAVE_MIN_VERSION"; then
            AFFECTED_VALIDATORS["$NETWORK"]+="🔻 $ALIAS ($NETWORK): Current $VERSION_TYPE Version $VALIDATOR_VERSION < Future Min ▶️ $FUTURE_AGAVE_MIN_VERSION\n"
        elif [ "$VERSION_TYPE" = "Firedancer" ] && [ -n "$FUTURE_FIREDANCER_MIN_VERSION" ] && ! compare_versions "$VALIDATOR_VERSION" "$FUTURE_FIREDANCER_MIN_VERSION"; then
            AFFECTED_VALIDATORS["$NETWORK"]+="🔻 $ALIAS ($NETWORK): Current $VERSION_TYPE Version $VALIDATOR_VERSION < Future Min ▶️ $FUTURE_FIREDANCER_MIN_VERSION\n"
        fi
    fi

    # Prepare future info for individual validator message
    FUTURE_INFO=""
    if [ -n "$FUTURE_EPOCH" ] && [ "$FUTURE_EPOCH" != "null" ]; then
        FUTURE_INFO="⚠️ **Upcoming Requirements for Epoch $FUTURE_EPOCH**\n"
        if [ "$MAJOR_VERSION" = "2" ]; then
            FUTURE_INFO+="⚙️ Future Agave ▶️ Min: ${FUTURE_AGAVE_MIN_VERSION:-N/A} ◀️ Max: ${FUTURE_AGAVE_MAX_VERSION:-N/A}\n"
        elif [ "$MAJOR_VERSION" = "0" ]; then
            FUTURE_INFO+="🔥 Future Firedancer ▶️ Min: ${FUTURE_FIREDANCER_MIN_VERSION:-N/A} ◀️ Max: ${FUTURE_FIREDANCER_MAX_VERSION:-N/A}\n"
        fi
    fi

    # Use enhanced message formatting
    MESSAGE=$(format_validator_message "$ALIAS" "$NETWORK" "$CURRENT_EPOCH" "$CLOSEST_EPOCH" "$VALIDATOR_IDENTITY" "$VERSION_TYPE" "$VALIDATOR_VERSION" "$MIN_VERSION" "$MAX_VERSION" "$VERSION_STATUS" "$FUTURE_INFO")

    # Store messages based on status
    if [ $STATUS_CODE -eq 0 ]; then
        GREEN_VALIDATORS+=("✅ $ALIAS ($NETWORK): $VERSION_TYPE Version $VALIDATOR_VERSION")
        SUCCESS_COUNT=$((SUCCESS_COUNT + 1))
        log_info "Validator $ALIAS ($NETWORK) is compliant: $VERSION_TYPE $VALIDATOR_VERSION"
    else
        RED_MESSAGES+=("{\"username\":\"VersionMonitorBot\",\"avatar_url\":\"$VALIDATOR_LOGO\",\"content\":\"$(echo "$MESSAGE" | sed 's/"/\\"/g')\"}")
        ERROR_COUNT=$((ERROR_COUNT + 1))
        log_warn "Validator $ALIAS ($NETWORK) is non-compliant: $VERSION_TYPE $VALIDATOR_VERSION"
    fi
    
    track_performance "validator_check_$ALIAS" "$validator_start_time"
done

log_info "Validator processing completed. Preparing summary messages."

# UPDATED: Streamlined summary message generation (no redundant summary report)
summary_start_time=$(date +%s)

# Generate the single comprehensive summary message
GREEN_MESSAGE=$(generate_optimized_summary_message)

# Send summary message
cat > "$MESSAGE_FILE" << EOF
$GREEN_MESSAGE
EOF

if [ ! -f "$MESSAGE_FILE" ] || [ ! -r "$MESSAGE_FILE" ]; then
    log_error "Failed to create message file '$MESSAGE_FILE' for validator summary"
else
    log_info "Sending summary Discord message with ${#GREEN_VALIDATORS[@]} compliant, ${#RED_MESSAGES[@]} non-compliant, and ${#FAILED_VALIDATORS[@]} failed validators"
    
    if send_discord_alert "info" "$(cat "$MESSAGE_FILE")" "📊"; then
        log_info "Summary Discord alert sent successfully"
    else
        log_error "Failed to send summary Discord alert"
    fi
    
    sleep 2
fi

track_performance "summary_generation" "$summary_start_time"

# Send individual Discord messages for each non-compliant validator
if [ ${#RED_MESSAGES[@]} -gt 0 ]; then
    log_info "Sending ${#RED_MESSAGES[@]} individual alerts for non-compliant validators"
    
    for i in "${!RED_MESSAGES[@]}"; do
        red_message="${RED_MESSAGES[$i]}"
        
        # Extract validator info for logging
        validator_info=$(echo "$red_message" | jq -r '.content' | grep "VALIDATOR MONITOR" | head -1)
        
        cat > "$MESSAGE_FILE" << EOF
$(echo "$red_message" | jq -r '.content' | sed 's/\\"/"/g')
EOF

        if [ ! -f "$MESSAGE_FILE" ] || [ ! -r "$MESSAGE_FILE" ]; then
            log_error "Failed to create message file '$MESSAGE_FILE' for non-compliant validator $((i+1))"
            continue
        fi

        log_debug "Sending individual alert for non-compliant validator $((i+1))/${#RED_MESSAGES[@]}"

        # Send with enhanced error handling
        alert_start_time=$(date +%s)
        if curl -s -H "Content-Type: application/json" -X POST -d "$red_message" "$DISCORD_WEBHOOK" | grep -q "\"id\""; then
            log_info "Individual Discord alert sent successfully for non-compliant validator $((i+1))"
        else
            log_error "Failed to send individual Discord alert for non-compliant validator $((i+1))"
        fi
        
        track_performance "individual_alert_$((i+1))" "$alert_start_time"
        sleep 1
    done
else
    log_info "No non-compliant validators found - no individual alerts to send"
fi

# Cleanup temporary files
rm -f "$MESSAGE_FILE" 2>/dev/null

SCRIPT_NAME=$(basename "$0")
log_info "Script $SCRIPT_NAME completed successfully"
log_info "Final stats: Processed=$VALIDATOR_COUNT, Success=$SUCCESS_COUNT, Errors=$ERROR_COUNT, Runtime=$(($(date +%s) - SCRIPT_START_TIME))s"

exit 0