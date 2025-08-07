#!/bin/bash

# Source path initialization
source "$(dirname "$0")/000_init_paths.sh" || {
    echo "❌ Failed to source path initialization script" >&2
    exit 1
}

source $HOME/trillium_api/scripts/bash/999_common_log.sh

# Enable strict mode for safer scripting
set -euo pipefail

###############################################
# Configuration
###############################################

readonly API_KEY="405f0ba7-0177-4c6c-8b2a-f4d5375726a2"
readonly URL="https://pro-api.coinmarketcap.com/v1/cryptocurrency/quotes/latest"
readonly PRICE_FILE="/home/smilax/trillium_api/data/monitoring/sol_price.txt"
readonly FLAG_FILE="/home/smilax/trillium_api/data/monitoring/sol_discord_flag.txt"
readonly FLAG_FILE_1H="/home/smilax/trillium_api/data/monitoring/sol_discord_1h_flag.txt"
readonly FLAG_FILE_24H="/home/smilax/trillium_api/data/monitoring/sol_discord_24h_flag.txt"
readonly BOUNDARY_FLAG_FILE="/home/smilax/trillium_api/data/monitoring/sol_discord_boundary_flag.txt"
readonly BOUNDARY_THRESHOLD=5
readonly DISCORD_WEBHOOK_URL="https://discord.com/api/webhooks/1353842448685863014/3bCLiX_VDix5xwMChAOCHj74vQ8OqVKbAQZ62xHu1GdsbbEPVCOzAsGQzxrfhkwcGW4g"
readonly DISCORD_USERNAME="SOL Price Monitor"
readonly DISCORD_AVATAR_URL="https://trillium.so/images/sol_price.png"
readonly BYPASS_TOP_OF_HOUR=true

# Emoji constants
readonly WARNING_EMOJI="🚨"
readonly ROCKET="🚀"
readonly FIRE="🔥"
readonly ICE="🧊"
readonly BELL="🔔"
readonly CLOCK="🕐"
readonly GREEN_ARROW_UP="▲"
readonly RED_ARROW_DOWN="🔻"
readonly DIAMOND="💎"
readonly LIGHTNING="⚡"
readonly DOLLAR_SIGN="💲"
readonly CHART_EMOJI="📊"

###############################################
# Utility Functions
###############################################

# Create directory if it doesn't exist
ensure_directory() {
    local dir
    dir=$(dirname "$1")
    if [[ ! -d "$dir" ]]; then
        mkdir -p "$dir"
        log_message "INFO" "Created directory: $dir"
    fi
}

# Validate numeric value
validate_number() {
    local value="$1"
    local name="$2"
    
    if [[ -z "$value" || "$value" == "null" ]]; then
        log_message "WARNING" "Empty or null $name: '$value'"
        return 1
    fi
    
    # Check if it's a valid number (including negative and decimal)
    if [[ ! "$value" =~ ^-?[0-9]+(\.[0-9]+)?$ ]]; then
        log_message "WARNING" "Invalid $name format: '$value'"
        return 1
    fi
    
    return 0
}

# Format price with appropriate precision
format_price() {
    local price="$1"
    printf "%.2f" "$price"
}

# Format percentage with color emoji and arrows
format_percentage() {
    local percent="$1"
    local formatted
    formatted=$(printf "%.2f" "$percent")
    
    if (( $(echo "$percent >= 0" | bc -l) )); then
        echo "$GREEN_ARROW_UP $formatted%"
    else
        echo "$RED_ARROW_DOWN $formatted%"
    fi
}

# Format percentage for performance overview (simplified with colored arrows)
format_percentage_overview() {
    local percent="$1"
    local time_period="$2"
    local formatted
    formatted=$(printf "%.2f" "$percent")
    
    if (( $(echo "$percent >= 0" | bc -l) )); then
        echo "$GREEN_ARROW_UP $formatted% ($time_period)"
    else
        echo "$RED_ARROW_DOWN $formatted% ($time_period)"
    fi
}

# Get change emoji based on percentage
get_change_emoji() {
    local percent="$1"
    local abs_percent
    abs_percent=$(echo "$percent" | tr -d '-')
    
    if (( $(echo "$abs_percent > 10" | bc -l) )); then
        if (( $(echo "$percent > 0" | bc -l) )); then
            echo "$ROCKET"
        else
            echo "$ICE"
        fi
    elif (( $(echo "$abs_percent > 5" | bc -l) )); then
        if (( $(echo "$percent > 0" | bc -l) )); then
            echo "$FIRE"
        else
            echo "$WARNING_EMOJI"
        fi
    else
        echo "$BELL"
    fi
}

# Retry mechanism for API calls
retry_api_call() {
    local max_attempts=3
    local attempt=1
    local response
    local temp_file
    
    # Create a temporary file to store clean response
    temp_file=$(mktemp)
    
    while [[ $attempt -le $max_attempts ]]; do
        log_message "INFO" "API call attempt $attempt/$max_attempts"
        
        # Write response directly to temp file to avoid stdout contamination
        if curl -s -G "$URL" \
            -H "Accept: application/json" \
            -H "User-Agent: SOL-Price-Monitor/1.0" \
            -d "CMC_PRO_API_KEY=$API_KEY" \
            -d "symbol=SOL" \
            --connect-timeout 30 \
            --max-time 60 \
            -o "$temp_file"; then
            
            # Read the clean response from file
            response=$(cat "$temp_file")
            local curl_exit_code=$?
            
            if [[ $curl_exit_code -eq 0 && -n "$response" ]]; then
                # Check if response is valid JSON first
                if ! echo "$response" | jq empty > /dev/null 2>&1; then
                    log_message "WARNING" "API returned invalid JSON (attempt $attempt)"
                    log_message "WARNING" "Response length: ${#response} chars"
                    log_message "WARNING" "First 100 chars: ${response:0:100}"
                else
                    # Check if response contains valid data structure
                    if echo "$response" | jq -e '.data.SOL.quote.USD.price' > /dev/null 2>&1; then
                        local test_price
                        test_price=$(echo "$response" | jq -r '.data.SOL.quote.USD.price')
                        if [[ "$test_price" != "null" && -n "$test_price" ]]; then
                            log_message "INFO" "Valid API response received (price: $test_price)"
                            rm -f "$temp_file"
                            echo "$response"
                            return 0
                        else
                            log_message "WARNING" "API returned null price (attempt $attempt)"
                        fi
                    else
                        log_message "WARNING" "API response missing expected data structure (attempt $attempt)"
                        # Check if there's an error in the response
                        local error_message
                        error_message=$(echo "$response" | jq -r '.status.error_message // empty' 2>/dev/null)
                        if [[ -n "$error_message" ]]; then
                            log_message "ERROR" "API error: $error_message"
                        fi
                    fi
                fi
            else
                log_message "WARNING" "Empty or invalid response (attempt $attempt)"
            fi
        else
            log_message "WARNING" "curl command failed (attempt $attempt)"
        fi
        
        if [[ $attempt -lt $max_attempts ]]; then
            local delay=$((attempt * 5))
            log_message "INFO" "Retrying in $delay seconds..."
            sleep $delay
        fi
        
        ((attempt++))
    done
    
    rm -f "$temp_file"
    log_message "ERROR" "All API attempts failed"
    return 1
}

###############################################
# Main Functions
###############################################

# Fetch and parse SOL price data
fetch_sol_data() {
    local response
    
    if ! response=$(retry_api_call); then
        log_message "ERROR" "Failed to fetch data from CoinMarketCap after all retries"
        exit 1
    fi
    
    log_message "INFO" "Successfully received API response"
    log_message "INFO" "Response starts with: ${response:0:50}..."
    log_message "INFO" "Response ends with: ...${response: -50}"
    log_message "INFO" "Parsing price data..."
    
    # Test if jq is working at all
    log_message "INFO" "Testing jq with simple command..."
    local jq_test
    jq_test=$(echo '{"test": "value"}' | jq -r '.test' 2>&1)
    local jq_test_exit=$?
    log_message "INFO" "jq test result: '$jq_test' (exit code: $jq_test_exit)"
    
    if [[ $jq_test_exit -ne 0 ]]; then
        log_message "ERROR" "jq is not working properly"
        exit 1
    fi
    
    # Parse data with individual error checking
    log_message "INFO" "Extracting current price..."
    # Redirect both stdout and stderr to capture any error messages
    current_price=$(echo "$response" | jq -r '.data.SOL.quote.USD.price // empty')
    local jq_exit_code=$?
    log_message "INFO" "jq exit code: $jq_exit_code"
    
    if [[ $jq_exit_code -ne 0 ]]; then
        log_message "ERROR" "jq failed to extract price (exit code: $jq_exit_code)"
        # Try to get jq error by running command again with stderr captured
        local jq_error
        jq_error=$(echo "$response" | jq -r '.data.SOL.quote.USD.price // empty' 2>&1 >/dev/null)
        log_message "ERROR" "jq error output: $jq_error"
        exit 1
    fi
    log_message "INFO" "Raw current_price: '$current_price'"
    
    log_message "INFO" "Extracting timestamp..."
    timestamp=$(echo "$response" | jq -r '.data.SOL.quote.USD.last_updated // empty' 2>&1)
    jq_exit_code=$?
    if [[ $jq_exit_code -ne 0 ]]; then
        log_message "WARNING" "jq failed to extract timestamp (exit code: $jq_exit_code): $timestamp"
        timestamp=""
    fi
    log_message "INFO" "Raw timestamp: '$timestamp'"
    
    log_message "INFO" "Extracting percentage changes..."
    percent_change_1h=$(echo "$response" | jq -r '.data.SOL.quote.USD.percent_change_1h // "0"' 2>&1)
    jq_exit_code=$?
    if [[ $jq_exit_code -ne 0 ]]; then
        log_message "WARNING" "jq failed to extract 1h change: $percent_change_1h"
        percent_change_1h="0"
    fi
    
    percent_change_24h=$(echo "$response" | jq -r '.data.SOL.quote.USD.percent_change_24h // "0"' 2>&1)
    jq_exit_code=$?
    if [[ $jq_exit_code -ne 0 ]]; then
        log_message "WARNING" "jq failed to extract 24h change: $percent_change_24h"
        percent_change_24h="0"
    fi
    
    percent_change_7d=$(echo "$response" | jq -r '.data.SOL.quote.USD.percent_change_7d // "0"' 2>&1)
    jq_exit_code=$?
    if [[ $jq_exit_code -ne 0 ]]; then
        log_message "WARNING" "jq failed to extract 7d change: $percent_change_7d"
        percent_change_7d="0"
    fi
    
    percent_change_30d=$(echo "$response" | jq -r '.data.SOL.quote.USD.percent_change_30d // "0"' 2>&1)
    jq_exit_code=$?
    if [[ $jq_exit_code -ne 0 ]]; then
        log_message "WARNING" "jq failed to extract 30d change: $percent_change_30d"
        percent_change_30d="0"
    fi
    
    log_message "INFO" "Raw percentage values: 1h='$percent_change_1h' 24h='$percent_change_24h' 7d='$percent_change_7d' 30d='$percent_change_30d'"
    
    log_message "INFO" "Raw percentage values: 1h='$percent_change_1h' 24h='$percent_change_24h' 7d='$percent_change_7d' 30d='$percent_change_30d'"
    
    # Validate all critical values
    log_message "INFO" "Validating current price..."
    if ! validate_number "$current_price" "current price"; then
        log_message "ERROR" "Failed to parse current price from response"
        log_message "ERROR" "Raw price value: '$current_price'"
        log_message "ERROR" "First 500 chars of response: ${response:0:500}"
        exit 1
    fi
    log_message "INFO" "Current price validation passed"
    
    if [[ -z "$timestamp" ]]; then
        log_message "WARNING" "No timestamp in response, using current time"
        timestamp=$(date -u +"%Y-%m-%dT%H:%M:%S.%3NZ")
    fi
    log_message "INFO" "Timestamp processed: $timestamp"
    
    # Handle null percentage values (treat as 0)
    log_message "INFO" "Processing percentage values..."
    [[ "$percent_change_1h" == "null" || -z "$percent_change_1h" ]] && percent_change_1h="0"
    [[ "$percent_change_24h" == "null" || -z "$percent_change_24h" ]] && percent_change_24h="0"
    [[ "$percent_change_7d" == "null" || -z "$percent_change_7d" ]] && percent_change_7d="0"
    [[ "$percent_change_30d" == "null" || -z "$percent_change_30d" ]] && percent_change_30d="0"
    
    # Validate percentage values
    log_message "INFO" "Validating percentage values..."
    validate_number "$percent_change_1h" "1h change" || percent_change_1h="0"
    validate_number "$percent_change_24h" "24h change" || percent_change_24h="0"
    validate_number "$percent_change_7d" "7d change" || percent_change_7d="0"
    validate_number "$percent_change_30d" "30d change" || percent_change_30d="0"
    log_message "INFO" "Percentage validation completed"
    
    # Format prices
    log_message "INFO" "Formatting values..."
    current_price=$(format_price "$current_price")
    percent_change_1h=$(format_price "$percent_change_1h")
    percent_change_24h=$(format_price "$percent_change_24h")
    percent_change_7d=$(format_price "$percent_change_7d")
    percent_change_30d=$(format_price "$percent_change_30d")
    log_message "INFO" "Formatting completed"
    
    log_message "INFO" "Successfully parsed SOL data: \$$current_price USD"
    log_message "INFO" "• 1h: $(format_percentage "$percent_change_1h") | 24h: $(format_percentage "$percent_change_24h") | 7d: $(format_percentage "$percent_change_7d")"
}

# Display formatted price data
display_price_data() {
    log_message "INFO" ""
    log_message "INFO" "$DIAMOND Current SOL Price: \$$current_price USD"
    log_message "INFO" "$CLOCK Last Updated: $timestamp"
    log_message "INFO" ""
    log_message "INFO" "$(format_percentage "$percent_change_1h") (1h)"
    log_message "INFO" "$(format_percentage "$percent_change_24h") (24h)"
    log_message "INFO" "$(format_percentage "$percent_change_7d") (7d)"
    log_message "INFO" "$(format_percentage "$percent_change_30d") (30d)"
    log_message "INFO" ""
}

# Check for significant price changes
check_significant_change() {
    significant_change=false
    significant_change_percent="0"
    previous_price_global=""
    local previous_price
    
    if [[ ! -f "$PRICE_FILE" ]]; then
        log_message "INFO" "$BELL First run detected - establishing baseline price"
        return 0
    fi
    
    previous_price=$(cat "$PRICE_FILE" 2>/dev/null || echo "0")
    
    if [[ -z "$previous_price" || "$previous_price" == "0" ]]; then
        log_message "WARNING" "Previous price not valid, skipping comparison"
        return 0
    fi
    
    previous_price=$(format_price "$previous_price")
    local percent_change
    percent_change=$(echo "scale=2; (($current_price - $previous_price) / $previous_price) * 100" | bc)
    local abs_change
    abs_change=$(echo "$percent_change" | tr -d '-')
    
    significant_change_percent="$percent_change"
    previous_price_global="$previous_price"
    
    if (( $(echo "$abs_change > 5" | bc -l) )); then
        log_message "INFO" "$(get_change_emoji "$percent_change") Significant change detected: $(format_percentage "$percent_change") (was \$$previous_price)"
        significant_change=true
    else
        log_message "INFO" "$BELL Price change since last run: $(format_percentage "$percent_change") (was \$$previous_price) - within normal range"
    fi
}

# Check boundary crossing
check_boundary_crossing() {
    boundary_crossed=false
    
    current_boundary=$(echo "scale=0; $current_price / $BOUNDARY_THRESHOLD" | bc)
    current_boundary_price=$(echo "scale=2; $current_boundary * $BOUNDARY_THRESHOLD" | bc | xargs printf "%.2f")
    
    if [[ ! -f "$BOUNDARY_FLAG_FILE" ]]; then
        log_message "INFO" "No previous boundary found. Setting initial boundary at \$$current_boundary_price"
        echo "$current_boundary" > "$BOUNDARY_FLAG_FILE"
        return 0
    fi
    
    local last_boundary
    last_boundary=$(cat "$BOUNDARY_FLAG_FILE" 2>/dev/null || echo "")
    
    if [[ -n "$last_boundary" && "$last_boundary" != "$current_boundary" ]]; then
        boundary_crossed=true
        local last_boundary_price
        last_boundary_price=$(echo "scale=2; $last_boundary * $BOUNDARY_THRESHOLD" | bc | xargs printf "%.2f")
        
        if (( $(echo "$current_boundary > $last_boundary" | bc -l) )); then
            crossing_direction="upward"
            boundary_crossed_price="$current_boundary_price"
            log_message "INFO" "$ROCKET SOL crossed upward boundary: \$$last_boundary_price → \$$current_boundary_price"
        else
            crossing_direction="downward"
            boundary_crossed_price="$last_boundary_price"
            log_message "INFO" "$ICE SOL crossed downward boundary: \$$last_boundary_price → \$$current_boundary_price"
        fi
    else
        log_message "INFO" "SOL price within same \$5 boundary: \$$current_boundary_price"
    fi
}

# Send Discord notification
send_discord_notification() {
    local reasons=("$@")
    local emoji="$DOLLAR_SIGN"
    local title="SOL Price Update"
    local reason_text
    
    # Determine emoji and title based on reasons
    if [[ " ${reasons[*]} " =~ " significant_change " ]] || 
       [[ " ${reasons[*]} " =~ " boundary " ]] ||
       [[ " ${reasons[*]} " =~ " 1h_change " ]] ||
       [[ " ${reasons[*]} " =~ " 24h_change " ]]; then
        emoji="$WARNING_EMOJI"
        title="SOL Price Alert"
    fi
    
    # Build reason text
    local reason_parts=()
    for reason in "${reasons[@]}"; do
        case "$reason" in
            "significant_change")
                reason_parts+=("Significant price change (>5%)")
                ;;
            "boundary")
                reason_parts+=("Crossed \$5 boundary")
                ;;
            "1h_change")
                reason_parts+=("1h change >5%")
                ;;
            "24h_change")
                reason_parts+=("24h change >5%")
                ;;
            "top_of_hour")
                reason_parts+=("Hourly update")
                ;;
        esac
    done
    
    # Join reasons with commas
    reason_text=$(IFS=', '; echo "${reason_parts[*]}")
    
    # Get timestamps
    local current_datetime_utc current_datetime_cst
    current_datetime_utc=$(date -u +"%A, %B %d, %Y at %I:%M:%S %p UTC")
    current_datetime_cst=$(TZ="America/Chicago" date +"%A, %B %d, %Y at %I:%M:%S %p %Z")
    
    # Build message
    local message
    message=$(cat << EOF
$emoji SOL Price Update $emoji

\`\`\`
💎 Current Price:        \$$current_price USD
$CLOCK Last Updated:         $timestamp
EOF
)
    
    # Add specific data based on reasons
    if [[ " ${reasons[*]} " =~ " significant_change " ]]; then
        message="$message
$(get_change_emoji "$significant_change_percent") Change:              $(format_percentage "$significant_change_percent")
📊 Previous Price:       \$$previous_price_global USD"
    fi
    
    if [[ " ${reasons[*]} " =~ " boundary " ]]; then
        if [[ "$crossing_direction" == "upward" ]]; then
            message="$message
$ROCKET Boundary Crossed:     \$$boundary_crossed_price USD $GREEN_ARROW_UP"
        else
            message="$message
$ICE Boundary Crossed:     \$$boundary_crossed_price USD $RED_ARROW_DOWN"
        fi
    fi
    
    message="$message
\`\`\`

**Reason:** $reason_text

**$CHART_EMOJI Performance Overview**
\`\`\`
$(format_percentage_overview "$percent_change_1h" "1 hour")
$(format_percentage_overview "$percent_change_24h" "24 hours")
$(format_percentage_overview "$percent_change_7d" "7 days")
$(format_percentage_overview "$percent_change_30d" "30 days")
\`\`\`

$CLOCK *Report generated at $current_datetime_utc ($current_datetime_cst)*"
    
    # Create JSON payload with proper escaping
    local json_payload
    json_payload=$(jq -n \
        --arg content "$message" \
        --arg username "$DISCORD_USERNAME" \
        --arg avatar_url "$DISCORD_AVATAR_URL" \
        '{content: $content, username: $username, avatar_url: $avatar_url}')
    
    # Send to Discord with retry logic
    local max_attempts=3
    local attempt=1
    
    while [[ $attempt -le $max_attempts ]]; do
        local discord_response
        discord_response=$(curl -s -H "Content-Type: application/json" \
            -X POST \
            -d "$json_payload" \
            "$DISCORD_WEBHOOK_URL" \
            --connect-timeout 30 \
            --max-time 60)
        
        local curl_exit_code=$?
        
        if [[ $curl_exit_code -eq 0 && -z "$discord_response" ]]; then
            log_message "INFO" "$LIGHTNING Message successfully sent to Discord"
            return 0
        else
            log_message "WARNING" "Discord send attempt $attempt failed (exit code: $curl_exit_code): $discord_response"
            if [[ $attempt -lt $max_attempts ]]; then
                sleep $((attempt * 2))
            fi
        fi
        
        ((attempt++))
    done
    
    log_message "ERROR" "Failed to send Discord message after all attempts"
    return 1
}

# Main execution logic
main() {
    log_message "INFO" "Starting SOL Price Monitor..."

    # Declare all global variables to avoid unbound variable errors
    current_price=""
    timestamp=""
    percent_change_1h=""
    percent_change_24h=""
    percent_change_7d=""
    percent_change_30d=""
    significant_change=false
    significant_change_percent="0"
    previous_price_global=""
    boundary_crossed=false
    crossing_direction=""
    boundary_crossed_price=""
    current_boundary=""
    current_boundary_price=""
    last_boundary_price=""
    
    # Ensure all required directories exist
    ensure_directory "$PRICE_FILE"
    ensure_directory "$FLAG_FILE"
    ensure_directory "$FLAG_FILE_1H"
    ensure_directory "$FLAG_FILE_24H"
    ensure_directory "$BOUNDARY_FLAG_FILE"
    
    # Fetch data
    fetch_sol_data
    display_price_data
    
    # Check for changes
    check_significant_change
    check_boundary_crossing
    
    # Save current price
    echo "$current_price" > "$PRICE_FILE"
    log_message "INFO" "$DIAMOND Price saved: \$$current_price USD"
    
    # Determine notification triggers
    local notification_reasons=()
    local current_time current_minute
    current_time=$(date +%s)
    current_minute=$(date +%M)
    
    # Check for significant change
    if [[ "$significant_change" == "true" ]]; then
        notification_reasons+=("significant_change")
    fi
    
    # Check for boundary crossing
    if [[ "$boundary_crossed" == "true" ]]; then
        notification_reasons+=("boundary")
    fi
    
    # Check for 1h change
    local abs_percent_change_1h
    abs_percent_change_1h=$(echo "$percent_change_1h" | tr -d '-')
    if (( $(echo "$abs_percent_change_1h > 5" | bc -l) )); then
        if [[ -f "$FLAG_FILE_1H" ]]; then
            local last_1h_time time_diff_1h
            last_1h_time=$(cat "$FLAG_FILE_1H" 2>/dev/null || echo "0")
            time_diff_1h=$((current_time - last_1h_time))
            if [[ $time_diff_1h -ge 3600 ]]; then
                notification_reasons+=("1h_change")
                log_message "INFO" "$WARNING_EMOJI 1h change trigger: $(format_percentage "$percent_change_1h") (threshold met)"
            else
                log_message "INFO" "$CLOCK 1h change: $(format_percentage "$percent_change_1h") (cooldown active)"
            fi
        else
            notification_reasons+=("1h_change")
            log_message "INFO" "$WARNING_EMOJI 1h change trigger: $(format_percentage "$percent_change_1h") (first detection)"
        fi
    else
        log_message "INFO" "$BELL 1h change: $(format_percentage "$percent_change_1h") (within normal range)"
    fi
    
    # Check for 24h change
    local abs_percent_change_24h
    abs_percent_change_24h=$(echo "$percent_change_24h" | tr -d '-')
    if (( $(echo "$abs_percent_change_24h > 5" | bc -l) )); then
        if [[ -f "$FLAG_FILE_24H" ]]; then
            local last_24h_time time_diff_24h
            last_24h_time=$(cat "$FLAG_FILE_24H" 2>/dev/null || echo "0")
            time_diff_24h=$((current_time - last_24h_time))
            if [[ $time_diff_24h -ge 86400 ]]; then
                notification_reasons+=("24h_change")
                log_message "INFO" "$WARNING_EMOJI 24h change trigger: $(format_percentage "$percent_change_24h") (threshold met)"
            else
                log_message "INFO" "$CLOCK 24h change: $(format_percentage "$percent_change_24h") (cooldown active)"
            fi
        else
            notification_reasons+=("24h_change")
            log_message "INFO" "$WARNING_EMOJI 24h change trigger: $(format_percentage "$percent_change_24h") (first detection)"
        fi
    else
        log_message "INFO" "$BELL 24h change: $(format_percentage "$percent_change_24h") (within normal range)"
    fi
    
    # Check for top of hour (if enabled)
    if [[ "$BYPASS_TOP_OF_HOUR" != "true" ]]; then
        if [[ ($current_minute -ge 55 && $current_minute -le 59) || ($current_minute -ge 0 && $current_minute -le 5) ]]; then
            # Add top of hour logic here if needed
            # notification_reasons+=("top_of_hour")
            :
        fi
    fi
    
    # Send notification if we have reasons
    if [[ ${#notification_reasons[@]} -gt 0 ]]; then
        if send_discord_notification "${notification_reasons[@]}"; then
            # Update flag files
            if [[ " ${notification_reasons[*]} " =~ " significant_change " ]] || 
               [[ " ${notification_reasons[*]} " =~ " top_of_hour " ]]; then
                echo "$current_time" > "$FLAG_FILE"
            fi
            if [[ " ${notification_reasons[*]} " =~ " 1h_change " ]]; then
                echo "$current_time" > "$FLAG_FILE_1H"
            fi
            if [[ " ${notification_reasons[*]} " =~ " 24h_change " ]]; then
                echo "$current_time" > "$FLAG_FILE_24H"
            fi
            if [[ " ${notification_reasons[*]} " =~ " boundary " ]]; then
                echo "$current_boundary" > "$BOUNDARY_FLAG_FILE"
            fi
        fi
    else
        log_message "INFO" "No notification triggers met - all conditions normal"
        log_message "INFO" ""
        log_message "INFO" "$DIAMOND Summary: SOL monitoring cycle completed"
        log_message "INFO" "• Current Price: \$${current_price} USD"
        log_message "INFO" "• Price Change: $(format_percentage "$significant_change_percent" 2>/dev/null || echo "< 5%")"
        log_message "INFO" "• 1h Change: $(format_percentage "$percent_change_1h")"
        log_message "INFO" "• 24h Change: $(format_percentage "$percent_change_24h")"
        log_message "INFO" "• Boundary: \$${current_boundary_price} (no crossing)"
        log_message "INFO" "• Next check: $(date -d '+5 minutes' '+%H:%M')"
    fi
    
    log_message "INFO" ""
    log_message "INFO" "$LIGHTNING SOL Price Monitor completed successfully"
}

###############################################
# Execute Main Function
###############################################

main "$@"
exit 0