#!/bin/bash

# Source path initialization
source "$(dirname "$0")/000_init_paths.sh" || {
    echo "❌ Failed to source path initialization script" >&2
    exit 1
}

# Source common logging and configuration
source $TRILLIUM_SCRIPTS_BASH/999_common_log.sh
source $TRILLIUM_SCRIPTS_BASH/999_config_loader.sh

# Initialize logging
init_logging

log_info "🔍 Starting null MEV check for SS and Trillium validators"

# Configuration
DATA_DIR="/home/smilax/trillium_api/data/monitoring"
RESULTS_FILE="$DATA_DIR/null_mev_check_results.json"

# Load Discord webhook from centralized config
DISCORD_WEBHOOK=$(get_discord_webhook mev_monitoring)
if [ $? -ne 0 ]; then
    log_error "❌ Failed to load Discord webhook configuration"
    exit 1
fi

# Solana configuration
readonly MAINNET_RPC_URL="https://wiser-young-star.solana-mainnet.quiknode.pro/887452d66f8b645b8824eab20011dbd3c315d84f/"
readonly TESTNET_RPC_URL="https://wiser-thrilling-reel.solana-testnet.quiknode.pro/d05bbe3aa7a9377d63a89a869a3fba1093555029/"
readonly RPC_URL="$MAINNET_RPC_URL"

# Ensure data directory exists
mkdir -p "$DATA_DIR"

# Configuration - Load from configuration files
readonly TARGET_SCRIPT="$TRILLIUM_SCRIPTS_BASH/stakenet-validator-history.sh"
readonly PARAMETERS=("trillium" "ofv" "laine" "cogent" "ss" "pengu")
readonly TIMEOUT_SECONDS=300
readonly MAX_RETRIES=3
readonly RETRY_DELAY=10

# Validator identity mappings
declare -A VALIDATOR_IDENTITIES=(
    ["trillium"]="Tri1F8B6YtjkBztGCwBNSLEZib1EAqMUEUM7dTT7ZG3"
    ["ofv"]="DB7DNWMVQASMFxcjkwdr4w4eg3NmfjWTk2rqFMMbrPLA"
    ["laine"]="LA1NEzryoih6CQW3gwQqJQffK2mKgnXcjSQZSRpM3wc"
    ["cogent"]="Cogent51kHgGLHr7zpkpRjGYFXM57LgjHjDdqXd4ypdA"
    ["ss"]="SSmBEooM7RkmyuXxuKgAhTvhQZ36Z3G2WsmLGJKoQLY"
    ["pengu"]="peNgUgnzs1jGogUPW8SThXMvzNpzKSNf3om78xVPAYx"
)

#==============================================================================
# LEADER SLOT FUNCTIONS
#==============================================================================

# Function to get current slot and calculate slot duration
get_slot_info() {
    local current_slot performance_samples num_slots sample_period_secs slot_duration
    
    # Get current slot
    current_slot=$(solana -u "$RPC_URL" slot 2>/dev/null || echo "0")
    
    # Fetch performance samples to calculate slot duration
    performance_samples=$(curl -s "$RPC_URL" -X POST -H "Content-Type: application/json" \
        -d '{"jsonrpc":"2.0","id":1,"method":"getRecentPerformanceSamples","params":[1]}' 2>/dev/null)
    
    if [[ -n "$performance_samples" ]]; then
        num_slots=$(echo "$performance_samples" | jq -r '.result[0].numSlots' 2>/dev/null || echo "432000")
        sample_period_secs=$(echo "$performance_samples" | jq -r '.result[0].samplePeriodSecs' 2>/dev/null || echo "1800")
        slot_duration=$(echo "scale=6; $sample_period_secs / $num_slots" | bc -l 2>/dev/null || echo "0.417")
    else
        slot_duration="0.417"  # Default slot duration
    fi
    
    echo "$current_slot|$slot_duration"
}

# Function to get epoch information
get_epoch_info() {
    local epoch_details epoch_number epoch_current_slot epoch_completed_slots epoch_slot_count
    local epoch_first_slot
    
    epoch_details=$(solana -u "$RPC_URL" epoch-info 2>/dev/null || echo "")
    
    if [[ -n "$epoch_details" ]]; then
        epoch_number=$(echo "$epoch_details" | grep ^Epoch: | awk '{ print $2 }' || echo "0")
        epoch_current_slot=$(echo "$epoch_details" | grep ^Slot: | awk '{ print $2 }' || echo "0")
        epoch_completed_slots=$(echo "$epoch_details" | grep "^Epoch Completed Slots:" | awk '{ print $4 }' | cut -d '/' -f 1 || echo "0")
        epoch_slot_count=$(echo "$epoch_details" | grep "^Epoch Completed Slots:" | awk '{ print $4 }' | cut -d '/' -f 2 || echo "432000")
        
        epoch_first_slot=$((epoch_current_slot - epoch_completed_slots))
        
        echo "$epoch_number|$epoch_first_slot|$epoch_slot_count"
    else
        echo "0|0|432000"
    fi
}

# Function to check if validator has had a leader slot in current epoch
has_leader_slot_in_current_epoch() {
    local param="$1"
    local validator_identity="${VALIDATOR_IDENTITIES[$param]}"
    local current_slot slot_duration epoch_info epoch_first_slot
    
    # Skip check if validator identity is not defined
    if [[ -z "$validator_identity" ]]; then
        log_warning "⚠️ Validator identity not defined for '$param', proceeding with check"
        return 0  # Proceed with the check
    fi
    
    # Get current slot and epoch information
    local slot_info=$(get_slot_info)
    current_slot=$(echo "$slot_info" | cut -d'|' -f1)
    slot_duration=$(echo "$slot_info" | cut -d'|' -f2)
    
    epoch_info=$(get_epoch_info)
    epoch_first_slot=$(echo "$epoch_info" | cut -d'|' -f2)
    
    # Get leader schedule for this validator
    local leader_schedule=$(solana -u "$RPC_URL" leader-schedule 2>/dev/null | grep "$validator_identity" | awk '{print $1}' | sort -n || echo "")
    
    if [[ -z "$leader_schedule" ]]; then
        log_info "ℹ️ No leader slots found for '$param' in current epoch"
        return 1  # Skip this validator
    fi
    
    # Check if any leader slot in current epoch has already passed
    local has_past_leader_slot=false
    while IFS= read -r leader_slot; do
        if [[ -n "$leader_slot" ]] && [[ "$leader_slot" -ge "$epoch_first_slot" ]] && [[ "$leader_slot" -le "$current_slot" ]]; then
            has_past_leader_slot=true
            break
        fi
    done <<< "$leader_schedule"
    
    if [[ "$has_past_leader_slot" == "true" ]]; then
        log_info "👑 Validator '$param' has had leader slot(s) in current epoch"
        return 0  # Proceed with the check
    else
        # Find next leader slot to report timing
        local next_leader_slot=""
        while IFS= read -r leader_slot; do
            if [[ -n "$leader_slot" ]] && [[ "$leader_slot" -gt "$current_slot" ]]; then
                next_leader_slot="$leader_slot"
                break
            fi
        done <<< "$leader_schedule"
        
        if [[ -n "$next_leader_slot" ]]; then
            local seconds_to_slot=$(echo "($next_leader_slot - $current_slot) * $slot_duration" | bc 2>/dev/null || echo "0")
            local seconds_to_slot_int=${seconds_to_slot%.*}
            local hours=$((seconds_to_slot_int / 3600))
            local minutes=$(((seconds_to_slot_int % 3600) / 60))
            
            log_info "⏭️ Validator '$param' has not had leader slot in current epoch yet"
            log_info "ℹ️ Next leader slot: $next_leader_slot (in ~${hours}h ${minutes}m)"
        else
            log_info "⏭️ Validator '$param' has no upcoming leader slots in current epoch"
        fi
        
        return 1  # Skip this validator
    fi
}

# Function to run stakenet validator history check with retry logic
run_validator_check() {
    local param="$1"
    local validator_name="$param"
    local validator_pubkey="${VALIDATOR_IDENTITIES[$param]}"
    local attempt=1
    local max_attempts=$((MAX_RETRIES + 1))
    
    log_info "🔍 Checking $validator_name ($validator_pubkey)"
    
    if [ ! -f "$TARGET_SCRIPT" ]; then
        log_error "❌ Target script not found: $TARGET_SCRIPT"
        return 1
    fi
    
    while [[ $attempt -le $max_attempts ]]; do
        local temp_output=$(mktemp)
        local exit_code=0
        
        if [[ $attempt -gt 1 ]]; then
            log_info "🔄 Retrying $validator_name (attempt $attempt/$max_attempts)"
            sleep $RETRY_DELAY
        fi
        
        # Run the stakenet validator history check with timeout
        if timeout "$TIMEOUT_SECONDS" bash "$TARGET_SCRIPT" "$param" > "$temp_output" 2>&1; then
            log_info "✅ Successfully completed check for $validator_name"
            exit_code=0
            
            # Parse the output looking specifically for MEV column with null value
            local has_null_mev=false
            
            # Look for patterns that indicate null MEV in the actual MEV column/field
            if grep -E "mev.*:.*null|mev_commission.*:.*null|\"mev\".*:.*null" "$temp_output" > /dev/null 2>&1; then
                has_null_mev=true
                log_info "⚠️ NULL MEV detected for $validator_name in MEV column"
            elif grep -E "mev.*:.*0\.00|mev_commission.*:.*0\.00" "$temp_output" > /dev/null 2>&1; then
                # Also check for 0.00 which might indicate null MEV
                has_null_mev=true
                log_info "⚠️ Zero MEV commission detected for $validator_name"
            fi
            
            # Additional check: if the output contains validator data in JSON format
            if command -v jq &> /dev/null && jq empty "$temp_output" 2>/dev/null; then
                # If it's valid JSON, try to parse MEV field directly
                local mev_value=$(jq -r '.mev // .mev_commission // empty' "$temp_output" 2>/dev/null)
                if [[ "$mev_value" == "null" ]] || [[ "$mev_value" == "0" ]] || [[ -z "$mev_value" ]]; then
                    has_null_mev=true
                    log_info "⚠️ NULL or zero MEV value found in JSON for $validator_name"
                fi
            fi
            
            rm -f "$temp_output"
            
            if [ "$has_null_mev" = true ]; then
                echo "null"
            else
                echo "enabled"
            fi
            return 0
        else
            exit_code=$?
            log_error "❌ Check failed for $validator_name (exit code: $exit_code, attempt $attempt)"
            rm -f "$temp_output"
            
            if [[ $attempt -lt $max_attempts ]]; then
                log_info "⏳ Will retry in $RETRY_DELAY seconds..."
                ((attempt++))
            else
                log_error "❌ Max retries exceeded for $validator_name"
                echo "error"
                return $exit_code
            fi
        fi
    done
    
    echo "error"
    return 1
}

# Function to send Discord notification
send_discord_notification() {
    local message="$1"
    local color="$2"  # 16711680 for red
    
    local payload=$(cat <<EOF
{
    "embeds": [{
        "title": "🔍 NULL MEV Check Results",
        "description": "$message",
        "color": $color,
        "timestamp": "$(date -u +%Y-%m-%dT%H:%M:%S.000Z)",
        "footer": {
            "text": "Trillium API Monitoring"
        }
    }]
}
EOF
    )
    
    if curl -H "Content-Type: application/json" \
           -d "$payload" \
           "$DISCORD_WEBHOOK" > /dev/null 2>&1; then
        log_info "✅ Discord notification sent successfully"
    else
        log_error "❌ Failed to send Discord notification"
    fi
}

# Initialize results
null_mev_validators=()
total_checked=0
skipped_validators=0
failed_validators=0

log_info "📊 Checking ${#PARAMETERS[@]} validators: ${PARAMETERS[*]}"

# Check all validators
for param in "${PARAMETERS[@]}"; do
    if [ -n "$param" ]; then
        validator_name="$param"
        validator_pubkey="${VALIDATOR_IDENTITIES[$param]}"
        
        log_info "🔍 Processing validator: $validator_name"
        
        # Check if validator has had a leader slot in current epoch
        if ! has_leader_slot_in_current_epoch "$param"; then
            skipped_validators=$((skipped_validators + 1))
            log_info "⏭️ Skipping $validator_name - no leader slot in current epoch yet"
            continue
        fi
        
        mev_status=$(run_validator_check "$param")
        
        if [ "$mev_status" = "error" ]; then
            failed_validators=$((failed_validators + 1))
            log_error "❌ Failed to check validator $validator_name after retries"
        else
            total_checked=$((total_checked + 1))
            
            if [ "$mev_status" = "null" ]; then
                null_mev_validators+=("$validator_name: $validator_pubkey")
                log_info "⚠️ Validator $validator_name has null MEV"
            else
                log_info "✅ Validator $validator_name MEV status: $mev_status"
            fi
        fi
    fi
done

# Create results JSON
results_json=$(cat <<EOF
{
    "timestamp": "$(date -u +%Y-%m-%dT%H:%M:%S.000Z)",
    "total_validators": ${#PARAMETERS[@]},
    "total_validators_checked": $total_checked,
    "skipped_validators": $skipped_validators,
    "failed_validators": $failed_validators,
    "null_mev_count": ${#null_mev_validators[@]},
    "null_mev_validators": $(printf '%s\n' "${null_mev_validators[@]}" | jq -R . | jq -s .)
}
EOF
)

echo "$results_json" > "$RESULTS_FILE"
log_info "💾 Results saved to $RESULTS_FILE"

# Send Discord notification only if there are issues
if [ ${#null_mev_validators[@]} -gt 0 ] || [ $failed_validators -gt 0 ]; then
    message="🚨 **MEV CHECK ISSUES DETECTED**\n\n"
    if [ ${#null_mev_validators[@]} -gt 0 ]; then
        message="$message**NULL MEV DETECTED**\nFound ${#null_mev_validators[@]} validator(s) with null MEV:\n"
        for validator in "${null_mev_validators[@]}"; do
            message="$message• $validator\n"
        done
    fi
    if [ $failed_validators -gt 0 ]; then
        message="$message\n**CHECK FAILURES**\n$failed_validators validator(s) failed to check.\n"
    fi
    message="$message\n📊 **Summary:**"
    message="$message\n• Total validators: ${#PARAMETERS[@]}"
    message="$message\n• Checked: $total_checked"
    message="$message\n• Skipped (no leader slot): $skipped_validators"
    message="$message\n• Failed: $failed_validators"
    message="$message\n\nChecked at: $(date)"
    
    send_discord_notification "$message" 16711680  # Red color
    log_info "🚨 Alert sent for issues detected (${#null_mev_validators[@]} null MEV, $failed_validators failed)"
fi

# Print final summary
log_info "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
log_info "🏁 NULL MEV Check Summary:"
log_info "• Total validators: ${#PARAMETERS[@]}"
log_info "• Validators checked: $total_checked"
log_info "• Validators skipped: $skipped_validators"
if [ $failed_validators -gt 0 ]; then
    log_info "• Validators failed: $failed_validators"
fi
log_info "• NULL MEV found: ${#null_mev_validators[@]}"
log_info "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

log_info "🎉 NULL MEV check completed"

# Exit with appropriate code
if [ $failed_validators -gt 0 ]; then
    log_warning "⚠️ Some validators failed to check"
    exit 1
fi

# Cleanup logging
cleanup_logging