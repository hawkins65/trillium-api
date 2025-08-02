#!/bin/bash
# 999_leader_schedule_check.sh - Check for existence of leader schedule files for current and next epoch
set -euo pipefail

# Configuration
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
HOME_DIR="${HOME:-/home/smilax}"
LEADER_SCHEDULE_DIR="$HOME_DIR/block-production/leaderboard/leader_schedules"
PAGERDUTY_SCRIPT="$SCRIPT_DIR/999_pagerduty.sh"

# RPC configuration
DEFAULT_RPC_URL="https://api.mainnet-beta.solana.com"
FALLBACK_RPC_URL="https://wiser-young-star.solana-mainnet.quiknode.pro/887452d66f8b645b8824eab20011dbd3c315d84f/"
RPC_URL="${SOLANA_RPC_URL:-$DEFAULT_RPC_URL}"
RPC_TIMEOUT=10
RPC_MAX_RETRIES=3

# Source common logging functions
source "$SCRIPT_DIR/999_common_log.sh"

# Usage function
usage() {
    cat << EOF
Usage: $0 <current_epoch_number>

Check for existence of leader schedule files for the current and next epoch and alert if missing.
Also performs RPC connectivity checks.

ARGUMENTS:
    current_epoch_number    The current epoch number

FILES CHECKED:
    \$HOME/block-production/leaderboard/leader_schedules/epoch<current_epoch>-leaderschedule.json
    \$HOME/block-production/leaderboard/leader_schedules/epoch<current_epoch + 1>-leaderschedule.json

ENVIRONMENT VARIABLES:
    SOLANA_RPC_URL         RPC URL to use for connectivity checks (default: $DEFAULT_RPC_URL)
                          Falls back to QuickNode RPC after 3 failed attempts

EXAMPLES:
    $0 123    # Check leader schedule files for epochs 123 and 124
    SOLANA_RPC_URL=https://my-rpc.com $0 123    # Use custom RPC URL

EXIT CODES:
    0 - All files found and RPC accessible
    1 - Some files missing or RPC errors (alert sent)
    2 - Error (invalid arguments, missing dependencies)
EOF
}

# Check if epoch number is valid
validate_epoch() {
    local epoch="$1"
    if ! [[ "$epoch" =~ ^[0-9]+$ ]]; then
        log "ERROR" "Invalid epoch number '$epoch'. Must be a positive integer."
        return 1
    fi
    return 0
}

# Test RPC endpoint with a simple call
test_rpc_endpoint() {
    local rpc_url="$1"
    local test_name="$2"
    
    log "INFO" "Testing RPC endpoint ($test_name): $rpc_url"
    
    # Test basic connectivity with getHealth
    if curl -s --max-time "$RPC_TIMEOUT" \
        -H "Content-Type: application/json" \
        -d '{"jsonrpc":"2.0","id":1,"method":"getHealth"}' \
        "$rpc_url" > /dev/null 2>&1; then
        log "INFO" "RPC endpoint test passed ($test_name)"
        return 0
    else
        log "WARN" "RPC endpoint test failed ($test_name)"
        return 1
    fi
}

# Determine which RPC URL to use with fallback logic
select_rpc_url() {
    local current_rpc_url="$RPC_URL"
    
    # If user specified a custom RPC URL, use it without fallback
    if [[ -n "${SOLANA_RPC_URL:-}" ]]; then
        log "INFO" "Using custom RPC URL (no fallback): $current_rpc_url"
        echo "$current_rpc_url"
        return 0
    fi
    
    # Test default RPC URL with retries
    log "INFO" "Testing default RPC URL with up to $RPC_MAX_RETRIES attempts"
    local attempt=1
    while [[ $attempt -le $RPC_MAX_RETRIES ]]; do
        log "INFO" "Attempt $attempt/$RPC_MAX_RETRIES for default RPC"
        if test_rpc_endpoint "$DEFAULT_RPC_URL" "default-attempt-$attempt"; then
            log "INFO" "Default RPC URL is working, using: $DEFAULT_RPC_URL"
            echo "$DEFAULT_RPC_URL"
            return 0
        fi
        ((attempt++))
        if [[ $attempt -le $RPC_MAX_RETRIES ]]; then
            log "INFO" "Waiting 2 seconds before retry..."
            sleep 2
        fi
    done
    
    # Default RPC failed all attempts, try fallback
    log "WARN" "Default RPC URL failed $RPC_MAX_RETRIES attempts, testing fallback RPC"
    if test_rpc_endpoint "$FALLBACK_RPC_URL" "fallback"; then
        log "INFO" "Fallback RPC URL is working, using: $FALLBACK_RPC_URL"
        echo "$FALLBACK_RPC_URL"
        return 0
    else
        log "ERROR" "Both default and fallback RPC URLs failed"
        echo "$DEFAULT_RPC_URL"  # Return default anyway for error reporting
        return 1
    fi
}

# Check RPC connectivity and basic functionality
check_rpc_connectivity() {
    local rpc_url="$1"
    local errors=()
    
    log "INFO" "Performing comprehensive RPC connectivity checks on: $rpc_url"
    
    # Test 1: Basic connectivity with getHealth
    if ! curl -s --max-time "$RPC_TIMEOUT" \
        -H "Content-Type: application/json" \
        -d '{"jsonrpc":"2.0","id":1,"method":"getHealth"}' \
        "$rpc_url" > /dev/null 2>&1; then
        errors+=("RPC_CONNECTIVITY: Unable to connect to RPC endpoint")
        log "ERROR" "Failed to connect to RPC endpoint: $rpc_url"
    else
        log "INFO" "RPC connectivity test passed"
    fi
    
    # Test 2: Check if we can get slot information
    local slot_response
    if slot_response=$(curl -s --max-time "$RPC_TIMEOUT" \
        -H "Content-Type: application/json" \
        -d '{"jsonrpc":"2.0","id":1,"method":"getSlot"}' \
        "$rpc_url" 2>/dev/null); then
        
        # Check if response contains an error
        if echo "$slot_response" | jq -e '.error' > /dev/null 2>&1; then
            local error_msg
            error_msg=$(echo "$slot_response" | jq -r '.error.message // "Unknown RPC error"')
            errors+=("RPC_ERROR: $error_msg")
            log "ERROR" "RPC returned error for getSlot: $error_msg"
        else
            # Check if we got a valid slot number
            local slot
            slot=$(echo "$slot_response" | jq -r '.result // empty')
            if [[ -n "$slot" && "$slot" =~ ^[0-9]+$ ]]; then
                log "INFO" "RPC slot query successful (current slot: $slot)"
            else
                errors+=("RPC_INVALID_RESPONSE: Invalid slot response format")
                log "ERROR" "Invalid slot response from RPC: $slot_response"
            fi
        fi
    else
        errors+=("RPC_SLOT_QUERY: Failed to query current slot")
        log "ERROR" "Failed to query current slot from RPC"
    fi
    
    # Test 3: Check if we can get epoch information
    local epoch_response
    if epoch_response=$(curl -s --max-time "$RPC_TIMEOUT" \
        -H "Content-Type: application/json" \
        -d '{"jsonrpc":"2.0","id":1,"method":"getEpochInfo"}' \
        "$rpc_url" 2>/dev/null); then
        
        # Check if response contains an error
        if echo "$epoch_response" | jq -e '.error' > /dev/null 2>&1; then
            local error_msg
            error_msg=$(echo "$epoch_response" | jq -r '.error.message // "Unknown RPC error"')
            errors+=("RPC_EPOCH_ERROR: $error_msg")
            log "ERROR" "RPC returned error for getEpochInfo: $error_msg"
        else
            # Check if we got valid epoch info
            local epoch_info
            epoch_info=$(echo "$epoch_response" | jq -r '.result.epoch // empty')
            if [[ -n "$epoch_info" && "$epoch_info" =~ ^[0-9]+$ ]]; then
                log "INFO" "RPC epoch query successful (current epoch: $epoch_info)"
            else
                errors+=("RPC_INVALID_EPOCH: Invalid epoch response format")
                log "ERROR" "Invalid epoch response from RPC: $epoch_response"
            fi
        fi
    else
        errors+=("RPC_EPOCH_QUERY: Failed to query epoch information")
        log "ERROR" "Failed to query epoch information from RPC"
    fi
    
    # Return results
    if [[ ${#errors[@]} -eq 0 ]]; then
        log "INFO" "All RPC connectivity checks passed"
        return 0
    else
        log "WARN" "RPC connectivity issues detected: ${#errors[@]} error(s)"
        printf '%s\n' "${errors[@]}"
        return 1
    fi
}

# Check for leader schedule files
check_leader_schedule_files() {
    local current_epoch="$1"
    local next_epoch=$((current_epoch + 1))
    
    local found_files=()
    local missing_files=()
    local expected_files=(
        "epoch${current_epoch}-leaderschedule.json"
        "epoch${next_epoch}-leaderschedule.json"
    )
    
    log "INFO" "Checking for leader schedule files for epochs $current_epoch and $next_epoch in $LEADER_SCHEDULE_DIR"
    
    # Check each expected file
    for file in "${expected_files[@]}"; do
        local full_path="$LEADER_SCHEDULE_DIR/$file"
        log "INFO" "Checking: $full_path"
        if [[ -f "$full_path" ]]; then
            found_files+=("$file")
            log "INFO" "Found: $full_path"
        else
            missing_files+=("$file")
            log "WARN" "Missing: $full_path"
        fi
    done
    
    # Report results
    if [[ ${#missing_files[@]} -eq 0 ]]; then
        log "INFO" "SUCCESS: All leader schedule files found for epochs $current_epoch and $next_epoch"
        return 0
    else
        log "WARN" "WARNING: Missing ${#missing_files[@]} leader schedule file(s): ${missing_files[*]}"
        # Return the missing files list for alerting
        printf '%s\n' "${missing_files[@]}"
        return 1
    fi
}

# Send PagerDuty alert for missing files and/or RPC errors
send_alert() {
    local current_epoch="$1"
    local next_epoch=$((current_epoch + 1))
    shift
    local issues=("$@")
    
    local expected_files=(
        "epoch${current_epoch}-leaderschedule.json"
        "epoch${next_epoch}-leaderschedule.json"
    )
    
    local missing_files=()
    local rpc_errors=()
    
    # Separate missing files from RPC errors
    for issue in "${issues[@]}"; do
        if [[ "$issue" =~ ^RPC_ ]]; then
            rpc_errors+=("$issue")
        else
            missing_files+=("$issue")
        fi
    done
    
    # Create custom details JSON
    local custom_details
    custom_details=$(jq -n \
        --arg current_epoch "$current_epoch" \
        --arg next_epoch "$next_epoch" \
        --arg search_directory "$LEADER_SCHEDULE_DIR" \
        --arg rpc_url "$RPC_URL" \
        --arg rpc_selection_method "$(if [[ -n "${SOLANA_RPC_URL:-}" ]]; then echo "custom"; elif [[ "$RPC_URL" == "$DEFAULT_RPC_URL" ]]; then echo "default"; else echo "fallback"; fi)" \
        --arg timestamp "$(date -u --iso-8601=seconds)" \
        --argjson expected_files "$(printf '%s\n' "${expected_files[@]}" | jq -R . | jq -s .)" \
        --argjson missing_files "$(printf '%s\n' "${missing_files[@]}" | jq -R . | jq -s .)" \
        --argjson rpc_errors "$(printf '%s\n' "${rpc_errors[@]}" | jq -R . | jq -s .)" \
        '{
            current_epoch: $current_epoch,
            target_epochs: [$current_epoch, $next_epoch],
            file_type: "leader_schedule_files",
            expected_files: $expected_files,
            missing_files: $missing_files,
            rpc_errors: $rpc_errors,
            rpc_url: $rpc_url,
            rpc_selection_method: $rpc_selection_method,
            search_directory: $search_directory,
            timestamp: $timestamp,
            check_type: "current_and_next_epoch"
        }')
    
    # Create summary message
    local summary_parts=()
    if [[ ${#missing_files[@]} -gt 0 ]]; then
        summary_parts+=("Missing leader schedule files for epochs $current_epoch and/or $next_epoch")
    fi
    if [[ ${#rpc_errors[@]} -gt 0 ]]; then
        summary_parts+=("RPC connectivity issues detected")
    fi
    
    local summary
    summary=$(IFS='; '; echo "${summary_parts[*]}")
    
    # Determine severity based on issues
    local severity="warning"
    if [[ ${#missing_files[@]} -eq ${#expected_files[@]} ]] || [[ ${#rpc_errors[@]} -gt 2 ]]; then
        severity="critical"  # All files missing or multiple RPC errors
    fi
    
    log "INFO" "Sending PagerDuty alert (severity: $severity)"
    
    if [[ -x "$PAGERDUTY_SCRIPT" ]]; then
        if "$PAGERDUTY_SCRIPT" \
            --severity "$severity" \
            --source "solana-leader-schedule-monitor" \
            --details "$custom_details" \
            "$summary"; then
            log "INFO" "PagerDuty alert sent successfully"
            return 0
        else
            log "ERROR" "Failed to send PagerDuty alert"
            return 1
        fi
    else
        log "ERROR" "PagerDuty script not found or not executable: $PAGERDUTY_SCRIPT"
        return 1
    fi
}

# Main function
main() {
    # Check arguments
    if [[ $# -ne 1 ]]; then
        log "ERROR" "Exactly one argument (current epoch number) is required"
        usage
        exit 2
    fi
    
    local current_epoch="$1"
    
    # Validate epoch number
    if ! validate_epoch "$current_epoch"; then
        usage
        exit 2
    fi
    
    # Check if leader schedule directory exists
    if [[ ! -d "$LEADER_SCHEDULE_DIR" ]]; then
        log "ERROR" "Leader schedule directory does not exist: $LEADER_SCHEDULE_DIR"
        exit 2
    fi
    
    # Check if required tools are available
    if ! command -v curl &> /dev/null; then
        log "ERROR" "curl is required but not installed"
        exit 2
    fi
    
    if ! command -v jq &> /dev/null; then
        log "ERROR" "jq is required but not installed"
        exit 2
    fi
    
    log "INFO" "Starting leader schedule files and RPC connectivity check for current epoch $current_epoch"
    
    # Select RPC URL with fallback logic
    local selected_rpc_url
    if selected_rpc_url=$(select_rpc_url); then
        log "INFO" "Selected RPC URL: $selected_rpc_url"
        RPC_URL="$selected_rpc_url"
    else
        log "WARN" "RPC URL selection encountered issues, proceeding with: $RPC_URL"
    fi
    
    local all_issues=()
    local has_file_issues=false
    local has_rpc_issues=false
    
    # Check RPC connectivity
    local rpc_issues_output
    if rpc_issues_output=$(check_rpc_connectivity "$RPC_URL" 2>&1); then
        log "INFO" "RPC connectivity checks passed"
    else
        has_rpc_issues=true
        # Extract RPC error lines (lines that start with "RPC_")
        while IFS= read -r line; do
            if [[ "$line" =~ ^RPC_ ]]; then
                all_issues+=("$line")
            fi
        done <<< "$rpc_issues_output"
    fi
    
    # Check for leader schedule files
    local missing_files_output
    if missing_files_output=$(check_leader_schedule_files "$current_epoch" 2>&1); then
        log "INFO" "Leader schedule files check completed successfully for epochs $current_epoch and $((current_epoch + 1))"
    else
        has_file_issues=true
        # Extract missing files from output (lines that don't start with timestamp)
        while IFS= read -r line; do
            if [[ ! "$line" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}T ]] && [[ -n "$line" ]]; then
                # This line doesn't start with a timestamp, so it's a missing file
                all_issues+=("$line")
            fi
        done <<< "$missing_files_output"
    fi
    
    # Handle results
    if [[ "$has_file_issues" == "false" && "$has_rpc_issues" == "false" ]]; then
        log "INFO" "All checks completed successfully - files found and RPC accessible"
        exit 0
    else
        log "WARN" "Issues detected, sending alert"
        if send_alert "$current_epoch" "${all_issues[@]}"; then
            log "INFO" "Alert sent successfully"
        else
            log "ERROR" "Failed to send alert"
        fi
        exit 1
    fi
}

# Only run main if script is executed directly -- otherwise, we can `source` this file and use the functions internally
if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
    main "$@"
fi