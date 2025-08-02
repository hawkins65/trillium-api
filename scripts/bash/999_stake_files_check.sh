#!/bin/bash
# 999_stake_files_check.sh - Check for existence of stake files for a given epoch
set -euo pipefail

# Configuration
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
HOME_DIR="${HOME:-/home/smilax}"
API_DIR="$HOME_DIR/api"
PAGERDUTY_SCRIPT="$SCRIPT_DIR/999_pagerduty.sh"

# Source common logging functions
source "$SCRIPT_DIR/999_common_log.sh"

# Usage function
usage() {
    cat << EOF
Usage: $0 <epoch_number>

Check for existence of stake files for the specified epoch and alert if missing.

ARGUMENTS:
    epoch_number    The epoch number to check files for

FILES CHECKED:
    \$HOME/api/solana-stakes_<epoch>.json
    \$HOME/api/solana-stakes_<epoch>_v1.json
    \$HOME/api/solana-stakes_<epoch>_v2.json

EXAMPLES:
    $0 123    # Check stake files for epoch 123

EXIT CODES:
    0 - All files found or at least one file exists
    1 - No files found (alert sent)
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

# Check for stake files
check_stake_files() {
    local epoch="$1"
    local found_files=()
    local expected_files=(
        "solana-stakes_${epoch}.json"
        "solana-stakes_${epoch}_v1.json"
        "solana-stakes_${epoch}_v2.json"
    )
    
    log "INFO" "Checking for stake files for epoch $epoch in $API_DIR"
    
    # Check each expected file
    for file in "${expected_files[@]}"; do
        local full_path="$API_DIR/$file"
        log "INFO" "Checking: $full_path"
        if [[ -f "$full_path" ]]; then
            found_files+=("$file")
            log "INFO" "Found: $full_path"
        else
            log "WARN" "Missing: $full_path"
        fi
    done
    
    # Report results
    if [[ ${#found_files[@]} -eq 0 ]]; then
        log "ERROR" "CRITICAL: No stake files found for epoch $epoch"
        return 1
    else
        log "INFO" "SUCCESS: Found ${#found_files[@]} stake file(s) for epoch $epoch: ${found_files[*]}"
        return 0
    fi
}

# Send PagerDuty alert for missing files
send_missing_files_alert() {
    local epoch="$1"
    local expected_files=(
        "solana-stakes_${epoch}.json"
        "solana-stakes_${epoch}_v1.json"
        "solana-stakes_${epoch}_v2.json"
    )
    
    # Create custom details JSON
    local custom_details
    custom_details=$(jq -n \
        --arg epoch "$epoch" \
        --arg search_directory "$API_DIR" \
        --arg timestamp "$(date -u --iso-8601=seconds)" \
        --argjson expected_files "$(printf '%s\n' "${expected_files[@]}" | jq -R . | jq -s .)" \
        '{
            epoch: $epoch,
            file_type: "stake_files",
            expected_files: $expected_files,
            search_directory: $search_directory,
            timestamp: $timestamp,
            check_type: "90_percent_epoch_completion"
        }')
    
    local summary="Missing stake files for Solana epoch $epoch"
    
    log "INFO" "Sending PagerDuty alert for missing stake files"
    
    if [[ -x "$PAGERDUTY_SCRIPT" ]]; then
        if "$PAGERDUTY_SCRIPT" \
            --severity critical \
            --source "solana-stake-monitor" \
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
        log "ERROR" "Exactly one argument (epoch number) is required"
        usage
        exit 2
    fi
    
    local epoch="$1"
    
    # Validate epoch number
    if ! validate_epoch "$epoch"; then
        usage
        exit 2
    fi
    
    # Check if API directory exists
    if [[ ! -d "$API_DIR" ]]; then
        log "ERROR" "API directory does not exist: $API_DIR"
        exit 2
    fi
    
    log "INFO" "Starting stake files check for epoch $epoch"
    
    # Check for stake files
    if check_stake_files "$epoch"; then
        log "INFO" "Stake files check completed successfully for epoch $epoch"
        exit 0
    else
        log "WARN" "No stake files found for epoch $epoch, sending alert"
        if send_missing_files_alert "$epoch"; then
            log "INFO" "Alert sent successfully for missing stake files in epoch $epoch"
        else
            log "ERROR" "Failed to send alert for missing stake files in epoch $epoch"
        fi
        exit 1
    fi
}

# Only run main if script is executed directly -- otherwise, we can `source` this file and use the functions internally
if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
    main "$@"
fi