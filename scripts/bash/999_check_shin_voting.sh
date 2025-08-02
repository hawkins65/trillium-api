#!/bin/bash
# 999_check_shin_voting.sh - Verify epoch presence in good.json and poor.json for Solana voting data
set -euo pipefail

# Source the common logging functions
source /home/smilax/api/999_common_log.sh
# Initialize enhanced logging
init_logging

# Get the basename of the script for Discord notifications
script_name=$(basename "$0")

log "INFO" "🚀 Starting Shin voting files validation process"

# Configuration
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
HOME_DIR="${HOME:-/home/smilax}"
BASE_DIR="$HOME_DIR/get_slots"
PAGERDUTY_SCRIPT="$HOME_DIR/api/999_pagerduty.sh"

log "INFO" "📁 Configuration - Base dir: $BASE_DIR, PagerDuty script: $PAGERDUTY_SCRIPT"

# Usage function
usage() {
    cat << EOF
Usage: $0 <epoch_number>

Verify that the specified Solana epoch number is listed in the data_epochs array
of both good.json and poor.json files located in:
$BASE_DIR/epoch<epoch_number>/run0/

ARGUMENTS:
    epoch_number    The Solana epoch number to verify

FILES CHECKED:
    $BASE_DIR/epoch<epoch_number>/run0/good.json
    $BASE_DIR/epoch<epoch_number>/run0/poor.json

EXAMPLES:
    $0 819    # Check if epoch 819 is in data_epochs of both good.json and poor.json

EXIT CODES:
    0 - All checks passed (epoch found in both files)
    1 - Some checks failed (epoch missing in one or both files, alerts sent)
    2 - Error (invalid arguments, missing files, or dependencies)
EOF
}

# Check if epoch number is valid
validate_epoch() {
    local epoch="$1"
    if ! [[ "$epoch" =~ ^[0-9]+$ ]]; then
        log "ERROR" "❌ Invalid epoch number '$epoch'. Must be a positive integer."
        return 1
    fi
    log "INFO" "✅ Epoch number validation passed: $epoch"
    return 0
}

# Check if required dependencies exist
check_dependencies() {
    log "INFO" "🔍 Checking for required dependencies"
    
    if [[ ! -f "$PAGERDUTY_SCRIPT" ]]; then
        log "ERROR" "❌ PagerDuty script not found: $PAGERDUTY_SCRIPT"
        
        # Send error notification using centralized script
        bash 999_discord_notify.sh error "$script_name" "Dependency missing" "PagerDuty script not found: $PAGERDUTY_SCRIPT" "2" ""
        
        return 1
    elif [[ ! -x "$PAGERDUTY_SCRIPT" ]]; then
        log "ERROR" "❌ PagerDuty script is not executable: $PAGERDUTY_SCRIPT"
        
        # Send error notification using centralized script
        bash 999_discord_notify.sh error "$script_name" "Dependency issue" "PagerDuty script not executable: $PAGERDUTY_SCRIPT" "2" ""
        
        return 1
    fi

    if ! command -v jq >/dev/null 2>&1; then
        log "ERROR" "❌ jq command not found. Please install jq."
        
        # Send error notification using centralized script
        bash 999_discord_notify.sh error "$script_name" "Dependency missing" "jq command not found" "2" ""
        
        return 1
    fi

    log "INFO" "✅ All dependencies are available"
    return 0
}

# Check if epoch exists in data_epochs array of a JSON file
check_epoch_in_file() {
    local epoch="$1"
    local file="$2"
    local file_name=$(basename "$file")

    log "INFO" "🔍 Checking epoch $epoch in file: $file_name"

    if [[ ! -f "$file" ]]; then
        log "ERROR" "❌ File $file_name does not exist at path: $file"
        return 1
    fi

    if [[ ! -r "$file" ]]; then
        log "ERROR" "❌ File $file_name is not readable at path: $file"
        return 1
    fi

    log "INFO" "📖 File $file_name exists and is readable"

    # Check if epoch is in data_epochs array
    if jq --argjson epoch "$epoch" '.data_epochs | index($epoch)' "$file" >/dev/null 2>&1; then
        if [[ $(jq --argjson epoch "$epoch" '.data_epochs | index($epoch)' "$file") != "null" ]]; then
            log "INFO" "✅ Epoch $epoch found in $file_name"
            return 0
        else
            log "ERROR" "❌ Epoch $epoch not found in data_epochs of $file_name"
            return 1
        fi
    else
        log "ERROR" "❌ Failed to parse $file_name or invalid JSON format"
        return 1
    fi
}

# Send PagerDuty alert for failed checks
send_pagerduty_alert() {
    local epoch="$1"
    local failed_files=("${@:2}")

    log "INFO" "📤 Preparing PagerDuty alert for failed checks"

    local custom_details
    custom_details=$(jq -n \
        --arg epoch "$epoch" \
        --arg timestamp "$(date -u --iso-8601=seconds)" \
        --argjson failed_files "$(printf '%s\n' "${failed_files[@]}" | jq -R . | jq -s .)" \
        '{
            epoch: $epoch,
            check_type: "shin_voting_epoch_check",
            failed_files: $failed_files,
            timestamp: $timestamp
        }')

    local summary="Failed to verify epoch $epoch in Solana voting data files"

    log "INFO" "🚨 Sending PagerDuty alert for failed checks in ${failed_files[*]}"
    
    if "$PAGERDUTY_SCRIPT" \
        --severity critical \
        --source "solana-shin-voting-monitor" \
        --details "$custom_details" \
        "$summary"; then
        log "INFO" "✅ PagerDuty alert sent successfully"
        return 0
    else
        log "ERROR" "❌ Failed to send PagerDuty alert"
        return 1
    fi
}

# Main function
main() {
    # Check arguments
    if [[ $# -ne 1 ]]; then
        log "ERROR" "❌ Invalid number of arguments provided"
        echo "ERROR: Exactly one argument (epoch number) is required" >&2
        usage
        
        # Send error notification using centralized script
        bash 999_discord_notify.sh error "$script_name" "Invalid arguments" "Expected 1 argument, got $#" "2" ""
        
        exit 2
    fi

    local epoch="$1"
    log "INFO" "📊 Processing epoch: $epoch"

    # Validate epoch number
    if ! validate_epoch "$epoch"; then
        usage
        
        # Send error notification using centralized script
        bash 999_discord_notify.sh error "$script_name" "Invalid epoch number" "Epoch '$epoch' is not a valid positive integer" "2" "$epoch"
        
        exit 2
    fi

    # Check dependencies
    if ! check_dependencies; then
        exit 2
    fi

    local epoch_dir="$BASE_DIR/epoch$epoch/run0"
    local good_file="$epoch_dir/good.json"
    local poor_file="$epoch_dir/poor.json"

    log "INFO" "📁 Target directory: $epoch_dir"
    log "INFO" "📄 Files to check:"
    log "INFO" "   • good.json: $good_file"
    log "INFO" "   • poor.json: $poor_file"

    log "INFO" "🎯 Starting Solana voting data checks for epoch $epoch"

    # Track failed checks
    local failed_files=()
    local overall_success=true

    # Check good.json
    if ! check_epoch_in_file "$epoch" "$good_file"; then
        failed_files+=("good.json")
        overall_success=false
    fi

    # Check poor.json
    if ! check_epoch_in_file "$epoch" "$poor_file"; then
        failed_files+=("poor.json")
        overall_success=false
    fi

    # Report results
    if [[ "$overall_success" == "true" ]]; then
        log "INFO" "🎉 SUCCESS: Epoch $epoch found in both good.json and poor.json"
        
        # Send success notification using centralized script
        components_processed="   • good.json epoch verification
   • poor.json epoch verification
   • JSON parsing and data_epochs array validation"
        
        bash 999_discord_notify.sh success "$script_name" "$epoch" "Shin Voting Files Validation Passed" "$components_processed"
        cleanup_logging
        
        exit 0
    else
        log "ERROR" "❌ FAILURE: Epoch $epoch missing in ${failed_files[*]}"

        # Send PagerDuty alert for failed checks
        send_pagerduty_alert "$epoch" "${failed_files[@]}" || true
        
        # Send error notification using centralized script
        bash 999_discord_notify.sh error "$script_name" "Voting files validation failed" "Epoch $epoch missing in: ${failed_files[*]}" "1" "$epoch"
        
        exit 1
    fi
}

# Only run main if script is executed directly
if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
    main "$@"
fi
