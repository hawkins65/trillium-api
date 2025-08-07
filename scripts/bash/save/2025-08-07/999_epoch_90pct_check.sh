#!/bin/bash

# Source path initialization
source "$(dirname "$0")/000_init_paths.sh" || {
    echo "❌ Failed to source path initialization script" >&2
    exit 1
}
# 999_epoch_90pct_check.sh - Combined checker for stake files, leader schedule files, and shin voting files at 90% epoch completion
set -euo pipefail

# Source the common logging functions
source $TRILLIUM_SCRIPTS_BASH/999_common_log.sh
# Initialize enhanced logging
init_logging

# Get the basename of the script for Discord notifications
script_name=$(basename "$0")

log "INFO" "🚀 Starting comprehensive file checks for 90% epoch completion"

# Configuration
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
HOME_DIR="${HOME:-/home/smilax}"
API_DIR="$HOME_DIR/trillium_api"

log "INFO" "📁 Configuration - Script dir: $SCRIPT_DIR, Home dir: $HOME_DIR, API dir: $API_DIR"

# Script paths (all scripts should be in the same directory or API directory)
STAKE_FILES_CHECKER="$API_DIR/999_stake_files_check.sh"
LEADER_SCHEDULE_CHECKER="$API_DIR/999_leader_schedule_check.sh"
SHIN_VOTING_CHECKER="$API_DIR/999_check_shin_voting.sh"
PAGERDUTY_SCRIPT="$API_DIR/999_pagerduty.sh"

# Usage function
usage() {
    cat << EOF
Usage: $0 <epoch_number>

Perform comprehensive file checks at 90% epoch completion.
This script calls other 999_ scripts to check for:
1. Stake files for the current epoch
2. Leader schedule files for the next two epochs
3. Shin voting files (good.json and poor.json) for the current epoch

ARGUMENTS:
    epoch_number    The current epoch number

CHECKS PERFORMED:
    Stake Files (current epoch):
        \$HOME/trillium_api/solana-stakes_<epoch>.json
        \$HOME/trillium_api/solana-stakes_<epoch>._v1.json
        \$HOME/trillium_api/solana-stakes_<epoch>._v2.json
    
    Leader Schedule Files (future epochs):
        \$HOME/block-production/leaderboard/leader_schedules/epoch<epoch>-leaderschedule.json
        \$HOME/block-production/leaderboard/leader_schedules/epoch<epoch + 1>-leaderschedule.json

    Shin Voting Files (current epoch):
        \$HOME/get_slots/epoch<epoch>/run0/good.json
        \$HOME/get_slots/epoch<epoch>/run0/poor.json

EXAMPLES:
    $0 123    # Check all files for epoch 123 (stake, shin voting) and epochs 124, 125 (leader schedules)

EXIT CODES:
    0 - All checks passed
    1 - Some checks failed (alerts sent)
    2 - Error (invalid arguments, missing dependencies, missing scripts)
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

# Check if required scripts exist and are executable
check_dependencies() {
    log "INFO" "🔍 Checking for required script dependencies"
    
    local missing_scripts=()
    local scripts=(
        "$STAKE_FILES_CHECKER"
        "$LEADER_SCHEDULE_CHECKER"
        "$SHIN_VOTING_CHECKER"
        "$PAGERDUTY_SCRIPT"
    )
    
    for script in "${scripts[@]}"; do
        log "INFO" "🔍 Checking script: $(basename "$script")"
        if [[ ! -f "$script" ]]; then
            missing_scripts+=("$script (not found)")
            log "ERROR" "❌ Script not found: $script"
        elif [[ ! -x "$script" ]]; then
            missing_scripts+=("$script (not executable)")
            log "ERROR" "❌ Script not executable: $script"
        else
            log "INFO" "✅ Script OK: $(basename "$script")"
        fi
    done
    
    if [[ ${#missing_scripts[@]} -gt 0 ]]; then
        log "ERROR" "❌ Missing or non-executable required scripts:"
        printf '  %s\n' "${missing_scripts[@]}" >&2
        
        # Send error notification using centralized script
        bash "$DISCORD_NOTIFY_SCRIPT" error "$script_name" "Dependency check failed" "Missing scripts: ${missing_scripts[*]}" "1" ""
        
        return 1
    fi
    
    log "INFO" "✅ All script dependencies are available"
    return 0
}

# Send summary alert if any checks failed
send_summary_alert() {
    local epoch="$1"
    local failed_checks=("${@:2}")
    
    log "INFO" "📤 Preparing summary alert for multiple failed checks"
    
    local custom_details
    custom_details=$(jq -n \
        --arg epoch "$epoch" \
        --arg timestamp "$(date -u --iso-8601=seconds)" \
        --argjson failed_checks "$(printf '%s\n' "${failed_checks[@]}" | jq -R . | jq -s .)" \
        '{
            epoch: $epoch,
            check_type: "90_percent_epoch_completion_summary",
            failed_checks: $failed_checks,
            timestamp: $timestamp
        }')
    
    local summary="Multiple file validation failures for Solana epoch $epoch"
    
    log "INFO" "🚨 Sending summary PagerDuty alert for multiple failed checks"
    
    if "$PAGERDUTY_SCRIPT" \
        --severity critical \
        --source "solana-epoch-monitor-summary" \
        --details "$custom_details" \
        "$summary"; then
        log "INFO" "✅ Summary PagerDuty alert sent successfully"
        return 0
    else
        log "ERROR" "❌ Failed to send summary PagerDuty alert"
        return 1
    fi
}

# Run stake files check
run_stake_files_check() {
    local epoch="$1"
    
    log "INFO" "💰 Running stake files check for epoch $epoch"
    
    if "$STAKE_FILES_CHECKER" "$epoch"; then
        log "INFO" "✅ Stake files check PASSED for epoch $epoch"
        return 0
    else
        log "ERROR" "❌ Stake files check FAILED for epoch $epoch"
        return 1
    fi
}

# Run leader schedule files check
run_leader_schedule_check() {
    local epoch="$1"
    
    log "INFO" "👑 Running leader schedule files check for epoch $epoch"
    
    if "$LEADER_SCHEDULE_CHECKER" "$epoch"; then
        log "INFO" "✅ Leader schedule files check PASSED for epoch $epoch"
        return 0
    else
        log "ERROR" "❌ Leader schedule files check FAILED for epoch $epoch"
        return 1
    fi
}

# Run shin voting files check
run_shin_voting_check() {
    local epoch="$1"
    
    log "INFO" "🗳️ Running shin voting files check for epoch $epoch"
    
    if "$SHIN_VOTING_CHECKER" "$epoch"; then
        log "INFO" "✅ Shin voting files check PASSED for epoch $epoch"
        return 0
    else
        log "ERROR" "❌ Shin voting files check FAILED for epoch $epoch"
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
        bash "$DISCORD_NOTIFY_SCRIPT" error "$script_name" "Invalid arguments" "Expected 1 argument, got $#" "2" ""
        
        exit 2
    fi
    
    local epoch="$1"
    log "INFO" "📊 Processing epoch: $epoch"
    
    # Validate epoch number
    if ! validate_epoch "$epoch"; then
        usage
        
        # Send error notification using centralized script
        bash "$DISCORD_NOTIFY_SCRIPT" error "$script_name" "Invalid epoch number" "Epoch '$epoch' is not a valid positive integer" "2" "$epoch"
        
        exit 2
    fi
    
    # Check dependencies
    if ! check_dependencies; then
        exit 2
    fi
    
    log "INFO" "🎯 Starting comprehensive file checks for epoch $epoch at 90% completion"
    
    # Track failed checks
    local failed_checks=()
    local overall_success=true
    
    # Run stake files check
    if ! run_stake_files_check "$epoch"; then
        failed_checks+=("stake_files")
        overall_success=false
    fi
    
    # Run leader schedule files check
    if ! run_leader_schedule_check "$epoch"; then
        failed_checks+=("leader_schedule_files")
        overall_success=false
    fi
    
    # Run shin voting files check
    if ! run_shin_voting_check "$epoch"; then
        failed_checks+=("shin_voting_files")
        overall_success=false
    fi
    
    # Report results
    if [[ "$overall_success" == "true" ]]; then
        log "INFO" "🎉 SUCCESS: All file checks passed for epoch $epoch"
        
        # Send success notification using centralized script
        components_processed="   • Stake files validation
   • Leader schedule files validation  
   • Shin voting files validation
   • All checks completed at 90% epoch completion"
        
        bash "$DISCORD_NOTIFY_SCRIPT" success "$script_name" "$epoch" "90% Epoch Completion Checks Passed" "$components_processed"
        cleanup_logging
        
        exit 0
    else
        log "ERROR" "❌ FAILURE: Some file checks failed for epoch $epoch: ${failed_checks[*]}"
        
        # Send summary alert if multiple checks failed
        if [[ ${#failed_checks[@]} -gt 1 ]]; then
            log "WARN" "⚠️ Multiple checks failed, sending summary alert"
            send_summary_alert "$epoch" "${failed_checks[@]}" || true
        fi
        
        # Send error notification using centralized script
        bash "$DISCORD_NOTIFY_SCRIPT" error "$script_name" "File validation failures" "Failed checks: ${failed_checks[*]}" "1" "$epoch"
        
        exit 1
    fi
}

# Only run main if script is executed directly
if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
    main "$@"
fi
