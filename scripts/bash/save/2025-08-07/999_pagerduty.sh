#!/bin/bash

# Source path initialization
source "$(dirname "$0")/000_init_paths.sh" || {
    echo "❌ Failed to source path initialization script" >&2
    exit 1
}
# 999_pagerduty.sh - Universal PagerDuty alerting script
set -euo pipefail

# Configuration
## TESTNET_PAGERDUTY_INTEGRATION_KEY="84e552aeb34d4904d01066680fc04e76"
PAGERDUTY_INTEGRATION_KEY="4ce4a8e4c8294705c03e342e6e3c475c"
PAGERDUTY_URL="https://events.pagerduty.com/v2/enqueue"

# Default values
DEFAULT_SEVERITY="error"
DEFAULT_SOURCE="$(hostname)"

# Usage function
usage() {
    cat << EOF
Usage: $0 [OPTIONS] <summary>

Send alerts to PagerDuty via Events API v2

OPTIONS:
    -s, --severity LEVEL    Alert severity: info, warning, error, critical (default: error)
    -o, --source SOURCE     Alert source (default: hostname)
    -f, --file FILE         JSON file containing custom_details
    -d, --details JSON      Custom details as JSON string
    -k, --key KEY           PagerDuty integration key (overrides default)
    -a, --action ACTION     Event action: trigger, acknowledge, resolve (default: trigger)
    -h, --help              Show this help message

EXAMPLES:
    # Simple alert
    $0 "Database connection failed"
    
    # Critical alert with custom source
    $0 -s critical -o "web-server-01" "Service unavailable"
    
    # Alert with custom details from JSON string
    $0 -d '{"epoch": 123, "missing_files": ["file1.json"]}' "Missing stake files"
    
    # Alert with custom details from file
    $0 -f /tmp/alert_details.json "Epoch validation failed"

ENVIRONMENT VARIABLES:
    PAGERDUTY_INTEGRATION_KEY - Default integration key (can be overridden with -k)

EXIT CODES:
    0 - Success
    1 - Error (invalid arguments, missing dependencies, API failure)
EOF
}

# Source common logging functions
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/999_common_log.sh"

# Check dependencies
check_dependencies() {
    local missing_deps=()
    for cmd in curl jq; do
        if ! command -v "$cmd" >/dev/null 2>&1; then
            missing_deps+=("$cmd")
        fi
    done
    
    if [[ ${#missing_deps[@]} -gt 0 ]]; then
        log "ERROR" "Missing required dependencies: ${missing_deps[*]}"
        log "INFO" "Please install: sudo apt-get install ${missing_deps[*]}"
        exit 1
    fi
}

# Validate JSON
validate_json() {
    local json="$1"
    if ! echo "$json" | jq . >/dev/null 2>&1; then
        log "ERROR" "Invalid JSON provided: $json"
        return 1
    fi
    return 0
}

# Send PagerDuty alert
send_alert() {
    local severity="$1"
    local summary="$2"
    local source="$3"
    local custom_details="$4"
    local integration_key="$5"
    local action="$6"
    
    # Validate severity
    case "$severity" in
        info|warning|error|critical) ;;
        *) 
            log "ERROR" "Invalid severity '$severity'. Must be: info, warning, error, critical"
            return 1
            ;;
    esac
    
    # Validate action
    case "$action" in
        trigger|acknowledge|resolve) ;;
        *)
            log "ERROR" "Invalid action '$action'. Must be: trigger, acknowledge, resolve"
            return 1
            ;;
    esac
    
    # Build payload
    local payload
    payload=$(jq -n \
        --arg routing_key "$integration_key" \
        --arg event_action "$action" \
        --arg summary "$summary" \
        --arg source "$source" \
        --arg severity "$severity" \
        --argjson custom_details "$custom_details" \
        '{
            routing_key: $routing_key,
            event_action: $event_action,
            payload: {
                summary: $summary,
                source: $source,
                severity: $severity,
                timestamp: (now | strftime("%Y-%m-%dT%H:%M:%S.%fZ")),
                custom_details: $custom_details
            }
        }')
    
    log "INFO" "Sending PagerDuty alert..."
    log "INFO" "Severity: $severity"
    log "INFO" "Summary: $summary"
    log "INFO" "Source: $source"
    log "INFO" "Action: $action"
    
    # Send the alert
    local response http_code
    response=$(curl -s -w "%{http_code}" \
        -X POST \
        -H "Content-Type: application/json" \
        -d "$payload" \
        "$PAGERDUTY_URL")
    
    # Extract HTTP code (last 3 characters)
    http_code="${response: -3}"
    response="${response%???}"  # Remove last 3 characters
    
    # Check response
    if [[ "$http_code" -eq 202 ]]; then
        if echo "$response" | jq -e '.status == "success"' >/dev/null 2>&1; then
            local dedup_key
            dedup_key=$(echo "$response" | jq -r '.dedup_key // "unknown"')
            log "INFO" "SUCCESS: Alert sent successfully (dedup_key: $dedup_key)"
            return 0
        else
            log "ERROR" "PagerDuty API returned success HTTP code but failed status"
            log "ERROR" "Response: $response"
            return 1
        fi
    else
        log "ERROR" "PagerDuty API request failed (HTTP $http_code)"
        log "ERROR" "Response: $response"
        return 1
    fi
}

# Main function
main() {
    local severity="$DEFAULT_SEVERITY"
    local source="$DEFAULT_SOURCE"
    local custom_details="{}"
    local integration_key="${PAGERDUTY_INTEGRATION_KEY:-}"
    local action="trigger"
    local summary=""
    local details_file=""
    
    # Parse arguments
    while [[ $# -gt 0 ]]; do
        case $1 in
            -s|--severity)
                severity="$2"
                shift 2
                ;;
            -o|--source)
                source="$2"
                shift 2
                ;;
            -f|--file)
                details_file="$2"
                shift 2
                ;;
            -d|--details)
                custom_details="$2"
                shift 2
                ;;
            -k|--key)
                integration_key="$2"
                shift 2
                ;;
            -a|--action)
                action="$2"
                shift 2
                ;;
            -h|--help)
                usage
                exit 0
                ;;
            -*)
                log "ERROR" "Unknown option $1"
                usage
                exit 1
                ;;
            *)
                if [[ -z "$summary" ]]; then
                    summary="$1"
                else
                    log "ERROR" "Multiple summary arguments provided"
                    usage
                    exit 1
                fi
                shift
                ;;
        esac
    done
    
    # Validate required arguments
    if [[ -z "$summary" ]]; then
        log "ERROR" "Summary is required"
        usage
        exit 1
    fi
    
    if [[ -z "$integration_key" ]]; then
        log "ERROR" "PagerDuty integration key is required"
        log "INFO" "Set PAGERDUTY_INTEGRATION_KEY environment variable or use -k option"
        exit 1
    fi
    
    # Load custom details from file if specified
    if [[ -n "$details_file" ]]; then
        if [[ ! -f "$details_file" ]]; then
            log "ERROR" "Details file does not exist: $details_file"
            exit 1
        fi
        
        if ! custom_details=$(cat "$details_file"); then
            log "ERROR" "Failed to read details file: $details_file"
            exit 1
        fi
    fi
    
    # Validate custom details JSON
    if ! validate_json "$custom_details"; then
        exit 1
    fi
    
    # Check dependencies
    check_dependencies
    
    # Send the alert
    send_alert "$severity" "$summary" "$source" "$custom_details" "$integration_key" "$action"
}

# Only run main if script is executed directly -- otherwise, we can `source` this file and use the functions internally
if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
    main "$@"
fi