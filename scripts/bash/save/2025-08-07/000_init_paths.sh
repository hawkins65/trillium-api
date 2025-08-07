#!/usr/bin/env bash
# Path initialization script - source this at the beginning of every script
# This ensures consistent path resolution across all scripts

# Determine the base directory dynamically
# This works regardless of where the script is called from
if [[ -n "${BASH_SOURCE[0]}" ]]; then
    SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
    export TRILLIUM_BASE="$(cd "${SCRIPT_DIR}/../.." && pwd)"
elif [[ -n "$0" && "$0" != "bash" ]]; then
    SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
    export TRILLIUM_BASE="$(cd "${SCRIPT_DIR}/../.." && pwd)"
else
    # Fallback to hardcoded path if dynamic detection fails
    export TRILLIUM_BASE="/home/smilax/trillium_api"
fi

# Source the paths configuration
if [[ -f "${TRILLIUM_BASE}/config/paths.conf" ]]; then
    source "${TRILLIUM_BASE}/config/paths.conf"
else
    echo "❌ ERROR: Cannot find paths.conf at ${TRILLIUM_BASE}/config/paths.conf" >&2
    exit 1
fi

# Source common logging if available and not already sourced
if [[ -z "$COMMON_LOG_SOURCED" && -f "$COMMON_LOG_SCRIPT" ]]; then
    source "$COMMON_LOG_SCRIPT"
    export COMMON_LOG_SOURCED=1
fi

# Function to resolve script paths
resolve_script_path() {
    local script_name="$1"
    local script_type="${2:-bash}"  # Default to bash scripts
    
    case "$script_type" in
        bash)
            echo "${TRILLIUM_SCRIPTS_BASH}/${script_name}"
            ;;
        python)
            echo "${TRILLIUM_SCRIPTS_PYTHON}/${script_name}"
            ;;
        nodejs|node)
            echo "${TRILLIUM_SCRIPTS_NODEJS}/${script_name}"
            ;;
        sql)
            echo "${TRILLIUM_SCRIPTS_SQL}/${script_name}"
            ;;
        getslots|get_slots)
            echo "${TRILLIUM_SCRIPTS_GETSLOTS}/${script_name}"
            ;;
        *)
            echo "${TRILLIUM_SCRIPTS}/${script_name}"
            ;;
    esac
}

# Function to resolve data paths
resolve_data_path() {
    local data_file="$1"
    local data_type="${2:-epochs}"  # Default to epochs data
    
    case "$data_type" in
        epochs|epoch)
            echo "${TRILLIUM_DATA_EPOCHS}/${data_file}"
            ;;
        configs|config)
            echo "${TRILLIUM_DATA_CONFIGS}/${data_file}"
            ;;
        json)
            echo "${TRILLIUM_DATA_JSON}/${data_file}"
            ;;
        images|image)
            echo "${TRILLIUM_DATA_IMAGES}/${data_file}"
            ;;
        *)
            echo "${TRILLIUM_DATA}/${data_file}"
            ;;
    esac
}

# Function to safely change directory with logging
safe_cd() {
    local target_dir="$1"
    local description="${2:-directory}"
    
    if [[ -d "$target_dir" ]]; then
        cd "$target_dir" || {
            log_error "Failed to change to ${description}: ${target_dir}"
            return 1
        }
        log_debug "Changed to ${description}: ${target_dir}"
        return 0
    else
        log_error "${description} does not exist: ${target_dir}"
        return 1
    fi
}

# Export functions for use in other scripts
export -f resolve_script_path
export -f resolve_data_path
export -f safe_cd

# Validate paths on initialization
if ! validate_paths; then
    echo "❌ Path validation failed. Please check your installation." >&2
    exit 1
fi