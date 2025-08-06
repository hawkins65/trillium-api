#!/bin/bash

# Source path initialization only if not already loaded
if [[ -z "$TRILLIUM_PATHS_LOADED" ]]; then
    source "$(dirname "$0")/000_init_paths.sh" || {
        echo "❌ Failed to source path initialization script" >&2
        exit 1
    }
fi

# Enhanced common logging functions for Solana data processing pipeline
# Version: 2.0
# Updated: $(date +'%Y-%m-%d')

# Configuration
LOG_BASE_DIR="${LOG_BASE_DIR:-$HOME/log}"
LOG_LEVEL="${LOG_LEVEL:-INFO}"  # DEBUG, INFO, WARN, ERROR
LOG_RETENTION_DAYS="${LOG_RETENTION_DAYS:-7}"  # How long to keep log files
MAX_LOG_SIZE="${MAX_LOG_SIZE:-100M}"  # Maximum size per log file before rotation

# Color codes for console output (only used if terminal supports colors)
declare -A LOG_COLORS
LOG_COLORS[DEBUG]="\033[37m"    # White
LOG_COLORS[INFO]="\033[32m"     # Green  
LOG_COLORS[WARN]="\033[33m"     # Yellow
LOG_COLORS[ERROR]="\033[31m"    # Red
LOG_COLORS[CRITICAL]="\033[41m" # Red background
LOG_COLORS[RESET]="\033[0m"     # Reset

# Script type colors
declare -A SCRIPT_COLORS
SCRIPT_COLORS[BASH]="\033[32m"      # Green
SCRIPT_COLORS[PYTHON]="\033[36m"    # Cyan
SCRIPT_COLORS[JS]="\033[33m"        # Yellow
SCRIPT_COLORS[SQL]="\033[35m"       # Magenta
SCRIPT_COLORS[SYSTEM]="\033[90m"    # Dark Gray
SCRIPT_COLORS[RESET]="\033[0m"      # Reset

# Log level priorities (for filtering)
declare -A LOG_PRIORITIES
LOG_PRIORITIES[DEBUG]=0
LOG_PRIORITIES[INFO]=1
LOG_PRIORITIES[WARN]=2
LOG_PRIORITIES[ERROR]=3

# Check if we should use colors (terminal supports it and output is to terminal)
use_colors() {
    [[ -t 2 ]] && [[ "${TERM:-}" != "dumb" ]] && command -v tput >/dev/null 2>&1
}

# Get the current log level priority
get_log_priority() {
    local level="$1"
    echo "${LOG_PRIORITIES[$level]:-1}"
}

# Check if message should be logged based on log level
should_log() {
    local message_level="$1"
    local current_priority=$(get_log_priority "$LOG_LEVEL")
    local message_priority=$(get_log_priority "$message_level")
    
    [[ $message_priority -ge $current_priority ]]
}

# Rotate log file if it exceeds maximum size
rotate_log_if_needed() {
    local log_file="$1"
    
    if [[ -f "$log_file" ]] && command -v du >/dev/null 2>&1; then
        local file_size
        file_size=$(du -h "$log_file" | cut -f1)
        
        # Simple size check - if file is larger than MAX_LOG_SIZE, rotate it  
        if [[ -n "$file_size" ]] && [[ "$file_size" =~ ^[0-9]+M$ ]]; then
            local current_size_mb="${file_size%M}"
            local max_size_mb="${MAX_LOG_SIZE%M}"
            # Ensure both variables are numeric before comparison, with safe defaults
            current_size_mb="${current_size_mb:-0}"
            max_size_mb="${max_size_mb:-100}"
            if [[ "$current_size_mb" =~ ^[0-9]+$ ]] && [[ "$max_size_mb" =~ ^[0-9]+$ ]] && [[ "${current_size_mb}" -gt "${max_size_mb}" ]] 2>/dev/null; then
                local rotated_file="${log_file}.$(date +'%Y%m%d_%H%M%S')"
                mv "$log_file" "$rotated_file" 2>/dev/null || true
                
                # Compress rotated log file to save space
                if command -v gzip >/dev/null 2>&1; then
                    gzip "$rotated_file" 2>/dev/null || true
                fi
            fi
        fi
    fi
}

# Clean up old log files
cleanup_old_logs() {
    local script_name_no_ext="$1"
    
    if [[ -n "$LOG_RETENTION_DAYS" ]] && command -v find >/dev/null 2>&1; then
        # Clean up old log files and rotated logs
        find "$LOG_BASE_DIR" -name "${script_name_no_ext}.log.*" -type f -mtime +${LOG_RETENTION_DAYS} -delete 2>/dev/null || true
        find "$LOG_BASE_DIR" -name "${script_name_no_ext}.log.*.gz" -type f -mtime +${LOG_RETENTION_DAYS} -delete 2>/dev/null || true
    fi
}

# Enhanced logging function with more features
log() {
    local level="$1"
    local message="$2"
    local caller_info="${3:-}"  # Optional: calling function/line info
    
    # Validate log level
    if [[ -z "${LOG_PRIORITIES[$level]:-}" ]]; then
        level="INFO"  # Default to INFO for invalid levels
    fi
    
    # Check if we should log this message
    if ! should_log "$level"; then
        return 0
    fi
    
    local script_name_no_ext=$(basename "$0" | sed 's/\.sh$//')
    local log_file="$LOG_BASE_DIR/${script_name_no_ext}.log"
    local timestamp="$(date +'%Y-%m-%d %H:%M:%S')"
    local pid="$$"
    
    # Ensure the log directory exists
    mkdir -p "$LOG_BASE_DIR" 2>/dev/null || true
    
    # Rotate log file if needed
    rotate_log_if_needed "$log_file"
    
    # Clean up old logs (only occasionally to avoid performance impact)
    if [[ $((RANDOM % 100)) -eq 0 ]]; then
        cleanup_old_logs "$script_name_no_ext" &
    fi
    
    # Determine script type and get colors
    local script_type="BASH"
    local script_color="${SCRIPT_COLORS[BASH]}"
    local level_color="${LOG_COLORS[$level]}"
    local reset_color="${LOG_COLORS[RESET]}"
    
    # Format the log message with script type
    local log_entry_plain
    local log_entry_colored
    
    if [[ -n "$caller_info" ]]; then
        log_entry_plain="[$timestamp] [$level] [${script_type}:${script_name_no_ext}] [PID:$pid] [$caller_info] - $message"
        log_entry_colored="[$timestamp] [${level_color}${level}${reset_color}] [${script_color}🐚${script_type}:${script_name_no_ext}${reset_color}] [PID:$pid] [$caller_info] - $message"
    else
        log_entry_plain="[$timestamp] [$level] [${script_type}:${script_name_no_ext}] [PID:$pid] - $message"
        log_entry_colored="[$timestamp] [${level_color}${level}${reset_color}] [${script_color}🐚${script_type}:${script_name_no_ext}${reset_color}] [PID:$pid] - $message"
    fi
    
    # Output to console with colors if supported
    if use_colors; then
        printf "%b\n" "$log_entry_colored" >&2
    else
        printf "%s\n" "$log_entry_plain" >&2
    fi
    
    # Always write to log file without colors
    printf "%s\n" "$log_entry_plain" >> "$log_file" 2>/dev/null || true
}

# Convenience functions for different log levels
log_debug() {
    log "DEBUG" "$1" "${2:-}"
}

log_info() {
    log "INFO" "$1" "${2:-}"
}

log_warn() {
    log "WARN" "$1" "${2:-}"
}

log_error() {
    log "ERROR" "$1" "${2:-}"
}

# Enhanced logging with automatic caller detection
log_with_caller() {
    local level="$1" 
    local message="$2"
    local caller_info=""
    
    # Try to get caller information
    if declare -f caller >/dev/null 2>&1; then
        local line_no=$(caller 1 | cut -d' ' -f1)
        local func_name=$(caller 1 | cut -d' ' -f2)
        caller_info="${func_name}:${line_no}"
    fi
    
    log "$level" "$message" "$caller_info"
}

# Log with context (useful for complex operations)
log_context() {
    local level="$1"
    local context="$2"  # e.g., "EPOCH_123", "DB_OPERATION"
    local message="$3"
    
    log "$level" "[$context] $message"
}

# Log execution time of a command or function
log_execution_time() {
    local level="${1:-INFO}"
    local description="$2"
    local start_time="$3"
    local end_time="${4:-$(date +%s.%N)}"
    
    if command -v bc >/dev/null 2>&1; then
        local duration=$(echo "$end_time - $start_time" | bc)
        log "$level" "⏱️ $description completed in ${duration}s"
    else
        # Fallback for systems without bc
        local duration=$((${end_time%.*} - ${start_time%.*}))
        log "$level" "⏱️ $description completed in ~${duration}s"
    fi
}

# Log system resource usage (CPU, Memory)
log_system_stats() {
    local level="${1:-DEBUG}"
    
    if command -v ps >/dev/null 2>&1; then
        local cpu_mem
        cpu_mem=$(ps -p $$ -o %cpu,%mem --no-headers 2>/dev/null || echo "N/A N/A")
        log "$level" "📊 System stats - CPU: $(echo $cpu_mem | cut -d' ' -f1)%, Memory: $(echo $cpu_mem | cut -d' ' -f2)%"
    fi
}

# Legacy function for backward compatibility
log_message() {
    log "$1" "$2"
}

# Initialize logging (call this at the start of scripts for better performance)
init_logging() {
    local script_name_no_ext=$(basename "$0" | sed 's/\.sh$//')
    
    # Ensure log directory exists
    mkdir -p "$LOG_BASE_DIR" 2>/dev/null || {
        echo "Warning: Could not create log directory $LOG_BASE_DIR" >&2
        LOG_BASE_DIR="/tmp"
    }
    
    # Set up signal handlers for cleanup
    trap 'log_info "🏁 Script terminated by signal"; exit 1' TERM INT
    
    # Log script start
    log_info "🚀 Script started: $0 with PID $$"
    log_debug "🔧 Log level: $LOG_LEVEL, Log directory: $LOG_BASE_DIR"
}

# Cleanup function (call this at the end of scripts)
cleanup_logging() {
    log_info "🏁 Script completed: $0"
}

# Export functions for use in subshells if needed
export -f log log_debug log_info log_warn log_error log_message should_log use_colors get_log_priority rotate_log_if_needed cleanup_old_logs log_with_caller log_context log_execution_time log_system_stats init_logging cleanup_logging
