#!/bin/bash

# Enable strict mode for safer scripting
set -euo pipefail

# Source the common logging functions
source /home/smilax/api/999_common_log.sh
# Initialize enhanced logging
init_logging

# Get the basename of the script for Discord notifications
script_name=$(basename "$0")

log "INFO" "🚀 Starting orchestrated file compression process"

# Function to execute compression with error handling
execute_compression() {
    local file_pattern="$1"
    local output_directory="$2"
    local description="$3"
    
    log "INFO" "🗜️ Starting $description compression"
    log "INFO" "   📂 Pattern: $file_pattern"
    log "INFO" "   📁 Output directory: $output_directory"
    
    if bash 999_compress_files.sh "$file_pattern" "$output_directory"; then
        log "INFO" "✅ Successfully completed $description compression"
        return 0
    else
        local exit_code=$?
        log "ERROR" "❌ Failed $description compression (exit code: $exit_code)"
        
        # Send error notification using centralized script
        bash 999_discord_notify.sh error "$script_name" "$description compression failed" "bash 999_compress_files.sh \"$file_pattern\" \"$output_directory\"" "$exit_code" ""
        
        return $exit_code
    fi
}

# Define compression tasks with descriptions
log "INFO" "📋 Orchestrating multiple file compression tasks"

# Call compress_files.sh for each file pattern
execute_compression "solana-stakes_*.json" "solana-stakes" "Solana stakes files"

execute_compression "90_xshin_all_validators_*.json" "xshin_all_validators" "Xshin all validators files"

execute_compression "90_xshin_all_award_winners_*.json" "xshin" "Xshin award winners files"

# Note: This appears to be a duplicate of the second task, keeping as-is for compatibility
execute_compression "90_xshin_all_validators_*.json" "xshin" "Xshin validators files (duplicate pattern)"

log "INFO" "🎉 Orchestrated file compression process completed successfully"

# Send success notification using centralized script
components_processed="   • Solana stakes files compression
   • Xshin all validators files compression
   • Xshin award winners files compression
   • Xshin validators files compression (duplicate pattern)
   • All compression tasks orchestrated successfully"

bash 999_discord_notify.sh success "$script_name" "" "File Compression Orchestration Completed Successfully" "$components_processed"
cleanup_logging
