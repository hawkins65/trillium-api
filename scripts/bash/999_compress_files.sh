#!/bin/bash

# Enable strict mode for safer scripting
set -euo pipefail

# Source the common logging functions
source /home/smilax/api/999_common_log.sh
# Initialize enhanced logging
init_logging

# Get the basename of the script for Discord notifications
script_name=$(basename "$0")

log "INFO" "🚀 Starting file compression process"

# Check for required arguments
if [ $# -ne 2 ]; then
    log "ERROR" "❌ Invalid number of arguments provided"
    echo "Usage: $0 <file_pattern> <output_directory>"
    
    # Send error notification using centralized script
    bash 999_discord_notify.sh error "$script_name" "Invalid arguments" "Usage: $0 <file_pattern> <output_directory>" "1" ""
    
    exit 1
fi

FILE_PATTERN="$1"
OUTPUT_DIR="$2"

log "INFO" "📂 File pattern: $FILE_PATTERN"
log "INFO" "📁 Output directory: $OUTPUT_DIR"

log "INFO" "📁 Creating output directory if it doesn't exist"
# Create output directory if it doesn't exist
if mkdir -p "$OUTPUT_DIR"; then
    log "INFO" "✅ Output directory ready: $OUTPUT_DIR"
else
    log "ERROR" "❌ Failed to create output directory: $OUTPUT_DIR"
    
    # Send error notification using centralized script
    bash 999_discord_notify.sh error "$script_name" "Directory creation failed" "mkdir -p $OUTPUT_DIR" "1" ""
    
    exit 1
fi

log "INFO" "🔍 Searching for files matching pattern: $FILE_PATTERN"

# Find all files matching the pattern in the current directory
shopt -s nullglob # Handle case where no files match
files_found=0
files_processed=0
files_skipped=0
files_failed=0

for input_file in $FILE_PATTERN; do
    # Check if file exists and is a regular file
    if [[ -f "$input_file" ]]; then
        files_found=$((files_found + 1))
        
        # Generate output filename by replacing .json with .zst
        output_file="$OUTPUT_DIR/${input_file%.json}.zst"
        
        log "INFO" "📄 Processing file: $input_file"
        log "INFO" "   🎯 Target: $output_file"
        
        # Skip if output file already exists
        if [[ -f "$output_file" ]]; then
            log "INFO" "⏭️ SKIPPING: Output file $output_file already exists"
            files_skipped=$((files_skipped + 1))
            continue
        fi
        
        log "INFO" "🗜️ Compressing $input_file"
        # Attempt to compress the file
        if zstd "$input_file" -o "$output_file" >/dev/null 2>&1; then
            log "INFO" "✅ Successfully compressed $input_file to $output_file"
            
            # Remove original file only if compression was successful
            if rm "$input_file" >/dev/null 2>&1; then
                log "INFO" "🗑️ Successfully deleted original file: $input_file"
                files_processed=$((files_processed + 1))
            else
                log "WARN" "⚠️ Failed to delete original file: $input_file"
                files_processed=$((files_processed + 1))
            fi
        else
            log "ERROR" "❌ Failed to compress $input_file"
            files_failed=$((files_failed + 1))
            
            # Send error notification for individual file compression failure
            bash 999_discord_notify.sh error "$script_name" "File compression failed" "zstd $input_file -o $output_file" "1" ""
        fi
    fi
done

# Check if no files were found
if [[ $files_found -eq 0 ]]; then
    log "WARN" "⚠️ No files found matching pattern: $FILE_PATTERN"
fi

# Summary logging
log "INFO" "📊 Compression summary for pattern: $FILE_PATTERN"
log "INFO" "   📁 Files found: $files_found"
log "INFO" "   ✅ Files processed: $files_processed"
log "INFO" "   ⏭️ Files skipped: $files_skipped"
log "INFO" "   ❌ Files failed: $files_failed"

# Determine overall success
if [[ $files_failed -eq 0 ]]; then
    if [[ $files_found -gt 0 ]]; then
        log "INFO" "🎉 Compression completed successfully for pattern: $FILE_PATTERN"
        
        # Send success notification using centralized script
        components_processed="   • Files found: $files_found
   • Files processed: $files_processed
   • Files skipped (already exist): $files_skipped
   • Pattern: $FILE_PATTERN
   • Output directory: $OUTPUT_DIR"
        
        bash 999_discord_notify.sh success "$script_name" "" "File Compression Completed Successfully" "$components_processed"
        cleanup_logging
    else
        log "INFO" "ℹ️ Compression completed (no files found) for pattern: $FILE_PATTERN"
    fi
else
    log "ERROR" "❌ Compression completed with $files_failed failures for pattern: $FILE_PATTERN"
    
    # Send error notification for overall failures
    bash 999_discord_notify.sh error "$script_name" "Compression failures" "$files_failed files failed to compress for pattern: $FILE_PATTERN" "1" ""
    
    exit 1
fi
