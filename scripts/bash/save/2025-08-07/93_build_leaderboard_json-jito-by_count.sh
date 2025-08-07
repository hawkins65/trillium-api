#!/bin/bash

# Source path initialization
source "$(dirname "$0")/000_init_paths.sh" || {
    echo "❌ Failed to source path initialization script" >&2
    exit 1
}

# Source the common logging functions
source $TRILLIUM_SCRIPTS_BASH/999_common_log.sh
# Initialize enhanced logging
init_logging

# Get the basename of the script for Discord notifications
script_name=$(basename "$0")

log "INFO" "🚀 Starting Jito leaderboard JSON building process"

# Check if epoch number is provided
if [ -z "$1" ]; then
    log "ERROR" "❌ No epoch number provided"
    echo "Error: No epoch number provided."
    echo "Usage: $0 <epoch_number>"
    
    # Send error notification using centralized script
    bash "$DISCORD_NOTIFY_SCRIPT" error "$script_name" "Missing parameter" "No epoch number provided as argument" "1" ""
    
    exit 1
fi

EPOCH="$1"
log "INFO" "📊 Processing epoch: $EPOCH"

# Define file paths
# Use environment variable if set, otherwise use default
HTML_DIR="${TRILLIUM_LEADERBOARD_HTML:-${TRILLIUM_DATA}/leaderboard/html}"
FILE_200="epoch${EPOCH}_validator_counts_charts-jito-200.html"
FILE_350="epoch${EPOCH}_validator_counts_charts-jito-350.html"
FILE_200_PATH="${HTML_DIR}/${FILE_200}"
FILE_350_PATH="${HTML_DIR}/${FILE_350}"
FILE_200_DEFAULT="validator_counts_charts-jito-200.html"
FILE_350_DEFAULT="validator_counts_charts-jito-350.html"

log "INFO" "📁 Expected output files:"
log "INFO" "   • $FILE_200"
log "INFO" "   • $FILE_350"
log "INFO" "   • $FILE_200_DEFAULT (copy)"
log "INFO" "   • $FILE_350_DEFAULT (copy)"

# Step 1: Run Python script
log "INFO" "🐍 Running Python script for epoch $EPOCH"
if python3 $TRILLIUM_SCRIPTS_PYTHON/93_build_leaderboard_json-jito-by_count.py "$EPOCH"; then
    log "INFO" "✅ Python script executed successfully"
else
    exit_code=$?
    log "ERROR" "❌ Python script failed for epoch $EPOCH (exit code: $exit_code)"
    
    # Send error notification using centralized script
    bash "$DISCORD_NOTIFY_SCRIPT" error "$script_name" "Python script execution" "python3 $TRILLIUM_SCRIPTS_PYTHON/93_build_leaderboard_json-jito-by_count.py $EPOCH" "$exit_code" "$EPOCH"
    
    echo "Error: Python script failed for epoch $EPOCH."
    exit 1
fi

# Check if both files were created
log "INFO" "🔍 Verifying HTML files were created in ${HTML_DIR}"
if [ ! -f "$FILE_200_PATH" ] || [ ! -f "$FILE_350_PATH" ]; then
    log "ERROR" "❌ One or both HTML files were not created"
    echo "Error: One or both HTML files were not created."
    echo "Expected files: $FILE_200_PATH and $FILE_350_PATH"
    ls -l "$FILE_200_PATH" "$FILE_350_PATH" 2>/dev/null || true
    
    # Send error notification using centralized script
    bash "$DISCORD_NOTIFY_SCRIPT" error "$script_name" "File creation verification" "HTML files not found after Python script execution" "1" "$EPOCH"
    
    exit 1
fi

log "INFO" "✅ Both HTML files created successfully"

# Step 2: Copy first file to web
log "INFO" "🌐 Copying $FILE_200 to web from ${HTML_DIR}"
if bash $TRILLIUM_SCRIPTS_BASH/copy-pages-to-web.sh "$FILE_200_PATH"; then
    log "INFO" "✅ Successfully copied $FILE_200 to web"
else
    exit_code=$?
    log "ERROR" "❌ Failed to copy $FILE_200 to web (exit code: $exit_code)"
    
    # Send error notification using centralized script
    bash "$DISCORD_NOTIFY_SCRIPT" error "$script_name" "Web copy operation" "bash $TRILLIUM_SCRIPTS_BASH/copy-pages-to-web.sh $FILE_200" "$exit_code" "$EPOCH"
    
    echo "Error: Failed to copy $FILE_200 to web."
    exit 1
fi

# Step 2.1: Copy first default file to web
log "INFO" "📋 Creating and copying default version: $FILE_200_DEFAULT"
if cp "$FILE_200_PATH" "${HTML_DIR}/$FILE_200_DEFAULT"; then
    log "INFO" "✅ Created default copy: $FILE_200_DEFAULT"
else
    exit_code=$?
    log "ERROR" "❌ Failed to create default copy (exit code: $exit_code)"
    
    # Send error notification using centralized script
    bash "$DISCORD_NOTIFY_SCRIPT" error "$script_name" "File copy operation" "cp $FILE_200_PATH ${HTML_DIR}/$FILE_200_DEFAULT" "$exit_code" "$EPOCH"
    
    echo "Error: Failed to create default copy."
    exit 1
fi

if bash $TRILLIUM_SCRIPTS_BASH/copy-pages-to-web.sh "${HTML_DIR}/$FILE_200_DEFAULT"; then
    log "INFO" "✅ Successfully copied $FILE_200_DEFAULT to web"
else
    exit_code=$?
    log "ERROR" "❌ Failed to copy $FILE_200_DEFAULT to web (exit code: $exit_code)"
    
    # Send error notification using centralized script
    bash "$DISCORD_NOTIFY_SCRIPT" error "$script_name" "Web copy operation" "bash $TRILLIUM_SCRIPTS_BASH/copy-pages-to-web.sh ${HTML_DIR}/$FILE_200_DEFAULT" "$exit_code" "$EPOCH"
    
    echo "Error: Failed to copy $FILE_200_DEFAULT to web."
    exit 1
fi

# Step 3: Copy second file to web
log "INFO" "🌐 Copying $FILE_350 to web from ${HTML_DIR}"
if bash $TRILLIUM_SCRIPTS_BASH/copy-pages-to-web.sh "$FILE_350_PATH"; then
    log "INFO" "✅ Successfully copied $FILE_350 to web"
else
    exit_code=$?
    log "ERROR" "❌ Failed to copy $FILE_350 to web (exit code: $exit_code)"
    
    # Send error notification using centralized script
    bash "$DISCORD_NOTIFY_SCRIPT" error "$script_name" "Web copy operation" "bash $TRILLIUM_SCRIPTS_BASH/copy-pages-to-web.sh $FILE_350" "$exit_code" "$EPOCH"
    
    echo "Error: Failed to copy $FILE_350 to web."
    exit 1
fi

# Step 3.1: Copy second default file to web
log "INFO" "📋 Creating and copying default version: $FILE_350_DEFAULT"
if cp "$FILE_350_PATH" "${HTML_DIR}/$FILE_350_DEFAULT"; then
    log "INFO" "✅ Created default copy: $FILE_350_DEFAULT"
else
    exit_code=$?
    log "ERROR" "❌ Failed to create default copy (exit code: $exit_code)"
    
    # Send error notification using centralized script
    bash "$DISCORD_NOTIFY_SCRIPT" error "$script_name" "File copy operation" "cp $FILE_350_PATH ${HTML_DIR}/$FILE_350_DEFAULT" "$exit_code" "$EPOCH"
    
    echo "Error: Failed to create default copy."
    exit 1
fi

if bash $TRILLIUM_SCRIPTS_BASH/copy-pages-to-web.sh "${HTML_DIR}/$FILE_350_DEFAULT"; then
    log "INFO" "✅ Successfully copied $FILE_350_DEFAULT to web"
else
    exit_code=$?
    log "ERROR" "❌ Failed to copy $FILE_350_DEFAULT to web (exit code: $exit_code)"
    
    # Send error notification using centralized script
    bash "$DISCORD_NOTIFY_SCRIPT" error "$script_name" "Web copy operation" "bash $TRILLIUM_SCRIPTS_BASH/copy-pages-to-web.sh ${HTML_DIR}/$FILE_350_DEFAULT" "$exit_code" "$EPOCH"
    
    echo "Error: Failed to copy $FILE_350_DEFAULT to web."
    exit 1
fi

# All steps succeeded, delete local files
log "INFO" "🧹 All steps completed successfully. Deleting local files..."
if rm -f "$FILE_200" "$FILE_350" "$FILE_200_DEFAULT" "$FILE_350_DEFAULT"; then
    log "INFO" "✅ Local files deleted successfully"
    echo "Local files $FILE_200 and $FILE_350 and $FILE_200_DEFAULT and $FILE_350_DEFAULT deleted."
else
    log "WARN" "⚠️ Failed to delete one or all local files"
    echo "Warning: Failed to delete one or all local files."
    ls -l "$FILE_200" "$FILE_350" "$FILE_200_DEFAULT" "$FILE_350_DEFAULT" 2>/dev/null || true
fi

log "INFO" "🌐 Deployment URLs:"
log "INFO" "   • https://trillium.so/pages/$FILE_200"
log "INFO" "   • https://trillium.so/pages/$FILE_350"
log "INFO" "   • https://trillium.so/pages/$FILE_200_DEFAULT"
log "INFO" "   • https://trillium.so/pages/$FILE_350_DEFAULT"

echo "Deployment complete for epoch $EPOCH!"
echo "Files are accessible at:"
echo "https://trillium.so/pages/$FILE_200"
echo "https://trillium.so/pages/$FILE_350"
echo "https://trillium.so/pages/$FILE_200_DEFAULT"
echo "https://trillium.so/pages/$FILE_350_DEFAULT"

log "INFO" "🎉 Jito leaderboard JSON building process completed successfully for epoch $EPOCH"

# Send success notification using centralized script
components_processed="   • Python script execution for Jito leaderboard data
   • HTML file generation (200 and 350 validator counts)
   • Web deployment of epoch-specific files
   • Web deployment of default files
   • Local file cleanup"

additional_notes="Files deployed to:
• https://trillium.so/pages/$FILE_200
• https://trillium.so/pages/$FILE_350
• https://trillium.so/pages/$FILE_200_DEFAULT
• https://trillium.so/pages/$FILE_350_DEFAULT"

bash "$DISCORD_NOTIFY_SCRIPT" success "$script_name" "$EPOCH" "Jito Leaderboard JSON Build Completed Successfully" "$components_processed" "$additional_notes"
cleanup_logging

exit 0
