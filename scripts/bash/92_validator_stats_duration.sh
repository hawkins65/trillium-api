#!/bin/bash

# Source path initialization
source "$(dirname "$0")/000_init_paths.sh" || {
    echo "❌ Failed to source path initialization script" >&2
    exit 1
}

#
# 92_validator_stats_duration.sh
#
# This script runs the corresponding SQL file (92_validator_stats_duration.sql)
# to calculate and populate validator slot duration statistics for a given epoch.
#
# Usage:
#   ./92_validator_stats_duration.sh [EPOCH_NUMBER]
#
# If EPOCH_NUMBER is not provided, the script will automatically use the
# latest epoch found in the slot_duration table.
#

# Source the common logging functions
source $TRILLIUM_SCRIPTS_BASH/999_common_log.sh
# Initialize enhanced logging
init_logging

# Get the basename of the script for Discord notifications
script_name=$(basename "$0")

log "INFO" "🚀 Starting validator stats duration calculation"

# --- Database Connection Details ---
DB_HOST="localhost"
DB_PORT="5432"
DB_USER="smilax"
DB_NAME="sol_blocks"

log "INFO" "🗄️ Database connection: $DB_USER@$DB_HOST:$DB_PORT/$DB_NAME"

# --- Main Script Logic ---

# Check if an epoch was passed as a parameter; otherwise, fetch the latest one.
if [ $# -eq 1 ]; then
    EPOCH=$1
    log "INFO" "📊 Using epoch from parameter: $EPOCH"
else
    # Fetch the latest epoch from the slot_duration table.
    EPOCH_QUERY="SELECT MAX(epoch) FROM slot_duration;"
    log "INFO" "🔍 No epoch provided. Fetching the latest epoch with query: $EPOCH_QUERY"
    
    EPOCH=$(psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" -t -c "$EPOCH_QUERY")
    
    # Remove any whitespace from the result.
    EPOCH=$(echo "$EPOCH" | tr -d '[:space:]')
    log "INFO" "📈 Latest epoch found in slot_duration table: $EPOCH"
fi

# Exit if the epoch could not be determined.
if [[ -z "$EPOCH" ]]; then
    log "ERROR" "❌ Failed to retrieve or set epoch"
    echo "Error: Failed to retrieve or set epoch." >&2
    
    # Send error notification using centralized script
    bash "$DISCORD_NOTIFY_SCRIPT" error "$script_name" "Epoch retrieval" "Failed to get epoch from database or parameter" "1" ""
    
    exit 1
fi

log "INFO" "🎯 Processing validator stats duration for epoch: $EPOCH"

# Define the SQL file to be executed.
SQL_FILE="92_validator_stats_duration.sql"
log "INFO" "🗄️ Executing SQL file: $SQL_FILE"

# Execute the SQL file using psql, passing the EPOCH as a variable.
# The '-v' flag sets the 'epoch' variable within the SQL script.
# PAGER="" disables interactive paging for clean, non-interactive output.
log "INFO" "🔄 Running SQL query with epoch variable: $EPOCH"

PAGER="" psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" -v epoch="$EPOCH" -f "$SQL_FILE"

# Store the exit code of the psql command to check for success or failure.
EXIT_CODE=$?

# Check if the command succeeded and provide feedback.
if [[ $EXIT_CODE -eq 0 ]]; then
    log "INFO" "✅ SQL file $SQL_FILE successfully executed for epoch $EPOCH"
else
    log "ERROR" "❌ Error running $SQL_FILE for epoch $EPOCH (exit code: $EXIT_CODE)"
    
    # Send error notification using centralized script
    bash "$DISCORD_NOTIFY_SCRIPT" error "$script_name" "SQL execution" "psql -f $SQL_FILE" "$EXIT_CODE" "$EPOCH"
    
    echo "❌ $script_name: Error running $SQL_FILE for epoch $EPOCH. Exit code: $EXIT_CODE" >&2
    exit 1
fi

log "INFO" "🎉 Validator stats duration calculation completed successfully for epoch $EPOCH"

# Send success notification using centralized script
components_processed="   • Validator slot duration statistics calculation
   • Database updates for epoch $EPOCH
   • SQL file execution: $SQL_FILE"

bash "$DISCORD_NOTIFY_SCRIPT" success "$script_name" "$EPOCH" "Validator Stats Duration Completed Successfully" "$components_processed"
cleanup_logging
