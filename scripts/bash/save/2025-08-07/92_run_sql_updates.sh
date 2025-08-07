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

log "INFO" "🚀 Starting SQL updates execution process"

# Database connection details
DB_HOST="localhost"
DB_PORT="5432"
DB_USER="smilax"
DB_NAME="sol_blocks"

log "INFO" "🗄️ Database connection: $DB_USER@$DB_HOST:$DB_PORT/$DB_NAME"

# List of SQL files to process (alphabetically ordered)
SQL_FILES=(
    "92_update_city_names_special_characters.sql"
    "92_city-to-metro.sql"
    "92_set-country.sql"
    "92_set-continent-from-unknown.sql"
    "92_move_to_vs_low_votes.sql"
)

log "INFO" "📋 SQL files to process: ${#SQL_FILES[@]} files"
for file in "${SQL_FILES[@]}"; do
    log "INFO" "   • $file"
done

# Check if epoch was passed as parameter, otherwise fetch latest
if [ $# -eq 1 ]; then
    EPOCH=$1
    log "INFO" "📊 Using epoch from parameter: $EPOCH"
else
    log "INFO" "🔍 No epoch provided, fetching latest epoch from database"
    # Fetch the latest epoch
    EPOCH=$(psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" -t -c "
        SELECT MAX(epoch)
        FROM validator_stats;
    ")
    # Remove any whitespace from the epoch number
    EPOCH=$(echo "$EPOCH" | tr -d '[:space:]')
    log "INFO" "📈 Latest epoch found in database: $EPOCH"
fi

if [[ -z "$EPOCH" ]]; then
    log "ERROR" "❌ Failed to retrieve or set epoch"
    echo "Failed to retrieve or set epoch" >&2
    
    # Send error notification using centralized script
    bash "$DISCORD_NOTIFY_SCRIPT" error "$script_name" "Epoch retrieval" "Failed to get epoch from database or parameter" "1" ""
    
    exit 1
fi

log "INFO" "🎯 Processing SQL updates for epoch: $EPOCH"

# Iterate over each SQL file
for sql_file in "${SQL_FILES[@]}"; do
    log "INFO" "🔄 Processing SQL file: $sql_file"
    
    # Run the query and append the output to the epoch-based file, passing EPOCH as a variable
    if psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" -v epoch="$EPOCH" -f "$sql_file"; then
        log "INFO" "✅ SQL file $sql_file executed successfully"
    else
        exit_code=$?
        log "ERROR" "❌ Error running $sql_file (exit code: $exit_code)"
        
        # Send error notification using centralized script
        bash "$DISCORD_NOTIFY_SCRIPT" error "$script_name" "SQL file execution" "psql -f $sql_file" "$exit_code" "$EPOCH"
        
        echo "Error running $sql_file" >&2
        exit $exit_code
    fi
done

log "INFO" "🎉 All SQL updates completed successfully for epoch $EPOCH"

# Send success notification using centralized script
components_processed="   • City names special characters update
   • City to metro mapping
   • Country assignment
   • Continent assignment from unknown
   • Low votes validator movement"

bash "$DISCORD_NOTIFY_SCRIPT" success "$script_name" "$EPOCH" "SQL Updates Completed Successfully" "$components_processed"
cleanup_logging
