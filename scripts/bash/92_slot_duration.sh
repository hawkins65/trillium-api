#!/bin/bash

# Source the common logging functions
source /home/smilax/api/999_common_log.sh
# Initialize enhanced logging
init_logging

# Get the basename of the script for Discord notifications
script_name=$(basename "$0")

log "INFO" "🚀 Starting slot duration processing pipeline"

# Database connection details
DB_HOST="localhost"
DB_PORT="5432"
DB_USER="smilax"
DB_NAME="sol_blocks"

log "INFO" "🗄️ Database connection: $DB_USER@$DB_HOST:$DB_PORT/$DB_NAME"

# Path to CSV directory
CSV_DIR="/home/smilax/api/wss_slot_duration"

log "INFO" "📁 CSV directory: $CSV_DIR"

# Check if epoch was passed as parameter, otherwise fetch latest
if [ $# -eq 1 ]; then
    EPOCH=$1
    log "INFO" "📊 Using epoch from parameter: $EPOCH"
else
    log "INFO" "🔍 No epoch provided, fetching latest epoch from database"
    # Fetch the latest epoch
    EPOCH=$(psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" -t -c "SELECT MAX(epoch) FROM leader_schedule;")
    # Remove any whitespace from the epoch number
    EPOCH=$(echo "$EPOCH" | tr -d '[:space:]')
    log "INFO" "📈 Latest epoch found in database: $EPOCH"
fi

if [[ -z "$EPOCH" ]]; then
    log "ERROR" "❌ Failed to retrieve or set epoch"
    echo "Failed to retrieve or set epoch" >&2
    
    # Send error notification using centralized script
    bash 999_discord_notify.sh error "$script_name" "Epoch retrieval" "Failed to get epoch from database or parameter" "1" ""
    
    exit 1
fi

log "INFO" "🎯 Processing slot duration data for epoch: $EPOCH"

# Define the path to the combined CSV file
CSV_FILE="${CSV_DIR}/epoch${EPOCH}_slot_duration.csv"

log "INFO" "📄 Expected CSV file: $CSV_FILE"

# Check if the combined CSV file exists
if [[ ! -f "$CSV_FILE" ]]; then
    log "INFO" "⚠️ Combined CSV file not found: $CSV_FILE"
    log "INFO" "🐍 Running 92_wss_slot_duration.py to generate the CSV file..."

    # Run the Python script to generate the CSV file
    if python3 92_wss_slot_duration.py "$EPOCH"; then
        log "INFO" "✅ Successfully generated CSV file using Python script"
    else
        PYTHON_EXIT_CODE=$?
        log "ERROR" "❌ Failed to generate CSV file using 92_wss_slot_duration.py (exit code: $PYTHON_EXIT_CODE)"
        
        # Send error notification using centralized script
        bash 999_discord_notify.sh error "$script_name" "CSV generation" "python3 92_wss_slot_duration.py $EPOCH" "$PYTHON_EXIT_CODE" "$EPOCH"
        
        echo "Error: Failed to generate CSV file using 92_wss_slot_duration.py" >&2
        exit 1
    fi

    # Check again if the CSV file was created
    if [[ ! -f "$CSV_FILE" ]]; then
        log "ERROR" "❌ CSV file still not found after running Python script: $CSV_FILE"
        
        # Send error notification using centralized script
        bash 999_discord_notify.sh error "$script_name" "CSV file verification" "CSV file not created despite successful Python script execution" "1" "$EPOCH"
        
        echo "Error: CSV file still not found after running 92_wss_slot_duration.py: $CSV_FILE" >&2
        exit 1
    fi

    log "INFO" "✅ CSV file successfully generated: $CSV_FILE"
else
    log "INFO" "✅ Found existing CSV file: $CSV_FILE"
fi

# Run the slot duration SQL file
SQL_FILE="92_slot_duration.sql"
log "INFO" "🗄️ Running SQL file: $SQL_FILE"

# Create a temporary SQL file with the CSV path substituted
TEMP_SQL=$(mktemp)
log "INFO" "📝 Creating temporary SQL file: $TEMP_SQL"

sed "s|PLACEHOLDER_CSV_FILE|$CSV_FILE|g" "$SQL_FILE" > "$TEMP_SQL"

# Run the query, passing EPOCH as a variable and disable paging
log "INFO" "🔄 Executing SQL query with epoch variable: $EPOCH"
PAGER="" psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" -v epoch="$EPOCH" -f "$TEMP_SQL"

# Store the exit code
EXIT_CODE=$?

# Clean up temporary file
rm "$TEMP_SQL"
log "INFO" "🧹 Cleaned up temporary SQL file"

# Check if the command succeeded
if [[ $EXIT_CODE -eq 0 ]]; then
    log "INFO" "✅ SQL file $SQL_FILE successfully executed"
else
    log "ERROR" "❌ Error running $SQL_FILE (exit code: $EXIT_CODE)"
    
    # Send error notification using centralized script
    bash 999_discord_notify.sh error "$script_name" "SQL execution" "psql -f $SQL_FILE" "$EXIT_CODE" "$EPOCH"
    
    echo "❌ Error running $SQL_FILE" >&2
    exit 1
fi

log "INFO" "📊 Running validator stats calculation"

# Call the validator stats SQL script
SQL_STATS_FILE="92_validator_stats_duration.sql"
log "INFO" "🔄 Running SQL file: $SQL_STATS_FILE"
PAGER="" psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" -v epoch="$EPOCH" -f "$SQL_STATS_FILE"

# Check the exit code
STATS_EXIT_CODE=$?
if [[ $STATS_EXIT_CODE -ne 0 ]]; then
    log "ERROR" "❌ Error running $SQL_STATS_FILE (exit code: $STATS_EXIT_CODE)"
    
    # Send error notification using centralized script
    bash 999_discord_notify.sh error "$script_name" "Validator stats SQL" "psql -f $SQL_STATS_FILE" "$STATS_EXIT_CODE" "$EPOCH"
    
    echo "❌ Error running $SQL_STATS_FILE" >&2
    exit 1
fi

log "INFO" "✅ SQL file $SQL_STATS_FILE successfully executed"

log "INFO" "🐍 Running validator statistics Python script"

# Call the validator statistics Python script
if python3 92_slot_duration_statistics.py "$EPOCH"; then
    log "INFO" "✅ Python script 92_slot_duration_statistics.py successfully executed"
else
    PYTHON_STATS_EXIT_CODE=$?
    log "ERROR" "❌ Error running 92_slot_duration_statistics.py (exit code: $PYTHON_STATS_EXIT_CODE)"
    
    # Send error notification using centralized script
    bash 999_discord_notify.sh error "$script_name" "Statistics Python script" "python3 92_slot_duration_statistics.py $EPOCH" "$PYTHON_STATS_EXIT_CODE" "$EPOCH"
    
    echo "❌ Error running 92_slot_duration_statistics.py" >&2
    exit 1
fi

log "INFO" "📈 Running epoch aggregate data update"

# Call the epoch aggregate data update script
if python3 92_update_ead_slot_duration_stats.py "$EPOCH"; then
    log "INFO" "✅ Python script 92_update_ead_slot_duration_stats.py successfully executed"
else
    EAD_EXIT_CODE=$?
    log "ERROR" "❌ Error running 92_update_ead_slot_duration_stats.py (exit code: $EAD_EXIT_CODE)"
    
    # Send error notification using centralized script
    bash 999_discord_notify.sh error "$script_name" "EAD update Python script" "python3 92_update_ead_slot_duration_stats.py $EPOCH" "$EAD_EXIT_CODE" "$EPOCH"
    
    echo "❌ Error running 92_update_ead_slot_duration_stats.py" >&2
    exit 1
fi

log "INFO" "🎉 All slot duration processing scripts completed successfully for epoch $EPOCH"

# Send success notification using centralized script
components_processed="   • CSV file generation (if needed)
   • Slot duration SQL processing
   • Validator stats duration calculations
   • Slot duration statistics analysis
   • Epoch aggregate data updates"

bash 999_discord_notify.sh success "$script_name" "$EPOCH" "Slot Duration Processing Completed Successfully" "$components_processed"
cleanup_logging
