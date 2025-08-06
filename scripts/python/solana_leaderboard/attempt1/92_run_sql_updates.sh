#!/bin/bash

source 999_common_log.sh
# Initialize enhanced logging
init_logging

# Database connection details
DB_HOST="localhost"
DB_PORT="5432"
DB_USER="smilax"
DB_NAME="sol_blocks"

# List of SQL files to process (alphabetically ordered)
SQL_FILES=(
    "92_update_city_names_special_characters.sql"
    "92_city-to-metro.sql"
    "92_set-country.sql"
    "92_set-continent-from-unknown.sql"
    "92_move_to_vs_low_votes.sql"
)

# Check if epoch was passed as parameter, otherwise fetch latest
if [ $# -eq 1 ]; then
    EPOCH=$1
else
    # Fetch the latest epoch
    EPOCH=$(psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" -t -c "
        SELECT MAX(epoch)
        FROM validator_stats;
    ")
    # Remove any whitespace from the epoch number
    EPOCH=$(echo "$EPOCH" | tr -d '[:space:]')
fi

if [[ -z "$EPOCH" ]]; then
    log_message "ERROR" "Failed to retrieve or set epoch"
    exit 1
fi

BASENAME=$(basename "$0")
log_message "INFO" "$BASENAME Using epoch: $EPOCH"

# Iterate over each SQL file
for sql_file in "${SQL_FILES[@]}"; do
    log_message "INFO" "Running SQL file $sql_file..."
    # Run the query and append the output to the epoch-based file, passing EPOCH as a variable
    psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" -v epoch="$EPOCH" -f "$sql_file"

    # Check if the command succeeded
    if [[ $? -eq 0 ]]; then
        log_message "INFO" "SQL file $sql_file successfully ran"
    else
        log_message "ERROR" "Error running $sql_file"
    fi
done
