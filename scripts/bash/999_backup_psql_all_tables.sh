#!/bin/bash

# Define user home directory for consistency
USER_HOME="/home/smilax"

# Source path initialization
source "$USER_HOME/api/scripts/bash/000_init_paths.sh" || {
    echo "❌ Failed to source path initialization script" >&2
    exit 1
}

# Source common logging
source "$USER_HOME/api/scripts/bash/999_common_log.sh"

# Initialize logging
init_logging

log_info "💾 Starting individual PostgreSQL table backups"

# Check for required commands
if ! command -v psql >/dev/null 2>&1; then
    log_error "❌ psql command not found"
    exit 1
fi
if ! command -v /usr/lib/postgresql/17/bin/pg_dump >/dev/null 2>&1; then
    log_error "❌ pg_dump command not found at /usr/lib/postgresql/17/bin/pg_dump"
    exit 1
fi
if ! command -v zstd >/dev/null 2>&1; then
    log_error "❌ zstd command not found"
    exit 1
fi

# Configuration
DB_NAME="sol_blocks"
DB_USER="smilax"
BACKUP_DIR="$USER_HOME/trillium_api/data/backups/psql/tables"
DATE=$(date +%Y%m%d_%H%M%S)
BACKUP_SUBFOLDER="$BACKUP_DIR/$DATE"
SUMMARY_FILE="$BACKUP_SUBFOLDER/_backup_summary.log"
SUCCESS_COUNT=0
FAIL_COUNT=0
TOTAL_TABLES=0
SKIPPED_COUNT=0
UNKNOWN_TABLES=()
PARALLEL_JOBS=10

# Backup configuration file
BACKUP_CONFIG="$TRILLIUM_CONFIG/backup_tables.conf"

# Discord notification script
DISCORD_NOTIFY_SCRIPT="$USER_HOME/api/scripts/bash/999_discord_notify.sh"

# Remote backup directories
REMOTE_DIR1="/mnt/gdrive/backups/psql_backup"
REMOTE_DIR2="/mnt/idrive/backups/psql_backup"

# Ensure backup directory exists
mkdir -p "$BACKUP_SUBFOLDER"

log_info "📊 Database: $DB_NAME"
log_info "👤 User: $DB_USER"
log_info "📁 Backup directory: $BACKUP_SUBFOLDER"

# Load backup configuration
declare -A BACKUP_FLAGS
if [[ -f "$BACKUP_CONFIG" ]]; then
    log_info "📋 Loading backup configuration from: $BACKUP_CONFIG"
    while IFS=':' read -r table flag; do
        [[ "$table" =~ ^#.*$ || -z "$table" ]] && continue
        table=$(echo "$table" | xargs)
        flag=$(echo "$flag" | xargs)
        BACKUP_FLAGS["$table"]="$flag"
    done < "$BACKUP_CONFIG"
    log_info "✅ Loaded configuration for ${#BACKUP_FLAGS[@]} tables"
else
    log_error "❌ Backup configuration file not found: $BACKUP_CONFIG"
    log_error "Creating default configuration file..."
    echo "# PostgreSQL Table Backup Configuration" > "$BACKUP_CONFIG"
    echo "# Format: table_name:backup_flag (y/n)" >> "$BACKUP_CONFIG"
    echo "# Tables marked with 'y' will be backed up" >> "$BACKUP_CONFIG"
    echo "# Tables marked with 'n' will be skipped" >> "$BACKUP_CONFIG"
    echo "" >> "$BACKUP_CONFIG"
fi

# Get list of all tables in the public schema
log_info "🔍 Retrieving list of all tables in public schema..."
TABLES=$(psql -h localhost -U "$DB_USER" -d "$DB_NAME" -t -c "SELECT tablename FROM pg_tables WHERE schemaname = 'public' ORDER BY tablename;")

# Check if table list is empty
if [ -z "$TABLES" ]; then
    log_error "❌ No tables found in database $DB_NAME (public schema)"
    if [[ -f "$DISCORD_NOTIFY_SCRIPT" ]]; then
        discord_message="🚨 **Backup Failure**\n\nScript: \`999_backup_psql_all_tables.sh\`\nStatus: **No tables found**\nDatabase: \`$DB_NAME\`\nPlease check database connectivity or schema."
        bash "$DISCORD_NOTIFY_SCRIPT" "custom" "999_backup_psql_all_tables" "Backup Failure" "$discord_message" "🚨" 2>/dev/null || \
            log_error "Failed to send Discord notification for empty table list"
    fi
    cleanup_logging
    exit 1
fi

# Count total tables
TOTAL_TABLES=$(echo "$TABLES" | grep -v "^$" | wc -l)
log_info "📊 Found $TOTAL_TABLES tables to backup"

# Initialize summary file
echo "PostgreSQL Individual Table Backup Summary" > "$SUMMARY_FILE"
echo "=========================================" >> "$SUMMARY_FILE"
echo "Date: $(date)" >> "$SUMMARY_FILE"
echo "Database: $DB_NAME" >> "$SUMMARY_FILE"
echo "Total tables: $TOTAL_TABLES" >> "$SUMMARY_FILE"
echo "----------------------------------------" >> "$SUMMARY_FILE"
echo "" >> "$SUMMARY_FILE"
echo "Backup Results:" >> "$SUMMARY_FILE"
echo "----------------------------------------" >> "$SUMMARY_FILE"

# Function to backup a single table
backup_table() {
    local table=$1
    local backup_file="$BACKUP_SUBFOLDER/${table}_backup.sql"
    
    log_info "🔄 Backing up table: $table"
    
    # Create table backup with schema and data in one command
    if /usr/lib/postgresql/17/bin/pg_dump -h localhost -U "$DB_USER" -d "$DB_NAME" --table="$table" --file="$backup_file" 2>/dev/null; then
        # Get backup file size
        backup_size=$(du -h "$backup_file" | cut -f1)
        
        # Add success to summary
        echo "✅ $table - Success - Size: $backup_size" >> "$SUMMARY_FILE"
        log_info "✅ Table '$table' backed up successfully - Size: $backup_size"
        echo "0" > "$backup_file.status"
    else
        log_error "❌ Failed to backup table '$table'"
        echo "❌ $table - Failed to backup" >> "$SUMMARY_FILE"
        rm -f "$backup_file"
        echo "1" > "$backup_file.status"
    fi
}

# Export backup_table function for parallel execution
export -f backup_table
export DB_USER DB_NAME BACKUP_SUBFOLDER SUMMARY_FILE

# Process tables in parallel
log_info "🔄 Backing up tables in parallel (up to $PARALLEL_JOBS jobs)..."
echo "$TABLES" | grep -v "^$" | while read -r table; do
    table=$(echo "$table" | xargs)
    
    if [[ -v BACKUP_FLAGS["$table"] ]]; then
        backup_flag="${BACKUP_FLAGS[$table]}"
        
        if [[ "$backup_flag" == "y" ]]; then
            echo "$table"
        else
            log_info "⏭️ Skipping table (marked as 'n' in config): $table"
            echo "⏭️ $table - Skipped (configured not to backup)" >> "$SUMMARY_FILE"
            echo "0" > "$BACKUP_SUBFOLDER/$table.sql.status"
            SKIPPED_COUNT=$((SKIPPED_COUNT + 1))
        fi
    else
        log_warn "⚠️ Unknown table not in configuration: $table"
        UNKNOWN_TABLES+=("$table")
        echo "⚠️ $table - Not in configuration (not backed up)" >> "$SUMMARY_FILE"
        echo "0" > "$BACKUP_SUBFOLDER/$table.sql.status"
        
        if [[ -f "$DISCORD_NOTIFY_SCRIPT" ]]; then
            discord_message="🚨 **Unknown Table Alert**\n\nTable: \`$table\`\nStatus: **Not backed up**\nScript: \`999_backup_psql_all_tables.sh\`\nPlease add it to: \`$BACKUP_CONFIG\`"
            bash "$DISCORD_NOTIFY_SCRIPT" "custom" "999_backup_psql_all_tables" "Unknown Table Alert" "$discord_message" "🚨" 2>/dev/null || \
                log_error "Failed to send Discord notification for unknown table: $table"
        fi
    fi
done | xargs -n 1 -P "$PARALLEL_JOBS" -I {} bash -c 'backup_table "{}"'

# Count successes and failures
for table in $TABLES; do
    table=$(echo "$table" | xargs)
    if [[ -f "$BACKUP_SUBFOLDER/$table.sql.status" ]]; then
        status=$(cat "$BACKUP_SUBFOLDER/$table.sql.status")
        if [ "$status" = "0" ]; then
            if [[ -v BACKUP_FLAGS["$table"] && "${BACKUP_FLAGS[$table]}" == "y" ]]; then
                SUCCESS_COUNT=$((SUCCESS_COUNT + 1))
            fi
        else
            FAIL_COUNT=$((FAIL_COUNT + 1))
        fi
        rm -f "$BACKUP_SUBFOLDER/$table.sql.status"
    fi
done

# Update summary with final counts
echo "" >> "$SUMMARY_FILE"
echo "=========================================" >> "$SUMMARY_FILE"
echo "Summary:" >> "$SUMMARY_FILE"
echo "- Total tables: $TOTAL_TABLES" >> "$SUMMARY_FILE"
echo "- Successfully backed up: $SUCCESS_COUNT" >> "$SUMMARY_FILE"
echo "- Failed: $FAIL_COUNT" >> "$SUMMARY_FILE"
echo "- Skipped (configured): $SKIPPED_COUNT" >> "$SUMMARY_FILE"
echo "- Unknown (not in config): ${#UNKNOWN_TABLES[@]}" >> "$SUMMARY_FILE"
if [ ${#UNKNOWN_TABLES[@]} -gt 0 ]; then
    echo "" >> "$SUMMARY_FILE"
    echo "Unknown tables:" >> "$SUMMARY_FILE"
    for unknown_table in "${UNKNOWN_TABLES[@]}"; do
        echo "  - $unknown_table" >> "$SUMMARY_FILE"
    done
fi
echo "=========================================" >> "$SUMMARY_FILE"

# Create a compressed archive of all backups
log_info "🗜️ Creating a compressed archive of all table backups..."
ARCHIVE_FILE="$BACKUP_DIR/all_tables_backup_$DATE.tar.zst"
TEMP_ARCHIVE_FILE="$BACKUP_DIR/all_tables_backup_$DATE.temp.tar.zst"

if tar -cf - -C "$BACKUP_DIR" "$DATE" | zstd -3 > "$TEMP_ARCHIVE_FILE"; then
    if zstd -t "$TEMP_ARCHIVE_FILE" >/dev/null 2>&1; then
        mv "$TEMP_ARCHIVE_FILE" "$ARCHIVE_FILE"
        archive_size=$(du -h "$ARCHIVE_FILE" | cut -f1)
        log_info "✅ Archive created successfully - Size: $archive_size"
        log_info "📁 Archive file: $ARCHIVE_FILE"
        
        # Copy the archive to both remote backup locations in parallel
        log_info "📤 Copying backup archive to remote locations..."
        mkdir -p "$REMOTE_DIR1" "$REMOTE_DIR2"
        
        copy_jobs=()
        if cp "$ARCHIVE_FILE" "$REMOTE_DIR1/" & copy_jobs+=($!); then
            log_info "Started copy to $REMOTE_DIR1"
        else
            log_error "❌ Failed to start copy to $REMOTE_DIR1"
        fi
        
        if cp "$ARCHIVE_FILE" "$REMOTE_DIR2/" & copy_jobs+=($!); then
            log_info "Started copy to $REMOTE_DIR2"
        else
            log_error "❌ Failed to start copy to $REMOTE_DIR2"
        fi
        
        # Wait for all copy jobs to complete
        for pid in "${copy_jobs[@]}"; do
            wait "$pid" || log_error "❌ Copy job (PID $pid) failed"
        done
        
        # Verify copied archives
        if [[ -f "$REMOTE_DIR1/$(basename "$ARCHIVE_FILE")" ]] && zstd -t "$REMOTE_DIR1/$(basename "$ARCHIVE_FILE")" >/dev/null 2>&1; then
            log_info "✅ Successfully copied and verified backup archive to $REMOTE_DIR1"
        else
            log_error "❌ Copied archive to $REMOTE_DIR1 is missing or corrupted"
        fi
        
        if [[ -f "$REMOTE_DIR2/$(basename "$ARCHIVE_FILE")" ]] && zstd -t "$REMOTE_DIR2/$(basename "$ARCHIVE_FILE")" >/dev/null 2>&1; then
            log_info "✅ Successfully copied and verified backup archive to $REMOTE_DIR2"
        else
            log_error "❌ Copied archive to $REMOTE_DIR2 is missing or corrupted"
        fi
    else
        log_error "❌ Created archive is corrupted"
        rm -f "$TEMP_ARCHIVE_FILE"
    fi
else
    log_error "❌ Failed to create archive"
    rm -f "$TEMP_ARCHIVE_FILE"
fi

# Clean up old backup archives (keep last 7 days + first and mid-month archives)
log_info "🧹 Cleaning up old backup archives..."
for dir in "$BACKUP_DIR" "$REMOTE_DIR1" "$REMOTE_DIR2"; do
    log_info "Cleaning up $dir..."
    old_files=$(find "$dir" -name "all_tables_backup_*.tar.zst" -type f -mtime +7 -printf '%P\n' | sort) &
    all_files=$(find "$dir" -name "all_tables_backup_*.tar.zst" -type f -printf '%P\n' | sort) &
    wait
    
    if [ -z "$old_files" ]; then
        log_info "No old files to clean in $dir"
        continue
    fi
    
    declare -A month_files keepers
    for file in $all_files; do
        date_str=${file#all_tables_backup_}
        date_str=${date_str%.tar.zst}
        ymd=${date_str%%_*}
        year=${ymd:0:4}
        mon=${ymd:4:2}
        ym="$year-$mon"
        month_files["$ym"]+="$file "
    done
    
    for ym in "${!month_files[@]}"; do
        target1_epoch=$(date --date="$ym-01 00:00:00" +%s)
        target15_epoch=$(date --date="$ym-15 00:00:00" +%s)
        min_dist1=""
        keeper1=""
        keeper1_epoch=""
        min_dist15=""
        keeper15=""
        keeper15_epoch=""
        
        month_list=(${month_files[$ym]})
        for file in "${month_list[@]}"; do
            date_str=${file#all_tables_backup_}
            date_str=${date_str%.tar.zst}
            ymd=${date_str%%_*}
            time_part=${date_str#*_}
            year=${ymd:0:4}
            mon=${ymd:4:2}
            day=${ymd:6:2}
            hour=${time_part:0:2}
            min=${time_part:2:2}
            sec=${time_part:4:2}
            file_epoch=$(date --date="$year-$mon-$day $hour:$min:$sec" +%s)
            
            dist1=$((file_epoch - target1_epoch))
            dist1=${dist1#-}
            if [ -z "$min_dist1" ] || [ "$dist1" -lt "$min_dist1" ] || { [ "$dist1" -eq "$min_dist1" ] && [ "$file_epoch" -lt "$keeper1_epoch" ]; }; then
                min_dist1="$dist1"
                keeper1="$file"
                keeper1_epoch="$file_epoch"
            fi
            
            dist15=$((file_epoch - target15_epoch))
            dist15=${dist15#-}
            if [ -z "$min_dist15" ] || [ "$dist15" -lt "$min_dist15" ] || { [ "$dist15" -eq "$min_dist15" ] && [ "$file_epoch" -lt "$keeper15_epoch" ]; }; then
                min_dist15="$dist15"
                keeper15="$file"
                keeper15_epoch="$file_epoch"
            fi
        done
        
        if [ -n "$keeper1" ]; then
            keepers["$keeper1"]=1
        fi
        if [ -n "$keeper15" ]; then
            keepers["$keeper15"]=1
        fi
    done
    
    for old_file in $old_files; do
        if [ -z "${keepers[$old_file]}" ]; then
            rm "$dir/$old_file"
            log_info "🗑️ Removed old archive: $old_file from $dir"
        fi
    done
done

# Send final Discord notification
if [[ -f "$DISCORD_NOTIFY_SCRIPT" ]]; then
    discord_message="📊 **PostgreSQL Table Backup Summary**\n\n"
    discord_message+="Script: \`999_backup_psql_all_tables.sh\`\n"
    discord_message+="Database: \`$DB_NAME\`\n"
    discord_message+="Total Tables: $TOTAL_TABLES\n"
    discord_message+="Success: $SUCCESS_COUNT | Failed: $FAIL_COUNT | Skipped: $SKIPPED_COUNT | Unknown: ${#UNKNOWN_TABLES[@]}\n"
    if [ -f "$ARCHIVE_FILE" ]; then
        discord_message+="Archive Size: $archive_size\n"
        discord_message+="Archive File: \`$ARCHIVE_FILE\`\n"
    else
        discord_message+="Archive: **Failed to create**\n"
    fi
    if [ ${#UNKNOWN_TABLES[@]} -gt 0 ]; then
        discord_message+="\n**⚠️ Unknown Tables (${#UNKNOWN_TABLES[@]}):**\n"
        for unknown_table in "${UNKNOWN_TABLES[@]}"; do
            discord_message+="• \`$unknown_table\`\n"
        done
    fi
    discord_message+="\nSummary File: \`$SUMMARY_FILE\`"
    
    status_emoji=$([ $FAIL_COUNT -eq 0 ] && [ ${#UNKNOWN_TABLES[@]} -eq 0 ] && echo "✅" || echo "⚠️")
    bash "$DISCORD_NOTIFY_SCRIPT" "custom" "999_backup_psql_all_tables" "Backup Summary" "$discord_message" "$status_emoji" 2>/dev/null || \
        log_error "Failed to send final Discord notification"
fi

# Final status
log_info "📊 Backup complete - Success: $SUCCESS_COUNT, Failed: $FAIL_COUNT, Skipped: $SKIPPED_COUNT, Unknown: ${#UNKNOWN_TABLES[@]}, Total: $TOTAL_TABLES"
if [ $FAIL_COUNT -eq 0 ] && [ ${#UNKNOWN_TABLES[@]} -eq 0 ]; then
    log_info "✅ All configured tables backed up successfully"
elif [ ${#UNKNOWN_TABLES[@]} -gt 0 ]; then
    log_warn "⚠️ Found ${#UNKNOWN_TABLES[@]} unknown table(s) not in configuration"
    log_info "📄 Summary file: $SUMMARY_FILE"
else
    log_warn "⚠️ Some table backups failed. Check the summary file for details."
    log_info "📄 Summary file: $SUMMARY_FILE"
fi

# List remaining backup archives
archive_count=$(find "$BACKUP_DIR" -name "all_tables_backup_*.tar.zst" -type f | wc -l)
log_info "📊 Total backup archives retained: $archive_count"

log_info "🎉 PostgreSQL individual table backup process completed"

# Cleanup logging
cleanup_logging