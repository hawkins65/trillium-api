#!/bin/bash

# Source path initialization
source "$(dirname "$0")/000_init_paths.sh" || {
    echo "❌ Failed to source path initialization script" >&2
    exit 1
}

# Source common logging
source $TRILLIUM_SCRIPTS_BASH/999_common_log.sh

# Initialize logging
init_logging

log_info "💾 Starting individual PostgreSQL table backups"

# Configuration
DB_NAME="sol_blocks"
DB_USER="smilax"
BACKUP_DIR="/home/smilax/trillium_api/data/backups/psql/tables"
DATE=$(date +%Y%m%d_%H%M%S)
BACKUP_SUBFOLDER="$BACKUP_DIR/$DATE"
SUMMARY_FILE="$BACKUP_SUBFOLDER/_backup_summary.log"
SUCCESS_COUNT=0
FAIL_COUNT=0
TOTAL_TABLES=0
SKIPPED_COUNT=0
UNKNOWN_TABLES=()

# Backup configuration file
BACKUP_CONFIG="$TRILLIUM_CONFIG/backup_tables.conf"

# Discord notification script
DISCORD_NOTIFY_SCRIPT="$TRILLIUM_SCRIPTS_BASH/999_discord_notify.sh"

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
        # Skip comments and empty lines
        [[ "$table" =~ ^#.*$ || -z "$table" ]] && continue
        # Trim whitespace
        table=$(echo "$table" | xargs)
        flag=$(echo "$flag" | xargs)
        BACKUP_FLAGS["$table"]="$flag"
    done < "$BACKUP_CONFIG"
    log_info "✅ Loaded configuration for ${#BACKUP_FLAGS[@]} tables"
else
    log_error "❌ Backup configuration file not found: $BACKUP_CONFIG"
    log_error "Creating default configuration file..."
    # Create a default config file with all tables marked as 'y'
    echo "# PostgreSQL Table Backup Configuration" > "$BACKUP_CONFIG"
    echo "# Format: table_name:backup_flag (y/n)" >> "$BACKUP_CONFIG"
    echo "# Tables marked with 'y' will be backed up" >> "$BACKUP_CONFIG"
    echo "# Tables marked with 'n' will be skipped" >> "$BACKUP_CONFIG"
    echo "" >> "$BACKUP_CONFIG"
fi

# Get list of all tables in the public schema
log_info "🔍 Retrieving list of all tables in public schema..."
TABLES=$(psql -h localhost -U "$DB_USER" -d "$DB_NAME" -t -c "SELECT tablename FROM pg_tables WHERE schemaname = 'public' ORDER BY tablename;")

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
    
    # Create table backup with schema and data
    if pg_dump -h localhost -U "$DB_USER" -d "$DB_NAME" --table="$table" --schema-only --file="${backup_file}.schema" 2>/dev/null &&
       pg_dump -h localhost -U "$DB_USER" -d "$DB_NAME" --table="$table" --data-only --file="${backup_file}.data" 2>/dev/null; then
        
        # Combine schema and data into a single file
        cat "${backup_file}.schema" "${backup_file}.data" > "$backup_file"
        rm "${backup_file}.schema" "${backup_file}.data"
        
        # Get backup file size
        backup_size=$(du -h "$backup_file" | cut -f1)
        
        # Compress the backup with zstd
        if zstd -3 "$backup_file"; then
            compressed_file="${backup_file}.zst"
            compressed_size=$(du -h "$compressed_file" | cut -f1)
            
            # Remove the original uncompressed file
            rm "$backup_file"
            
            # Add success to summary
            echo "✅ $table - Success - Size: $compressed_size" >> "$SUMMARY_FILE"
            log_info "✅ Table '$table' backed up successfully - Size: $compressed_size"
            return 0
        else
            # Compression failed
            log_error "❌ Failed to compress backup for table '$table'"
            echo "❌ $table - Failed to compress backup" >> "$SUMMARY_FILE"
            # Clean up the uncompressed file on failure
            rm -f "$backup_file"
            return 1
        fi
    else
        # Backup failed
        log_error "❌ Failed to backup table '$table'"
        echo "❌ $table - Failed to backup" >> "$SUMMARY_FILE"
        # Clean up any partial files
        rm -f "${backup_file}.schema" "${backup_file}.data" "$backup_file"
        return 1
    fi
}

# Process each table
for table in $TABLES; do
    # Skip empty lines
    if [ -z "$table" ]; then
        continue
    fi
    
    # Trim whitespace
    table=$(echo "$table" | xargs)
    
    # Check if table is in configuration
    if [[ -v BACKUP_FLAGS["$table"] ]]; then
        backup_flag="${BACKUP_FLAGS[$table]}"
        
        if [[ "$backup_flag" == "y" ]]; then
            # Backup the table
            if backup_table "$table"; then
                SUCCESS_COUNT=$((SUCCESS_COUNT + 1))
            else
                FAIL_COUNT=$((FAIL_COUNT + 1))
            fi
        else
            # Skip table marked as 'n'
            log_info "⏭️  Skipping table (marked as 'n' in config): $table"
            SKIPPED_COUNT=$((SKIPPED_COUNT + 1))
            echo "⏭️  $table - Skipped (configured not to backup)" >> "$SUMMARY_FILE"
        fi
    else
        # Table not in configuration - alert via Discord
        log_warn "⚠️  Unknown table not in configuration: $table"
        UNKNOWN_TABLES+=("$table")
        echo "⚠️  $table - Not in configuration (not backed up)" >> "$SUMMARY_FILE"
        
        # Send Discord notification for unknown table
        if [[ -f "$DISCORD_NOTIFY_SCRIPT" ]]; then
            discord_message="🚨 **Unknown Table Alert**\n\n"
            discord_message+="Table: \`$table\`\n"
            discord_message+="Status: **Not backed up**\n"
            discord_message+="Script: \`999_backup_psql_all_tables.sh\`\n\n"
            discord_message+="⚠️ This table is not in the backup configuration file.\n"
            discord_message+="Please add it to: \`$BACKUP_CONFIG\`"
            
            bash "$DISCORD_NOTIFY_SCRIPT" "custom" "999_backup_psql_all_tables" "Unknown Table Alert" "$discord_message" "🚨" 2>/dev/null || \
                log_error "Failed to send Discord notification for unknown table: $table"
        fi
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

# Final status
log_info "📊 Backup complete - Success: $SUCCESS_COUNT, Failed: $FAIL_COUNT, Skipped: $SKIPPED_COUNT, Unknown: ${#UNKNOWN_TABLES[@]}, Total: $TOTAL_TABLES"

if [ $FAIL_COUNT -eq 0 ] && [ ${#UNKNOWN_TABLES[@]} -eq 0 ]; then
    log_info "✅ All configured tables backed up successfully"
elif [ ${#UNKNOWN_TABLES[@]} -gt 0 ]; then
    log_warn "⚠️ Found ${#UNKNOWN_TABLES[@]} unknown table(s) not in configuration"
    log_info "📄 Summary file: $SUMMARY_FILE"
    
    # Send summary Discord notification if there were unknown tables
    if [[ -f "$DISCORD_NOTIFY_SCRIPT" ]] && [ ${#UNKNOWN_TABLES[@]} -gt 0 ]; then
        summary_message="📊 **Backup Summary with Unknown Tables**\n\n"
        summary_message+="Script: \`999_backup_psql_all_tables.sh\`\n"
        summary_message+="Success: $SUCCESS_COUNT | Failed: $FAIL_COUNT | Skipped: $SKIPPED_COUNT\n\n"
        summary_message+="**⚠️ Unknown Tables (${#UNKNOWN_TABLES[@]}):**\n"
        for unknown_table in "${UNKNOWN_TABLES[@]}"; do
            summary_message+="• \`$unknown_table\`\n"
        done
        summary_message+="\nPlease update: \`$BACKUP_CONFIG\`"
        
        bash "$DISCORD_NOTIFY_SCRIPT" "custom" "999_backup_psql_all_tables" "Backup Summary with Unknown Tables" "$summary_message" "📊" 2>/dev/null || \
            log_error "Failed to send summary Discord notification"
    fi
else
    log_warn "⚠️ Some table backups failed. Check the summary file for details."
    log_info "📄 Summary file: $SUMMARY_FILE"
fi

# Create a compressed archive of all backups for easier handling
log_info "🗜️ Creating a compressed archive of all table backups..."
ARCHIVE_FILE="$BACKUP_DIR/all_tables_backup_$DATE.tar.zst"

if tar -cf - -C "$BACKUP_DIR" "$DATE" | zstd > "$ARCHIVE_FILE"; then
    archive_size=$(du -h "$ARCHIVE_FILE" | cut -f1)
    log_info "✅ Archive created successfully - Size: $archive_size"
    log_info "📁 Archive file: $ARCHIVE_FILE"
    
    # Create destination directory if it doesn't exist
    IDRIVE_BACKUP_DIR="/mnt/idrive/backups/psql/tables"
    mkdir -p "$IDRIVE_BACKUP_DIR"
    
    # Copy the archive to the iDrive backup location
    log_info "📤 Copying backup archive to iDrive..."
    if cp "$ARCHIVE_FILE" "$IDRIVE_BACKUP_DIR/"; then
        log_info "✅ Successfully copied backup archive to $IDRIVE_BACKUP_DIR/"
    else
        log_error "❌ Failed to copy backup archive to iDrive"
    fi
else
    log_error "❌ Failed to create archive"
fi

log_info "🎉 PostgreSQL individual table backup process completed"

# Cleanup logging
cleanup_logging