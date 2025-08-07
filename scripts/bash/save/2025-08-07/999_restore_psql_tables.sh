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

log_info "🔄 Starting PostgreSQL table restore from archive"

# Check if archive file is provided
if [ -z "$1" ]; then
    log_error "❌ No archive file specified. Usage: $0 <archive_file.tar.zst> [table1 table2 ...]"
    exit 1
fi

ARCHIVE_FILE="$1"
shift # Remove first argument (archive file) from the argument list

# Check if file exists
if [ ! -f "$ARCHIVE_FILE" ]; then
    log_error "❌ Archive file not found: $ARCHIVE_FILE"
    exit 1
fi

# Configuration
DB_NAME="sol_blocks"
DB_USER="smilax"
EXTRACT_DIR="/tmp/psql_restore_$(date +%s)"
SUMMARY_FILE="$EXTRACT_DIR/restore_summary.log"
SUCCESS_COUNT=0
FAIL_COUNT=0

log_info "📊 Database: $DB_NAME"
log_info "👤 User: $DB_USER"
log_info "📁 Archive file: $ARCHIVE_FILE"
log_info "📁 Temporary extraction directory: $EXTRACT_DIR"

# Create temporary directory for extraction
mkdir -p "$EXTRACT_DIR"

# List contents of the archive to find the backup folder
log_info "📂 Examining archive contents..."
ARCHIVE_CONTENTS=$(tar -I zstd -tf "$ARCHIVE_FILE")
BACKUP_FOLDER_NAME=$(echo "$ARCHIVE_CONTENTS" | grep -o '^[0-9]\{8\}_[0-9]\{6\}/' | head -n 1 | tr -d '/')

if [ -z "$BACKUP_FOLDER_NAME" ]; then
    log_error "❌ Could not find backup folder in the archive"
    rm -rf "$EXTRACT_DIR"
    exit 1
fi

log_info "📁 Found backup folder in archive: $BACKUP_FOLDER_NAME"
BACKUP_FOLDER="$EXTRACT_DIR/$BACKUP_FOLDER_NAME"

# Check if specific tables were requested
if [ "$#" -gt 0 ]; then
    # Create the backup folder structure
    mkdir -p "$BACKUP_FOLDER"
    
    # For specific tables, extract only those files plus the summary
    log_info "🔍 Extracting only requested table backups from archive..."
    
    # Extract the summary file if it exists
    tar -I zstd -xf "$ARCHIVE_FILE" -C "$EXTRACT_DIR" "$BACKUP_FOLDER_NAME/_backup_summary.log" 2>/dev/null
    
    # Extract only the requested table backups
    for table in "$@"; do
        log_info "📂 Extracting backup for table: $table"
        if tar -I zstd -xf "$ARCHIVE_FILE" -C "$EXTRACT_DIR" "$BACKUP_FOLDER_NAME/${table}_backup.sql.zst" 2>/dev/null; then
            log_info "✅ Extracted backup for table: $table"
        else
            log_warn "⚠️ Could not find backup for table '$table' in the archive"
        fi
    done
else
    # No specific tables requested, extract the entire archive
    log_info "📂 Extracting entire archive (all tables)..."
    if tar -I zstd -xf "$ARCHIVE_FILE" -C "$EXTRACT_DIR"; then
        log_info "✅ Archive extracted successfully"
    else
        log_error "❌ Failed to extract archive"
        rm -rf "$EXTRACT_DIR"
        exit 1
    fi
fi

# Initialize summary file
echo "PostgreSQL Table Restore Summary" > "$SUMMARY_FILE"
echo "===============================" >> "$SUMMARY_FILE"
echo "Date: $(date)" >> "$SUMMARY_FILE"
echo "Database: $DB_NAME" >> "$SUMMARY_FILE"
echo "Archive: $(basename "$ARCHIVE_FILE")" >> "$SUMMARY_FILE"
echo "-------------------------------" >> "$SUMMARY_FILE"
echo "" >> "$SUMMARY_FILE"
echo "Restore Results:" >> "$SUMMARY_FILE"
echo "-------------------------------" >> "$SUMMARY_FILE"

# Function to restore a single table
restore_table() {
    local backup_file="$1"
    local table_name=$(basename "$backup_file" | sed 's/_backup\.sql\.zst//')
    local temp_sql_file="$EXTRACT_DIR/temp_${table_name}.sql"
    
    log_info "🔄 Restoring table: $table_name"
    
    # Decompress the backup file
    if zstd -d "$backup_file" -o "$temp_sql_file"; then
        log_info "✅ Backup file decompressed successfully"
    else
        log_error "❌ Failed to decompress backup file for table '$table_name'"
        echo "❌ $table_name - Failed to decompress backup" >> "$SUMMARY_FILE"
        return 1
    fi
    
    # Check if target table exists in the database
    table_exists=$(psql -h localhost -U "$DB_USER" -d "$DB_NAME" -t -c "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_schema = 'public' AND table_name = '$table_name');")
    
    if [[ $table_exists == *"t"* ]]; then
        log_warn "⚠️ Table '$table_name' already exists in database"
        
        # Drop the existing table
        log_info "🗑️ Dropping existing table '$table_name'..."
        if ! psql -h localhost -U "$DB_USER" -d "$DB_NAME" -c "DROP TABLE IF EXISTS $table_name CASCADE;"; then
            log_error "❌ Failed to drop existing table '$table_name'"
            echo "❌ $table_name - Failed to drop existing table" >> "$SUMMARY_FILE"
            rm -f "$temp_sql_file"
            return 1
        fi
    fi
    
    # Restore table
    if psql -h localhost -U "$DB_USER" -d "$DB_NAME" -f "$temp_sql_file"; then
        log_info "✅ Table '$table_name' restored successfully"
        echo "✅ $table_name - Successfully restored" >> "$SUMMARY_FILE"
        rm -f "$temp_sql_file"
        return 0
    else
        log_error "❌ Failed to restore table '$table_name'"
        echo "❌ $table_name - Failed to restore" >> "$SUMMARY_FILE"
        rm -f "$temp_sql_file"
        return 1
    fi
}

# Check if specific tables were requested
if [ "$#" -gt 0 ]; then
    # Restore only specified tables
    log_info "🔍 Restoring specified tables: $@"
    
    for table in "$@"; do
        backup_file="$BACKUP_FOLDER/${table}_backup.sql.zst"
        
        if [ ! -f "$backup_file" ]; then
            log_error "❌ Backup for table '$table' not found in the archive"
            echo "❌ $table - Backup not found in archive" >> "$SUMMARY_FILE"
            FAIL_COUNT=$((FAIL_COUNT + 1))
            continue
        fi
        
        # Restore the table
        if restore_table "$backup_file"; then
            SUCCESS_COUNT=$((SUCCESS_COUNT + 1))
        else
            FAIL_COUNT=$((FAIL_COUNT + 1))
        fi
    done
else
    # No specific tables requested, restore all tables
    log_info "🔍 No specific tables requested. Restoring all tables from the archive."
    
    # Check if user wants to proceed
    echo ""
    log_warn "⚠️ WARNING: This will restore ALL tables from the backup"
    log_warn "⚠️ Existing tables with the same names will be dropped and recreated"
    echo ""
    read -p "Are you sure you want to proceed? (y/n): " -n 1 -r
    echo ""
    
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        log_info "❌ Restore operation cancelled by user"
        rm -rf "$EXTRACT_DIR"
        cleanup_logging
        exit 0
    fi
    
    # Find all table backups
    BACKUP_FILES=$(find "$BACKUP_FOLDER" -name "*_backup.sql.zst" | sort)
    TOTAL_TABLES=$(echo "$BACKUP_FILES" | wc -l)
    
    log_info "📊 Found $TOTAL_TABLES tables to restore"
    
    # Process each backup file
    for backup_file in $BACKUP_FILES; do
        # Skip summary file or other non-backup files
        if [[ "$backup_file" == *"_summary"* ]]; then
            continue
        fi
        
        # Restore the table
        if restore_table "$backup_file"; then
            SUCCESS_COUNT=$((SUCCESS_COUNT + 1))
        else
            FAIL_COUNT=$((FAIL_COUNT + 1))
        fi
    done
fi

# Update summary with final counts
echo "" >> "$SUMMARY_FILE"
echo "===============================" >> "$SUMMARY_FILE"
echo "Summary:" >> "$SUMMARY_FILE"
echo "- Successfully restored: $SUCCESS_COUNT" >> "$SUMMARY_FILE"
echo "- Failed: $FAIL_COUNT" >> "$SUMMARY_FILE"
echo "===============================" >> "$SUMMARY_FILE"

# Final status
log_info "📊 Restore complete - Success: $SUCCESS_COUNT, Failed: $FAIL_COUNT"

if [ $FAIL_COUNT -eq 0 ]; then
    log_info "✅ All tables restored successfully"
else
    log_warn "⚠️ Some table restores failed. Check the summary file for details."
    log_info "📄 Summary file: $SUMMARY_FILE"
fi

# Clean up
log_info "🧹 Cleaning up temporary files..."
rm -rf "$EXTRACT_DIR"

log_info "🎉 PostgreSQL table restore process completed"

# Cleanup logging
cleanup_logging