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

log_info "💾 Starting PostgreSQL database backup"

# Configuration
DB_NAME="sol_blocks"
DB_USER="smilax"
BACKUP_DIR="/home/smilax/trillium_api/data/backups/postgresql"
DATE=$(date +%Y%m%d_%H%M%S)
BACKUP_FILE="$BACKUP_DIR/sol_blocks_backup_$DATE.sql"

# Ensure backup directory exists
mkdir -p "$BACKUP_DIR"

log_info "📊 Database: $DB_NAME"
log_info "👤 User: $DB_USER"
log_info "📁 Backup file: $BACKUP_FILE"

# Create database backup
log_info "🔄 Creating database backup..."
if pg_dump -h localhost -U "$DB_USER" -d "$DB_NAME" --verbose --file="$BACKUP_FILE" 2>/dev/null; then
    log_info "✅ Database backup completed successfully"
    
    # Get backup file size
    backup_size=$(du -h "$BACKUP_FILE" | cut -f1)
    log_info "📏 Backup file size: $backup_size"
    
    # Compress the backup with zstd
    log_info "🗜️ Compressing backup file with zstd..."
    if zstd -19 "$BACKUP_FILE"; then
        compressed_file="${BACKUP_FILE}.zst"
        compressed_size=$(du -h "$compressed_file" | cut -f1)
        log_info "✅ Backup compressed successfully"
        log_info "📏 Compressed file size: $compressed_size"
        
        # Create destination directory if it doesn't exist
        IDRIVE_BACKUP_DIR="/mnt/idrive/backups/postgresql"
        mkdir -p "$IDRIVE_BACKUP_DIR"
        
        # Copy the compressed backup to the iDrive backup location
        log_info "📤 Copying backup file to iDrive..."
        if cp "$compressed_file" "$IDRIVE_BACKUP_DIR/"; then
            log_info "✅ Successfully copied backup file to $IDRIVE_BACKUP_DIR/"
        else
            log_error "❌ Failed to copy backup file to iDrive"
        fi
        
        # Remove the original uncompressed file
        log_info "🧹 Removing uncompressed SQL file..."
        if rm "$BACKUP_FILE"; then
            log_info "✅ Uncompressed SQL file removed successfully"
        else
            log_error "❌ Failed to remove uncompressed SQL file"
        fi
    else
        log_error "❌ Failed to compress backup file"
    fi
else
    log_error "❌ Database backup failed"
    exit 1
fi

# Clean up old backups (keep last 7 days + first and mid-month archives)
log_info "🧹 Cleaning up old backup files (keeping last 7 days + first and mid-month archives)"

# Find all old files (older than 7 days)
old_files=$(find "$BACKUP_DIR" -name "sol_blocks_backup_*.sql.zst" -type f -mtime +7 -printf '%P\n' | sort)

if [ -z "$old_files" ]; then
    log_info "No old files to clean"
else
    # Get all files sorted
    all_files=$(find "$BACKUP_DIR" -name "sol_blocks_backup_*.sql.zst" -type f -printf '%P\n' | sort)

    # Group by month
    declare -A month_files
    for file in $all_files; do
        date_str=${file#sol_blocks_backup_}
        date_str=${date_str%.sql.zst}
        ymd=${date_str%%_*}
        year=${ymd:0:4}
        mon=${ymd:4:2}
        ym="$year-$mon"
        month_files["$ym"]+="$file "
    done

    # Determine keepers
    declare -A keepers
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
            date_str=${file#sol_blocks_backup_}
            date_str=${date_str%.sql.zst}
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

    # Delete old files not in keepers
    for old_file in $old_files; do
        if [ -z "${keepers[$old_file]}" ]; then
            rm "$BACKUP_DIR/$old_file"
        fi
    done
fi

# List remaining backup files
backup_count=$(find "$BACKUP_DIR" -name "sol_blocks_backup_*.sql.zst" -type f | wc -l)
log_info "📊 Total backup files retained: $backup_count"

log_info "🎉 PostgreSQL backup process completed"

# Cleanup logging
cleanup_logging