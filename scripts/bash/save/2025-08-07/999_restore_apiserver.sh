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

log_info "🔄 Starting API Server Restore Process"

# Check if backup file is provided
if [ -z "$1" ]; then
    log_error "❌ No backup file specified. Usage: $0 <backup_file.sql.zst>"
    exit 1
fi

BACKUP_FILE="$1"

# Check if file exists
if [ ! -f "$BACKUP_FILE" ]; then
    log_error "❌ Backup file not found: $BACKUP_FILE"
    exit 1
fi

# Configuration
DB_NAME="sol_blocks"
DB_USER="smilax"
API_SERVICE_NAME="trillium-api"
TEMP_SQL_FILE="/tmp/temp_restore_$(date +%s).sql"

log_info "📊 Database: $DB_NAME"
log_info "👤 User: $DB_USER"
log_info "📁 Backup file: $BACKUP_FILE"
log_info "🔧 API Service: $API_SERVICE_NAME"

# Ask for confirmation before proceeding
echo ""
log_warn "⚠️  WARNING: This will restore the entire API server"
log_warn "⚠️  The database '$DB_NAME' will be overwritten"
log_warn "⚠️  The API service will be stopped during restoration"
echo ""
read -p "Are you sure you want to proceed? (y/n): " -n 1 -r
echo ""

if [[ ! $REPLY =~ ^[Yy]$ ]]; then
    log_info "❌ Restore operation cancelled by user"
    cleanup_logging
    exit 0
fi

# Stop the API service
log_info "🛑 Stopping API service..."
if systemctl is-active --quiet $API_SERVICE_NAME; then
    if sudo systemctl stop $API_SERVICE_NAME; then
        log_info "✅ API service stopped successfully"
    else
        log_error "❌ Failed to stop API service"
        log_warn "⚠️ Continuing with restore, but service may need to be restarted manually"
    fi
else
    log_warn "⚠️ API service is not running"
fi

# Decompress the backup file
log_info "📂 Decompressing backup file..."
if zstd -d "$BACKUP_FILE" -o "$TEMP_SQL_FILE"; then
    log_info "✅ Backup file decompressed successfully"
else
    log_error "❌ Failed to decompress backup file"
    
    # Try to restart the API service if it was running
    if systemctl is-active --quiet $API_SERVICE_NAME; then
        log_info "🔄 Attempting to restart API service..."
        sudo systemctl start $API_SERVICE_NAME
    fi
    
    cleanup_logging
    exit 1
fi

# Get database size before restore
log_info "📊 Getting current database size..."
DB_SIZE_BEFORE=$(psql -h localhost -U "$DB_USER" -d "$DB_NAME" -t -c "SELECT pg_size_pretty(pg_database_size('$DB_NAME'));")
log_info "📏 Current database size: $DB_SIZE_BEFORE"

# Restore database
log_info "🔄 Restoring database from backup..."
log_info "📊 This may take some time depending on the size of the backup..."

if psql -h localhost -U "$DB_USER" -d "$DB_NAME" -c "DROP SCHEMA public CASCADE; CREATE SCHEMA public;" && \
   psql -h localhost -U "$DB_USER" -d "$DB_NAME" -f "$TEMP_SQL_FILE"; then
    log_info "✅ Database restored successfully"
    
    # Get database size after restore
    DB_SIZE_AFTER=$(psql -h localhost -U "$DB_USER" -d "$DB_NAME" -t -c "SELECT pg_size_pretty(pg_database_size('$DB_NAME'));")
    log_info "📏 New database size: $DB_SIZE_AFTER"
    
    # Verify database integrity
    log_info "🔍 Verifying database integrity..."
    TABLE_COUNT=$(psql -h localhost -U "$DB_USER" -d "$DB_NAME" -t -c "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'public';")
    log_info "📊 Public schema contains $TABLE_COUNT tables"
    
    # Check if any critical tables are missing
    # Add your critical tables here
    CRITICAL_TABLES=("blocks" "transactions" "accounts" "programs")
    MISSING_TABLES=()
    
    for table in "${CRITICAL_TABLES[@]}"; do
        if ! psql -h localhost -U "$DB_USER" -d "$DB_NAME" -t -c "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_schema = 'public' AND table_name = '$table');" | grep -q "t"; then
            MISSING_TABLES+=("$table")
        fi
    done
    
    if [ ${#MISSING_TABLES[@]} -eq 0 ]; then
        log_info "✅ All critical tables are present"
    else
        log_warn "⚠️ The following critical tables are missing: ${MISSING_TABLES[*]}"
    fi
else
    log_error "❌ Database restore failed"
    
    # Try to restart the API service if it was running
    if systemctl is-active --quiet $API_SERVICE_NAME; then
        log_info "🔄 Attempting to restart API service..."
        sudo systemctl start $API_SERVICE_NAME
    fi
    
    log_info "🧹 Cleaning up temporary files..."
    rm -f "$TEMP_SQL_FILE"
    cleanup_logging
    exit 1
fi

# Clean up
log_info "🧹 Cleaning up temporary files..."
rm -f "$TEMP_SQL_FILE"

# Restart the API service
log_info "🔄 Starting API service..."
if sudo systemctl start $API_SERVICE_NAME; then
    log_info "✅ API service started successfully"
    
    # Check if service is running properly
    sleep 5
    if systemctl is-active --quiet $API_SERVICE_NAME; then
        log_info "✅ API service is running"
    else
        log_warn "⚠️ API service failed to start properly"
    fi
else
    log_error "❌ Failed to start API service"
    log_warn "⚠️ You may need to start the API service manually"
fi

# Verify API server functionality
log_info "🔍 Verifying API server functionality..."
API_URL="http://localhost:3000/api/health" # Adjust to your actual health endpoint
API_RESPONSE=$(curl -s -o /dev/null -w "%{http_code}" $API_URL || echo "failed")

if [[ "$API_RESPONSE" == "200" ]]; then
    log_info "✅ API server is responding normally"
else
    log_warn "⚠️ API server health check failed (status: $API_RESPONSE)"
    log_warn "⚠️ You may need to investigate further"
fi

log_info "🎉 API Server restore process completed"

# Cleanup logging
cleanup_logging