#!/bin/bash

# Test script to verify backup logic without actually performing backups

# Source path initialization
source "$(dirname "$0")/000_init_paths.sh" || {
    echo "❌ Failed to source path initialization script" >&2
    exit 1
}

# Configuration
DB_NAME="sol_blocks"
DB_USER="smilax"
BACKUP_CONFIG="$TRILLIUM_CONFIG/backup_tables.conf"

echo "========================================="
echo "Testing Backup Table Configuration Logic"
echo "========================================="
echo ""

# Load backup configuration
declare -A BACKUP_FLAGS
if [[ -f "$BACKUP_CONFIG" ]]; then
    echo "✅ Found configuration file: $BACKUP_CONFIG"
    echo ""
    echo "Loading configuration..."
    while IFS=':' read -r table flag; do
        # Skip comments and empty lines
        [[ "$table" =~ ^#.*$ || -z "$table" ]] && continue
        # Trim whitespace
        table=$(echo "$table" | xargs)
        flag=$(echo "$flag" | xargs)
        BACKUP_FLAGS["$table"]="$flag"
    done < "$BACKUP_CONFIG"
    echo "✅ Loaded configuration for ${#BACKUP_FLAGS[@]} tables"
else
    echo "❌ Configuration file not found: $BACKUP_CONFIG"
    exit 1
fi

echo ""
echo "Getting list of tables from database..."
TABLES=$(psql -h localhost -U "$DB_USER" -d "$DB_NAME" -t -c "SELECT tablename FROM pg_tables WHERE schemaname = 'public' ORDER BY tablename;" 2>/dev/null)

if [ $? -ne 0 ]; then
    echo "❌ Failed to query database. Make sure PostgreSQL is running and accessible."
    exit 1
fi

# Count statistics
TOTAL_TABLES=0
WILL_BACKUP=0
WILL_SKIP=0
UNKNOWN_TABLES=()

echo ""
echo "Table Analysis:"
echo "----------------------------------------"

for table in $TABLES; do
    # Skip empty lines
    if [ -z "$table" ]; then
        continue
    fi
    
    # Trim whitespace
    table=$(echo "$table" | xargs)
    TOTAL_TABLES=$((TOTAL_TABLES + 1))
    
    # Check if table is in configuration
    if [[ -v BACKUP_FLAGS["$table"] ]]; then
        backup_flag="${BACKUP_FLAGS[$table]}"
        
        if [[ "$backup_flag" == "y" ]]; then
            echo "✅ $table - Will backup"
            WILL_BACKUP=$((WILL_BACKUP + 1))
        else
            echo "⏭️  $table - Will skip (marked as 'n')"
            WILL_SKIP=$((WILL_SKIP + 1))
        fi
    else
        echo "⚠️  $table - UNKNOWN (not in config)"
        UNKNOWN_TABLES+=("$table")
    fi
done

echo ""
echo "========================================="
echo "Summary:"
echo "----------------------------------------"
echo "Total tables in database: $TOTAL_TABLES"
echo "Tables to backup: $WILL_BACKUP"
echo "Tables to skip: $WILL_SKIP"
echo "Unknown tables: ${#UNKNOWN_TABLES[@]}"

if [ ${#UNKNOWN_TABLES[@]} -gt 0 ]; then
    echo ""
    echo "⚠️  Unknown tables that need to be added to config:"
    for unknown_table in "${UNKNOWN_TABLES[@]}"; do
        echo "  - $unknown_table"
    done
    echo ""
    echo "These tables will trigger Discord notifications when the backup script runs."
fi

echo "========================================="
echo ""
echo "Test complete. No actual backups were performed."