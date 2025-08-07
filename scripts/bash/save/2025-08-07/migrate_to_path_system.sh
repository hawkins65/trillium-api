#!/bin/bash
# Migration script to update all scripts to use the new path system

# Source path initialization
source "$(dirname "$0")/000_init_paths.sh" || {
    echo "❌ Failed to source path initialization script" >&2
    exit 1
}

echo "🔄 Migrating scripts to use centralized path system..."

# Function to update a single script
update_script() {
    local script_file="$1"
    local backup_file="${script_file}.backup"
    
    echo "📝 Updating: $script_file"
    
    # Create backup
    cp "$script_file" "$backup_file"
    
    # Add path initialization at the top (after shebang)
    sed -i '1a\\n# Source path initialization\nsource "$(dirname "$0")/000_init_paths.sh" || {\n    echo "❌ Failed to source path initialization script" >&2\n    exit 1\n}' "$script_file"
    
    # Replace common hardcoded paths
    sed -i 's|/home/smilax/trillium_api/scripts/bash/|$TRILLIUM_SCRIPTS_BASH/|g' "$script_file"
    sed -i 's|/home/smilax/trillium_api/scripts/python/|$TRILLIUM_SCRIPTS_PYTHON/|g' "$script_file"
    sed -i 's|/home/smilax/trillium_api/scripts/nodejs/|$TRILLIUM_SCRIPTS_NODEJS/|g' "$script_file"
    sed -i 's|/home/smilax/trillium_api/data/epochs/|$TRILLIUM_DATA_EPOCHS/|g' "$script_file"
    sed -i 's|/home/smilax/trillium_api/config/|$TRILLIUM_CONFIG/|g' "$script_file"
    
    # Replace specific script calls
    sed -i 's|bash 999_discord_notify.sh|bash "$DISCORD_NOTIFY_SCRIPT"|g' "$script_file"
    sed -i 's|source /home/smilax/trillium_api/scripts/bash/999_common_log.sh|# Common logging already sourced by init_paths|g' "$script_file"
    
    echo "✅ Updated: $script_file (backup: $backup_file)"
}

# Get all bash scripts except the ones we've already updated
scripts=($(find "$TRILLIUM_SCRIPTS_BASH" -name "*.sh" -not -name "000_init_paths.sh" -not -name "migrate_to_path_system.sh" -not -name "90_get_block_data.sh"))

echo "📊 Found ${#scripts[@]} scripts to update"

for script in "${scripts[@]}"; do
    if [[ -f "$script" ]]; then
        update_script "$script"
    fi
done

echo "✅ Migration completed!"
echo "📋 Summary:"
echo "   - Updated ${#scripts[@]} scripts"
echo "   - Created backups with .backup extension"
echo "   - Added path initialization to all scripts"
echo "   - Replaced hardcoded paths with variables"
echo ""
echo "🔍 To test: Run any script and verify paths resolve correctly"
echo "🔄 To rollback: Restore from .backup files if needed"