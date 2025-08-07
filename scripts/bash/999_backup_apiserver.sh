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

# Get the current date
current_date=$(date +%Y%m%d_%H%M%S)

# Define backup paths
backup_dir="$USER_HOME/api/data/backups/apiserver/apiserver-backup"
block_backup_dir="$USER_HOME/api/data/backups/apiserver/block-production-backup"
apiserver_backup_dir="$backup_dir"
final_backup_dir="$USER_HOME/api/data/backups/apiserver"
remote_dir1="/mnt/gdrive/backups/apiserver"
remote_dir2="/mnt/idrive/backups/apiserver"

# Create necessary directories and verify writability
mkdir -p "$backup_dir" "$block_backup_dir" "$final_backup_dir" "$remote_dir1" "$remote_dir2"
if [[ ! -w "$backup_dir" ]]; then
    log_error "❌ Backup directory $backup_dir is not writable"
    exit 1
fi

# Define backup file paths
tar_file_api="$block_backup_dir/api-$current_date.tar.zst"
tar_file_apiserver="$final_backup_dir/apiserver_backup_$current_date.tar.zst"

log_info "💾 Starting API server backup process"
log_info "📁 Backup directory: $backup_dir"
log_info "📁 API backup: $tar_file_api"
log_info "📁 API server backup: $tar_file_apiserver"

# Sync home directory
log_info "🔄 Syncing home directory..."
RSYNC_LOG_FILE="$backup_dir/rsync_backup_$current_date.log"
if sudo rsync -av --no-links "$USER_HOME/" "$backup_dir/home/smilax/" \
    --exclude='.avm' --exclude='.cache' --exclude='.cargo' --exclude='.claude' --exclude='.config' \
    --exclude='.cursor-server' --exclude='.gemini' --exclude='.local' --exclude='.npm' --exclude='.nvm' \
    --exclude='.pki' --exclude='.python_env' --exclude='.rustup' --exclude='.subversion' --exclude='.wdm' \
    --exclude='agave' --exclude='backup' --exclude='ddos_restore' --exclude='dolphin-v2' --exclude='gemini-cli' \
    --exclude='jito-programs' --exclude='log' --exclude='media-hygiene' --exclude='old-block-production' \
    --exclude='old-www' --exclude='postgres_exporter' --exclude='rclone-cache' --exclude='rclone-cache-idrive' \
    --exclude='solana-dekey' --exclude='solxact' --exclude='stakenet' --exclude='stakenet-save' \
    --exclude='stakenet-save2' --exclude='sui-doctor' --exclude='sui-rgp' --exclude='sui-watcher' --exclude='temp' \
    --exclude='trillium_api' \
    > "$RSYNC_LOG_FILE" 2>&1; then
    log_info "✅ Home directory synced successfully"
else
    log_error "❌ Failed to sync home directory"
fi

# Backup system files
log_info "🔄 Backing up system files..."
if sudo rsync -av --no-links /etc/systemd/system/*.service "$backup_dir/etc/systemd/system/" >> "$RSYNC_LOG_FILE" 2>&1; then
    log_info "✅ System services backed up successfully"
else
    log_error "❌ Failed to copy system services"
fi

if sudo rsync -av --no-links /etc/systemd/*.conf "$backup_dir/etc/systemd/" >> "$RSYNC_LOG_FILE" 2>&1; then
    log_info "✅ System configs backed up successfully"
else
    log_error "❌ Failed to copy system configs"
fi

if sudo rsync -av --no-links /etc/logrotate.d/* "$backup_dir/etc/logrotate.d/" >> "$RSYNC_LOG_FILE" 2>&1; then
    log_info "✅ Logrotate configs backed up successfully"
else
    log_error "❌ Failed to copy logrotate configs"
fi

if sudo rsync -av --no-links /etc/ssh/* "$backup_dir/etc/ssh/" >> "$RSYNC_LOG_FILE" 2>&1; then
    log_info "✅ SSH configs backed up successfully"
else
    log_error "❌ Failed to copy SSH configs"
fi

if sudo rsync -av --no-links /etc/nginx/* "$backup_dir/etc/nginx/" >> "$RSYNC_LOG_FILE" 2>&1; then
    log_info "✅ Nginx configs backed up successfully"
else
    log_error "❌ Failed to copy Nginx configs"
fi

if sudo rsync -av --no-links /etc/grafana/* "$backup_dir/etc/grafana/" >> "$RSYNC_LOG_FILE" 2>&1; then
    log_info "✅ Grafana configs backed up successfully"
else
    log_error "❌ Failed to copy Grafana configs"
fi

if sudo rsync -av --no-links /etc/prometheus/* "$backup_dir/etc/prometheus/" >> "$RSYNC_LOG_FILE" 2>&1; then
    log_info "✅ Prometheus configs backed up successfully"
else
    log_error "❌ Failed to copy Prometheus configs"
fi

if sudo rsync -av --no-links /etc/fail2ban/* "$backup_dir/etc/fail2ban/" >> "$RSYNC_LOG_FILE" 2>&1; then
    log_info "✅ Fail2ban configs backed up successfully"
else
    log_error "❌ Failed to copy Fail2ban configs"
fi

if sudo rsync -av --no-links /etc/cron.* "$backup_dir/etc/cron/" >> "$RSYNC_LOG_FILE" 2>&1; then
    log_info "✅ Cron configs backed up successfully"
else
    log_error "❌ Failed to copy Cron configs"
fi

if sudo rsync -av --no-links /etc/ufw/* "$backup_dir/etc/ufw/" >> "$RSYNC_LOG_FILE" 2>&1; then
    log_info "✅ UFW configs backed up successfully"
else
    log_error "❌ Failed to copy UFW configs"
fi

if sudo rsync -av --no-links /etc/rsyslog.d/* "$backup_dir/etc/rsyslog.d/" >> "$RSYNC_LOG_FILE" 2>&1; then
    log_info "✅ Rsyslog configs backed up successfully"
else
    log_error "❌ Failed to copy Rsyslog configs"
fi

if sudo rsync -av --no-links /etc/hosts "$backup_dir/etc/" >> "$RSYNC_LOG_FILE" 2>&1; then
    log_info "✅ Hosts file backed up successfully"
else
    log_error "❌ Failed to copy hosts file"
fi

if sudo rsync -av --no-links /etc/hostname "$backup_dir/etc/" >> "$RSYNC_LOG_FILE" 2>&1; then
    log_info "✅ Hostname file backed up successfully"
else
    log_error "❌ Failed to copy hostname file"
fi

if sudo rsync -av --no-links /etc/environment "$backup_dir/etc/" >> "$RSYNC_LOG_FILE" 2>&1; then
    log_info "✅ Environment file backed up successfully"
else
    log_error "❌ Failed to copy environment file"
fi

if sudo rsync -av --no-links /etc/apt/sources.list "$backup_dir/etc/apt/" >> "$RSYNC_LOG_FILE" 2>&1; then
    log_info "✅ Apt sources list backed up successfully"
else
    log_error "❌ Failed to copy apt sources list"
fi

# Capture installed package lists and crontabs
log_info "🔄 Capturing installed package lists and crontabs..."

# Capture Python packages
PYTHON_REQUIREMENTS_FILE="$backup_dir/python_requirements_$current_date.txt"
if command -v $USER_HOME/.python_env/bin/pip >/dev/null 2>&1 && $USER_HOME/.python_env/bin/pip freeze > "$PYTHON_REQUIREMENTS_FILE"; then
    log_info "✅ Python packages list captured successfully"
    requirements_size=$(du -h "$PYTHON_REQUIREMENTS_FILE" | cut -f1)
    log_info "📏 Python requirements file size: $requirements_size"
else
    log_error "❌ Failed to capture Python packages list"
fi

# Capture apt packages
APT_PACKAGES_FILE="$backup_dir/apt_packages_$current_date.txt"
if command -v dpkg >/dev/null 2>&1 && sudo dpkg --list > "$APT_PACKAGES_FILE"; then
    log_info "✅ Apt packages list captured successfully"
    apt_size=$(du -h "$APT_PACKAGES_FILE" | cut -f1)
    log_info "📏 Apt packages file size: $apt_size"
else
    log_error "❌ Failed to capture apt packages list"
fi

# Capture npm packages
NPM_PACKAGES_FILE="$backup_dir/npm_packages_$current_date.txt"
if command -v npm >/dev/null 2>&1 && npm list -g --depth=0 > "$NPM_PACKAGES_FILE"; then
    log_info "✅ npm packages list captured successfully"
    npm_size=$(du -h "$NPM_PACKAGES_FILE" | cut -f1)
    log_info "📏 npm packages file size: $npm_size"
else
    log_error "❌ Failed to capture npm packages list"
fi

# Capture NVM versions
NVM_VERSIONS_FILE="$backup_dir/nvm_versions_$current_date.txt"
if [ -f "$USER_HOME/.nvm/nvm.sh" ] && source "$USER_HOME/.nvm/nvm.sh" && command -v nvm >/dev/null 2>&1 && nvm list > "$NVM_VERSIONS_FILE"; then
    log_info "✅ NVM versions list captured successfully"
    nvm_size=$(du -h "$NVM_VERSIONS_FILE" | cut -f1)
    log_info "📏 NVM versions file size: $nvm_size"
else
    log_error "❌ Failed to capture NVM versions list"
fi

# Capture cargo packages
CARGO_PACKAGES_FILE="$backup_dir/cargo_packages_$current_date.txt"
if command -v cargo >/dev/null 2>&1 && cargo install --list > "$CARGO_PACKAGES_FILE"; then
    log_info "✅ Cargo packages list captured successfully"
    cargo_size=$(du -h "$CARGO_PACKAGES_FILE" | cut -f1)
    log_info "📏 Cargo packages file size: $cargo_size"
else
    log_error "❌ Failed to capture cargo packages list"
fi

# Capture rust version
RUST_VERSION_FILE="$backup_dir/rust_version_$current_date.txt"
if command -v rustc >/dev/null 2>&1 && rustc --version > "$RUST_VERSION_FILE"; then
    log_info "✅ Rust version captured successfully"
    rust_size=$(du -h "$RUST_VERSION_FILE" | cut -f1)
    log_info "📏 Rust version file size: $rust_size"
else
    log_error "❌ Failed to capture Rust version"
fi

# Capture user crontab
CRONTAB_SMILAX_FILE="$backup_dir/crontab_smilax_$current_date.txt"
if crontab -l > "$CRONTAB_SMILAX_FILE" 2>/dev/null; then
    log_info "✅ User crontab (smilax) captured successfully"
    crontab_smilax_size=$(du -h "$CRONTAB_SMILAX_FILE" | cut -f1)
    log_info "📏 User crontab file size: $crontab_smilax_size"
else
    log_info "ℹ️ No user crontab found for smilax or failed to capture"
fi

# Capture root crontab
CRONTAB_ROOT_FILE="$backup_dir/crontab_root_$current_date.txt"
if sudo crontab -l > "$CRONTAB_ROOT_FILE" 2>/dev/null; then
    log_info "✅ Root crontab captured successfully"
    crontab_root_size=$(du -h "$CRONTAB_ROOT_FILE" | cut -f1)
    log_info "📏 Root crontab file size: $crontab_root_size"
else
    log_info "ℹ️ No root crontab found or failed to capture"
fi

# Backup API directory
log_info "🔄 Creating API backup archive..."
if [[ -f "$tar_file_api" && -s "$tar_file_api" && -r "$tar_file_api" ]]; then
    log_info "⏭️ Skipping API archive creation: $tar_file_api already exists"
else
    if tar --zstd -cf "$tar_file_api" --dereference --directory="$USER_HOME" api --exclude='data/backups' --exclude='data/epochs'; then
        log_info "✅ API archive created successfully"
        backup_size=$(du -h "$tar_file_api" | cut -f1)
        log_info "📏 API archive size: $backup_size"
    else
        log_error "❌ Failed to create API archive"
    fi
fi

# Verify API archive
log_info "🔍 Verifying API archive..."
if [[ -f "$tar_file_api" && -s "$tar_file_api" && -r "$tar_file_api" ]] && zstd -t "$tar_file_api" >/dev/null 2>&1; then
    log_info "✅ API archive is valid"
else
    log_error "❌ API archive is corrupted or invalid"
fi

# Create final API server backup archive
log_info "🔄 Creating API server backup archive..."
if [[ -f "$tar_file_apiserver" && -s "$tar_file_apiserver" && -r "$tar_file_apiserver" ]]; then
    log_info "⏭️ Skipping API server archive creation: $tar_file_apiserver already exists"
else
    if sudo tar --zstd -cf "$tar_file_apiserver" --directory="$USER_HOME/api/data/backups" apiserver; then
        log_info "✅ API server archive created successfully"
        backup_size=$(du -h "$tar_file_apiserver" | cut -f1)
        log_info "📏 API server archive size: $backup_size"
    else
        log_error "❌ Failed to create API server archive"
    fi
fi

# Verify API server archive
log_info "🔍 Verifying API server archive..."
if [[ -f "$tar_file_apiserver" && -s "$tar_file_apiserver" && -r "$tar_file_apiserver" ]] && zstd -t "$tar_file_apiserver" >/dev/null 2>&1; then
    log_info "✅ API server archive is valid"
else
    log_error "❌ API server archive is corrupted or invalid"
fi

# Transfer backups to remote servers
log_info "📤 Transferring backups to remote servers..."
if cp "$tar_file_api" "$remote_dir1"; then
    log_info "✅ API backup transferred to $remote_dir1"
else
    log_error "❌ Failed to transfer API backup to $remote_dir1"
fi

if cp "$tar_file_api" "$remote_dir2"; then
    log_info "✅ API backup transferred to $remote_dir2"
else
    log_error "❌ Failed to transfer API backup to $remote_dir2"
fi

if cp "$tar_file_apiserver" "$remote_dir1"; then
    log_info "✅ API server backup transferred to $remote_dir1"
else
    log_error "❌ Failed to transfer API server backup to $remote_dir1"
fi

if cp "$tar_file_apiserver" "$remote_dir2"; then
    log_info "✅ API server backup transferred to $remote_dir2"
else
    log_error "❌ Failed to transfer API server backup to $remote_dir2"
fi

if cp "$tar_file_api" "$final_backup_dir"; then
    log_info "✅ API backup copied to $final_backup_dir"
else
    log_error "❌ Failed to copy API backup to $final_backup_dir"
fi

# Verify copied API archive
log_info "🔍 Verifying copied API archive..."
if [[ -f "$final_backup_dir/$(basename "$tar_file_api")" && -s "$final_backup_dir/$(basename "$tar_file_api")" && -r "$final_backup_dir/$(basename "$tar_file_api")" ]] && zstd -t "$final_backup_dir/$(basename "$tar_file_api")" >/dev/null 2>&1; then
    log_info "✅ Copied API archive is valid"
else
    log_error "❌ Copied API archive is corrupted or invalid"
fi

# Clean up old backups (keep last 7 days + first and mid-month archives)
log_info "🧹 Cleaning up old backup files (keeping last 7 days + first and mid-month archives)"
old_api_files=$(find "$block_backup_dir" -name "api-*.tar.zst" -type f -mtime +7 -printf '%P\n' | sort)
old_apiserver_files=$(find "$final_backup_dir" -name "apiserver_backup_*.tar.zst" -type f -mtime +7 -printf '%P\n' | sort)

# Process API backups
if [ -z "$old_api_files" ]; then
    log_info "No old API files to clean"
else
    all_api_files=$(find "$block_backup_dir" -name "api-*.tar.zst" -type f -printf '%P\n' | sort)
    declare -A month_files keepers
    for file in $all_api_files; do
        date_str=${file#api-}
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
            date_str=${file#api-}
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

    for old_file in $old_api_files; do
        if [ -z "${keepers[$old_file]}" ]; then
            rm "$block_backup_dir/$old_file"
            log_info "🗑️ Removed old API backup: $old_file"
        fi
    done
fi

# Process API server backups
if [ -z "$old_apiserver_files" ]; then
    log_info "No old API server files to clean"
else
    all_apiserver_files=$(find "$final_backup_dir" -name "apiserver_backup_*.tar.zst" -type f -printf '%P\n' | sort)
    declare -A month_files keepers
    for file in $all_apiserver_files; do
        date_str=${file#apiserver_backup_}
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
            date_str=${file#apiserver_backup_}
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

    for old_file in $old_apiserver_files; do
        if [ -z "${keepers[$old_file]}" ]; then
            rm "$final_backup_dir/$old_file"
            log_info "🗑️ Removed old API server backup: $old_file"
        fi
    done
fi

# List remaining backup files
api_backup_count=$(find "$block_backup_dir" -name "api-*.tar.zst" -type f | wc -l)
apiserver_backup_count=$(find "$final_backup_dir" -name "apiserver_backup_*.tar.zst" -type f | wc -l)
log_info "📊 Total API backup files retained: $api_backup_count"
log_info "📊 Total API server backup files retained: $apiserver_backup_count"

log_info "🎉 API server backup process completed"

# Cleanup logging
cleanup_logging