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

log_info "🗂️ Starting iDrive mount process"

# Configuration
MOUNT_POINT="/mnt/idrive"
REMOTE="idrive:"
CACHE_DIR="/home/smilax/rclone-cache-idrive"
LOG_FILE="/tmp/rclone-idrive.log"

# Check if rclone is installed
if ! command -v rclone &> /dev/null; then
    log_error "❌ rclone is not installed or not in PATH"
    exit 1
fi

# Check if already mounted and healthy
if mountpoint -q "$MOUNT_POINT"; then
    log_info "ℹ️ iDrive is already mounted at $MOUNT_POINT"
    
    # Test if mount is working by listing contents
    if timeout 10 ls "$MOUNT_POINT" > /dev/null 2>&1; then
        log_info "✅ iDrive mount is healthy"
        cleanup_logging
        exit 0
    else
        log_info "⚠️ iDrive mount appears stale, unmounting..."
        fusermount -u "$MOUNT_POINT" || fusermount -uz "$MOUNT_POINT"
        sleep 2
    fi
fi

# Kill any existing rclone daemon for this mount
log_info "🔄 Terminating any existing rclone processes for iDrive..."
pkill -f "rclone mount.*$REMOTE" || true
sleep 1

# Ensure mount point and cache dir exist
if [ ! -d "$MOUNT_POINT" ]; then
    log_info "📁 Creating mount point: $MOUNT_POINT"
    mkdir -p "$MOUNT_POINT" || {
        log_error "❌ Failed to create mount point"
        exit 1
    }
fi

if [ ! -d "$CACHE_DIR" ]; then
    log_info "📁 Creating cache directory: $CACHE_DIR"
    mkdir -p "$CACHE_DIR" || {
        log_error "❌ Failed to create cache directory"
        exit 1
    }
fi

# Clear cache
log_info "🧹 Clearing rclone VFS cache in $CACHE_DIR..."
rm -rf "$CACHE_DIR"/vfs/* || true

# Mount the drive using the exact parameters from your working script
log_info "🔗 Mounting iDrive at $MOUNT_POINT (cache → $CACHE_DIR)..."
if rclone mount "$REMOTE" "$MOUNT_POINT" \
  --cache-dir "$CACHE_DIR" \
  --vfs-cache-mode full \
  --vfs-read-chunk-size 128M \
  --vfs-cache-max-size 4G \
  --vfs-cache-max-age 1h \
  --tpslimit 2 \
  --tpslimit-burst 5 \
  --retries 3 \
  --low-level-retries 3 \
  --buffer-size 64M \
  --daemon \
  --log-file="$LOG_FILE" \
  --log-level INFO; then
    
    # Give it a moment
    sleep 5
    
    # Verify using the method from your working script
    if pgrep -f "rclone mount.*$REMOTE" >/dev/null && mount | grep -q "$MOUNT_POINT"; then
        log_info "✅ iDrive mounted successfully, using cache under $CACHE_DIR"
        echo "$(date) - Mount successful" >> "$LOG_FILE"
    else
        log_error "❌ Mount failed—see $LOG_FILE"
        echo "$(date) - Mount failed" >> "$LOG_FILE"
        exit 1
    fi
else
    log_error "❌ Failed to mount iDrive"
    echo "$(date) - Mount command failed" >> "$LOG_FILE"
    exit 1
fi

log_info "🎉 iDrive mount process completed"

# Cleanup logging
cleanup_logging