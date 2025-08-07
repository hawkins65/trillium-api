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

log_info "🗂️ Starting Google Drive mount process"

# Configuration
MOUNT_POINT="/mnt/gdrive"
RCLONE_REMOTE="gdrive"

# Check if rclone is installed
if ! command -v rclone &> /dev/null; then
    log_error "❌ rclone is not installed or not in PATH"
    exit 1
fi

# Check if mount point exists
if [ ! -d "$MOUNT_POINT" ]; then
    log_info "📁 Creating mount point: $MOUNT_POINT"
    if sudo mkdir -p "$MOUNT_POINT"; then
        log_info "✅ Mount point created successfully"
    else
        log_error "❌ Failed to create mount point"
        exit 1
    fi
fi

# Check if already mounted
if mountpoint -q "$MOUNT_POINT"; then
    log_info "ℹ️ Google Drive is already mounted at $MOUNT_POINT"
    
    # Test if mount is working by listing contents
    if timeout 10 ls "$MOUNT_POINT" > /dev/null 2>&1; then
        log_info "✅ Google Drive mount is healthy"
        cleanup_logging
        exit 0
    else
        log_info "⚠️ Google Drive mount appears stale, unmounting..."
        if sudo umount "$MOUNT_POINT" 2>/dev/null; then
            log_info "✅ Stale mount unmounted"
        else
            log_info "⚠️ Failed to unmount stale mount, forcing..."
            sudo umount -f "$MOUNT_POINT" 2>/dev/null || true
        fi
    fi
fi

# Mount Google Drive
log_info "🔗 Mounting Google Drive at $MOUNT_POINT"
if rclone mount "$RCLONE_REMOTE:" "$MOUNT_POINT" \
    --daemon \
    --allow-other \
    --dir-cache-time 5m \
    --vfs-cache-mode writes \
    --vfs-cache-max-age 1h; then
    
    # Wait a moment for mount to stabilize
    sleep 3
    
    # Verify mount is working
    if mountpoint -q "$MOUNT_POINT" && timeout 10 ls "$MOUNT_POINT" > /dev/null 2>&1; then
        log_info "✅ Google Drive mounted successfully at $MOUNT_POINT"
        
        # Create necessary subdirectories if they don't exist
        mkdir -p "$MOUNT_POINT/epochs" 2>/dev/null || true
        log_info "📁 Ensured epochs directory exists"
    else
        log_error "❌ Google Drive mount verification failed"
        exit 1
    fi
else
    log_error "❌ Failed to mount Google Drive"
    exit 1
fi

log_info "🎉 Google Drive mount process completed"

# Cleanup logging
cleanup_logging