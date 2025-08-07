#!/bin/bash
# Script to mount iDrive nyc-api directory with optimized settings

MOUNT_POINT="/mnt/idrive"
REMOTE="idrive:nyc-api"  # Changed to point directly to nyc-api directory

# Where you want your cache:
CACHE_DIR="/home/smilax/rclone-cache-idrive"

# Unmount if already mounted
if mount | grep -q "$MOUNT_POINT"; then
  echo "Unmounting existing mount at $MOUNT_POINT..."
  fusermount -u "$MOUNT_POINT" || fusermount -uz "$MOUNT_POINT"
  sleep 2
fi

# Kill any existing rclone daemon for this mount
echo "Terminating any existing rclone processes for iDrive..."
pkill -f "rclone mount.*idrive:" || true
sleep 1

# Ensure mount point and cache dir exist
mkdir -p "$MOUNT_POINT" "$CACHE_DIR"

# Clear cache
echo "Clearing rclone VFS cache in $CACHE_DIR..."
rm -rf "$CACHE_DIR"/vfs/* || true

# Mount the drive, pointing cache at CACHE_DIR
echo "Mounting iDrive nyc-api at $MOUNT_POINT (cache → $CACHE_DIR)..."
rclone mount "$REMOTE" "$MOUNT_POINT" \
  --cache-dir "$CACHE_DIR" \
  --vfs-cache-mode full \
  --vfs-read-chunk-size 64M \
  --vfs-cache-max-size 8G \
  --vfs-cache-max-age 24h \
  --tpslimit 10 \
  --tpslimit-burst 20 \
  --retries 5 \
  --low-level-retries 10 \
  --transfers 4 \
  --checkers 8 \
  --buffer-size 64M \
  --timeout 30s \
  --contimeout 15s \
  --daemon \
  --log-file=/tmp/rclone-idrive.log \
  --log-level INFO

# Give it a moment
sleep 5

# Verify
if pgrep -f "rclone mount.*idrive:" >/dev/null && mount | grep -q "$MOUNT_POINT"; then
  echo "✅ iDrive nyc-api directory mounted, using cache under $CACHE_DIR"
  echo "   $(date) - Mount successful" >> /tmp/rclone-idrive.log
else
  echo "❌ Mount failed—see /tmp/rclone-idrive.log"
  echo "   $(date) - Mount failed" >> /tmp/rclone-idrive.log
  exit 1
fi