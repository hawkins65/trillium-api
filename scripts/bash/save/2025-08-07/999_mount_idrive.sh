#!/bin/bash
# Script to mount iDrive nyc-api directory with optimized settings

MOUNT_POINT="/mnt/idrive"
REMOTE="idrive:nyc-api"  # Changed to point directly to nyc-api directory

# Where you want your cache:
CACHE_DIR="/home/smilax/rclone-cache-idrive"

# Unmount if already mounted with retries and force option
unmount_with_retry() {
  local mount_point="$1"
  local max_attempts=3
  local attempt=1
  
  while mount | grep -q "$mount_point" && [ $attempt -le $max_attempts ]; do
    echo "Unmounting existing mount at $mount_point (attempt $attempt/$max_attempts)..."
    
    # Try regular unmount first
    fusermount -u "$mount_point" && return 0
    
    # If that fails, try to find what processes are using the mount
    echo "Mount point is busy. Checking what's using it..."
    lsof "$mount_point" 2>/dev/null || echo "No processes found by lsof"
    
    # Try lazy unmount if regular unmount fails
    echo "Trying lazy unmount (fusermount -uz)..."
    fusermount -uz "$mount_point" && { sleep 2; return 0; }
    
    # Kill any rclone processes for this mount
    echo "Terminating rclone processes for iDrive..."
    pkill -f "rclone mount.*idrive:" || true
    
    # Wait a bit longer between attempts
    sleep $(( attempt * 2 ))
    (( attempt++ ))
  done
  
  if mount | grep -q "$mount_point"; then
    echo "WARNING: Could not unmount $mount_point after $max_attempts attempts."
    echo "The script will continue, but this might cause issues."
    return 1
  fi
  
  return 0
}

# Try to unmount existing mount
if mount | grep -q "$MOUNT_POINT"; then
  unmount_with_retry "$MOUNT_POINT"
fi

# Make doubly sure any rclone processes are terminated
echo "Ensuring all rclone processes for iDrive are terminated..."
pkill -f "rclone mount.*idrive:" || true
sleep 2

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