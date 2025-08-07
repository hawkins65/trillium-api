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