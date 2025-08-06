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

log_info "📅 Starting epoch Discord notification check"

# Configuration
DATA_DIR="/home/smilax/trillium_api/data/monitoring"
EPOCH_STATUS_FILE="$DATA_DIR/epoch_status.json"
DISCORD_WEBHOOK="https://discord.com/api/webhooks/1267145835699769409/7b6MXomGQkOHcSi6_YLld4srbOu0CdZtC9PJqNgTbnM0KPgU8wg_SOMMMQkdBWkjAPom"

# Ensure data directory exists
mkdir -p "$DATA_DIR"

# Get current epoch info
current_epoch=$(/home/smilax/agave/bin/solana epoch --url https://api.mainnet-beta.solana.com)
epoch_info=$(/home/smilax/agave/bin/solana epoch-info --url https://api.mainnet-beta.solana.com --output json)

if [ $? -ne 0 ]; then
    log_error "❌ Failed to get epoch information"
    exit 1
fi

# Parse epoch information
slot_index=$(echo "$epoch_info" | jq -r '.slotIndex')
slots_in_epoch=$(echo "$epoch_info" | jq -r '.slotsInEpoch')
absolute_slot=$(echo "$epoch_info" | jq -r '.absoluteSlot')

# Calculate progress
progress_percent=$(echo "scale=2; ($slot_index / $slots_in_epoch) * 100" | bc)
progress_int=$(echo "$progress_percent" | cut -d. -f1)

# Calculate remaining time (assuming ~400ms per slot)
remaining_slots=$((slots_in_epoch - slot_index))
remaining_seconds=$(echo "scale=0; $remaining_slots * 0.4" | bc)
remaining_minutes=$(echo "scale=0; $remaining_seconds / 60" | bc)
remaining_hours=$(echo "scale=1; $remaining_minutes / 60" | bc)

log_info "📊 Epoch $current_epoch: ${progress_percent}% complete"
log_info "🕐 Estimated time remaining: ${remaining_hours} hours"

# Function to send Discord notification
send_epoch_notification() {
    local title="$1"
    local description="$2"
    local color="$3"
    local emoji="$4"
    
    local payload=$(cat <<EOF
{
    "embeds": [{
        "title": "$emoji $title",
        "description": "$description",
        "color": $color,
        "fields": [
            {
                "name": "📊 Progress",
                "value": "${progress_percent}% (${slot_index}/${slots_in_epoch} slots)",
                "inline": true
            },
            {
                "name": "🕐 Time Remaining",
                "value": "${remaining_hours} hours",
                "inline": true
            },
            {
                "name": "📈 Current Slot",
                "value": "$absolute_slot",
                "inline": true
            }
        ],
        "timestamp": "$(date -u +%Y-%m-%dT%H:%M:%S.000Z)",
        "footer": {
            "text": "Trillium Epoch Monitor"
        }
    }]
}
EOF
    )
    
    if curl -H "Content-Type: application/json" \
           -d "$payload" \
           "$DISCORD_WEBHOOK" > /dev/null 2>&1; then
        log_info "✅ Discord notification sent: $title"
    else
        log_error "❌ Failed to send Discord notification"
    fi
}

# Read previous status
previous_status=""
if [ -f "$EPOCH_STATUS_FILE" ]; then
    previous_status=$(jq -r '.last_notification // ""' "$EPOCH_STATUS_FILE" 2>/dev/null || echo "")
fi

# Check for notification triggers
notification_sent=false

# 90% completion check
if [ $progress_int -ge 90 ] && [[ "$previous_status" != *"90_percent"* ]]; then
    send_epoch_notification \
        "Epoch $current_epoch - 90% Complete!" \
        "Epoch $current_epoch is now 90% complete. Estimated ${remaining_hours} hours remaining." \
        16776960 \
        "⚠️"
    
    previous_status="${previous_status},90_percent"
    notification_sent=true
    log_info "📢 Sent 90% completion notification"
fi

# 1 hour remaining check (approximately 9000 slots at 400ms each)
if [ $remaining_slots -le 9000 ] && [ $remaining_slots -gt 8000 ] && [[ "$previous_status" != *"1_hour"* ]]; then
    send_epoch_notification \
        "Epoch $current_epoch - 1 Hour Remaining!" \
        "Epoch $current_epoch has approximately 1 hour remaining. Current progress: ${progress_percent}%" \
        16753920 \
        "⏰"
    
    previous_status="${previous_status},1_hour"
    notification_sent=true
    log_info "📢 Sent 1 hour remaining notification"
fi

# New epoch start check (first 1% of epoch)
if [ $progress_int -le 1 ] && [[ "$previous_status" != *"new_epoch_$current_epoch"* ]]; then
    send_epoch_notification \
        "New Epoch Started - Epoch $current_epoch" \
        "🎉 Epoch $current_epoch has begun! Previous epoch has completed." \
        65280 \
        "🚀"
    
    # Reset status for new epoch
    previous_status="new_epoch_$current_epoch"
    notification_sent=true
    log_info "📢 Sent new epoch notification"
fi

# Save current status
status_json=$(cat <<EOF
{
    "timestamp": "$(date -u +%Y-%m-%dT%H:%M:%S.000Z)",
    "current_epoch": $current_epoch,
    "progress_percent": $progress_percent,
    "remaining_hours": $remaining_hours,
    "absolute_slot": $absolute_slot,
    "last_notification": "$previous_status"
}
EOF
)

echo "$status_json" > "$EPOCH_STATUS_FILE"

if [ "$notification_sent" = true ]; then
    log_info "🎉 Epoch notification check completed with notifications sent"
else
    log_info "ℹ️ Epoch notification check completed - no notifications needed"
fi

# Cleanup logging
cleanup_logging