#!/bin/bash
# realtime-bandwidth.sh - Show real-time bandwidth usage and send Discord alerts.
# FINAL VERSION with FIXED-DURATION captures for faster analysis.

# --- Configuration ---
interface="enp9s0f0np0"
interval=2 # seconds
rx_hard_limit_mbps=15 # MB/s

# --- NEW: Capture & Cooldown Configuration ---
CAPTURE_DURATION_SECONDS=30 # Capture for this many seconds when a spike is detected.
CAPTURE_COOLDOWN_SECONDS=300 # Wait this many seconds before starting a new capture.

# --- Paths & Cleanup ---
PCAP_DIR="/home/smilax/api/tmp"                   # Store large .pcap files
ANALYSIS_DIR="${HOME}/log/net_analysis"          # Store final analysis reports in ~/log/net_analysis
DELETE_PCAP_AFTER_ANALYSIS=true                  # Set to 'true' to delete large .pcap files after analysis

# --- Discord Settings ---
webhook_url="https://discord.com/api/webhooks/1400600986065567824/om62uY9kkUFAPzG86G2vHjVO63AZ1FkECLHbFG6ywFDBLSDfx02MgauiRQgrxasXYilw"
avatar_url="https://trillium.so/images/philly-network-alerts.png"
discord_username="Philly Network Alerts"

# --- Runtime variables ---
last_capture_time=0 # Timestamp of the last capture trigger

# --- Functions ---
send_discord_notification() {
    local message="$1"
    local utc_timestamp=$(date -u '+%Y-%m-%d %H:%M:%S UTC')
    local full_message="**${utc_timestamp}**\\n${message}"
    local json_payload
    json_payload=$(printf '{"username": "%s", "avatar_url": "%s", "content": "%s"}' \
        "$discord_username" "$avatar_url" "$full_message")
    curl -H "Content-Type: application/json" -X POST -d "$json_payload" "$webhook_url" &>/dev/null
}

# Function to handle the entire capture and analysis process in the background
capture_and_analyze() {
    local pcap_file="$1"
    local analysis_file="$2"
    local trigger_rate="$3"
    local capture_duration="$4"

    # 1. Send initial alert
    local alert_msg="🚨 **Spike Detected (> ${trigger_rate} MB/s)**. Starting ${capture_duration}s packet capture..."
    send_discord_notification "$alert_msg"
    echo "$(date '+%H:%M:%S') - ${alert_msg}"

    # 2. Run the fixed-duration capture
    timeout ${capture_duration}s tcpdump -i "$interface" -w "$pcap_file" -s 0
    echo "$(date '+%H:%M:%S') - BG: Capture finished. Analyzing $pcap_file..."

    # 3. Analyze the capture
    {
        echo "--- Network Spike Analysis Report ---"
        echo "Interface: $interface"; echo "Capture Timestamp: $(date +'%Y-%m-%d %H:%M:%S')"
        echo "Trigger Rate: $trigger_rate MB/s"; echo "Capture Duration: ${capture_duration}s"; echo ""
        echo "--- Top IP Conversations (by Total Bytes) ---"
        tshark -r "$pcap_file" -q -z conv,ip | sort -k9 -rn | head -n 20; echo ""
        echo "--- Top TCP Conversations (by Total Bytes) ---"
        tshark -r "$pcap_file" -q -z conv,tcp | sort -k7 -rn | head -n 20; echo ""
        echo "--- Top UDP Conversations (by Total Bytes) ---"
        tshark -r "$pcap_file" -q -z conv,udp | sort -k7 -rn | head -n 20
    } > "$analysis_file"
    echo "$(date '+%H:%M:%S') - BG: Analysis complete. Report saved to $analysis_file"

    # 4. Clean up if configured
    if [ "$DELETE_PCAP_AFTER_ANALYSIS" = true ]; then
        rm "$pcap_file"
        echo "$(date '+%H:%M:%S') - BG: Deleted $pcap_file."
    fi

    # 5. Send the final notification with the report path
    local final_alert_msg="✅ **Analysis Complete** for spike detected at $(date -d @$(($(date +%s) - capture_duration)) +%H:%M:%S).\\n📄 **Report available:** \`${analysis_file}\`"
    send_discord_notification "$final_alert_msg"
}

cleanup() {
    echo -e "\n🛑 Stopping script..."
    # Kill any running tcpdump or tshark processes spawned by this script
    pkill -P $$ tcpdump
    pkill -P $$ tshark
    exit 0
}
trap cleanup SIGINT SIGTERM

# --- Main Script ---
if ! command -v tshark &> /dev/null || ! command -v tcpdump &> /dev/null || ! command -v timeout &> /dev/null; then
    echo "Error: 'tcpdump', 'tshark', and 'timeout' are required." >&2; exit 1;
fi
mkdir -p "$PCAP_DIR" "$ANALYSIS_DIR"

echo "=== Real-time Bandwidth Monitor for $interface ==="
echo "Triggering ${CAPTURE_DURATION_SECONDS}s capture for RX > ${rx_hard_limit_mbps} MB/s."
echo "Cooldown between captures is ${CAPTURE_COOLDOWN_SECONDS}s."
echo ""

# --- Main Monitoring Loop ---
while true; do
    # Get stats using a subshell to avoid overwriting variables in the main loop
    stats=$(grep "$interface:" /proc/net/dev | cut -d ':' -f 2 | awk '{print $1}')
    rx1=${stats}

    sleep $interval

    stats=$(grep "$interface:" /proc/net/dev | cut -d ':' -f 2 | awk '{print $1}')
    rx2=${stats}

    rx_diff=$((rx2 - rx1))
    rx_rate=$((rx_diff / interval))
    rx_mbps=$(printf "%.2f" $(echo "scale=2; $rx_rate / 1024 / 1024" | bc -l))

    timestamp=$(date '+%H:%M:%S')
    printf "%s - RX: %6.2f MB/s\n" "$timestamp" "$rx_mbps"

    # --- Spike Detection Logic ---
    if (( $(echo "$rx_mbps > $rx_hard_limit_mbps" | bc -l) )); then
        current_time=$(date +%s)
        # Check if we are outside the cooldown period
        if (( current_time - last_capture_time > CAPTURE_COOLDOWN_SECONDS )); then
            # Set the timestamp immediately to enforce cooldown
            last_capture_time=$current_time

            # Define filenames for this event
            SPIKE_TIMESTAMP=$(date '+%Y-%m-%d_%H-%M-%S')
            PCAP_FILE="${PCAP_DIR}/spike_${SPIKE_TIMESTAMP}.pcap"
            ANALYSIS_FILE="${ANALYSIS_DIR}/analysis_report_${SPIKE_TIMESTAMP}.txt"

            # Launch the entire capture-and-analyze process in the background
            capture_and_analyze "$PCAP_FILE" "$ANALYSIS_FILE" "$rx_mbps" "$CAPTURE_DURATION_SECONDS" &
        fi
    fi
done