# --- Configuration ---
# Use the full path to the .pcap file
PCAP_FILE="/home/smilax/api/tmp/spike_2025-07-31_23-33-57.pcap"

# Define the directory for temporary and final reports
TMP_DIR="/home/smilax/api/tmp"
FINAL_REPORT_DIR="${HOME}" # Save final report to your home directory

# Construct full paths for the report files
RAW_REPORT_FILE="${TMP_DIR}/raw_tshark_output.txt"
FINAL_REPORT_FILE="${FINAL_REPORT_DIR}/analysis_of_spike.txt"


# 1. Run tshark ONCE and save the raw, combined output to the specified temp file.
echo "➡️ Running tshark and saving raw output to $RAW_REPORT_FILE..."
tshark -r "$PCAP_FILE" -q -z conv,ip -z conv,tcp -z conv,udp > "$RAW_REPORT_FILE"
echo "✅ Raw report saved. Now creating final summary."


# 2. Process the temporary file to create the final, sorted report.
{
    echo "--- Final Summary of $PCAP_FILE ---"
    echo ""
    echo "--- Top IP Conversations (by Total Bytes) ---"
    awk '/^IPv4 Conversations/,/^================/' "$RAW_REPORT_FILE" | sort -k9 -rn | head -n 20
    echo ""
    echo "--- Top TCP Conversations (by Total Bytes) ---"
    awk '/^TCP Conversations/,/^================/' "$RAW_REPORT_FILE" | sort -k7 -rn | head -n 20
    echo ""
    echo "--- Top UDP Conversations (by Total Bytes) ---"
    awk '/^UDP Conversations/,/^================/' "$RAW_REPORT_FILE" | sort -k7 -rn | head -n 20
} > "$FINAL_REPORT_FILE"


# 3. The raw file cleanup is now commented out as requested.
# rm "$RAW_REPORT_FILE"

echo "📄 Final summary saved to: $FINAL_REPORT_FILE"
echo "✅ Raw report was NOT deleted and is available at: $RAW_REPORT_FILE"