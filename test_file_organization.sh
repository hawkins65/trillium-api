#!/bin/bash

# Test script to verify file organization for leaderboard outputs

echo "Testing file organization for leaderboard generation..."
echo "======================================================="

# Source the paths
source "/home/smilax/trillium_api/scripts/bash/000_init_paths.sh"

# Set up the same environment variables as the main script
export TRILLIUM_LEADERBOARD_DIR="${TRILLIUM_DATA}/leaderboard"
export TRILLIUM_LEADERBOARD_JSON="${TRILLIUM_LEADERBOARD_DIR}/json"
export TRILLIUM_LEADERBOARD_CSV="${TRILLIUM_LEADERBOARD_DIR}/csv"
export TRILLIUM_LEADERBOARD_HTML="${TRILLIUM_LEADERBOARD_DIR}/html"
export TRILLIUM_LEADERBOARD_LOGS="${TRILLIUM_DATA}/logs"

echo "Environment variables set:"
echo "  TRILLIUM_LEADERBOARD_JSON: $TRILLIUM_LEADERBOARD_JSON"
echo "  TRILLIUM_LEADERBOARD_CSV: $TRILLIUM_LEADERBOARD_CSV"
echo "  TRILLIUM_LEADERBOARD_HTML: $TRILLIUM_LEADERBOARD_HTML"
echo "  TRILLIUM_LEADERBOARD_LOGS: $TRILLIUM_LEADERBOARD_LOGS"
echo ""

# Test Python output_paths module
echo "Testing Python output_paths module..."
python3 -c "
from scripts.python.output_paths import get_json_path, get_csv_path, get_html_path, get_log_path
import os

# Test file paths
test_files = {
    'test.json': get_json_path('test.json'),
    'test.csv': get_csv_path('test.csv'),
    'test.html': get_html_path('test.html'),
    'test.log': get_log_path('test.log'),
}

print('File paths that would be used:')
for filename, path in test_files.items():
    print(f'  {filename} -> {path}')
    # Check if directory exists
    dir_path = os.path.dirname(path)
    if os.path.exists(dir_path):
        print(f'    ✓ Directory exists: {dir_path}')
    else:
        print(f'    ✗ Directory missing: {dir_path}')
"

echo ""
echo "Directory structure:"
tree -d "${TRILLIUM_DATA}/leaderboard" "${TRILLIUM_DATA}/logs" 2>/dev/null || {
    echo "Directories:"
    ls -la "${TRILLIUM_DATA}/leaderboard"
    ls -la "${TRILLIUM_DATA}/logs"
}

echo ""
echo "Test complete."