#!/bin/bash

# Source common logging
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/../bash/999_common_log.sh"

# Initialize logging
init_logging

# Create the run0 directory if it doesn't exist
mkdir -p ./run0

# Array of URLs to download
urls=(
  "https://stakeview.app/good.json"
  "https://stakeview.app/poor.json"
)

# Loop through the URLs
for url in "${urls[@]}"; do
  # Extract the filename from the URL
  filename=$(basename "$url")
  
  # Check if the file already exists
  if [ -f "./run0/$filename" ]; then
    log_info "⏭️ File $filename already exists. Skipping download"
  else
    log_info "📥 Downloading $filename..."
    # Download the file using curl
    curl -o "./run0/$filename" "$url"
    
    # Check if the download was successful
    if [ $? -eq 0 ]; then
      log_info "✅ Successfully downloaded $filename"
    else
      log_error "❌ Failed to download $filename"
    fi
  fi
done

log_info "🎉 Script execution completed"

# Cleanup logging
cleanup_logging