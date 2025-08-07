#!/bin/bash

# Source path initialization
source "$(dirname "$0")/../bash/000_init_paths.sh" || {
    echo "❌ Failed to source path initialization script" >&2
    exit 1
}

# Source common logging (already sourced by init_paths but ensure it's available)
source "$COMMON_LOG_SCRIPT"

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