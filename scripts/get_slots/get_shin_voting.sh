#!/bin/bash

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
    echo "File $filename already exists. Skipping download."
  else
    echo "Downloading $filename..."
    # Download the file using curl
    curl -o "./run0/$filename" "$url"
    
    # Check if the download was successful
    if [ $? -eq 0 ]; then
      echo "Successfully downloaded $filename"
    else
      echo "Failed to download $filename"
    fi
  fi
done

echo "Script execution completed."