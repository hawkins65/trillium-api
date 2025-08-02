#!/bin/bash

# Set the API endpoint and zone ID
API_ENDPOINT="https://api.cloudflare.com/client/v4/zones/44f0ee1e44a8d12b72c9660e1af9a9ea/purge_cache"
API_KEY="34fb1d5af01415fea2f3247826035c31c6fb3" 
API_EMAIL="cloudflare@trillium.so"  

# Set the headers
HEADERS="Content-Type: application/json"
AUTHORIZATION="Bearer $API_KEY"
X_AUTH_KEY=$API_KEY
X_AUTH_EMAIL=$API_EMAIL

# Check if a parameter (filename or pattern) is provided
if [ $# -eq 1 ]; then
    # If a parameter is passed, set BODY to purge specific files
    FILE_PATTERN="$1"
    BODY="{\"files\": [\"$FILE_PATTERN\"]}"
    echo "Purging specific file/pattern: $FILE_PATTERN"
else
    # If no parameter is passed, purge everything (original behavior)
    BODY='{"purge_everything": true}'
    echo "Purging entire cache"
fi

# Send the POST request and capture the output
RESPONSE=$(curl -s -X POST \
  $API_ENDPOINT \
  -H "Content-Type: application/json" \
  -H "X-Auth-Key: $X_AUTH_KEY" \
  -H "X-Auth-Email: $X_AUTH_EMAIL" \
  -d "$BODY")

# Check for the "success" field in the JSON response
SUCCESS=$(echo "$RESPONSE" | jq -r '.success')

echo ""
if [[ "$SUCCESS" == "true" ]]; then
    echo "Cache purged successfully!"
    echo ""
else
    echo "Error: Failed to purge cache."
    echo "Response:"
    echo "$RESPONSE"
    echo ""
    exit 1
fi