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

# Set the request body
BODY='{"purge_everything": true}'

# Send the POST request
curl -X POST \
  $API_ENDPOINT \
  -H "Content-Type: application/json" \
  -H "X-Auth-Key: $X_AUTH_KEY" \
  -H "X-Auth-Email: $X_AUTH_EMAIL" \
  -d "$BODY"