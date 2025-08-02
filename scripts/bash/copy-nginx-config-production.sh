#!/bin/bash

source $HOME/api/999_common_log.sh

filename="/home/smilax/nginx-configuration/trillium.conf"

destination_dir="/etc/nginx/conf.d/"

# Copy the file to the web directory
sudo cp -v "$filename" "$destination_dir"

sudo nginx -t

sudo systemctl restart nginx

log_message "INFO" "File '$filename' has been successfully copied to '$destination_dir' and nginx tested and restarted"
