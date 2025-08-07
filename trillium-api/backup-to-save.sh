#!/bin/bash

# Get today's date in YYYY-MM-DD format
today=$(date +%Y-%m-%d)

# Define the target directory
target_dir="save/$today"

# Create the target directory if it does not exist
mkdir -p "$target_dir"

# Copy files with specific extensions to the target directory
cp *.py "$target_dir" 2>/dev/null || true
cp *.sh "$target_dir" 2>/dev/null || true
cp *.sql "$target_dir" 2>/dev/null || true
cp *.json "$target_dir" 2>/dev/null || true
