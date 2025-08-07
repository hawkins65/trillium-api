# Python script to generate CSV and create bash script
import os
import csv
from pathlib import Path

# Directory paths
source_dir = '/home/smilax/trillium_api/icon_dir'
target_dir = '/home/smilax/trillium_api/static/images'

def process_files():
    # Get list of files
    files = [f for f in os.listdir(source_dir) if os.path.isfile(os.path.join(source_dir, f))]
    
    # Create CSV output
    csv_output = []
    bash_commands = []
    sql_commands = []
    
    for filename in files:
        name_without_ext = os.path.splitext(filename)[0]
        trillium_url = f'https://trillium.so/images/{filename}'
        
        # Add to CSV data
        csv_output.append([name_without_ext, trillium_url, 'Found via browser'])
        
        # Add to bash commands
        bash_commands.append(f'cp "{source_dir}/{filename}" "{target_dir}/{filename}"')
        
        # Add to SQL commands
        sql_commands.append(f"UPDATE validator_info SET logo = '{filename}' WHERE identity_pubkey = '{name_without_ext}';")
    
    # Write CSV file
    with open('image_inventory.csv', 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(['name', 'url', 'source'])  # Header
        writer.writerows(csv_output)
    
    # Write bash script
    with open('copy_images.sh', 'w') as bashfile:
        bashfile.write('#!/bin/bash\n\n')
        bashfile.write('# Script to copy image files to static directory\n\n')
        bashfile.write(f'# Create target directory if it doesn\'t exist\nmkdir -p "{target_dir}"\n\n')
        bashfile.write('\n'.join(bash_commands))
        bashfile.write('\n')
    
    # Make bash script executable
    os.chmod('copy_images.sh', 0o755)
    
    # Write SQL commands
    with open('update_logos.sql', 'w') as sqlfile:
        sqlfile.write('-- SQL commands to update validator_info logos\n\n')
        sqlfile.write('BEGIN;\n\n')
        sqlfile.write('\n'.join(sql_commands))
        sqlfile.write('\n\nCOMMIT;\n')

if __name__ == '__main__':
    process_files()
    print("Processing complete!")
    print("Generated files:")
    print("1. image_inventory.csv - Contains file inventory")
    print("2. copy_images.sh - Bash script to copy files")
    print("3. update_logos.sql - SQL commands to update validator_info")