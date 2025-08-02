import json
import csv

# Read the JSON file
with open('country-region.json', 'r') as json_file:
    data = json.load(json_file)

# Write to CSV
with open('country-region.csv', 'w', newline='') as csv_file:
    writer = csv.writer(csv_file)
    # Write header
    writer.writerow(['country', 'region'])
    # Write data
    for entry in data:
        writer.writerow([entry['country'], entry['region']])

print("Converted JSON to CSV: country-region.csv")