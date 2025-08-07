import requests
import csv
import json
from datetime import datetime

# URL for the Stakewiz API
url = "https://api.stakewiz.com/validators"

# Fetch JSON data from the API
response = requests.get(url)
data = response.json()

# Prepare CSV file
csv_file = "stakewiz_age.csv"
fields = ["identity", "vote_identity", "activated_stake", "first_epoch_distance"]

# Write data to CSV
with open(csv_file, mode="w", newline="", encoding="utf-8") as file:
    writer = csv.DictWriter(file, fieldnames=fields)
    writer.writeheader()
    
    for validator in data:
        writer.writerow({
            "identity": validator.get("identity", ""),
            "vote_identity": validator.get("vote_identity", ""),
            "activated_stake": validator.get("activated_stake", 0),
            "first_epoch_distance": validator.get("first_epoch_distance", 0)
        })

print(f"Data has been saved to {csv_file}")