import requests
import json
import os

# Define the API endpoint
url = "https://api.vx.tools/epochs/leaderboard/voting"

# Headers to match the request
headers = {
    "Accept": "application/json",
    "Content-Type": "application/json",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/123.0.0.0"
}

# Payload specifying the epoch
payload = {"epoch": 758}

# File path for saving the output
output_file = f"epoch_{payload['epoch']}_leaderboard.json"

try:
    # Make the POST request
    response = requests.post(url, headers=headers, json=payload)

    # Check if the request was successful (status code 200)
    if response.status_code == 200:
        # Parse the JSON response
        data = response.json()

        # Print the full response to console
        print("Full Response:")
        print(json.dumps(data, indent=4))

        # Save to a JSON file
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=4)
        print(f"\nData saved to {output_file}")

        # Extract and display specific fields
        epoch = data["epoch"]
        records = data["records"]
        print(f"\nEpoch: {epoch}")
        print(f"Number of records: {len(records)}")

        # Loop through records and print details
        for record in records:
            print(f"\nNode Name: {record['nodeName']}")
            print(f"Vote Address: {record['voteAddress']}")
            print(f"Voted Slots: {record['votedSlots']}")
            print(f"Earned Credits: {record['earnedCredits']}")
            print(f"Country: {record['country']}")

    else:
        print(f"Failed to fetch data. Status code: {response.status_code}")
        print(f"Response: {response.text}")

except requests.exceptions.RequestException as e:
    print(f"An error occurred: {e}")