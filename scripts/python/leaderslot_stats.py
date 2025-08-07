import requests
import pandas as pd
import numpy as np

# Define the API endpoint
url = "https://api.trillium.so/ten_epoch_validator_rewards"

try:
    # Fetch data from the API
    response = requests.get(url)
    response.raise_for_status()  # Raise an exception for bad status codes
    data = response.json()

    # Convert JSON data to a pandas DataFrame
    df = pd.DataFrame(data)

    # Filter records where average_leader_slots is not null
    filtered_df = df[df['average_leader_slots'].notnull()]

    # Select the columns of interest
    columns_of_interest = [
        'average_leader_slots',
        'average_activated_stake',
        'avg_stake_per_leader_slot'
    ]

    # Calculate statistics
    stats = {
        'Metric': [],
        'Minimum': [],
        'Min_Identity_Pubkey': [],
        'Maximum': [],
        'Max_Identity_Pubkey': [],
        'Mean': [],
        'Median': []
    }

    for column in columns_of_interest:
        stats['Metric'].append(column)
        # Minimum value and corresponding identity_pubkey
        min_value = filtered_df[column].min()
        min_pubkey = filtered_df[filtered_df[column] == min_value]['identity_pubkey'].iloc[0]
        stats['Minimum'].append(min_value)
        stats['Min_Identity_Pubkey'].append(min_pubkey)
        # Maximum value and corresponding identity_pubkey
        max_value = filtered_df[column].max()
        max_pubkey = filtered_df[filtered_df[column] == max_value]['identity_pubkey'].iloc[0]
        stats['Maximum'].append(max_value)
        stats['Max_Identity_Pubkey'].append(max_pubkey)
        # Mean and Median
        stats['Mean'].append(filtered_df[column].mean())
        stats['Median'].append(filtered_df[column].median())

    # Create a DataFrame for the statistics
    stats_df = pd.DataFrame(stats)

    # Print the results
    print("Statistics for Validator Rewards (where average_leader_slots is not null):")
    print(stats_df.to_string(index=False))

except requests.exceptions.RequestException as e:
    print(f"Error fetching data from API: {e}")
except Exception as e:
    print(f"An error occurred: {e}")