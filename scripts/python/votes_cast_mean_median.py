import pandas as pd
import requests
from io import StringIO

# Make the request to the API
url = 'https://api.trillium.so/epoch_data/'
headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'}

response = requests.get(url, headers=headers)

# Load the JSON data into a pandas dataframe using StringIO
df = pd.read_json(StringIO(response.text))

# Calculate the average of total_votes_cast
average_votes_cast = df['total_votes_cast'].mean()

# Calculate the median of total_votes_cast
median_votes_cast = df['total_votes_cast'].median()

# Print the total_votes_cast for each epoch with comma thousand separators
for index, row in df.iterrows():
    print(f'Epoch {row["epoch"]}: {row["total_votes_cast"]:,.0f}')

print(f'\nAverage total_votes_cast: {average_votes_cast:,.2f}')
print(f'Median total_votes_cast: {median_votes_cast:,.2f}')