import pandas as pd
import psycopg2
from db_config import db_params
import sys

# Read the CSV file
csv_file = '/home/smilax/api/stakewiz_age.csv'  # Path to the correct CSV file

# Debug: Read the first few lines of the CSV file as text to inspect raw content
try:
    with open(csv_file, 'r', encoding='utf-8') as f:
        print("Raw CSV content (first 3 lines):")
        for i, line in enumerate(f):
            if i < 3:
                print(f"Line {i+1}: {line.strip()}")
            else:
                break
except FileNotFoundError:
    raise FileNotFoundError(f"CSV file '{csv_file}' not found. Please verify the file path.")
except UnicodeDecodeError:
    raise ValueError(f"Encoding error reading '{csv_file}'. Try specifying a different encoding (e.g., 'latin1').")

# Load the CSV with pandas
try:
    df_csv = pd.read_csv(csv_file, encoding='utf-8', on_bad_lines='warn')
except pd.errors.ParserError as e:
    raise ValueError(f"Error parsing CSV file: {e}. Please ensure the file is well-formed and contains the expected columns.")
except Exception as e:
    raise ValueError(f"Unexpected error reading CSV file: {e}")

# Strip whitespace from column names and print for debugging
df_csv.columns = df_csv.columns.str.strip()
print("CSV Columns:", df_csv.columns.tolist())

# Verify expected columns
expected_columns = ['identity', 'vote_identity', 'activated_stake', 'first_epoch_distance']
missing_columns = [col for col in expected_columns if col not in df_csv.columns]
if missing_columns:
    raise KeyError(f"The CSV file is missing the following required columns: {missing_columns}. Found columns: {df_csv.columns.tolist()}")

# Calculate first_epoch as 777 - first_epoch_distance
df_csv['first_epoch'] = 777 - df_csv['first_epoch_distance']

# Rename CSV columns to match validator_stats column names
df_csv = df_csv.rename(columns={
    'identity': 'identity_pubkey',
    'vote_identity': 'vote_account_pubkey',
    'activated_stake': 'activated_stake',
    'first_epoch_distance': 'first_epoch_distance'  # Keep original name or drop if not needed
})

# Filter CSV data for first_epoch < 750 and activated_stake > 0
df_csv = df_csv[(df_csv['first_epoch'] < 750) & (df_csv['activated_stake'] > 0)]

# Debug: Print number of filtered CSV rows
print(f"Number of CSV rows after filtering (first_epoch < 750 and activated_stake > 0): {len(df_csv)}")

# Connect to the database
conn = psycopg2.connect(**db_params)
cur = conn.cursor()

# Query validator_stats for epoch = 750, ensuring one row per identity_pubkey or vote_account_pubkey
query = """
    SELECT DISTINCT ON (COALESCE(identity_pubkey, vote_account_pubkey))
        identity_pubkey, vote_account_pubkey, activated_stake, epoch
    FROM validator_stats
    WHERE (identity_pubkey = ANY(%s) OR vote_account_pubkey = ANY(%s))
    AND epoch = 776
"""
cur.execute(query, (df_csv['identity_pubkey'].tolist(), df_csv['vote_account_pubkey'].tolist()))
db_results = cur.fetchall()

# Debug: Print sample database results
print("Sample database results (first 5 rows):")
for row in db_results[:5]:
    print(row)

# Close database connection
cur.close()
conn.close()

# Create a DataFrame from database results
df_db = pd.DataFrame(db_results, columns=['identity_pubkey', 'vote_account_pubkey', 'activated_stake', 'epoch'])

# Convert activated_stake from lamports to SOL
df_db['activated_stake'] = df_db['activated_stake'] / 1_000_000_000.0

# Merge CSV and database DataFrames on either identity_pubkey or vote_account_pubkey
df_merged = df_csv.merge(
    df_db,
    how='inner',
    left_on='identity_pubkey',
    right_on='identity_pubkey',
    suffixes=('_csv', '_db')
).combine_first(
    df_csv.merge(
        df_db,
        how='inner',
        left_on='vote_account_pubkey',
        right_on='vote_account_pubkey',
        suffixes=('_csv', '_db')
    )
)

# Calculate the difference in activated_stake
df_merged['stake_difference'] = df_merged['activated_stake_csv'] - df_merged['activated_stake_db']

# Select required output columns, with vs.epoch (epoch_db) first
output_columns = [
    'epoch_db',  # From validator_stats (vs.epoch)
    'identity_pubkey_csv',
    'identity_pubkey_db',
    'vote_account_pubkey_csv',
    'vote_account_pubkey_db',
    'activated_stake_csv',
    'activated_stake_db',
    'stake_difference'
]

# Ensure all columns exist, filling missing ones with None
for col in output_columns:
    if col not in df_merged:
        df_merged[col] = None

# Create output DataFrame
df_output = df_merged[output_columns]

# Rename columns for clarity in the output
df_output = df_output.rename(columns={
    'epoch_db': 'vs.epoch',
    'identity_pubkey_csv': 'csv_identity_pubkey',
    'identity_pubkey_db': 'vs.identity_pubkey',
    'vote_account_pubkey_csv': 'csv_vote_account_pubkey',
    'vote_account_pubkey_db': 'vs.vote_account_pubkey',
    'activated_stake_csv': 'csv_activated_stake',
    'activated_stake_db': 'vs.activated_stake',
    'stake_difference': 'csv_minus_vs_activated_stake'
})

# Save to CSV
df_output.to_csv('stakewiz_diff.csv', index=False)