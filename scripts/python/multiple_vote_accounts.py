import psycopg2
import pandas as pd
import json
from db_config import db_params

# Connect to the PostgreSQL database
conn = psycopg2.connect(**db_params)
cursor = conn.cursor()

# Query to find vote and identity key combinations with occurrences
query = """
WITH vote_keys_with_multiple_rows AS (
    SELECT
        epoch,
        vote_account_pubkey,
        COUNT(*) AS row_count
    FROM
        validator_stats_test
    GROUP BY
        epoch, vote_account_pubkey
    HAVING
        COUNT(*) > 1
),
filtered_combinations AS (
    SELECT
        v.vote_account_pubkey,
        v.identity_pubkey,
        vk.epoch
    FROM
        validator_stats_test v
    JOIN
        vote_keys_with_multiple_rows vk
    ON
        v.epoch = vk.epoch AND v.vote_account_pubkey = vk.vote_account_pubkey
)
SELECT
    vote_account_pubkey,
    identity_pubkey,
    epoch,
    COUNT(*) AS total_occurrences
FROM
    filtered_combinations
GROUP BY
    vote_account_pubkey, identity_pubkey, epoch
ORDER BY
    vote_account_pubkey ASC, identity_pubkey ASC, epoch ASC;
"""

# Execute the query
cursor.execute(query)
rows = cursor.fetchall()

# Fetch the column names from the cursor description
columns = [desc[0] for desc in cursor.description]

# Convert the results to a pandas DataFrame
df = pd.DataFrame(rows, columns=columns)

# Convert DataFrame columns to Python native types to avoid serialization issues
df = df.astype({"total_occurrences": int, "epoch": int})

# Group data by `vote_account_pubkey` and calculate totals, unique combinations, and epoch lists
output_json = {}
for vote_key, group in df.groupby("vote_account_pubkey"):
    identity_combinations = (
        group[["identity_pubkey", "total_occurrences", "epoch"]]
        .groupby(["identity_pubkey", "total_occurrences"])
        .apply(lambda x: x["epoch"].tolist())
        .reset_index()
        .rename(columns={0: "epochs"})
        .to_dict(orient="records")
    )
    total_occurrences = int(group["total_occurrences"].sum())  # Total occurrences for the vote key
    unique_combinations = group["identity_pubkey"].nunique()  # Count unique identity keys for the vote key
    epochs_with_duplicates = group["epoch"].unique().tolist()  # List of epochs with duplicates
    output_json[vote_key] = {
        "total_occurrences_of_duplicate_vote_key_per_epoch": total_occurrences,
        "number_of_unique_combinations": unique_combinations,
        "epochs_with_duplicates": epochs_with_duplicates,
        "identity_combinations": identity_combinations,
    }

# Sort the JSON by descending order of `total_occurrences_of_duplicate_vote_key_per_epoch`
sorted_output_json = dict(
    sorted(
        output_json.items(),
        key=lambda item: item[1]["total_occurrences_of_duplicate_vote_key_per_epoch"],
        reverse=True
    )
)

# Save the JSON to a file
output_file = 'vote_identity_combinations_with_totals_sorted.json'
with open(output_file, 'w') as json_file:
    json.dump(sorted_output_json, json_file, indent=4)

# Close the cursor and connection
cursor.close()
conn.close()

print(f"Results saved to: {output_file}")
