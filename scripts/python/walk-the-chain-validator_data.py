import psycopg2
import pandas as pd
from db_config import db_params

# Step 1: Establish connection to the PostgreSQL database
conn = psycopg2.connect(**db_params)
cur = conn.cursor()

# Step 2: SQL query to fetch the block_slot and parent_slot
query = """
    SELECT block_slot, parent_slot
    FROM validator_data
"""

# Step 3: Load data into a Pandas DataFrame
data = pd.read_sql(query, conn)

# Step 4: Close the cursor and connection
cur.close()
conn.close()

# Step 5: Find the highest block_slot to start
highest_slot = data['block_slot'].max()

# Step 6: Create a set of all block_slot values for quick lookup
block_slot_set = set(data['block_slot'])

# Step 7: Initialize a list to collect missing slots
missing_slots = []

# Step 8: Start tracing back from the highest slot
current_slot = highest_slot

while True:
    # Get the parent_slot for the current block_slot
    parent_slot = data.loc[data['block_slot'] == current_slot, 'parent_slot'].values
    
    if len(parent_slot) == 0:
        break  # No parent slot found, end the loop
    
    parent_slot = parent_slot[0]  # Get the single value
    
    # Check if the parent_slot exists in block_slot_set
    if parent_slot not in block_slot_set:
        missing_slots.append(parent_slot)
    
    # Move to the next parent_slot
    current_slot = parent_slot

    # Stop if we reach a point where there's no further parent_slot
    if current_slot is None or pd.isna(current_slot):
        break

# Step 9: Output the missing slots and save them to a CSV file
if missing_slots:
    print("Missing block_slots:")
    for slot in missing_slots:
        print(f"Missing block_slot: {slot}")
    
    # Save missing slots to a CSV file
    missing_slots_df = pd.DataFrame(missing_slots, columns=['missing_block_slot'])
    missing_slots_df.to_csv('missing_slots.csv', index=False)
    print("Missing slots have been saved to 'missing_slots.csv'.")
else:
    print("No missing block_slots found.")
