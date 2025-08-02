import psycopg2
import csv
from db_config import db_params  # Import the db_params for the PostgreSQL connection

# Connect to your PostgreSQL database
print("Connecting to the PostgreSQL database...")
conn = psycopg2.connect(**db_params)
cur = conn.cursor()
print("Connected successfully.")

# Fetch all parent slots
print("Fetching parent slots...")
cur.execute("SELECT DISTINCT parent_slot FROM validator_data WHERE parent_slot IS NOT NULL")
parent_slots = set(row[0] for row in cur.fetchall())
print(f"Fetched {len(parent_slots)} parent slots.")

# Fetch all block slots
print("Fetching block slots...")
cur.execute("SELECT DISTINCT block_slot FROM validator_data")
block_slots = set(row[0] for row in cur.fetchall())
print(f"Fetched {len(block_slots)} block slots.")

# Find missing slots
print("Calculating missing slots...")
missing_slots = parent_slots - block_slots
print(f"Found {len(missing_slots)} missing slots.")

# Write to CSV
print("Writing missing slots to 'missing-slots.csv'...")
with open('missing-slots.csv', 'w', newline='') as csvfile:
    writer = csv.writer(csvfile)
    writer.writerow(['parent_slot'])
    for slot in missing_slots:
        writer.writerow([slot])
print("Completed writing to 'missing-slots.csv'.")

# Close the connection
cur.close()
conn.close()
print("Database connection closed.")
