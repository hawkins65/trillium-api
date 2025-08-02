import pandas as pd
import matplotlib.pyplot as plt

# Read the data from the text file, skipping the first two rows
file_path = "./vote_latency_by_metro.txt"

# Read the file into a DataFrame while handling delimiters and spaces
df = pd.read_csv(file_path, delimiter="|", skiprows=2, skipinitialspace=True)

# Trim column names to remove leading/trailing spaces
df.columns = df.columns.str.strip()

# Print column names for debugging
print("Column Names:", df.columns.tolist())

# Ensure "Group Count" column exists
expected_column = "Group Count"
if expected_column not in df.columns:
    print(f"Error: Expected column '{expected_column}' not found. Available columns: {df.columns.tolist()}")
    exit(1)  # Exit if column is missing

# Convert necessary columns to numeric values
df["Group Count"] = pd.to_numeric(df["Group Count"], errors='coerce')
df["Avg Vote Latency"] = pd.to_numeric(df["Avg Vote Latency"], errors='coerce')

# Filter for metros with more than 5 validators
df_filtered = df[df["Group Count"] > 5]

# Sort by Avg Vote Latency in ascending order
df_sorted = df_filtered.sort_values(by="Avg Vote Latency", ascending=True)

# Plot
plt.figure(figsize=(10, 8))
bars = plt.barh(df_sorted["Metro"], df_sorted["Avg Vote Latency"], color='skyblue')

# Add group count labels at the end of each bar
for bar, count in zip(bars, df_sorted["Group Count"]):
    plt.text(bar.get_width(), bar.get_y() + bar.get_height()/2, f' {int(count)}', va='center', ha='left')

# Add dashed grid lines for both x-axis (vertical) and y-axis (horizontal)
plt.grid(axis="both", linestyle="--", alpha=0.7)

plt.xlabel("Avg Vote Latency")
plt.ylabel("Metro")
plt.title("Avg Vote Latency By Metro (> 5 validators)")
plt.gca().invert_yaxis()  # Invert y-axis for better readability

# Save the chart as a PNG file
output_filename = "vote-latency-by-metro.png"
plt.savefig(output_filename, bbox_inches='tight', dpi=300)

print(f"Chart saved as {output_filename}")
