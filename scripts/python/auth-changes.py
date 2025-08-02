import json
import csv

# Define the input and output file paths
input_file = "auth-changes.json"
output_file = "auth_changes.csv"

# Read the JSON file
with open(input_file, "r") as file:
    data = json.load(file)

# Extract the required fields and write to a CSV
with open(output_file, "w", newline="") as csvfile:
    fieldnames = [
        "vote_account_address",
        "epochs_voters_before",
        "epochs_voters_after",
        "epochs_voters_current"
    ]
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
    writer.writeheader()

    for change in data.get("authority_changes", []):
        authorized_voters_before = change.get("authorized_voters_before", {})
        authorized_voters_after = change.get("authorized_voters_after", {})
        vote_account = change.get("vote_account", {})
        current_authorized_voters = vote_account.get("authorized_voters", {})

        # Convert dictionary values to sets for comparison (ignoring epochs)
        before_voters_set = set(authorized_voters_before.values())
        after_voters_set = set(authorized_voters_after.values())

        # Skip if voters before and after are identical
        if before_voters_set == after_voters_set:
            continue

        # Convert the epoch/voter dictionaries into a readable string format
        # For example: "273:4cheZ7QmWi...; 274:AnotherVoter"
        def dict_to_str(d):
            return "; ".join([f"{epoch}:{voter}" for epoch, voter in d.items()])

        epochs_voters_before_str = dict_to_str(authorized_voters_before)
        epochs_voters_after_str = dict_to_str(authorized_voters_after)
        epochs_voters_current_str = dict_to_str(current_authorized_voters)

        # Write to CSV
        writer.writerow({
            "vote_account_address": change.get("vote_account_address", ""),
            "epochs_voters_before": epochs_voters_before_str,
            "epochs_voters_after": epochs_voters_after_str,
            "epochs_voters_current": epochs_voters_current_str,
        })

print(f"CSV file has been created: {output_file}")
