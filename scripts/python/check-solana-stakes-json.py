import json

# Define the expected fields in each record
expected_fields = {
    "account_balance", "activation_epoch", "activating_stake", "active_stake",
    "credits_observed", "custodian", "deactivating_stake", "deactivation_epoch",
    "delegated_stake", "delegatedvoteaccountaddress", "epoch", "rent_exempt_reserve",
    "stake_pubkey", "stake_type", "staker", "unix_timestamp",
    "vote_account_pubkey", "withdrawer"
}

print("Expected fields ", expected_fields)

def find_unexpected_fields(file_path):
    with open(file_path, 'r') as file:
        data = json.load(file)

    # Collect any unexpected fields
    unexpected_fields = set()
    for record in data:
        for key in record.keys():
            if key not in expected_fields:
                unexpected_fields.add(key)
    
    if unexpected_fields:
        print("Unexpected fields found:", unexpected_fields)
    else:
        print("No unexpected fields found.")

# Example usage
file_path = "solana-stakes_748.json"
find_unexpected_fields(file_path)
