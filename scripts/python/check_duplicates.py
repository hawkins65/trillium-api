import json

# File to check
filename = '92_validator-info.json'

# Fields to compare
fields = ['infoPubkey', 'name', 'website', 'details', 'keybaseUsername', 'iconUrl']

# Read the JSON file
try:
    with open(filename, 'r') as f:
        data = json.load(f)
except FileNotFoundError:
    print(f"Error: File '{filename}' not found.")
    exit(1)
except json.JSONDecodeError:
    print(f"Error: File '{filename}' is not valid JSON.")
    exit(1)

# Organize records by identityPubkey
records_by_pubkey = {}
for validator in data:
    pubkey = validator.get('identityPubkey', 'MISSING')
    info = validator.get('info', {})
    record = {
        'infoPubkey': validator.get('infoPubkey'),
        'name': info.get('name'),
        'website': info.get('website'),
        'details': info.get('details'),
        'keybaseUsername': info.get('keybaseUsername'),
        'iconUrl': info.get('iconUrl')
    }
    if pubkey in records_by_pubkey:
        records_by_pubkey[pubkey].append(record)
    else:
        records_by_pubkey[pubkey] = [record]

# Find and analyze duplicates
duplicates_found = False
for pubkey, records in records_by_pubkey.items():
    if len(records) > 1:
        if not duplicates_found:
            print("Duplicate identityPubkey values found:")
            duplicates_found = True
        
        print(f"\nIdentityPubkey: {pubkey} ({len(records)} occurrences)")
        
        # Use the first record as the baseline
        baseline = records[0]
        print(f"  Record 1 (baseline):")
        print(f"    infoPubkey: {baseline['infoPubkey']}")
        for field in fields[1:]:  # Skip infoPubkey since it's already printed
            print(f"    {field}: {baseline[field]}")
        
        # Compare subsequent records to the baseline
        for i, record in enumerate(records[1:], 2):
            print(f"  Record {i}:")
            print(f"    infoPubkey: {record['infoPubkey']}")
            differences = []
            for field in fields:
                if record[field] != baseline[field]:
                    differences.append(f"{field}: {record[field]} (vs {baseline[field]})")
            if differences:
                print("    Differences from baseline:")
                for diff in differences:
                    print(f"      - {diff}")
            else:
                print("    No differences from baseline.")

if not duplicates_found:
    print("No duplicate identityPubkey values found.")
print(f"Total records checked: {len(data)}")