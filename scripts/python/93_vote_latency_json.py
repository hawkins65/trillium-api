import json
import psycopg2
import sys
from db_config import db_params
from decimal import Decimal
from output_paths import get_json_path

def fetch_latest_epoch():
    conn = psycopg2.connect(**db_params)
    cur = conn.cursor()
    
    cur.execute("SELECT MAX(epoch) FROM validator_xshin")
    epoch = cur.fetchone()[0]
    
    cur.close()
    conn.close()
    return epoch

def fetch_data(epoch):
    conn = psycopg2.connect(**db_params)
    cur = conn.cursor()

    # Query for Metro data
    metro_query = f"""
    SELECT 
        vs.metro AS "Metro",
        COUNT(*) AS "Group Count",
        ROUND(AVG(vt.mean_vote_latency), 3) AS "Avg Vote Latency",
        ROUND(MIN(vt.mean_vote_latency), 3) AS "Min VL",
        ROUND(MAX(vt.max_vote_latency), 3) AS "Max VL",
        ROUND(AVG(vt.median_vote_latency), 3) AS "Median VL",
        ROUND(AVG(vs.skip_rate), 2) AS "Avg Skip Rate"
    FROM votes_table vt
    JOIN validator_stats vs
        ON vt.vote_account_pubkey = vs.vote_account_pubkey AND vt.epoch = vs.epoch
    WHERE vt.epoch = {epoch}
    GROUP BY vs.metro
    ORDER BY "Avg Vote Latency" DESC;
    """

    # Fetch Metro data
    cur.execute(metro_query)
    metro_results = cur.fetchall()
    metro_columns = [desc[0] for desc in cur.description]
    metro_data = [dict(zip(metro_columns, row)) for row in metro_results]

    cur.close()
    conn.close()

    def convert_decimals(obj):
        if isinstance(obj, Decimal):
            return float(obj)
        elif isinstance(obj, dict):
            return {k: convert_decimals(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [convert_decimals(i) for i in obj]
        return obj

    return convert_decimals({
        "epoch": epoch,
        "Metro": metro_data
    })

def save_to_json(data, epoch):
    # Save to vote_latency.json (generic file)
    generic_filename = "vote_latency.json"
    generic_path = get_json_path(generic_filename)
    with open(generic_path, "w") as f:
        json.dump(data, f, indent=4)
    print(f"JSON file created: {generic_path}")

    # Save to specific filename with epoch
    specific_filename = f"vote_latency_{epoch}.json"
    specific_path = get_json_path(specific_filename)
    with open(specific_path, "w") as f:
        json.dump(data, f, indent=4)
    print(f"JSON file created: {specific_path}")

def main():
    # If epoch is provided as argument, use it; otherwise get latest
    epoch = int(sys.argv[1]) if len(sys.argv) > 1 else fetch_latest_epoch()
    print(f"Processing epoch: {epoch}")
    
    data = fetch_data(epoch)
    save_to_json(data, epoch)

if __name__ == "__main__":
    main()
