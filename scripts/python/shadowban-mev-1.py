import psycopg2
from db_config import db_params

# Connect to database
conn = psycopg2.connect(**db_params)
cur = conn.cursor()

# Get top 30 epochs by avg_mev_per_block
cur.execute("""
    SELECT epoch, AVG(avg_mev_per_block) as avg_mev
    FROM validator_stats
    GROUP BY epoch
    ORDER BY avg_mev DESC
    LIMIT 30
""")
top_epochs = [row[0] for row in cur.fetchall()]

# Numeric/integer columns
num_cols = [
    "activated_stake", "avg_cu_per_block", "avg_rewards_per_block",
    "avg_tx_per_block", "avg_user_tx_per_block", "avg_vote_tx_per_block",
    "blocks_produced", "commission", "epoch_credits", "mev_commission",
    "skip_rate", "votes_cast", "client_type", "asn", "jito_rank",
    "jito_steward_list_index", "jito_steward_overall_rank",
    "jito_steward_validator_history_index"
]

# Text columns
text_cols = ["version", "country", "region"]

# Extract data
data = {}
for epoch in top_epochs:
    cur.execute(f"""
        SELECT avg_mev_per_block, {', '.join(num_cols + text_cols)}
        FROM validator_stats
        WHERE epoch = %s
    """, (epoch,))
    data[epoch] = cur.fetchall()

# Analyze numeric/integer correlations
for epoch in top_epochs:
    print(f"Epoch {epoch}:")
    for col_idx, col in enumerate(num_cols, 1):
        cur.execute(f"""
            SELECT CORR(avg_mev_per_block, {col})
            FROM validator_stats
            WHERE epoch = %s
        """, (epoch,))
        corr = cur.fetchone()[0]
        if corr is not None and abs(corr) > 0.5:
            print(f"  {col}: {round(corr, 3)}")

# Analyze text columns (average avg_mev_per_block by category)
    for col in text_cols:
        cur.execute(f"""
            SELECT {col}, AVG(avg_mev_per_block) as avg_mev, COUNT(*) as cnt
            FROM validator_stats
            WHERE epoch = %s
            GROUP BY {col}
            HAVING COUNT(*) > 5
            ORDER BY avg_mev DESC
            LIMIT 3
        """, (epoch,))
        results = cur.fetchall()
        if results:
            print(f"  Top {col} (avg_mev_per_block):")
            for val, avg_mev, cnt in results:
                print(f"    {val}: {round(avg_mev, 3)} (n={cnt})")

cur.close()
conn.close()

# For direct analysis, data is in `data` dict: {epoch: [(avg_mev_per_block, col1, col2, ...), ...]}