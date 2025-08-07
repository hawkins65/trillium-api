import psycopg2
from db_config import db_params

LAMPORTS_PER_SOL = 1_000_000_000

conn = psycopg2.connect(**db_params)
cur = conn.cursor()

cur.execute("""
    SELECT epoch, AVG(avg_mev_per_block) / %s as avg_mev_sol
    FROM validator_stats
    GROUP BY epoch
    ORDER BY AVG(avg_mev_per_block) DESC
    LIMIT 20
""", (LAMPORTS_PER_SOL,))
epochs_data = cur.fetchall()
epochs = [row[0] for row in epochs_data]

num_cols = [
    "activated_stake", "avg_cu_per_block", "avg_rewards_per_block",
    "avg_tx_per_block", "avg_user_tx_per_block", "avg_vote_tx_per_block",
    "blocks_produced", "commission", "epoch_credits", "mev_commission",
    "skip_rate", "votes_cast", "client_type", "asn", "jito_rank",
    "jito_steward_list_index", "jito_steward_overall_rank",
    "jito_steward_validator_history_index"
]
text_cols = ["version", "country", "region"]

averages = {}
for epoch in epochs:
    cols = ["avg_mev_per_block"] + num_cols
    query_cols = [f"AVG({col}) / {LAMPORTS_PER_SOL} as {col}" if col in ["avg_mev_per_block", "activated_stake", "avg_rewards_per_block"] else f"AVG({col}) as {col}" for col in cols]
    cur.execute(f"""
        SELECT {', '.join(query_cols)}
        FROM validator_stats
        WHERE epoch = %s
    """, (epoch,))
    averages[epoch] = cur.fetchone()

changes = {}
for i in range(1, len(epochs)):
    prev, curr = epochs[i-1], epochs[i]
    changes[curr] = tuple(curr_val - prev_val for prev_val, curr_val in zip(averages[prev], averages[curr]))

mev_changes = [changes[e][0] for e in epochs[1:]]
col_changes = {col: [changes[e][i+1] for e in epochs[1:]] for i, col in enumerate(num_cols)}

print("Correlations with avg_mev_per_block changes (in SOL):")
for col in num_cols:
    cur.execute("""
        SELECT CORR(a.mev_change, a.col_change)
        FROM (
            SELECT
                UNNEST(%s::numeric[]) as mev_change,
                UNNEST(%s::numeric[]) as col_change
        ) a
    """, (mev_changes, col_changes[col]))
    corr = cur.fetchone()[0]
    if corr is not None and abs(corr) > 0.3:
        print(f"  {col}: {round(corr, 3)}")

for col in text_cols:
    cur.execute(f"""
        SELECT t1.{col} as prev_val, t2.{col} as curr_val, 
               AVG(t2.avg_mev_per_block - t1.avg_mev_per_block) / %s as avg_change_sol
        FROM validator_stats t1
        JOIN validator_stats t2 ON t1.identity_pubkey = t2.identity_pubkey
        WHERE t1.epoch = ANY(%s::int[]) AND t2.epoch = t1.epoch + 1
        AND t1.{col} != t2.{col}
        GROUP BY t1.{col}, t2.{col}
        HAVING COUNT(*) > 10
        ORDER BY avg_change_sol DESC
        LIMIT 3
    """, (LAMPORTS_PER_SOL, epochs))
    results = cur.fetchall()
    if results:
        print(f"Top {col} transitions (avg_mev_per_block change in SOL):")
        for prev_val, curr_val, avg_change in results:
            print(f"  {prev_val} -> {curr_val}: {round(avg_change, 6)}")

cur.close()
conn.close()