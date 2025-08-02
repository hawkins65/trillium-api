import psycopg2
from db_config import db_params

LAMPORTS_PER_SOL = 1_000_000_000

conn = psycopg2.connect(**db_params)
cur = conn.cursor()

epoch_groups = [("755-760", list(range(755, 761))), ("761-770", list(range(761, 771)))]

for label, epochs in epoch_groups:
    cur.execute("""
        SELECT 
            STDDEV(avg_mev_per_block) / %s as mev_stddev_sol,
            STDDEV(avg_mev_per_block / NULLIF(activated_stake, 0)) as ratio_stddev
        FROM validator_stats
        WHERE epoch = ANY(%s::int[])
    """, (LAMPORTS_PER_SOL, epochs))
    mev_stddev, ratio_stddev = cur.fetchone()
    print(f"Epochs {label}:")
    print(f"  avg_mev_per_block deviation (SOL): {round(mev_stddev, 6)}")
    print(f"  avg_mev_per_block/activated_stake deviation: {round(ratio_stddev, 6)}")

cur.close()
conn.close()