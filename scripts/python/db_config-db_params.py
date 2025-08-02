from db_config import db_params
#  I store my password in a .pgpass file so it's not needed
conn = psycopg2.connect(**db_params)
cur = conn.cursor()
cur.execute(
    """
    SELECT DISTINCT identity_pubkey 
    FROM validator_stats 
    WHERE epoch = %s
    ORDER BY identity_pubkey
    """, 
    (epoch,)
)
validators = [row[0] for row in cur.fetchall()]
cur.close()
conn.close()