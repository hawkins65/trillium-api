import psycopg2
from db_config import db_params

def fetch_latest_epoch():
    conn = psycopg2.connect(**db_params)
    cur = conn.cursor()
    
    cur.execute("SELECT MAX(epoch) FROM validator_xshin")
    epoch = cur.fetchone()[0]
    
    cur.close()
    conn.close()
    return epoch

