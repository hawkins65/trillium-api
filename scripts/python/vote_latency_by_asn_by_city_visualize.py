import psycopg2
import pandas as pd
import matplotlib.pyplot as plt

# Database connection details
DB_HOST = "localhost"
DB_PORT = "5432"
DB_USER = "smilax"
DB_NAME = "sol_blocks"

# SQL query
SQL_QUERY = """
SELECT 
   vt.epoch AS "Epoch",
   vs.asn AS "ASN",
   vs.asn_org AS "ASN Org", 
   vs.city AS "City",
   vs.country AS "Country",
   COUNT(*) AS "Group Count",
   ROUND(AVG(vt.mean_vote_latency), 3) AS "Avg Vote Latency",
   ROUND(MIN(vt.mean_vote_latency), 3) AS "Min VL",
   ROUND(MAX(vt.mean_vote_latency), 3) AS "Max VL",
   ROUND(AVG(vt.median_vote_latency), 3) AS "Median VL",
   ROUND(AVG(vs.skip_rate), 2) AS "Avg Skip Rate",
   ROUND(SUM(vs.stake_percentage), 3) AS "Total Stake Percentage"
FROM votes_table vt
JOIN validator_stats vs
   ON vt.vote_account_pubkey = vs.vote_account_pubkey AND vt.epoch = vs.epoch
WHERE vt.epoch = %(epoch)s
  AND vs.asn IS NOT NULL
  AND vs.city IS NOT NULL
  AND vs.city != ''
GROUP BY vt.epoch, vs.asn, vs.asn_org, vs.city, vs.country
HAVING AVG(vt.mean_vote_latency) IS NOT NULL
ORDER BY "Avg Vote Latency" DESC;
"""

def fetch_epoch():
    """Fetch the latest epoch."""
    query = "SELECT MAX(epoch) FROM validator_xshin;"
    with psycopg2.connect(
        host=DB_HOST, port=DB_PORT, user=DB_USER, dbname=DB_NAME
    ) as conn:
        with conn.cursor() as cur:
            cur.execute(query)
            result = cur.fetchone()
            return result[0] if result else None

def execute_query(epoch):
    """Run the SQL query and return a DataFrame."""
    with psycopg2.connect(
        host=DB_HOST, port=DB_PORT, user=DB_USER, dbname=DB_NAME
    ) as conn:
        return pd.read_sql_query(SQL_QUERY, conn, params={"epoch": epoch})

def plot_relationship(df, output_file="vote_latency_by_asn_by_city.png"):
    """Visualize the relationship between Avg Vote Latency and Total Stake Percentage and save to a file."""
    plt.figure(figsize=(10, 6))
    plt.scatter(df["Avg Vote Latency"], df["Total Stake Percentage"], color='blue', alpha=0.7)

    # Add labels and title
    plt.title("Relationship Between Avg Vote Latency and Total Stake Percentage", fontsize=14)
    plt.xlabel("Avg Vote Latency (ms)", fontsize=12)
    plt.ylabel("Total Stake Percentage (%)", fontsize=12)

    # Annotate points with city or ASN if desired
    if "City" in df.columns:
        for i, txt in enumerate(df["City"]):
            plt.annotate(txt, (df["Avg Vote Latency"][i], df["Total Stake Percentage"][i]), fontsize=8)

    # Save the plot to a file
    plt.grid(True)
    plt.savefig(output_file, dpi=300, bbox_inches="tight")
    plt.close()
    print(f"Plot saved to {output_file}")

def main():
    epoch = fetch_epoch()
    if not epoch:
        print("Failed to retrieve the latest epoch.")
        return

    print(f"Using epoch: {epoch}")
    df = execute_query(epoch)

    if df.empty:
        print("No data returned for the given epoch.")
        return

    # Print the DataFrame (optional, for debugging)
    print(df)

    # Plot the relationship
    plot_relationship(df)

if __name__ == "__main__":
    main()
