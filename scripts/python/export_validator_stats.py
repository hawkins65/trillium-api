import csv
import json
import psycopg2
from db_config import db_params
import matplotlib.pyplot as plt
import folium
import requests

# Output file paths
CSV_FILE = '/home/smilax/trillium_api/data/exports/fd-validators.csv'
JSON_FILE = '/home/smilax/trillium_api/data/exports/fd-validators.json'
PIE_CHART_FILE = '/home/smilax/trillium_api/data/charts/validators_by_metro_pie.png'
BUBBLE_MAP_FILE = '/home/smilax/trillium_api/data/charts/validators_by_metro_bubble.html'

def create_table_if_not_exists(cursor):
    """Create the voter_classifications table if it doesn't exist."""
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS voter_classifications (
            vote_pubkey VARCHAR(44) PRIMARY KEY,
            identity_pubkey VARCHAR(44) NOT NULL,
            stake BIGINT,
            average_vl NUMERIC,
            average_llv NUMERIC,
            average_cv NUMERIC,
            vo BOOLEAN,
            is_foundation_staked BOOLEAN,
            classification TEXT NOT NULL,
            epoch INTEGER NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_voter_classifications_vote_pubkey 
        ON voter_classifications (vote_pubkey);
    """)

def import_voters_from_json(cursor, json_url, classification):
    """Import voters from a JSON URL into the voter_classifications table."""
    response = requests.get(json_url)
    response.raise_for_status()
    data = response.json()
    
    epoch = data['data_epochs'][0]
    voters = data['voters']
    
    insert_query = """
        INSERT INTO voter_classifications (
            vote_pubkey, identity_pubkey, stake, average_vl, average_llv, 
            average_cv, vo, is_foundation_staked, classification, epoch
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (vote_pubkey) DO UPDATE SET
            identity_pubkey = EXCLUDED.identity_pubkey,
            stake = EXCLUDED.stake,
            average_vl = EXCLUDED.average_vl,
            average_llv = EXCLUDED.average_llv,
            average_cv = EXCLUDED.average_cv,
            vo = EXCLUDED.vo,
            is_foundation_staked = EXCLUDED.is_foundation_staked,
            classification = EXCLUDED.classification,
            epoch = EXCLUDED.epoch;
    """
    
    for voter in voters:
        cursor.execute(insert_query, (
            voter['vote_pubkey'],
            voter['identity_pubkey'],
            voter['stake'],
            voter['average_vl'],
            voter['average_llv'],
            voter['average_cv'],
            voter['vo'],
            voter['is_foundation_staked'],
            classification,
            epoch
        ))

def execute_query(cursor):
    """Execute the query and return column names and results."""
    query = """
        WITH classified_validators AS (
            SELECT 
                vi.name,
                vs.identity_pubkey,
                vs.vote_account_pubkey,
                vs.epoch,
                COALESCE(vc.classification, 'unknown') AS classification,
                vs.version,
                vs.client_type,
                vc.average_vl,  -- Added: Average validator latency
                vc.average_llv, -- Added: Average leader latency variance
                vc.average_cv,  -- Added: Average consensus vote
                vc.vo,          -- Added: Vote-only indicator
                vs.avg_cu_per_block,
                vs.avg_mev_per_block,
                vs.avg_priority_fees_per_block,
                vs.avg_rewards_per_block,
                vs.avg_signature_fees_per_block,
                vs.avg_tx_per_block,
                vs.avg_user_tx_per_block,
                vs.avg_vote_tx_per_block,
                vs.epoch_credits,
                vs.ip,
                vs.skip_rate,
                vs.stake_percentage,
                vs.tx_included_in_blocks,
                vs.user_tx_included_in_blocks,
                vs.validator_signature_fees,
                vs.vote_tx_included_in_blocks,
                vs.city,
                vs.country,
                vs.continent,
                vs.asn,
                vs.asn_org,
                vs.region,
                vs.jito_rank,
                vs.metro,
                vs.jito_steward_list_index,
                vs.jito_steward_overall_rank,
                vs.jito_steward_validator_history_index
            FROM public.validator_stats vs
            LEFT JOIN public.validator_info vi
                ON vs.identity_pubkey = vi.identity_pubkey
            LEFT JOIN voter_classifications vc
                ON vs.vote_account_pubkey = vc.vote_pubkey
            WHERE vs.epoch = (
                SELECT MAX(epoch)
                FROM public.validator_stats
                WHERE version LIKE '0%'
            )
            AND vs.version LIKE '0%'
        )
        SELECT * FROM classified_validators
        ORDER BY version, name;
    """
    cursor.execute(query)
    column_names = [desc.name for desc in cursor.description]
    results = cursor.fetchall()
    return column_names, results

def execute_metro_aggregation(cursor):
    """Execute a query to aggregate the number of validators by metro."""
    query = """
        SELECT metro, COUNT(*) as validator_count
        FROM public.validator_stats
        WHERE epoch = (
            SELECT MAX(epoch)
            FROM public.validator_stats
            WHERE version LIKE '0%'
        )
        AND version LIKE '0%'
        GROUP BY metro
        ORDER BY validator_count DESC;
    """
    cursor.execute(query)
    metro_data = cursor.fetchall()
    return metro_data

def write_csv(column_names, results, csv_file):
    """Write query results to a CSV file."""
    with open(csv_file, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(column_names)
        writer.writerows(results)

def write_json(column_names, results, json_file):
    """Write query results to a JSON file."""
    json_data = []
    for row in results:
        row_dict = dict(zip(column_names, row))
        json_data.append(row_dict)
    
    with open(json_file, 'w') as f:
        json.dump(json_data, f, indent=2, default=str)

def generate_pie_chart(metro_data, pie_chart_file):
    """Generate a pie chart of validators by metro."""
    metros = [row[0] for row in metro_data if row[0] is not None]
    counts = [row[1] for row in metro_data if row[0] is not None]
    
    plt.figure(figsize=(10, 8))
    plt.pie(counts, labels=metros, autopct='%1.1f%%', startangle=140)
    plt.title('Validators by Metro Area')
    plt.axis('equal')
    plt.savefig(pie_chart_file, dpi=300, bbox_inches='tight')
    plt.close()
    print(f"Pie chart saved to {pie_chart_file}")

def generate_bubble_map(metro_data, bubble_map_file):
    """Generate a bubble map of validators by metro."""
    metro_coordinates = {
        'Amsterdam': [52.3676, 4.9041],
        'Bogota': [4.7110, -74.0721],
        'Chicago': [41.8781, -87.6298],
        'Frankfurt': [50.1109, 8.6821],
        'Groningen': [53.2194, 6.5665],
        'London': [51.5074, -0.1278],
        'Los Angeles': [34.0522, -118.2437],
        'Metairie': [29.9841, -90.1529],
        'Miami': [25.7617, -80.1918],
        'Munich': [48.1351, 11.5820],
        'Queretaro City': [20.5888, -100.3899],
        'Salt Lake City': [40.7608, -111.8910],
        'Singapore': [1.3521, 103.8198],
        'Tokyo': [35.6762, 139.6503],
        'Tseung Kwan O': [22.3119, 114.2569],
    }
    
    map_data = []
    for metro, count in metro_data:
        if metro in metro_coordinates and metro is not None:
            lat, lon = metro_coordinates[metro]
            map_data.append((metro, lat, lon, count))
    
    if not map_data:
        print("No valid metro coordinates found for bubble map. Skipping bubble map generation.")
        return
    
    m = folium.Map(location=[40, -100], zoom_start=2)
    
    for metro, lat, lon, count in map_data:
        radius = count * 0.5
        folium.CircleMarker(
            location=[lat, lon],
            radius=radius,
            popup=f"{metro}: {count} validators",
            tooltip=f"{metro}: {count}",
            fill=True,
            fill_color='blue',
            color='blue',
            fill_opacity=0.6
        ).add_to(m)
    
    m.save(bubble_map_file)
    print(f"Bubble map saved to {bubble_map_file}")

def main():
    GOOD_JSON = 'https://stakeview.app/good.json'
    POOR_JSON = 'https://stakeview.app/poor.json'

    conn = psycopg2.connect(**db_params)
    cur = conn.cursor()
    
    try:
        create_table_if_not_exists(cur)
        import_voters_from_json(cur, GOOD_JSON, 'good')
        import_voters_from_json(cur, POOR_JSON, 'poor')
        conn.commit()
        print("Voter data imported successfully from URLs.")
        
        column_names, results = execute_query(cur)
        write_csv(column_names, results, CSV_FILE)
        print(f"CSV output written to {CSV_FILE}")
        write_json(column_names, results, JSON_FILE)
        print(f"JSON output written to {JSON_FILE}")
        
        metro_data = execute_metro_aggregation(cur)
        generate_pie_chart(metro_data, PIE_CHART_FILE)
        generate_bubble_map(metro_data, BUBBLE_MAP_FILE)
    
    except Exception as e:
        print(f"Error: {e}")
        conn.rollback()
    
    finally:
        cur.close()
        conn.close()

if __name__ == "__main__":
    main()