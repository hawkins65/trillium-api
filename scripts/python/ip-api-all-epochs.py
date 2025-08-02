import sys
import json
import logging
import requests
import psycopg2
from psycopg2.extras import execute_batch
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
import os
import time
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry

# PostgreSQL database connection parameters
from db_config import db_params

# API base URL
API_URL = "http://ip-api.com/batch"
MAX_BATCH_SIZE = 100
SLEEP_INTERVAL = 2.0
REQUESTS_PER_MINUTE = 45

# Mapping of ip-api.com fields to validator_stats database fields
FIELD_MAPPING = {
    "city": "city",
    "country": "country",
    "continent": "continent",
    "regionName": "region",
    "isp": "asn_org",
    "as": "asn",
    "lat": "lat",
    "lon": "lon"
}

def setup_logging():
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)
    now = datetime.now()
    formatted_time = now.strftime('%Y-%m-%d_%H-%M')
    script_name = os.path.basename(__file__).replace('.py', '')
    log_dir = '/home/smilax/log'
    os.makedirs(log_dir, exist_ok=True)
    filename = f'{log_dir}/{script_name}_log_{formatted_time}.log'
    fh = logging.FileHandler(filename)
    fh.setLevel(logging.DEBUG)
    fh_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    fh.setFormatter(fh_formatter)
    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.INFO)
    ch_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    ch.setFormatter(ch_formatter)
    logger.addHandler(fh)
    logger.addHandler(ch)
    return logger

logger = setup_logging()

def get_db_connection():
    conn = psycopg2.connect(**db_params)
    logger.debug(f"Created new DB connection: {conn}")
    return conn

def fetch_ip_batch(ips, session):
    payload = json.dumps(ips)
    headers = {"Content-Type": "application/json"}
    logger.debug(f"Fetching batch with payload: {payload}")
    try:
        response = session.post(API_URL, data=payload, headers=headers, timeout=15)
        response.raise_for_status()
        data = response.json()
        logger.debug(f"Raw response: {json.dumps(data, indent=2)}")
        if not isinstance(data, list):
            logger.error(f"Expected list, got {type(data)}: {data}")
            return []
        logger.info(f"Fetched data for {len(data)} IPs")
        return data
    except requests.RequestException as e:
        logger.error(f"Error fetching batch: {e}")
        return []
    except json.JSONDecodeError as e:
        logger.error(f"JSON decode error: {e} - Response: {response.text}")
        return []

def process_ip_data(ip_data):
    if not isinstance(ip_data, dict):
        logger.error(f"Invalid ip_data type: {type(ip_data)} - {ip_data}")
        return None
    if ip_data.get("status") != "success":
        logger.warning(f"Failed lookup for IP {ip_data.get('query')}: {ip_data.get('message', 'No message')}")
        return None
    asn = ip_data.get("as")
    if asn and isinstance(asn, str):
        if asn.startswith("AS"):
            try:
                asn = int(asn.split()[0].replace("AS", ""))
            except (ValueError, IndexError):
                asn = None
        elif asn == "":
            asn = None
    else:
        asn = None

    result = {db_field: ip_data.get(api_field) for api_field, db_field in FIELD_MAPPING.items() if api_field != "as"}
    result["asn"] = asn
    result["ip"] = ip_data.get("query")
    return result

def fetch_ips_with_city(conn):
    query = """
        SELECT DISTINCT ip, city, country, continent, region, asn_org, asn, lat, lon
        FROM validator_stats
        WHERE city IS NOT NULL AND city != '' AND ip IS NOT NULL AND ip != '[NULL]' AND ip != ''
    """
    with conn.cursor() as cur:
        cur.execute(query)
        return {row[0]: row[1:] for row in cur.fetchall()}

def fetch_ips_without_city(conn):
    query = """
        SELECT DISTINCT ip
        FROM validator_stats
        WHERE ip IS NOT NULL AND ip != '[NULL]' AND ip != ''
        AND (city IS NULL OR city = '' OR city = 'N/A' OR city = 'UNKNOWN')
    """
    with conn.cursor() as cur:
        cur.execute(query)
        return [row[0] for row in cur.fetchall()]

def update_ips_with_city(conn, ip_geo_data):
    update_query = """
        UPDATE validator_stats
        SET city = COALESCE(%(city)s, city),
            country = COALESCE(%(country)s, country),
            continent = COALESCE(%(continent)s, continent),
            region = COALESCE(%(region)s, region),
            asn_org = COALESCE(%(asn_org)s, asn_org),
            asn = COALESCE(%(asn)s, asn),
            lat = COALESCE(%(lat)s, lat),
            lon = COALESCE(%(lon)s, lon)
        WHERE ip = %(ip)s
    """
    with conn.cursor() as cur:
        for ip, geo_data in ip_geo_data.items():
            data = {
                "ip": ip,
                "city": geo_data[0],
                "country": geo_data[1],
                "continent": geo_data[2],
                "region": geo_data[3],
                "asn_org": geo_data[4],
                "asn": geo_data[5],
                "lat": geo_data[6],
                "lon": geo_data[7]
            }
            cur.execute(update_query, data)
        conn.commit()
        logger.info(f"Updated {len(ip_geo_data)} IPs with existing city data")

def update_ips_without_city(conn, session):
    ips_to_lookup = fetch_ips_without_city(conn)
    logger.info(f"Found {len(ips_to_lookup)} IPs without city data")
    
    if not ips_to_lookup:
        return

    batches = [ips_to_lookup[i:i + MAX_BATCH_SIZE] for i in range(0, len(ips_to_lookup), MAX_BATCH_SIZE)]
    results = []
    request_times = []
    
    with ThreadPoolExecutor(max_workers=1) as executor:
        for batch in batches:
            if len(request_times) >= REQUESTS_PER_MINUTE:
                oldest = request_times.pop(0)
                sleep_time = max(0, 60 - (time.time() - oldest))
                if sleep_time > 0:
                    logger.info(f"Rate limit reached, sleeping for {sleep_time:.2f} seconds")
                    time.sleep(sleep_time)
            
            future = executor.submit(fetch_ip_batch, batch, session)
            batch_data = future.result()
            for ip_data in batch_data:
                processed = process_ip_data(ip_data)
                if processed:
                    results.append(processed)
            request_times.append(time.time())
            time.sleep(SLEEP_INTERVAL)

    if results:
        update_query = """
            UPDATE validator_stats
            SET city = COALESCE(%(city)s, city),
                country = COALESCE(%(country)s, country),
                continent = COALESCE(%(continent)s, continent),
                region = COALESCE(%(region)s, region),
                asn_org = COALESCE(%(asn_org)s, asn_org),
                asn = COALESCE(%(asn)s, asn),
                lat = COALESCE(%(lat)s, lat),
                lon = COALESCE(%(lon)s, lon)
            WHERE ip = %(ip)s
        """
        with conn.cursor() as cur:
            execute_batch(cur, update_query, results)
            conn.commit()
            logger.info(f"Updated {len(results)} IPs with new GeoIP data")

def main():
    conn = get_db_connection()
    session = requests.Session()
    retry_strategy = Retry(total=3, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("http://", adapter)

    try:
        # Step 1: Update IPs with city in at least one epoch
        ip_geo_data = fetch_ips_with_city(conn)
        if ip_geo_data:
            update_ips_with_city(conn, ip_geo_data)
        
        # Step 2: Fetch and update IPs with no city in any epoch
        update_ips_without_city(conn, session)
    finally:
        conn.close()
        logger.info("Database connection closed")

if __name__ == "__main__":
    main()