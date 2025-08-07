import sys
import json
import importlib.util
import os

# Setup unified logging
script_dir = os.path.dirname(os.path.abspath(__file__))
logging_config_path = os.path.join(script_dir, "999_logging_config.py")
spec = importlib.util.spec_from_file_location("logging_config", logging_config_path)
logging_config = importlib.util.module_from_spec(spec)
spec.loader.exec_module(logging_config)
logger = logging_config.setup_logging(os.path.basename(__file__).replace('.py', ''))
import requests
import psycopg2
from psycopg2.extras import execute_batch
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
import time
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry

# PostgreSQL database connection parameters
from db_config import db_params

# API base URL
API_URL = "http://ip-api.com/batch"
MAX_BATCH_SIZE = 100
REQUESTS_PER_MINUTE = 15  # Adjusted to ip-api.com limit

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
    # Logger setup moved to unified configuration
    logger.setLevel(logging.INFO)
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
    payload = json.dumps([{"query": ip, "lang": "en"} for ip in ips])
    headers = {"Content-Type": "application/json"}
    logger.debug(f"Fetching batch with payload: {payload}")
    try:
        response = session.post(API_URL, data=payload, headers=headers, timeout=15)
        # Check rate limit headers
        remaining_requests = int(response.headers.get('X-Rl', 15))
        reset_time = int(response.headers.get('X-Ttl', 60))
        logger.debug(f"Rate limit info - Remaining: {remaining_requests}, Reset in: {reset_time}s")
        
        response.raise_for_status()
        data = response.json()
        logger.debug(f"Raw response: {json.dumps(data, indent=2)}")
        if not isinstance(data, list):
            logger.error(f"Expected list, got {type(data)}: {data}")
            return [], remaining_requests, reset_time
        logger.info(f"Fetched data for {len(data)} IPs")
        return data, remaining_requests, reset_time
    except requests.RequestException as e:
        logger.error(f"Error fetching batch: {e}")
        return [], 15, 60  # Default values on error
    except json.JSONDecodeError as e:
        logger.error(f"JSON decode error: {e} - Response: {response.text}")
        return [], 15, 60

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

def fetch_epoch_ips(epoch, conn):
    query = """
        SELECT DISTINCT ip, identity_pubkey
        FROM validator_stats
        WHERE epoch = %s AND ip IS NOT NULL AND ip != '[NULL]' AND ip != ''
    """
    with conn.cursor() as cur:
        cur.execute(query, (epoch,))
        ip_dict = {row[0]: row[1] for row in cur.fetchall()}
        logger.info(f"Total distinct IPs for epoch {epoch}: {len(ip_dict)}")
        return ip_dict

def fetch_previous_geographic_data(epoch, conn):
    query = """
        SELECT ip, city, country, continent, region, asn_org, asn, lat, lon
        FROM validator_stats
        WHERE epoch = %s AND ip IS NOT NULL AND ip != '[NULL]' AND ip != ''
    """
    with conn.cursor() as cur:
        cur.execute(query, (epoch - 1,))
        return {row[0]: row[1:] for row in cur.fetchall()}

def update_epoch_geographic_data(epoch, conn, previous_geographic_data=None, session=None):
    logger.info(f"Processing epoch {epoch}")
    current_ips = fetch_epoch_ips(epoch, conn)
    
    if not current_ips:
        logger.info(f"No IPs found for epoch {epoch}")
        return {}

    ips_to_lookup = set()
    results = {}
    if previous_geographic_data:
        reused_count = 0
        for ip, pubkey in current_ips.items():
            if ip in previous_geographic_data and any(previous_geographic_data[ip]):  # Reuse if any field is non-null
                reused_count += 1
                logger.debug(f"Reusing previous geographic data for IP {ip} in epoch {epoch}")
                results[pubkey] = {
                    "city": previous_geographic_data[ip][0],
                    "country": previous_geographic_data[ip][1],
                    "continent": previous_geographic_data[ip][2],
                    "region": previous_geographic_data[ip][3],
                    "asn_org": previous_geographic_data[ip][4],
                    "asn": previous_geographic_data[ip][5],
                    "lat": previous_geographic_data[ip][6],
                    "lon": previous_geographic_data[ip][7],
                    "ip": ip,
                    "identity_pubkey": pubkey,
                    "epoch": epoch
                }
            else:
                logger.debug(f"IP {ip} changed or not in previous epoch, adding to lookup")
                ips_to_lookup.add(ip)
        logger.info(f"Epoch {epoch}: Reused {reused_count} IPs from previous epoch")
    else:
        ips_to_lookup = set(current_ips.keys())

    logger.info(f"Epoch {epoch}: {len(ips_to_lookup)} IPs to look up")
    if ips_to_lookup:
        batches = [list(ips_to_lookup)[i:i + MAX_BATCH_SIZE] for i in range(0, len(ips_to_lookup), MAX_BATCH_SIZE)]
        request_times = []  # Track request timestamps
        with ThreadPoolExecutor(max_workers=1) as executor:
            for batch in batches:
                # Rate limit: ensure < 15 requests per minute
                if len(request_times) >= REQUESTS_PER_MINUTE:
                    oldest = request_times.pop(0)
                    sleep_time = max(0, 60 - (time.time() - oldest))
                    if sleep_time > 0:
                        logger.info(f"Rate limit reached, sleeping for {sleep_time:.2f} seconds")
                        time.sleep(sleep_time)
                
                future = executor.submit(fetch_ip_batch, batch, session)
                batch_data, remaining_requests, reset_time = future.result()
                for ip_data in batch_data:
                    processed = process_ip_data(ip_data)
                    if processed:
                        pubkey = current_ips[processed["ip"]]
                        processed["identity_pubkey"] = pubkey
                        processed["epoch"] = epoch
                        results[pubkey] = processed
                
                # Check rate limit headers
                if remaining_requests == 0:
                    logger.info(f"No requests remaining, sleeping for {reset_time} seconds")
                    time.sleep(reset_time)
                else:
                    time.sleep(4.0)  # 60s / 15 requests ≈ 4s per request
                
                request_times.append(time.time())

    if results:
        with conn.cursor() as cur:
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
                WHERE identity_pubkey = %(identity_pubkey)s AND epoch = %(epoch)s
            """
            execute_batch(cur, update_query, list(results.values()))
            conn.commit()
            logger.info(f"Updated {len(results)} rows for epoch {epoch}")
    else:
        logger.info(f"No geographic data to update for epoch {epoch}")

    return {ip: (results[pubkey]["city"], results[pubkey]["country"], results[pubkey]["continent"],
                 results[pubkey]["region"], results[pubkey]["asn_org"], results[pubkey]["asn"],
                 results[pubkey]["lat"], results[pubkey]["lon"])
            for ip, pubkey in current_ips.items() if pubkey in results}

def get_epoch_range(conn):
    with conn.cursor() as cur:
        cur.execute("SELECT MIN(epoch), MAX(epoch) FROM validator_stats")
        min_epoch, max_epoch = cur.fetchone()
        return min_epoch, max_epoch

def main():
    conn = get_db_connection()
    min_epoch, max_epoch = get_epoch_range(conn)

    session = requests.Session()
    retry_strategy = Retry(total=3, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("http://", adapter)

    if len(sys.argv) > 1:
        param = sys.argv[1].lower()
        if param == "all":
            epochs = range(min_epoch, max_epoch + 1)
        else:
            try:
                epoch = int(param)
                if min_epoch <= epoch <= max_epoch:
                    epochs = [epoch]
                else:
                    logger.error(f"Epoch {epoch} out of range ({min_epoch}-{max_epoch})")
                    conn.close()
                    return
            except ValueError:
                logger.error("Invalid parameter: provide an epoch number or 'all'")
                conn.close()
                return
    else:
        print(f"Epoch range in database: {min_epoch} to {max_epoch}")
        user_input = input("Enter an epoch number or 'all' for all epochs: ").strip().lower()
        if user_input == "all":
            epochs = range(min_epoch, max_epoch + 1)
        else:
            try:
                epoch = int(user_input)
                if min_epoch <= epoch <= max_epoch:
                    epochs = [epoch]
                else:
                    print(f"Epoch {epoch} out of range ({min_epoch}-{max_epoch})")
                    conn.close()
                    return
            except ValueError:
                print("Invalid input: please enter a number or 'all'")
                conn.close()
                return

    try:
        previous_geographic_data = None
        if len(epochs) == 1:
            target_epoch = epochs[0]
            if target_epoch > min_epoch:
                previous_geographic_data = fetch_previous_geographic_data(target_epoch, conn)
            update_epoch_geographic_data(target_epoch, conn, previous_geographic_data, session)
        else:
            for epoch in epochs:
                previous_geographic_data = update_epoch_geographic_data(epoch, conn, previous_geographic_data, session)
    finally:
        conn.close()
        logger.info("Database connection closed")

if __name__ == "__main__":
    main()