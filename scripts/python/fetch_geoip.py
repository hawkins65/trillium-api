import json
import logging
import requests
import sys
import re
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
import time
import os
from datetime import datetime
import psycopg2
from psycopg2.extras import execute_batch
from db_config import db_params

# API base URL
API_URL = "http://ip-api.com/batch"
MAX_BATCH_SIZE = 100
REQUESTS_PER_MINUTE = 15

# Setup logging (to file only)
def setup_logging():
    logger = logging.getLogger()
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
    logger.addHandler(fh)
    return logger

logger = setup_logging()

def get_db_connection():
    try:
        conn = psycopg2.connect(**db_params)
        logger.debug(f"Created new DB connection: {conn}")
        return conn
    except Exception as e:
        logger.error(f"Failed to connect to database: {e}")
        raise

def is_valid_public_ip(ip):
    """Validate if the IP is a public IPv4 address."""
    try:
        if not re.match(r'^(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)$', ip):
            return False
        parts = ip.split('.')
        if parts[0] == '10':
            return False
        if parts[0] == '172' and 16 <= int(parts[1]) <= 31:
            return False
        if parts[0] == '192' and parts[1] == '168':
            return False
        return True
    except Exception:
        return False

def fetch_ip_batch(ips, session):
    payload = json.dumps([{"query": ip, "lang": "en"} for ip in ips])
    logger.debug(f"Fetching batch with payload: {payload}")
    try:
        response = session.post(API_URL, data=payload, headers={"Content-Type": "application/json"}, timeout=15)
        remaining_requests = int(response.headers.get('X-Rl', 15))
        reset_time = int(response.headers.get('X-Ttl', 60))
        logger.info(f"Rate limit info - Remaining: {remaining_requests}, Reset in: {reset_time}s")
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
        return [], 15, 60
    except json.JSONDecodeError as e:
        logger.error(f"JSON decode error: {e} - Response: {response.text}")
        return [], 15, 60

def process_ip_data(ip_data):
    query_ip = ip_data.get("query", "")
    if not isinstance(ip_data, dict):
        logger.error(f"Invalid ip_data type for IP {query_ip}: {type(ip_data)} - {ip_data}")
        return {"ip": query_ip, "city": "", "country": ""}
    if ip_data.get("status") != "success":
        logger.warning(f"Failed lookup for IP {query_ip}: {ip_data.get('message', 'No message')}")
        return {"ip": query_ip, "city": "", "country": ""}
    result = {
        "ip": query_ip,
        "city": ip_data.get("city", ""),
        "country": ip_data.get("country", "")
    }
    logger.debug(f"Processed IP {query_ip}: city={result['city']}, country={result['country']}")
    return result

def fetch_validator_info(pubkeys, conn):
    """Fetch name and website from validator_info table for given identity_pubkeys."""
    try:
        with conn.cursor() as cur:
            query = """
                SELECT identity_pubkey, COALESCE(name, ''), COALESCE(website, '')
                FROM validator_info
                WHERE identity_pubkey = ANY(%s)
            """
            cur.execute(query, (pubkeys,))
            results = {row[0]: {"name": row[1], "website": row[2]} for row in cur.fetchall()}
            logger.info(f"Fetched validator info for {len(results)} pubkeys")
            return results
    except Exception as e:
        logger.error(f"Failed to fetch validator info: {e}")
        return {}

def main():
    if len(sys.argv) != 2:
        logger.error("Usage: python3 fetch_geoip.py <input_json_file>")
        sys.exit(1)
    
    input_file = sys.argv[1]
    try:
        with open(input_file, 'r') as f:
            data = json.load(f)
            records = data.get("records", [])
            if not records:
                logger.info("No records to process")
                print(json.dumps([]))
                return
            ips = [r["ip"] for r in records]
            pubkeys = [r["pubkey"] for r in records]
    except Exception as e:
        logger.error(f"Failed to read input file {input_file}: {e}")
        sys.exit(1)

    # Filter valid public IPs
    valid_ips = [ip for ip in ips if is_valid_public_ip(ip)]
    invalid_ips = set(ips) - set(valid_ips)
    for ip in invalid_ips:
        logger.warning(f"Skipping invalid or private IP: {ip}")

    # Fetch validator info
    conn = get_db_connection()
    try:
        validator_info = fetch_validator_info(pubkeys, conn)
    finally:
        conn.close()
        logger.info("Database connection closed")

    # Fetch geolocation data
    session = requests.Session()
    retry_strategy = Retry(total=5, backoff_factor=2, status_forcelist=[429, 500, 502, 503, 504])
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("http://", adapter)

    results = []
    batches = [valid_ips[i:i + MAX_BATCH_SIZE] for i in range(0, len(valid_ips), MAX_BATCH_SIZE)]
    request_times = []

    for batch in batches:
        if len(request_times) >= REQUESTS_PER_MINUTE:
            oldest = request_times.pop(0)
            sleep_time = max(0, 60 - (time.time() - oldest))
            if sleep_time > 0:
                logger.info(f"Rate limit reached, sleeping for {sleep_time:.2f} seconds")
                time.sleep(sleep_time)
        
        batch_data, remaining_requests, reset_time = fetch_ip_batch(batch, session)
        for ip_data in batch_data:
            processed = process_ip_data(ip_data)
            if processed:
                results.append(processed)
        
        if remaining_requests == 0:
            logger.info(f"No requests remaining, sleeping for {reset_time} seconds")
            time.sleep(max(reset_time, 60))
        else:
            time.sleep(4.0)
        
        request_times.append(time.time())

    # Combine results with validator info
    ip_results = {r["ip"]: r for r in results}
    final_results = []
    for ip, pubkey in zip(ips, pubkeys):
        result = {
            "ip": ip,
            "pubkey": pubkey,
            "city": ip_results.get(ip, {"city": ""})["city"],
            "country": ip_results.get(ip, {"country": ""})["country"],
            "name": validator_info.get(pubkey, {"name": ""})["name"],
            "website": validator_info.get(pubkey, {"website": ""})["website"]
        }
        final_results.append(result)
        logger.debug(f"Combined result for IP {ip}, pubkey {pubkey}: {result}")

    # Include invalid or skipped IPs
    for ip in invalid_ips:
        idx = ips.index(ip)
        pubkey = pubkeys[idx]
        final_results.append({
            "ip": ip,
            "pubkey": pubkey,
            "city": "",
            "country": "",
            "name": validator_info.get(pubkey, {"name": ""})["name"],
            "website": validator_info.get(pubkey, {"website": ""})["website"]
        })

    print(json.dumps(final_results))
    logger.info(f"Processed {len(final_results)} records")

if __name__ == "__main__":
    main()