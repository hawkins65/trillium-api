import requests
import psycopg2
from psycopg2.extras import execute_batch
import logging
from db_config import db_params  # Import your db_params
import subprocess

PSQL_CMD = '/usr/bin/psql'

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def get_db_connection():
    conn = psycopg2.connect(**db_params)
    logger.debug(f"Created new DB connection: {conn}")
    return conn

# PostgreSQL database connection parameters
from db_config import db_params

def get_db_connection_string(db_params):
    return f"postgresql://{db_params['user']}@{db_params['host']}:{db_params['port']}/{db_params['database']}?sslmode={db_params['sslmode']}"

# Replace the hardcoded connection string with this function call
db_connection_string = get_db_connection_string(db_params)

def fetch_stakewiz_validators():
    url = "https://api.stakewiz.com/validators"
    try:
        logger.info("Fetching validators from Stakewiz API...")
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        validators = response.json()
        logger.info(f"Fetched {len(validators)} validators from Stakewiz API.")
        return validators
    except requests.RequestException as e:
        logger.error(f"Error fetching Stakewiz validators: {str(e)}")
        return []

def geoip_clean_asn(asn):
    """Clean ASN string (e.g., 'AS18450') to an integer (18450)."""
    if asn and isinstance(asn, str) and asn.startswith("AS"):
        try:
            return int(asn[2:])
        except ValueError:
            logger.error(f"Invalid ASN format: {asn}")
            return None
    elif isinstance(asn, int):
        return asn
    return None

def fetch_missing_ips_from_db():
    query = """
    SELECT identity_pubkey, ip 
    FROM validator_stats 
    WHERE city = 'N/A' OR city IS NULL
    """
    missing_ips = []
    
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(query)
            missing_ips = [(row[0], row[1]) for row in cur.fetchall()]
            logger.info(f"Fetched {len(missing_ips)} missing IPs from validator_stats (all epochs).")
            return missing_ips
    except Exception as e:
        logger.error(f"Error fetching missing IPs from database: {str(e)}")
        return []
    finally:
        conn.close()

def update_validator_stats(missing_ips, validators):
    # Create lookup dictionaries from validators JSON
    validator_dict_by_identity = {v['identity']: v for v in validators if 'identity' in v}
    validator_dict_by_tpu_ip = {v['tpu_ip']: v for v in validators if 'tpu_ip' in v}
    
    # Prepare update data for both identity and tpu_ip matches
    update_data_identity = []  # For updates based on identity_pubkey
    update_data_tpu_ip = []    # For updates based on tpu_ip
    
    conn = get_db_connection()
    try:
        # Match missing IPs with validator data (identity-based updates)
        for identity_pubkey, ip in missing_ips:
            validator = validator_dict_by_identity.get(identity_pubkey)
            if validator:
                city = validator.get('ip_city')
                country = validator.get('ip_country')
                asn = geoip_clean_asn(validator.get('ip_asn'))  # Convert ASN to integer
                asn_org = validator.get('ip_org')
                
                if city:  # Only update if city is present
                    update_data_identity.append((
                        city, country, asn, asn_org, identity_pubkey
                    ))
                    logger.info(f"Found identity-based update for {identity_pubkey}: city={city}, country={country}, asn={asn}, asn_org={asn_org}")
                else:
                    logger.warning(f"No city found for {identity_pubkey} in Stakewiz data (identity-based).")
            else:
                logger.warning(f"No matching validator found for {identity_pubkey} in Stakewiz data (identity-based).")
        
        # Match missing IPs with validator data (tpu_ip-based updates)
        for identity_pubkey, ip in missing_ips:
            validator = validator_dict_by_tpu_ip.get(ip)
            if validator:
                city = validator.get('ip_city')
                country = validator.get('ip_country')
                asn = geoip_clean_asn(validator.get('ip_asn'))  # Convert ASN to integer
                asn_org = validator.get('ip_org')
                
                if city:  # Only update if city is present
                    update_data_tpu_ip.append((
                        city, country, asn, asn_org, identity_pubkey
                    ))
                    logger.info(f"Found tpu_ip-based update for {identity_pubkey} (tpu_ip={ip}): city={city}, country={country}, asn={asn}, asn_org={asn_org}")
                else:
                    logger.warning(f"No city found for {identity_pubkey} (tpu_ip={ip}) in Stakewiz data (tpu_ip-based).")
            else:
                logger.warning(f"No matching validator found for {identity_pubkey} (tpu_ip={ip}) in Stakewiz data (tpu_ip-based).")
        
        # Perform batch update if there are updates (identity-based)
        if update_data_identity:
            with conn.cursor() as cur:
                execute_batch(cur, """
                    UPDATE validator_stats
                    SET city = COALESCE(%s, city),
                        country = COALESCE(%s, country),
                        asn = COALESCE(%s, asn),
                        asn_org = COALESCE(%s, asn_org)
                    WHERE identity_pubkey = %s AND (city = 'N/A' OR city IS NULL)
                """, update_data_identity)
                conn.commit()
                logger.info(f"Updated {len(update_data_identity)} rows in validator_stats (all epochs, identity-based)")
        else:
            logger.info("No identity-based updates to apply to validator_stats.")
        
        # Perform batch update if there are updates (tpu_ip-based)
        if update_data_tpu_ip:
            with conn.cursor() as cur:
                execute_batch(cur, """
                    UPDATE validator_stats
                    SET city = COALESCE(%s, city),
                        country = COALESCE(%s, country),
                        asn = COALESCE(%s, asn),
                        asn_org = COALESCE(%s, asn_org)
                    WHERE identity_pubkey = %s AND (city = 'N/A' OR city IS NULL)
                """, update_data_tpu_ip)
                conn.commit()
                logger.info(f"Updated {len(update_data_tpu_ip)} rows in validator_stats (all epochs, tpu_ip-based)")
        else:
            logger.info("No tpu_ip-based updates to apply to validator_stats.")
    
    except Exception as e:
        logger.error(f"Error updating validator_stats: {str(e)}")
    finally:
        conn.close()

def main():
    # Fetch Stakewiz validators
    validators = fetch_stakewiz_validators()
    if not validators:
        logger.error("No validators fetched. Exiting.")
        return
    
    # Fetch missing IPs directly from the database
    missing_ips = fetch_missing_ips_from_db()
    if not missing_ips:
        logger.error("No missing IPs found in database. Exiting.")
        return
    
    # Update validator_stats
    update_validator_stats(missing_ips, validators)

    subprocess.run([PSQL_CMD, db_connection_string, '-f', '92_set-country.sql'], check=True)
    subprocess.run([PSQL_CMD, db_connection_string, '-f', '92_set-continent-from-unknown.sql'], check=True)

if __name__ == '__main__':
    main()
