import psycopg2
from db_config import db_params
import uuid
import requests
from datetime import datetime
import pytz
import os
import sys
from logging_config import setup_logging

# Setup logging
script_name = os.path.basename(__file__).replace('.py', '')
logger = setup_logging(script_name)

def fetch_validator_data():
    """
    Fetch validator data from the Stakewiz API.
    Returns a list of dictionaries, each representing a validator record.
    """
    url = "https://api.stakewiz.com/validators"
    try:
        response = requests.get(url, timeout=60)
        response.raise_for_status()  # Raise an exception for HTTP errors
        validators = response.json()

        # Process each validator to handle missing updated_at
        for validator in validators:
            if "updated_at" not in validator or validator["updated_at"] is None:
                validator["updated_at"] = datetime.now(pytz.timezone("UTC")).strftime("%Y-%m-%d %H:%M:%S.%f%z")

        return validators
    except requests.RequestException as e:
        logger.error(f"Error fetching data from API: {e}")
        return []

def store_validators():
    """
    Fetch validator data from the API and store it in the stakewiz_validators table.
    The epoch is extracted from each validator record.
    """
    try:
        # Connect to the database
        conn = psycopg2.connect(**db_params)
        cur = conn.cursor()

        # Fetch validator data from the API
        validators = fetch_validator_data()

        if not validators:
            logger.info("No validator data to store.")
            return

        # Insert each validator record
        insert_query = """
        INSERT INTO stakewiz_validators (
            id, rank, identity, vote_identity, last_vote, root_slot, credits, epoch_credits,
            activated_stake, version, delinquent, skip_rate, updated_at, first_epoch_with_stake,
            name, keybase, description, website, commission, image, ip_latitude, ip_longitude,
            ip_city, ip_country, ip_asn, ip_org, mod, is_jito, jito_commission_bps, admin_comment,
            vote_success, vote_success_score, wiz_skip_rate, skip_rate_score, info_score,
            commission_score, first_epoch_distance, epoch_distance_score, stake_weight,
            above_halt_line, stake_weight_score, withdraw_authority_score, asn, asn_concentration,
            asn_concentration_score, tpu_ip, tpu_ip_concentration, tpu_ip_concentration_score,
            uptime, uptime_score, wiz_score, version_valid, city_concentration,
            city_concentration_score, invalid_version_score, superminority_penalty, score_version,
            no_voting_override, epoch, epoch_slot_height, asncity_concentration,
            asncity_concentration_score, skip_rate_ignored, stake_ratio, credit_ratio,
            apy_estimate, staking_apy, jito_apy, total_apy
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
        """

        inserted_count = 0
        for validator in validators:
            # Check if epoch is present and valid
            if "epoch" not in validator or validator["epoch"] is None:
                logger.warning(f"Skipping validator {validator.get('identity', 'unknown')} due to missing epoch")
                continue

            # Generate a unique ID for each record
            record_id = str(uuid.uuid4())

            # Prepare the data tuple for insertion, handling missing fields
            data = (
                record_id,
                validator.get("rank"),
                validator.get("identity"),
                validator.get("vote_identity"),
                validator.get("last_vote"),
                validator.get("root_slot"),
                validator.get("credits"),
                validator.get("epoch_credits"),
                validator.get("activated_stake"),
                validator.get("version"),
                validator.get("delinquent", False),
                validator.get("skip_rate"),
                validator.get("updated_at"),
                validator.get("first_epoch_with_stake"),
                validator.get("name"),
                validator.get("keybase"),
                validator.get("description"),
                validator.get("website"),
                validator.get("commission"),
                validator.get("image"),
                validator.get("ip_latitude"),
                validator.get("ip_longitude"),
                validator.get("ip_city"),
                validator.get("ip_country"),
                validator.get("ip_asn"),
                validator.get("ip_org"),
                validator.get("mod", False),
                validator.get("is_jito", False),
                validator.get("jito_commission_bps"),
                validator.get("admin_comment"),
                validator.get("vote_success"),
                validator.get("vote_success_score"),
                validator.get("wiz_skip_rate"),
                validator.get("skip_rate_score"),
                validator.get("info_score"),
                validator.get("commission_score"),
                validator.get("first_epoch_distance"),
                validator.get("epoch_distance_score"),
                validator.get("stake_weight"),
                validator.get("above_halt_line", False),
                validator.get("stake_weight_score"),
                validator.get("withdraw_authority_score"),
                validator.get("asn"),
                validator.get("asn_concentration"),
                validator.get("asn_concentration_score"),
                validator.get("tpu_ip"),
                validator.get("tpu_ip_concentration"),
                validator.get("tpu_ip_concentration_score"),
                validator.get("uptime"),
                validator.get("uptime_score"),
                validator.get("wiz_score"),
                validator.get("version_valid", True),
                validator.get("city_concentration"),
                validator.get("city_concentration_score"),
                validator.get("invalid_version_score"),
                validator.get("superminority_penalty"),
                validator.get("score_version"),
                validator.get("no_voting_override", False),
                validator.get("epoch"),
                validator.get("epoch_slot_height"),
                validator.get("asncity_concentration"),
                validator.get("asncity_concentration_score"),
                validator.get("skip_rate_ignored", False),
                validator.get("stake_ratio"),
                validator.get("credit_ratio"),
                validator.get("apy_estimate"),
                validator.get("staking_apy"),
                validator.get("jito_apy"),
                validator.get("total_apy")
            )

            # Verify the number of values in the data tuple
            if len(data) != 69:
                logger.error(f"Error: Data tuple has {len(data)} values, expected 69 for validator {validator.get('identity', 'unknown')}")
                logger.error(f"Validator data: {validator}")
                continue

            try:
                # Execute the insert query
                cur.execute(insert_query, data)
                inserted_count += 1
            except psycopg2.Error as e:
                logger.error(f"Database error for validator {validator.get('identity', 'unknown')}: {e}")
                logger.error(f"Validator data: {validator}")
                conn.rollback()
                continue

        # Commit the transaction
        conn.commit()
        logger.info(f"Successfully stored {inserted_count} validator records")

    except psycopg2.Error as e:
        logger.error(f"Database error: {e}")
        conn.rollback()
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        conn.rollback()
    finally:
        # Close the cursor and connection
        cur.close()
        conn.close()

if __name__ == "__main__":
    store_validators()