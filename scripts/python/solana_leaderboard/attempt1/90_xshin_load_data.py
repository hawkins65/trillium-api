import json
import psycopg2
import os
import sys
from db_config import db_params
from logging_config import setup_logging

# Setup logging
script_name = os.path.basename(__file__).replace('.py', '')
logger = setup_logging(script_name)

# Get epoch from command line or prompt
if len(sys.argv) < 2:
    epoch = input("Enter epoch number: ")
else:
    epoch = sys.argv[1]

# Read validator JSON file
validator_filename = f"90_xshin_all_validators_{epoch}.json"
try:
    logger.info(f"Reading validator JSON file: {validator_filename}")
    with open(validator_filename, 'r') as f:
        validator_data = json.load(f)
except Exception as e:
    logger.error(f"Error reading validator JSON file: {e}")
    sys.exit(1)

# Read award winners JSON file
award_filename = f"90_xshin_all_award_winners_{epoch}.json"
try:
    logger.info(f"Reading award winners JSON file: {award_filename}")
    with open(award_filename, 'r') as f:
        award_data = json.load(f)
except Exception as e:
    logger.error(f"Error reading award winners JSON file: {e}")
    sys.exit(1)

# Create a map of awards by pubkey
award_map = {}
for award in award_data:
    category = award['category']
    for winner in award['winners']:
        pubkey = winner['pubkey']
        try:
            metric = float(winner['metric']) if winner.get('metric') is not None else None
            ranking = int(winner['ranking']) if winner.get('ranking') is not None else None
            if pubkey not in award_map:
                award_map[pubkey] = {}
            award_map[pubkey][category] = {'metric': metric, 'ranking': ranking}
        except (ValueError, TypeError) as e:
            logger.error(f"Error processing award for pubkey {pubkey}, category {category}: {e}")
            continue

# Connect to database
try:
    logger.info("Connecting to database")
    conn = psycopg2.connect(**db_params)
    cur = conn.cursor()
except Exception as e:
    logger.error(f"Error connecting to database: {e}")
    sys.exit(1)

# Insert or update data
try:
    logger.info(f"Inserting/updating {len(validator_data)} validator records into xshin_data for epoch {epoch}")
    for validator in validator_data:
        vote_account_pubkey = validator.get('pubkey')
        total_score = validator.get('validator', {}).get('details', {}).get('total_score')
        details = validator.get('validator', {}).get('details', {})
        pool_stake = validator.get('validator', {}).get('pool_stake', {})
        noneligibility_reasons = validator.get('validator', {}).get('noneligibility_reasons', [])

        # Safely access stake and pool stake fields
        try:
            stake = details.get('stake', {})
            stake_active = int(stake.get('active', 0)) if stake.get('active') is not None else 0
            stake_activating = int(stake.get('activating', 0)) if stake.get('activating') is not None else 0
            stake_deactivating = int(stake.get('deactivating', 0)) if stake.get('deactivating') is not None else 0
            target_pool_stake = int(details.get('target_pool_stake', 0)) if details.get('target_pool_stake') is not None else 0
            pool_stake_active = int(pool_stake.get('active', 0)) if pool_stake.get('active') is not None else 0
            pool_stake_activating = int(pool_stake.get('activating', 0)) if pool_stake.get('activating') is not None else 0
            pool_stake_deactivating = int(pool_stake.get('deactivating', 0)) if pool_stake.get('deactivating') is not None else 0
        except (ValueError, TypeError) as e:
            logger.error(f"Error processing stake data for pubkey {vote_account_pubkey}: {e}")
            continue

        # Safely access score fields
        raw_score = details.get('raw_score', {})
        normalized_score = details.get('normalized_score', {})

        # Get awards for this validator
        awards = award_map.get(vote_account_pubkey, {})

        # Prepare award columns
        award_columns = {
            'best_skip_rate': {'category': False, 'metric': None, 'ranking': None},
            'best_cu': {'category': False, 'metric': None, 'ranking': None},
            'best_latency': {'category': False, 'metric': None, 'ranking': None},
            'best_llv': {'category': False, 'metric': None, 'ranking': None},
            'best_cv': {'category': False, 'metric': None, 'ranking': None},
            'best_vote_inclusion': {'category': False, 'metric': None, 'ranking': None},
            'best_apy': {'category': False, 'metric': None, 'ranking': None},
            'best_pool_extra_lamports': {'category': False, 'metric': None, 'ranking': None},
            'best_city_concentration': {'category': False, 'metric': None, 'ranking': None},
            'best_country_concentration': {'category': False, 'metric': None, 'ranking': None},
            'best_overall': {'category': False, 'metric': None, 'ranking': None}
        }

        # Set award data
        for category in awards:
            if category in award_columns:
                award_columns[category] = {
                    'category': True,
                    'metric': awards[category]['metric'],
                    'ranking': awards[category]['ranking']
                }
            else:
                logger.error(f"Unknown award category {category} for pubkey {vote_account_pubkey}")

        # Prepare values tuple
        values = (
            epoch, vote_account_pubkey, total_score,
            details.get('name'), details.get('icon_url'), details.get('details'),
            details.get('website_url'), details.get('city'), details.get('country'),
            stake_active, stake_activating, stake_deactivating, target_pool_stake,
            raw_score.get('skip_rate'), raw_score.get('prior_skip_rate'), raw_score.get('subsequent_skip_rate'),
            raw_score.get('cu'), raw_score.get('latency'), raw_score.get('llv'), raw_score.get('cv'),
            raw_score.get('vote_inclusion'), raw_score.get('apy'), raw_score.get('pool_extra_lamports'),
            raw_score.get('city_concentration'), raw_score.get('country_concentration'),
            normalized_score.get('skip_rate'), normalized_score.get('prior_skip_rate'),
            normalized_score.get('subsequent_skip_rate'), normalized_score.get('cu'),
            normalized_score.get('latency'), normalized_score.get('llv'), normalized_score.get('cv'),
            normalized_score.get('vote_inclusion'), normalized_score.get('apy'),
            normalized_score.get('pool_extra_lamports'), normalized_score.get('city_concentration'),
            normalized_score.get('country_concentration'),
            pool_stake_active, pool_stake_activating, pool_stake_deactivating,
            json.dumps(noneligibility_reasons),
            award_columns['best_skip_rate']['category'], award_columns['best_skip_rate']['metric'], award_columns['best_skip_rate']['ranking'],
            award_columns['best_cu']['category'], award_columns['best_cu']['metric'], award_columns['best_cu']['ranking'],
            award_columns['best_latency']['category'], award_columns['best_latency']['metric'], award_columns['best_latency']['ranking'],
            award_columns['best_llv']['category'], award_columns['best_llv']['metric'], award_columns['best_llv']['ranking'],
            award_columns['best_cv']['category'], award_columns['best_cv']['metric'], award_columns['best_cv']['ranking'],
            award_columns['best_vote_inclusion']['category'], award_columns['best_vote_inclusion']['metric'], award_columns['best_vote_inclusion']['ranking'],
            award_columns['best_apy']['category'], award_columns['best_apy']['metric'], award_columns['best_apy']['ranking'],
            award_columns['best_pool_extra_lamports']['category'], award_columns['best_pool_extra_lamports']['metric'], award_columns['best_pool_extra_lamports']['ranking'],
            award_columns['best_city_concentration']['category'], award_columns['best_city_concentration']['metric'], award_columns['best_city_concentration']['ranking'],
            award_columns['best_country_concentration']['category'], award_columns['best_country_concentration']['metric'], award_columns['best_country_concentration']['ranking'],
            award_columns['best_overall']['category'], award_columns['best_overall']['metric'], award_columns['best_overall']['ranking']
        )

        # Debug: Log tuple length and pubkey
        #log_with_epoch(f"Values tuple length for pubkey {vote_account_pubkey}: {len(values)}", epoch)
        if len(values) != 74:
            logger.error(f"Invalid tuple length for pubkey {vote_account_pubkey}: expected 74, got {len(values)}")

        cur.execute("""
            INSERT INTO xshin_data (
                epoch, vote_account_pubkey, total_score, name, icon_url, details, website_url, city, country,
                stake_active, stake_activating, stake_deactivating, target_pool_stake,
                raw_score_skip_rate, raw_score_prior_skip_rate, raw_score_subsequent_skip_rate,
                raw_score_cu, raw_score_latency, raw_score_llv, raw_score_cv,
                raw_score_vote_inclusion, raw_score_apy, raw_score_pool_extra_lamports,
                raw_score_city_concentration, raw_score_country_concentration,
                normalized_score_skip_rate, normalized_score_prior_skip_rate,
                normalized_score_subsequent_skip_rate, normalized_score_cu,
                normalized_score_latency, normalized_score_llv, normalized_score_cv,
                normalized_score_vote_inclusion, normalized_score_apy,
                normalized_score_pool_extra_lamports, normalized_score_city_concentration,
                normalized_score_country_concentration,
                pool_stake_active, pool_stake_activating, pool_stake_deactivating,
                noneligibility_reasons,
                award_best_skip_rate_category, award_best_skip_rate_metric, award_best_skip_rate_ranking,
                award_best_cu_category, award_best_cu_metric, award_best_cu_ranking,
                award_best_latency_category, award_best_latency_metric, award_best_latency_ranking,
                award_best_llv_category, award_best_llv_metric, award_best_llv_ranking,
                award_best_cv_category, award_best_cv_metric, award_best_cv_ranking,
                award_best_vote_inclusion_category, award_best_vote_inclusion_metric, award_best_vote_inclusion_ranking,
                award_best_apy_category, award_best_apy_metric, award_best_apy_ranking,
                award_best_pool_extra_lamports_category, award_best_pool_extra_lamports_metric, award_best_pool_extra_lamports_ranking,
                award_best_city_concentration_category, award_best_city_concentration_metric, award_best_city_concentration_ranking,
                award_best_country_concentration_category, award_best_country_concentration_metric, award_best_country_concentration_ranking,
                award_best_overall_category, award_best_overall_metric, award_best_overall_ranking
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
            ON CONFLICT (epoch, vote_account_pubkey) DO UPDATE SET
                total_score = EXCLUDED.total_score,
                name = EXCLUDED.name,
                icon_url = EXCLUDED.icon_url,
                details = EXCLUDED.details,
                website_url = EXCLUDED.website_url,
                city = EXCLUDED.city,
                country = EXCLUDED.country,
                stake_active = EXCLUDED.stake_active,
                stake_activating = EXCLUDED.stake_activating,
                stake_deactivating = EXCLUDED.stake_deactivating,
                target_pool_stake = EXCLUDED.target_pool_stake,
                raw_score_skip_rate = EXCLUDED.raw_score_skip_rate,
                raw_score_prior_skip_rate = EXCLUDED.raw_score_prior_skip_rate,
                raw_score_subsequent_skip_rate = EXCLUDED.raw_score_subsequent_skip_rate,
                raw_score_cu = EXCLUDED.raw_score_cu,
                raw_score_latency = EXCLUDED.raw_score_latency,
                raw_score_llv = EXCLUDED.raw_score_llv,
                raw_score_cv = EXCLUDED.raw_score_cv,
                raw_score_vote_inclusion = EXCLUDED.raw_score_vote_inclusion,
                raw_score_apy = EXCLUDED.raw_score_apy,
                raw_score_pool_extra_lamports = EXCLUDED.raw_score_pool_extra_lamports,
                raw_score_city_concentration = EXCLUDED.raw_score_city_concentration,
                raw_score_country_concentration = EXCLUDED.raw_score_country_concentration,
                normalized_score_skip_rate = EXCLUDED.normalized_score_skip_rate,
                normalized_score_prior_skip_rate = EXCLUDED.normalized_score_prior_skip_rate,
                normalized_score_subsequent_skip_rate = EXCLUDED.normalized_score_subsequent_skip_rate,
                normalized_score_cu = EXCLUDED.normalized_score_cu,
                normalized_score_latency = EXCLUDED.normalized_score_latency,
                normalized_score_llv = EXCLUDED.normalized_score_llv,
                normalized_score_cv = EXCLUDED.normalized_score_cv,
                normalized_score_vote_inclusion = EXCLUDED.normalized_score_vote_inclusion,
                normalized_score_apy = EXCLUDED.normalized_score_apy,
                normalized_score_pool_extra_lamports = EXCLUDED.normalized_score_pool_extra_lamports,
                normalized_score_city_concentration = EXCLUDED.normalized_score_city_concentration,
                normalized_score_country_concentration = EXCLUDED.normalized_score_country_concentration,
                pool_stake_active = EXCLUDED.pool_stake_active,
                pool_stake_activating = EXCLUDED.pool_stake_activating,
                pool_stake_deactivating = EXCLUDED.pool_stake_deactivating,
                noneligibility_reasons = EXCLUDED.noneligibility_reasons,
                award_best_skip_rate_category = EXCLUDED.award_best_skip_rate_category,
                award_best_skip_rate_metric = EXCLUDED.award_best_skip_rate_metric,
                award_best_skip_rate_ranking = EXCLUDED.award_best_skip_rate_ranking,
                award_best_cu_category = EXCLUDED.award_best_cu_category,
                award_best_cu_metric = EXCLUDED.award_best_cu_metric,
                award_best_cu_ranking = EXCLUDED.award_best_cu_ranking,
                award_best_latency_category = EXCLUDED.award_best_latency_category,
                award_best_latency_metric = EXCLUDED.award_best_latency_metric,
                award_best_latency_ranking = EXCLUDED.award_best_latency_ranking,
                award_best_llv_category = EXCLUDED.award_best_llv_category,
                award_best_llv_metric = EXCLUDED.award_best_llv_metric,
                award_best_llv_ranking = EXCLUDED.award_best_llv_ranking,
                award_best_cv_category = EXCLUDED.award_best_cv_category,
                award_best_cv_metric = EXCLUDED.award_best_cv_metric,
                award_best_cv_ranking = EXCLUDED.award_best_cv_ranking,
                award_best_vote_inclusion_category = EXCLUDED.award_best_vote_inclusion_category,
                award_best_vote_inclusion_metric = EXCLUDED.award_best_vote_inclusion_metric,
                award_best_vote_inclusion_ranking = EXCLUDED.award_best_vote_inclusion_ranking,
                award_best_apy_category = EXCLUDED.award_best_apy_category,
                award_best_apy_metric = EXCLUDED.award_best_apy_metric,
                award_best_apy_ranking = EXCLUDED.award_best_apy_ranking,
                award_best_pool_extra_lamports_category = EXCLUDED.award_best_pool_extra_lamports_category,
                award_best_pool_extra_lamports_metric = EXCLUDED.award_best_pool_extra_lamports_metric,
                award_best_pool_extra_lamports_ranking = EXCLUDED.award_best_pool_extra_lamports_ranking,
                award_best_city_concentration_category = EXCLUDED.award_best_city_concentration_category,
                award_best_city_concentration_metric = EXCLUDED.award_best_city_concentration_metric,
                award_best_city_concentration_ranking = EXCLUDED.award_best_city_concentration_ranking,
                award_best_country_concentration_category = EXCLUDED.award_best_country_concentration_category,
                award_best_country_concentration_metric = EXCLUDED.award_best_country_concentration_metric,
                award_best_country_concentration_ranking = EXCLUDED.award_best_country_concentration_ranking,
                award_best_overall_category = EXCLUDED.award_best_overall_category,
                award_best_overall_metric = EXCLUDED.award_best_overall_metric,
                award_best_overall_ranking = EXCLUDED.award_best_overall_ranking
        """, values)

    # Commit the transaction
    conn.commit()
    logger.info(f"Successfully processed validator data for epoch {epoch}")

except Exception as e:
    logger.error(f"Error processing validator data: {e}")
    conn.rollback()
finally:
    if 'cur' in locals():
        cur.close()
    if 'conn' in locals():
        conn.close()
