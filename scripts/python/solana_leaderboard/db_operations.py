import json
import os
import sys
from sqlalchemy import create_engine, text
from datetime import datetime

# Import from parent directory
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import importlib
logging_config = importlib.import_module('999_logging_config')
setup_logging = logging_config.setup_logging

# Import from current package
from .utils import format_lamports_to_sol, format_number, CLIENT_TYPE_MAP, add_trillium_attribution, decimal_default, format_elapsed_time, ensure_directory, get_output_path

# Initialize logger
logger = setup_logging('db_operations')

def get_validator_stats(epoch, engine, debug=False):
    query = """
    SELECT 
        vs.*,
        vsd.slot_duration_min,
        vsd.slot_duration_max,
        vsd.slot_duration_mean,
        vsd.slot_duration_median,
        vsd.slot_duration_stddev,
        vsd.slot_duration_p_value,
        vsd.slot_duration_confidence_interval_lower_ms,
        vsd.slot_duration_confidence_interval_upper_ms,
        vsd.slot_duration_is_lagging,
        vsd.slot_duration_coef,
        vsd.slot_duration_ci_lower_90_ms,
        vsd.slot_duration_ci_upper_90_ms,
        vsd.slot_duration_ci_lower_95_ms,
        vsd.slot_duration_ci_upper_95_ms,
        vi.name,
        vi.website,
        vi.details,
        vi.keybase_username,
        vi.icon_url,
        COALESCE(vi.logo, 'no-image-available12.webp') AS logo,
        vt.vote_account_pubkey,
        vt.vote_credits,
        vt.voted_slots,
        vt.avg_credit_per_voted_slot,
        vt.max_vote_latency,
        vt.mean_vote_latency,
        vt.median_vote_latency,
        vt.vote_credits_rank,
        COALESCE(vs.metro, vs.city) AS location,
        vs.client_type
    FROM validator_stats vs
    LEFT JOIN validator_info vi ON vs.identity_pubkey = vi.identity_pubkey
    LEFT JOIN votes_table vt ON vs.epoch = vt.epoch AND vs.vote_account_pubkey = vt.vote_account_pubkey
    LEFT JOIN validator_stats_slot_duration vsd ON vs.identity_pubkey = vsd.identity_pubkey AND vs.epoch = vsd.epoch
    WHERE vs.epoch = :epoch
        AND vs.activated_stake > 0;
    """
    with engine.connect() as conn:
        results = conn.execute(text(query), {"epoch": epoch}).fetchall()
        columns = conn.execute(text(query), {"epoch": epoch}).keys()

    data = []
    for row in results:
        record = dict(zip(columns, row))
        if debug and record['identity_pubkey'] == '5pPRHniefFjkiaArbGX3Y8NUysJmQ9tMZg3FrFGwHzSm':
            logger.info(f"Debug - Epoch: {record['epoch']}, Name: {record['name']}, "
                        f"Identity Pubkey: {record['identity_pubkey']}, Commission: {record['commission']}, "
                        f"Validator Inflation Reward: {record['validator_inflation_reward']}")

        lamport_fields = [
            ('total_block_rewards_before_burn', 5), ('total_block_rewards_after_burn', 5),
            ('validator_priority_fees', 5), ('validator_signature_fees', 5),
            ('validator_inflation_reward', 5), ('delegator_inflation_reward', 5),
            ('vote_cost', 5), ('mev_earned', 5), ('mev_to_validator', 5),
            ('mev_to_jito_block_engine', 5), ('mev_to_jito_tip_router', 5),
            ('mev_to_stakers', 5), ('avg_mev_per_block', 7), ('avg_priority_fees_per_block', 7),
            ('avg_rewards_per_block', 7), ('avg_signature_fees_per_block', 7),
            ('activated_stake', 0), ('rewards', 5), ('total_inflation_reward', 5)
        ]
        for field, precision in lamport_fields:
            if field in record and record[field] is not None:
                record[field] = format_lamports_to_sol(record[field], precision)

        number_fields = [
            ('avg_cu_per_block', 0), ('avg_tx_per_block', 0), ('avg_user_tx_per_block', 0),
            ('avg_vote_tx_per_block', 0), ('avg_votes_cast_per_block', 0),
            ('mean_vote_latency', 3), ('avg_credit_per_voted_slot', 5),
            ('avg_latency_per_voted_slot', 5), ('median_vote_latency', 5),
            ('delegator_inflation_apy', 5), ('delegator_compound_inflation_apy', 5),
            ('delegator_mev_apy', 5), ('delegator_compound_mev_apy', 5),
            ('delegator_total_apy', 5), ('delegator_compound_total_apy', 5),
            ('total_overall_apy', 5), ('compound_overall_apy', 5),
            ('validator_inflation_apy', 5), ('validator_mev_apy', 5),
            ('validator_block_rewards_apy', 5), ('validator_total_apy', 5),
            ('validator_compound_inflation_apy', 5), ('validator_compound_mev_apy', 5),
            ('validator_compound_block_rewards_apy', 5), ('validator_compound_total_apy', 5)
        ]
        for field, precision in number_fields:
            if field in record and record[field] is not None:
                record[field] = format_number(record[field], precision)

        # Format slot duration fields - nanoseconds to milliseconds
        duration_fields_nanoseconds = [
            'slot_duration_min', 'slot_duration_max', 'slot_duration_mean',
            'slot_duration_median', 'slot_duration_stddev'
        ]
        for field in duration_fields_nanoseconds:
            if field in record and record[field] is not None:
                # Convert from nanoseconds to milliseconds
                milliseconds = float(record[field]) / 1000000.0
                record[field] = f"{milliseconds:.5f}"

        # Format slot duration fields - already in milliseconds
        duration_fields_milliseconds = [
            'slot_duration_confidence_interval_lower_ms', 'slot_duration_confidence_interval_upper_ms',
            'slot_duration_ci_lower_90_ms', 'slot_duration_ci_upper_90_ms',
            'slot_duration_ci_lower_95_ms', 'slot_duration_ci_upper_95_ms'
        ]
        for field in duration_fields_milliseconds:
            if field in record and record[field] is not None:
                record[field] = f"{float(record[field]):.5f}"

        # Format p-value - extremely small number, use scientific notation or high precision
        if 'slot_duration_p_value' in record and record['slot_duration_p_value'] is not None:
            p_value = float(record['slot_duration_p_value'])
            if p_value < 0.0001:  # Use scientific notation for very small values
                record['slot_duration_p_value'] = f"{p_value:.2e}"
            else:
                record['slot_duration_p_value'] = f"{p_value:.7f}"
        else:
            record['slot_duration_p_value'] = "N/A"

        # Format coefficient with appropriate precision
        if 'slot_duration_coef' in record and record['slot_duration_coef'] is not None:
            record['slot_duration_coef'] = f"{float(record['slot_duration_coef']):.7f}"
        else:
            record['slot_duration_coef'] = "N/A"

        # Handle boolean field (slot_duration_is_lagging remains as boolean)

        if 'client_type' in record:
            record['client_type'] = CLIENT_TYPE_MAP.get(record['client_type'], 'Unknown')

        if epoch > 0:
            if (record['activated_stake'] is not None and 
                record['leader_slots'] is not None and 
                float(record['leader_slots']) > 0):
                try:
                    avg_stake_per_leader_slot = float(record['activated_stake']) / float(record['leader_slots'])
                    record['avg_stake_per_leader_slot'] = int(avg_stake_per_leader_slot)
                except (ValueError, TypeError):
                    record['avg_stake_per_leader_slot'] = None
            else:
                record['avg_stake_per_leader_slot'] = None
        else:
            record['avg_stake_per_leader_slot'] = None

        data.append(record)
    return data

def get_epoch_aggregate_data(epoch, engine):
    query = """
    SELECT * FROM epoch_aggregate_data WHERE epoch = :epoch
    """
    with engine.connect() as conn:
        result = conn.execute(text(query), {"epoch": epoch}).fetchone()
        if result is None:
            logger.warning(f"No data found for epoch {epoch} in epoch_aggregate_data table")
            return None
        columns = conn.execute(text(query), {"epoch": epoch}).keys()
        data = dict(zip(columns, result))

    if 'total_vote_cost' in data and 'total_active_validators' in data and data['total_vote_cost'] is not None and data['total_active_validators'] is not None and data['total_active_validators'] != 0:
        try:
            data['avg_vote_cost'] = data['total_vote_cost'] / data['total_active_validators']
        except (ValueError, TypeError) as e:
            logger.error(f"Error calculating avg_vote_cost for epoch {epoch}: {e}")
            data['avg_vote_cost'] = None
    else:
        data['avg_vote_cost'] = None

    lamport_fields = [
        ('avg_mev_per_block', 7), ('avg_mev_to_validator', 5), ('avg_mev_to_jito_block_engine', 5),
        ('avg_mev_to_jito_tip_router', 5), ('avg_priority_fees_per_block', 7),
        ('avg_rewards_per_block', 7), ('avg_signature_fees_per_block', 7),
        ('avg_vote_cost_per_block', 7), ('avg_vote_cost', 7), ('median_rewards_per_block', 7),
        ('median_mev_per_block', 7), ('total_mev_earned', 5), ('total_mev_to_validator', 5),
        ('total_mev_to_jito_block_engine', 5), ('total_mev_to_jito_tip_router', 5),
        ('total_mev_to_stakers', 5), ('total_validator_fees', 5), ('total_validator_priority_fees', 5),
        ('total_validator_signature_fees', 5), ('total_validator_inflation_rewards', 5),
        ('total_delegator_inflation_rewards', 5), ('total_block_rewards', 5),
        ('total_active_stake', 0), ('avg_active_stake', 0), ('median_active_stake', 0),
        ('total_vote_cost', 5)
    ]
    for field, precision in lamport_fields:
        if field in data and data[field] is not None:
            data[field] = format_lamports_to_sol(data[field], precision)

    number_fields = [
        ('avg_commission', 2), ('avg_credits', 0), ('median_credits', 0),
        ('avg_cu_per_block', 0), ('avg_mev_commission', 0), ('avg_tx_per_block', 0),
        ('avg_user_tx_per_block', 0), ('avg_vote_tx_per_block', 0),
        ('avg_votes_cast', 0), ('avg_stake_weighted_skip_rate', 2),
        ('avg_stake_weighted_leader_slots', 0), ('median_votes_cast', 0),
        ('total_blocks_produced', 0), ('total_credits', 0), ('total_cu', 0),
        ('total_signatures', 0), ('total_tx', 0), ('total_user_tx', 0),
        ('total_vote_tx', 0), ('total_votes_cast', 0), ('total_active_validators', 0),
        ('average_sol_per_4_slots', 0), ('median_sol_per_4_slots', 0),
        ('elapsed_time_per_epoch', 5), ('epochs_per_year', 2),
        ('avg_credit_per_voted_slot', 3), ('max_vote_latency', 3),
        ('mean_vote_latency', 3), ('median_vote_latency', 3),
        ('inflation_decay_rate', 2), ('inflation_rate', 2)
    ]
    for field, precision in number_fields:
        if field in data and data[field] is not None:
            data[field] = format_number(data[field], precision)

    if epoch > 0:
        if data['total_active_stake'] is not None:
            try:
                avg_stake_per_leader_slot = int(float(data['total_active_stake']) / 432000)
                data['avg_stake_per_leader_slot'] = avg_stake_per_leader_slot
            except (ValueError, TypeError) as e:
                logger.error(f"Error calculating avg_stake_per_leader_slot for epoch {epoch}: {e}")
                data['avg_stake_per_leader_slot'] = None
        else:
            data['avg_stake_per_leader_slot'] = None
    else:
        data['avg_stake_per_leader_slot'] = None

    data['min_slot'] = data.get('min_slot')
    data['max_slot'] = data.get('max_slot')
    data['epoch_start_slot'] = data.get('epoch_start_slot')
    data['epoch_end_slot'] = data.get('epoch_end_slot')

    if 'min_block_time' in data and data['min_block_time'] is not None:
        data['min_block_time_calendar'] = datetime.fromtimestamp(data['min_block_time']).isoformat()
    else:
        data['min_block_time_calendar'] = None

    if 'max_block_time' in data and data['max_block_time'] is not None:
        data['max_block_time_calendar'] = datetime.fromtimestamp(data['max_block_time']).isoformat()
    else:
        data['max_block_time_calendar'] = None

    elapsed_time_per_epoch = data.pop('elapsed_time_per_epoch', None)
    if elapsed_time_per_epoch is not None:
        data['elapsed_time_minutes'] = format_number(float(elapsed_time_per_epoch) / 60, 0)
        data['elapsed_time_DD_HH_MM'] = format_elapsed_time(elapsed_time_per_epoch)
    else:
        data['elapsed_time_minutes'] = None
        data['elapsed_time_DD_HH_MM'] = None

    return data

def write_validator_stats_to_json(epoch, data):
    filename = get_output_path(f"epoch{epoch}_validator_rewards.json", 'json')
    data_with_attribution = add_trillium_attribution(data)
    with open(filename, 'w') as f:
        json.dump(data_with_attribution, f, indent=4, default=decimal_default)
    logger.info(f"File created - {filename}")

def write_epoch_aggregate_data_to_json(epoch, data):
    filename = get_output_path(f"epoch{epoch}_epoch_aggregate_data.json", 'json')
    data_with_attribution = add_trillium_attribution(data)
    with open(filename, 'w') as f:
        json.dump(data_with_attribution, f, indent=4, default=decimal_default)
    logger.info(f"File created - {filename}")

def get_min_max_epochs(engine):
    query = "SELECT MIN(epoch), MAX(epoch) FROM validator_stats"
    with engine.connect() as conn:
        result = conn.execute(text(query)).fetchone()
    return result[0], result[1]