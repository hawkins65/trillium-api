import json
import os
import sys
from decimal import Decimal
from sqlalchemy import text

# Import from parent directory
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import importlib
logging_config = importlib.import_module('999_logging_config')
setup_logging = logging_config.setup_logging

# Import from current package
from .utils import format_lamports_to_sol, format_number, add_trillium_attribution, decimal_default, format_elapsed_time, ensure_directory, get_output_path
from .db_operations import get_epoch_aggregate_data

# Initialize logger
logger = setup_logging('epoch_aggregation')

def generate_last_ten_epochs_data(max_epoch, engine):
    last_ten_epochs = range(max_epoch - 9, max_epoch + 1)
    last_ten_epochs_data = []
    for epoch in last_ten_epochs:
        epoch_aggregate_data = get_epoch_aggregate_data(epoch, engine)
        if epoch_aggregate_data is not None:
            if epoch > 0 and epoch_aggregate_data['total_active_stake'] is not None:
                try:
                    avg_stake_per_leader_slot = int(float(epoch_aggregate_data['total_active_stake']) / 432000)
                    epoch_aggregate_data['avg_stake_per_leader_slot'] = avg_stake_per_leader_slot
                except (ValueError, TypeError) as e:
                    logger.error(f"Error calculating avg_stake_per_leader_slot for epoch {epoch}: {e}")
                    epoch_aggregate_data['avg_stake_per_leader_slot'] = None
            else:
                epoch_aggregate_data['avg_stake_per_leader_slot'] = None
            epoch_aggregate_data['_epoch'] = epoch
            last_ten_epochs_data.append(epoch_aggregate_data)

    last_ten_epochs_data.sort(key=lambda x: x['epoch'], reverse=True)
    last_ten_epochs_data = add_trillium_attribution(last_ten_epochs_data)
    filename = get_output_path("last_ten_epoch_aggregate_data.json", 'json')
    with open(filename, 'w') as f:
        json.dump(last_ten_epochs_data, f, indent=4, default=decimal_default)
    logger.info(f"File created - {filename}")

def generate_ten_epoch_validator_rewards(max_epoch, engine):
    last_ten_epochs = range(max_epoch - 9, max_epoch + 1)
    last_thirty_epochs = range(max_epoch - 29, max_epoch + 1)

    query = """
    WITH latest_validator_info AS (
        SELECT DISTINCT ON (identity_pubkey)
            identity_pubkey,
            details,
            icon_url,
            keybase_username,
            COALESCE(logo, 'no-image-available12.webp') AS logo,
            name,
            website
        FROM validator_info
        ORDER BY identity_pubkey
    ),
    thirty_epoch_data AS (
        SELECT 
            identity_pubkey,
            MAX(commission) as max_commission_30_epochs,
            AVG(epoch_credits) as average_epoch_credits_30_epochs
        FROM validator_stats
        WHERE epoch BETWEEN :thirty_start AND :thirty_end
        GROUP BY identity_pubkey
    ),
    epoch_aggregate_30_epochs AS (
        SELECT AVG(total_blocks_produced) as average_total_blocks_produced_30_epochs
        FROM epoch_aggregate_data
        WHERE epoch BETWEEN :thirty_start AND :thirty_end
    )
    SELECT 
        vs.identity_pubkey,
        AVG(vs.activated_stake) as activated_stake,
        SUM(vs.cu) / NULLIF(SUM(vs.blocks_produced), 0) as avg_cu_per_block,
        SUM(vs.mev_earned) / NULLIF(SUM(vs.blocks_produced), 0) as avg_mev_per_block,
        SUM(vs.validator_priority_fees) / NULLIF(SUM(vs.blocks_produced), 0) as avg_priority_fees_per_block,
        SUM(vs.rewards) / NULLIF(SUM(vs.blocks_produced), 0) as avg_rewards_per_block,
        SUM(vs.validator_signature_fees) / NULLIF(SUM(vs.blocks_produced), 0) as avg_signature_fees_per_block,
        SUM(vs.tx_included_in_blocks) / NULLIF(SUM(vs.blocks_produced), 0) as avg_tx_per_block,
        SUM(vs.user_tx_included_in_blocks) / NULLIF(SUM(vs.blocks_produced), 0) as avg_user_tx_per_block,
        SUM(vs.vote_tx_included_in_blocks) / NULLIF(SUM(vs.blocks_produced), 0) as avg_vote_tx_per_block,
        AVG(vs.blocks_produced) as average_blocks_produced,
        AVG(vs.commission) as commission,
        AVG(vs.cu) as cu,
        AVG(vs.epoch_credits) as epoch_credits,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY vs.epoch_credits) as median_credits,
        AVG(vs.mev_commission) as mev_commission,
        AVG(vs.mev_earned) as mev_earned,
        AVG(vs.mev_to_validator) as mev_to_validator,
        AVG(vs.mev_to_jito_block_engine) as mev_to_jito_block_engine,
        AVG(vs.mev_to_jito_tip_router) as mev_to_jito_tip_router,
        AVG(vs.rewards) as rewards,
        AVG(vs.signatures) as signatures,
        AVG(vs.skip_rate) AS avg_skip_rate,
        AVG(vs.stake_percentage) as stake_percentage,
        AVG(vs.total_block_rewards_before_burn) as total_block_rewards_before_burn,
        AVG(vs.tx_included_in_blocks) as tx_included_in_blocks,
        AVG(vs.user_tx_included_in_blocks) as user_tx_included_in_blocks,
        AVG(vs.total_block_rewards_after_burn) as total_block_rewards_after_burn,
        AVG(vs.validator_priority_fees) as validator_priority_fees,
        AVG(vs.validator_signature_fees) as validator_signature_fees,
        AVG(vs.validator_inflation_reward) as validator_inflation_reward,
        AVG(vs.delegator_inflation_reward) as delegator_inflation_reward,
        AVG(vs.vote_cost) as vote_cost,
        AVG(vs.vote_tx_included_in_blocks) as vote_tx_included_in_blocks,
        AVG(vs.votes_cast) as votes_cast,
        AVG(vs.leader_slots) as leader_slots,
        AVG(vs.jito_rank) as jito_rank,
        MIN(vs.epoch) as min_epoch,
        MAX(vs.epoch) as max_epoch,
        MAX(vs.vote_account_pubkey) as vote_account_pubkey,
        MAX(vs.ip) as ip,
        MAX(vs.client_type) as client_type,
        MAX(vs.version) as version,
        MAX(vs.asn) as asn,
        MAX(vs.asn_org) as asn_org,
        MAX(vs.city) as city,
        MAX(vs.continent) as continent,
        MAX(vs.country) as country,
        MAX(vs.region) as region,
        MAX(vs.superminority) as superminority,
        COALESCE(MAX(vi.details), ' ') as details,
        COALESCE(MAX(vi.icon_url), ' ') as icon_url,
        COALESCE(MAX(vi.keybase_username), ' ') as keybase_username,
        COALESCE(MAX(vi.logo), 'no-image-available12.webp') AS logo,
        COALESCE(MAX(vi.name), ' ') as name,
        COALESCE(MAX(vi.website), ' ') as website,
        ted.max_commission_30_epochs,
        ted.average_epoch_credits_30_epochs,
        ead.average_total_blocks_produced_30_epochs,
        AVG(vt.vote_credits) as vote_credits,
        AVG(vt.voted_slots) as voted_slots,
        AVG(vt.avg_credit_per_voted_slot) as avg_credit_per_voted_slot,
        AVG(vt.max_vote_latency) as max_vote_latency,
        AVG(vt.mean_vote_latency) as mean_vote_latency,
        AVG(vt.median_vote_latency) as median_vote_latency,
        AVG(vt.vote_credits_rank) as vote_credits_rank
    FROM validator_stats vs
    LEFT JOIN latest_validator_info vi ON vs.identity_pubkey = vi.identity_pubkey
    LEFT JOIN thirty_epoch_data ted ON vs.identity_pubkey = ted.identity_pubkey
    CROSS JOIN epoch_aggregate_30_epochs ead
    LEFT JOIN votes_table vt ON vs.epoch = vt.epoch AND vs.vote_account_pubkey = vt.vote_account_pubkey
    WHERE vs.epoch BETWEEN :ten_start AND :ten_end
        AND vs.activated_stake != 0
    GROUP BY vs.identity_pubkey, ted.max_commission_30_epochs, ted.average_epoch_credits_30_epochs, ead.average_total_blocks_produced_30_epochs;
    """
    with engine.connect() as conn:
        results = conn.execute(
            text(query),
            {"thirty_start": min(last_thirty_epochs), "thirty_end": max(last_thirty_epochs),
             "ten_start": min(last_ten_epochs), "ten_end": max(last_ten_epochs)}
        ).fetchall()
        columns = conn.execute(
            text(query),
            {"thirty_start": min(last_thirty_epochs), "thirty_end": max(last_thirty_epochs),
             "ten_start": min(last_ten_epochs), "ten_end": max(last_ten_epochs)}
        ).keys()

    data = []
    for row in results:
        record = dict(zip(columns, row))
        if record['identity_pubkey'] == '5pPRHniefFjkiaArbGX3Y8NUysJmQ9tMZg3FrFGwHzSm':
            logger.info(f"Debug - Name: {record['name']}, Identity Pubkey: {record['identity_pubkey']}, "
                        f"Commission: {record['commission']}, Validator Inflation Reward: {record['validator_inflation_reward']}")

        fields_to_prefix = [
            'activated_stake', 'blocks_produced', 'commission', 'cu', 'epoch_credits',
            'leader_slots', 'mev_commission', 'mev_earned', 'mev_to_validator',
            'mev_to_jito_block_engine', 'mev_to_jito_tip_router', 'rewards',
            'signatures', 'stake_percentage', 'total_block_rewards_after_burn',
            'total_block_rewards_before_burn', 'tx_included_in_blocks',
            'user_tx_included_in_blocks', 'validator_priority_fees',
            'validator_signature_fees', 'validator_inflation_reward',
            'delegator_inflation_reward', 'vote_cost', 'vote_tx_included_in_blocks',
            'votes_cast', 'vote_credits', 'voted_slots', 'max_vote_latency',
            'mean_vote_latency', 'median_vote_latency', 'vote_credits_rank'
        ]
        for field in fields_to_prefix:
            if field in record:
                record[f"average_{field}"] = record.pop(field)

        lamport_fields = [
            ('average_activated_stake', 0), ('avg_mev_per_block', 7),
            ('avg_priority_fees_per_block', 7), ('avg_rewards_per_block', 7),
            ('avg_signature_fees_per_block', 7), ('average_mev_earned', 5),
            ('average_mev_to_validator', 5), ('average_mev_to_jito_block_engine', 5),
            ('average_mev_to_jito_tip_router', 5), ('average_rewards', 5),
            ('average_total_block_rewards_before_burn', 5),
            ('average_total_block_rewards_after_burn', 5),
            ('average_validator_priority_fees', 5), ('average_validator_signature_fees', 5),
            ('average_validator_inflation_reward', 5), ('average_delegator_inflation_reward', 5),
            ('average_vote_cost', 5)
        ]
        for field, precision in lamport_fields:
            if field in record and record[field] is not None:
                record[field] = format_lamports_to_sol(record[field], precision)

        number_fields = [
            ('average_blocks_produced', 0), ('average_commission', 0),
            ('average_cu', 0), ('average_epoch_credits', 0), ('median_credits', 0),
            ('average_leader_slots', 0), ('average_mev_commission', 0),
            ('average_signatures', 0), ('average_stake_percentage', 5),
            ('average_tx_included_in_blocks', 0), ('average_user_tx_included_in_blocks', 0),
            ('average_vote_tx_included_in_blocks', 0), ('average_jito_rank', 0),
            ('average_votes_cast', 0), ('avg_cu_per_block', 0), ('avg_skip_rate', 2),
            ('avg_tx_per_block', 2), ('avg_user_tx_per_block', 2),
            ('avg_vote_tx_per_block', 2), ('average_vote_credits', 0),
            ('average_voted_slots', 0), ('avg_credit_per_voted_slot', 3),
            ('average_max_vote_latency', 3), ('average_mean_vote_latency', 3),
            ('average_median_vote_latency', 3), ('average_vote_credits_rank', 0)
        ]
        for field, precision in number_fields:
            if field in record and record[field] is not None:
                record[field] = format_number(record[field], precision)

        if max_epoch > 0:
            if (record['average_activated_stake'] is not None and 
                record['average_leader_slots'] is not None and 
                float(record['average_leader_slots']) > 0):
                try:
                    avg_stake_per_leader_slot = float(record['average_activated_stake']) / float(record['average_leader_slots'])
                    record['avg_stake_per_leader_slot'] = int(avg_stake_per_leader_slot)
                except (ValueError, TypeError):
                    record['avg_stake_per_leader_slot'] = None
            else:
                record['avg_stake_per_leader_slot'] = None
        else:
            record['avg_stake_per_leader_slot'] = None

        record['epoch_range'] = f"{record['min_epoch']}-{record['max_epoch']}"
        del record['min_epoch']
        del record['max_epoch']

        record['max_commission_30_epochs'] = format_number(record['max_commission_30_epochs'], 2)
        record['average_epoch_credits_30_epochs'] = format_number(record['average_epoch_credits_30_epochs'], 0)
        record['average_total_blocks_produced_30_epochs'] = format_number(record['average_total_blocks_produced_30_epochs'], 0)

        if (record['average_epoch_credits_30_epochs'] is not None and 
            record['average_total_blocks_produced_30_epochs'] is not None and 
            float(record['average_total_blocks_produced_30_epochs']) > 0):
            vote_credits_ratio = float(record['average_epoch_credits_30_epochs']) / float(record['average_total_blocks_produced_30_epochs'])
            record['vote_credits_ratio'] = format_number(vote_credits_ratio, 5)
        else:
            record['vote_credits_ratio'] = None

        data.append(record)

    data = add_trillium_attribution(data)
    filename = get_output_path('ten_epoch_validator_rewards.json', 'json')
    with open(filename, 'w') as f:
        json.dump(data, f, indent=4, default=decimal_default)
    logger.info(f"File created - {filename}")

def generate_ten_epoch_aggregate_data(max_epoch, engine):
    last_ten_epochs = range(max_epoch - 9, max_epoch + 1)
    last_thirty_epochs = range(max_epoch - 29, max_epoch + 1)

    query = """
    SELECT 
        AVG(avg_commission) as avg_commission,
        AVG(avg_credits) as avg_credits,
        AVG(median_credits) as median_credits,
        AVG(avg_cu_per_block) as avg_cu_per_block,
        AVG(avg_mev_commission) as avg_mev_commission,
        AVG(avg_mev_per_block) as avg_mev_per_block,
        AVG(avg_mev_to_validator) as avg_mev_to_validator,
        AVG(avg_mev_to_jito_block_engine) as avg_mev_to_jito_block_engine,
        AVG(avg_mev_to_jito_tip_router) as avg_mev_to_jito_tip_router,
        AVG(avg_priority_fees_per_block) as avg_priority_fees_per_block,
        AVG(avg_rewards_per_block) as avg_rewards_per_block,
        AVG(avg_signature_fees_per_block) as avg_signature_fees_per_block,
        AVG(avg_stake_weighted_skip_rate) as avg_stake_weighted_skip_rate,
        AVG(avg_stake_weighted_leader_slots) as avg_stake_weighted_leader_slots,
        AVG(avg_active_stake) as avg_active_stake,
        AVG(avg_tx_per_block) as avg_tx_per_block,
        AVG(avg_user_tx_per_block) as avg_user_tx_per_block,
        AVG(avg_vote_cost_per_block) as avg_vote_cost_per_block,
        AVG(avg_vote_tx_per_block) as avg_vote_tx_per_block,
        AVG(avg_votes_cast) as avg_votes_cast,
        AVG(total_vote_cost) / AVG(total_active_validators) as avg_vote_cost,
        AVG(median_rewards_per_block) as median_rewards_per_block,
        AVG(median_mev_per_block) as median_mev_per_block,
        AVG(median_votes_cast) as median_votes_cast,
        AVG(total_active_stake) as total_active_stake,
        AVG(total_blocks_produced) as total_blocks_produced,
        AVG(total_credits) as total_credits,
        AVG(total_cu) as total_cu,
        AVG(total_mev_earned) as total_mev_earned,
        AVG(total_mev_to_validator) as total_mev_to_validator,
        AVG(total_mev_to_jito_block_engine) as total_mev_to_jito_block_engine,
        AVG(total_mev_to_jito_tip_router) as total_mev_to_jito_tip_router,
        AVG(total_signatures) as total_signatures,
        AVG(total_tx) as total_tx,
        AVG(total_user_tx) as total_user_tx,
        AVG(total_validator_fees) as total_validator_fees,
        AVG(total_validator_priority_fees) as total_validator_priority_fees,
        AVG(total_validator_signature_fees) as total_validator_signature_fees,
        AVG(total_validator_inflation_rewards) as total_validator_inflation_rewards,
        AVG(total_delegator_inflation_rewards) as total_delegator_inflation_rewards,
        AVG(total_block_rewards) as total_block_rewards,
        AVG(total_vote_cost) as total_vote_cost,
        AVG(total_vote_tx) as total_vote_tx,
        AVG(total_votes_cast) as total_votes_cast,
        AVG(total_active_validators) as total_active_validators,
        MIN(epoch_start_slot) as min_epoch_start_slot,
        MAX(epoch_end_slot) as max_epoch_end_slot,
        MIN(epoch) as min_epoch,
        MAX(epoch) as max_epoch,
        AVG(average_sol_per_4_slots) as average_sol_per_4_slots,
        AVG(median_sol_per_4_slots) as median_sol_per_4_slots,
        AVG(elapsed_time_per_epoch) as average_elapsed_time_per_epoch,
        AVG(epochs_per_year) as average_epochs_per_year,
        AVG(avg_credit_per_voted_slot) as avg_credit_per_voted_slot,
        AVG(max_vote_latency) as average_max_vote_latency,
        AVG(mean_vote_latency) as average_mean_vote_latency,
        AVG(median_vote_latency) as average_median_vote_latency
    FROM epoch_aggregate_data
    WHERE epoch BETWEEN :ten_start AND :ten_end
    """
    query_30_epochs = """
    SELECT AVG(total_blocks_produced) as average_total_blocks_produced_30_epochs
    FROM epoch_aggregate_data
    WHERE epoch BETWEEN :thirty_start AND :thirty_end
    """
    with engine.connect() as conn:
        result = conn.execute(text(query), {"ten_start": min(last_ten_epochs), "ten_end": max(last_ten_epochs)}).fetchone()
        columns = conn.execute(text(query), {"ten_start": min(last_ten_epochs), "ten_end": max(last_ten_epochs)}).keys()
        data = dict(zip(columns, result))

        data = {f"average_{k}" if k.startswith(("median", "total")) else k: v for k, v in data.items()}

        lamport_fields = [
            ('avg_mev_per_block', 7), ('avg_mev_to_validator', 5),
            ('avg_mev_to_jito_block_engine', 5), ('avg_mev_to_jito_tip_router', 5),
            ('avg_priority_fees_per_block', 7), ('avg_rewards_per_block', 7),
            ('avg_signature_fees_per_block', 7), ('avg_vote_cost_per_block', 7),
            ('avg_vote_cost', 7), ('average_median_rewards_per_block', 7),
            ('average_median_mev_per_block', 7), ('average_total_active_stake', 0),
            ('avg_active_stake', 0), ('average_median_active_stake', 0),
            ('average_total_mev_earned', 5), ('average_total_mev_to_validator', 5),
            ('average_total_mev_to_jito_block_engine', 5), ('average_total_mev_to_jito_tip_router', 5),
            ('average_total_validator_fees', 5), ('average_total_validator_priority_fees', 5),
            ('average_total_validator_signature_fees', 5), ('average_total_validator_inflation_rewards', 5),
            ('average_total_delegator_inflation_rewards', 5), ('average_total_block_rewards', 5),
            ('average_total_vote_cost', 5)
        ]
        for field, precision in lamport_fields:
            if field in data and data[field] is not None:
                data[field] = format_lamports_to_sol(data[field], precision)

        number_fields = [
            ('avg_commission', 5), ('avg_credits', 0), ('median_credits', 0),
            ('avg_cu_per_block', 0), ('avg_mev_commission', 0), ('avg_tx_per_block', 0),
            ('avg_user_tx_per_block', 0), ('avg_vote_tx_per_block', 0),
            ('avg_votes_cast', 0), ('avg_stake_weighted_skip_rate', 2),
            ('avg_stake_weighted_leader_slots', 0), ('average_median_votes_cast', 0),
            ('average_total_blocks_produced', 0), ('average_total_credits', 0),
            ('average_total_cu', 0), ('average_total_signatures', 0),
            ('average_total_tx', 0), ('average_total_user_tx', 0),
            ('average_total_vote_tx', 0), ('average_total_votes_cast', 0),
            ('average_total_active_validators', 0), ('average_sol_per_4_slots', 0),
            ('average_median_sol_per_4_slots', 0), ('average_elapsed_time_per_epoch', 2),
            ('average_epochs_per_year', 2), ('avg_credit_per_voted_slot', 3),
            ('average_max_vote_latency', 3), ('average_mean_vote_latency', 3),
            ('average_median_vote_latency', 3)
        ]
        for field, precision in number_fields:
            if field in data and data[field] is not None:
                data[field] = format_number(data[field], precision)

        if max_epoch > 0:
            if data['average_total_active_stake'] is not None:
                try:
                    avg_stake_per_leader_slot = int(float(data['average_total_active_stake']) / 432000)
                    data['avg_stake_per_leader_slot'] = avg_stake_per_leader_slot
                except (ValueError, TypeError) as e:
                    logger.error(f"Error calculating avg_stake_per_leader_slot for ten epoch aggregate: {e}")
                    data['avg_stake_per_leader_slot'] = None
            else:
                data['avg_stake_per_leader_slot'] = None
        else:
            data['avg_stake_per_leader_slot'] = None

        min_epoch = data.pop('min_epoch')
        max_epoch = data.pop('max_epoch')
        data['epoch_range'] = f"{min_epoch}-{max_epoch}"
        data['_epoch_range'] = f"{min_epoch}-{max_epoch}"
        data['epoch_start_slot'] = data.pop('min_epoch_start_slot')
        data['epoch_end_slot'] = data.pop('max_epoch_end_slot')

        average_elapsed_time_per_epoch = data.pop('average_elapsed_time_per_epoch', None)
        if average_elapsed_time_per_epoch is not None:
            data['average_elapsed_time_minutes'] = format_number(float(average_elapsed_time_per_epoch) / 60, 0)
            data['average_elapsed_time_DD_HH_MM'] = format_elapsed_time(average_elapsed_time_per_epoch)
        else:
            data['average_elapsed_time_minutes'] = None
            data['average_elapsed_time_DD_HH_MM'] = None

        result_30_epochs = conn.execute(
            text(query_30_epochs),
            {"thirty_start": min(last_thirty_epochs), "thirty_end": max(last_thirty_epochs)}
        ).fetchone()
        data['average_total_blocks_produced_30_epochs'] = format_number(result_30_epochs[0], 0)

    data = add_trillium_attribution(data)
    filename = get_output_path('ten_epoch_aggregate_data.json', 'json')
    with open(filename, 'w') as f:
        json.dump(data, f, indent=4, default=decimal_default)
    logger.info(f"File created - {filename}")

def generate_weighted_average_validator_rewards(max_epoch, engine):
    weights = [Decimal('0.2649'), Decimal('0.1987'), Decimal('0.1490'), Decimal('0.1118'), Decimal('0.0838'), 
               Decimal('0.0629'), Decimal('0.0471'), Decimal('0.0354'), Decimal('0.0265'), Decimal('0.0199')]
    epochs = range(max_epoch - 9, max_epoch + 1)

    query = """
    WITH latest_validator_info AS (
        SELECT DISTINCT ON (identity_pubkey)
            identity_pubkey,
            name,
            website,
            details,
            keybase_username,
            icon_url,
            COALESCE(logo, 'no-image-available12.webp') AS logo            
        FROM validator_info
        ORDER BY identity_pubkey
    )
    SELECT 
        vs.identity_pubkey,
        vs.epoch,
        vs.activated_stake,
        vs.blocks_produced,
        vs.commission,
        vs.cu,
        vs.epoch_credits,
        vs.leader_slots,
        vs.mev_commission,
        vs.mev_earned,
        vs.mev_to_validator,
        vs.mev_to_jito_block_engine,
        vs.mev_to_jito_tip_router,
        vs.rewards,
        vs.signatures,
        vs.stake_percentage,
        vs.total_block_rewards_after_burn,
        vs.total_block_rewards_before_burn,
        vs.tx_included_in_blocks,
        vs.user_tx_included_in_blocks,
        vs.validator_priority_fees,
        vs.validator_signature_fees,
        vs.validator_inflation_reward,
        vs.delegator_inflation_reward,
        vs.vote_cost,
        vs.vote_tx_included_in_blocks,
        vs.votes_cast,
        vs.avg_cu_per_block,
        vs.avg_mev_per_block,
        vs.avg_priority_fees_per_block,
        vs.avg_rewards_per_block,
        vs.avg_signature_fees_per_block,
        vs.skip_rate,
        vs.avg_tx_per_block,
        vs.avg_user_tx_per_block,
        vs.avg_vote_tx_per_block,
        vs.vote_account_pubkey,
        vs.ip,
        vs.client_type,
        vs.version,
        vs.asn,
        vs.asn_org,
        vs.city,
        vs.continent,
        vs.country,
        vs.region,
        vs.superminority,
        vs.jito_rank,
        vt.vote_credits,
        vt.voted_slots,
        vt.avg_credit_per_voted_slot,
        vt.max_vote_latency,
        vt.mean_vote_latency,
        vt.median_vote_latency,
        vt.vote_credits_rank,
        COALESCE(vi.name, ' ') AS name,
        COALESCE(vi.website, ' ') AS website,
        COALESCE(vi.details, ' ') AS details,
        COALESCE(vi.keybase_username, ' ') AS keybase_username,
        COALESCE(vi.icon_url, ' ') AS icon_url,
        COALESCE(vi.logo, 'no-image-available12.webp') AS logo
    FROM validator_stats vs
    LEFT JOIN latest_validator_info vi ON vs.identity_pubkey = vi.identity_pubkey
    LEFT JOIN votes_table vt ON vs.epoch = vt.epoch AND vs.vote_account_pubkey = vt.vote_account_pubkey
    WHERE vs.epoch BETWEEN :epoch_start AND :epoch_end
        AND vs.activated_stake != 0;
    """
    with engine.connect() as conn:
        results = conn.execute(text(query), {"epoch_start": min(epochs), "epoch_end": max(epochs)}).fetchall()
        columns = conn.execute(text(query), {"epoch_start": min(epochs), "epoch_end": max(epochs)}).keys()

    validator_data = {}
    for row in results:
        record = dict(zip(columns, row))
        identity_pubkey = record['identity_pubkey']
        epoch = record['epoch']
        if identity_pubkey not in validator_data:
            validator_data[identity_pubkey] = {
                'weighted_sum': {col: Decimal('0') for col in columns if col not in ['identity_pubkey', 'epoch']},
                'total_weight': Decimal('0'),
                'latest': {col: record[col] for col in columns if col not in ['identity_pubkey', 'epoch']}
            }
        weight = weights[max_epoch - epoch]
        validator_data[identity_pubkey]['total_weight'] += weight
        for col in validator_data[identity_pubkey]['weighted_sum'].keys():
            val = record[col]
            if isinstance(val, (int, float, Decimal)) and val is not None:
                validator_data[identity_pubkey]['weighted_sum'][col] += Decimal(str(val)) * weight

    weighted_avg_data = []
    for identity_pubkey, data in validator_data.items():
        wsum = data['weighted_sum']
        tw = data['total_weight']
        avg_record = {
            'identity_pubkey': identity_pubkey,
            'epoch_range': f"{min(epochs)}-{max(epochs)}",
            'average_activated_stake': wsum['activated_stake'] / tw,
            'average_blocks_produced': wsum['blocks_produced'] / tw,
            'average_commission': wsum['commission'] / tw,
            'average_cu': wsum['cu'] / tw,
            'average_epoch_credits': wsum['epoch_credits'] / tw,
            'average_leader_slots': wsum['leader_slots'] / tw,
            'average_mev_commission': wsum['mev_commission'] / tw,
            'average_mev_earned': wsum['mev_earned'] / tw,
            'average_mev_to_validator': wsum['mev_to_validator'] / tw,
            'average_mev_to_jito_block_engine': wsum['mev_to_jito_block_engine'] / tw,
            'average_mev_to_jito_tip_router': wsum['mev_to_jito_tip_router'] / tw,
            'average_rewards': wsum['rewards'] / tw,
            'average_signatures': wsum['signatures'] / tw,
            'average_stake_percentage': wsum['stake_percentage'] / tw,
            'average_total_block_rewards_after_burn': wsum['total_block_rewards_after_burn'] / tw,
            'average_total_block_rewards_before_burn': wsum['total_block_rewards_before_burn'] / tw,
            'average_tx_included_in_blocks': wsum['tx_included_in_blocks'] / tw,
            'average_user_tx_included_in_blocks': wsum['user_tx_included_in_blocks'] / tw,
            'average_validator_priority_fees': wsum['validator_priority_fees'] / tw,
            'average_validator_signature_fees': wsum['validator_signature_fees'] / tw,
            'average_validator_inflation_reward': wsum['validator_inflation_reward'] / tw,
            'average_delegator_inflation_reward': wsum['delegator_inflation_reward'] / tw,
            'average_vote_cost': wsum['vote_cost'] / tw,
            'average_vote_tx_included_in_blocks': wsum['vote_tx_included_in_blocks'] / tw,
            'average_votes_cast': wsum['votes_cast'] / tw,
            'average_jito_rank': wsum['jito_rank'] / tw if wsum['jito_rank'] is not None else None,
            'avg_cu_per_block': wsum['avg_cu_per_block'] / tw,
            'avg_mev_per_block': wsum['avg_mev_per_block'] / tw,
            'avg_priority_fees_per_block': wsum['avg_priority_fees_per_block'] / tw,
            'avg_rewards_per_block': wsum['avg_rewards_per_block'] / tw,
            'avg_signature_fees_per_block': wsum['avg_signature_fees_per_block'] / tw,
            'avg_skip_rate': wsum['skip_rate'] / tw,
            'avg_tx_per_block': wsum['avg_tx_per_block'] / tw,
            'avg_user_tx_per_block': wsum['avg_user_tx_per_block'] / tw,
            'avg_vote_tx_per_block': wsum['avg_vote_tx_per_block'] / tw,
            'average_vote_credits': wsum['vote_credits'] / tw if wsum['vote_credits'] is not None else None,
            'average_voted_slots': wsum['voted_slots'] / tw if wsum['voted_slots'] is not None else None,
            'avg_credit_per_voted_slot': wsum['avg_credit_per_voted_slot'] / tw if wsum['avg_credit_per_voted_slot'] is not None else None,
            'average_max_vote_latency': wsum['max_vote_latency'] / tw if wsum['max_vote_latency'] is not None else None,
            'average_mean_vote_latency': wsum['mean_vote_latency'] / tw if wsum['mean_vote_latency'] is not None else None,
            'average_median_vote_latency': wsum['median_vote_latency'] / tw if wsum['median_vote_latency'] is not None else None,
            'average_vote_credits_rank': wsum['vote_credits_rank'] / tw if wsum['vote_credits_rank'] is not None else None,
            'vote_account_pubkey': data['latest']['vote_account_pubkey'],
            'ip': data['latest']['ip'],
            'client_type': data['latest']['client_type'],
            'version': data['latest']['version'],
            'name': data['latest']['name'],
            'website': data['latest']['website'],
            'details': data['latest']['details'],
            'keybase_username': data['latest']['keybase_username'],
            'icon_url': data['latest']['icon_url'],
            'logo': data['latest']['logo'],
            'asn': data['latest']['asn'],
            'asn_org': data['latest']['asn_org'],
            'city': data['latest']['city'],
            'continent': data['latest']['continent'],
            'country': data['latest']['country'],
            'region': data['latest']['region'],
            'superminority': data['latest']['superminority']
        }
        lamport_fields = [
            ('average_activated_stake', 0), ('average_mev_earned', 5),
            ('average_mev_to_validator', 5), ('average_mev_to_jito_block_engine', 5),
            ('average_mev_to_jito_tip_router', 5), ('average_rewards', 5),
            ('average_total_block_rewards_after_burn', 5), ('average_total_block_rewards_before_burn', 5),
            ('average_validator_priority_fees', 5), ('average_validator_signature_fees', 5),
            ('average_validator_inflation_reward', 5), ('average_delegator_inflation_reward', 5),
            ('average_vote_cost', 5), ('avg_mev_per_block', 7),
            ('avg_priority_fees_per_block', 7), ('avg_rewards_per_block', 7),
            ('avg_signature_fees_per_block', 7)
        ]
        for field, precision in lamport_fields:
            if field in avg_record and avg_record[field] is not None:
                avg_record[field] = format_lamports_to_sol(avg_record[field], precision)

        number_fields = [
            ('average_blocks_produced', 0), ('average_commission', 2),
            ('average_cu', 0), ('average_epoch_credits', 0), ('average_leader_slots', 0),
            ('average_mev_commission', 2), ('average_signatures', 0),
            ('average_stake_percentage', 5), ('average_tx_included_in_blocks', 0),
            ('average_user_tx_included_in_blocks', 0), ('average_vote_tx_included_in_blocks', 0),
            ('average_jito_rank', 0), ('average_votes_cast', 0), ('avg_cu_per_block', 0),
            ('avg_skip_rate', 2), ('avg_tx_per_block', 2), ('avg_user_tx_per_block', 2),
            ('avg_vote_tx_per_block', 2), ('average_vote_credits', 0),
            ('average_voted_slots', 0), ('avg_credit_per_voted_slot', 3),
            ('average_max_vote_latency', 3), ('average_mean_vote_latency', 3),
            ('average_median_vote_latency', 3), ('average_vote_credits_rank', 0)
        ]
        for field, precision in number_fields:
            if field in avg_record and avg_record[field] is not None:
                avg_record[field] = format_number(avg_record[field], precision)

        if (avg_record['average_activated_stake'] is not None and 
            avg_record['average_leader_slots'] is not None and 
            float(avg_record['average_leader_slots']) > 0):
            try:
                avg_stake_per_leader_slot = float(avg_record['average_activated_stake']) / float(avg_record['average_leader_slots'])
                avg_record['avg_stake_per_leader_slot'] = int(avg_stake_per_leader_slot)
            except (ValueError, TypeError):
                avg_record['avg_stake_per_leader_slot'] = None
        else:
            avg_record['avg_stake_per_leader_slot'] = None

        weighted_avg_data.append(avg_record)

    weighted_avg_data = add_trillium_attribution(weighted_avg_data)
    filename = get_output_path('recency_weighted_average_validator_rewards.json', 'json')
    with open(filename, 'w') as f:
        json.dump(weighted_avg_data, f, indent=4, default=decimal_default)
    logger.info(f"File created - {filename}")