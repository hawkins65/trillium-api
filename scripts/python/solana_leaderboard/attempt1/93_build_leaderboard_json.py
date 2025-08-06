# 93_build_leaderboard_json.py



# Standard library imports
import os
import json
import random
import logging
from datetime import datetime, timedelta
from decimal import Decimal
from collections import defaultdict
import re
import urllib.request
import sys

# Third-party library imports
import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from sqlalchemy import create_engine, text

# Custom modules
from db_config import db_params

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def format_elapsed_time(seconds):
    days = seconds // (24 * 3600)
    seconds = seconds % (24 * 3600)
    hours = seconds // 3600
    seconds %= 3600
    minutes = seconds // 60
    return f"{int(days):02}:{int(hours):02}:{int(minutes):02}"

def decimal_default(obj):
    if isinstance(obj, Decimal):
        return float(obj)
    raise TypeError

def format_lamports_to_sol(lamports, precision=7):
    if lamports is None:
        return None
    sol_amount = Decimal(lamports) / Decimal('1000000000')
    if precision == 0:
        return int(sol_amount)
    return float(f"{sol_amount:.{precision}f}")

def format_number(number, precision):
    if number is None:
        return None
    return float(f"{number:.{precision}f}") if precision > 0 else int(number)

def add_trillium_attribution(data):
    if isinstance(data, dict):
        data["_Trillium_Attribution"] = "Fueled By Trillium | Solana"
        return data
    elif isinstance(data, list):
        return [add_trillium_attribution(item) for item in data]
    else:
        return data

def get_validator_stats(epoch, engine):
    query = """
    SELECT 
        vs.*,
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
        vt.vote_credits_rank
    FROM validator_stats vs
    LEFT JOIN validator_info vi ON vs.identity_pubkey = vi.identity_pubkey
    LEFT JOIN votes_table vt ON vs.epoch = vt.epoch AND vs.vote_account_pubkey = vt.vote_account_pubkey
    WHERE vs.epoch = :epoch
        AND vs.activated_stake > 0;
    """

    with engine.connect() as conn:
        results = conn.execute(text(query), {"epoch": epoch}).fetchall()
        columns = conn.execute(text(query), {"epoch": epoch}).keys()

    data = []
    for row in results:
        record = dict(zip(columns, row))

        if record['identity_pubkey'] == '5pPRHniefFjkiaArbGX3Y8NUysJmQ9tMZg3FrFGwHzSm':
            logger.debug(f"Debug - Epoch: {record['epoch']}, "
                         f"Name: {record['name']}, "
                         f"Identity Pubkey: {record['identity_pubkey']}, "
                         f"Commission: {record['commission']}, "
                         f"Validator Inflation Reward: {record['validator_inflation_reward']}")

        lamport_fields = [
            ('total_block_rewards_before_burn', 5),
            ('total_block_rewards_after_burn', 5),
            ('validator_priority_fees', 5),
            ('validator_signature_fees', 5),
            ('validator_inflation_reward', 5),
            ('delegator_inflation_reward', 5),
            ('vote_cost', 5),
            ('mev_earned', 5),
            ('mev_to_validator', 5),
            ('mev_to_jito_block_engine', 5),
            ('mev_to_jito_tip_router', 5),
            ('mev_to_stakers', 5),
            ('avg_mev_per_block', 7),
            ('avg_priority_fees_per_block', 7),
            ('avg_rewards_per_block', 7),
            ('avg_signature_fees_per_block', 7),
            ('activated_stake', 0),
            ('rewards', 5),
            ('total_inflation_reward', 5)
        ]

        for field, precision in lamport_fields:
            if field in record and record[field] is not None:
                record[field] = format_lamports_to_sol(record[field], precision)

        number_fields = [
            ('avg_cu_per_block', 0),
            ('avg_tx_per_block', 0),
            ('avg_user_tx_per_block', 0),
            ('avg_vote_tx_per_block', 0),
            ('avg_votes_cast_per_block', 0),
            ('mean_vote_latency', 3),
            ('avg_credit_per_voted_slot', 5),
            ('avg_latency_per_voted_slot', 5),
            ('median_vote_latency', 5),
            ('delegator_inflation_apy', 5),
            ('delegator_compound_inflation_apy', 5),
            ('delegator_mev_apy', 5),
            ('delegator_compound_mev_apy', 5),
            ('delegator_total_apy', 5),
            ('delegator_compound_total_apy', 5),
            ('total_overall_apy', 5),
            ('compound_overall_apy', 5),
            ('validator_inflation_apy', 5),
            ('validator_mev_apy', 5),
            ('validator_block_rewards_apy', 5),
            ('validator_total_apy', 5),
            ('validator_compound_inflation_apy', 5),
            ('validator_compound_mev_apy', 5),
            ('validator_compound_block_rewards_apy', 5),
            ('validator_compound_total_apy', 5)
        ]
        for field, precision in number_fields:
            if field in record and record[field] is not None:
                record[field] = format_number(record[field], precision)

        if epoch > 0: # was missing active stake prior to epoch 632 -- we have it now
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

def write_validator_stats_to_json(epoch, data):
    filename = f"epoch{epoch}_validator_rewards.json"
    data_with_attribution = add_trillium_attribution(data)
    with open(filename, 'w') as f:
        json.dump(data, f, indent=4, default=decimal_default)
    logger.info(f"*** file created - {filename}")

def get_epoch_aggregate_data(epoch, engine):
    query = """
    SELECT * FROM epoch_aggregate_data WHERE epoch = :epoch
    """
    with engine.connect() as conn:
        result = conn.execute(text(query), {"epoch": epoch}).fetchone()
        if result is None:
            logging.warning(f"No data found for epoch {epoch} in epoch_aggregate_data table")
            return None
        columns = conn.execute(text(query), {"epoch": epoch}).keys()
        data = dict(zip(columns, result))

    # Calculate avg_vote_cost as total_vote_cost / total_active_validators
    if 'total_vote_cost' in data and 'total_active_validators' in data and data['total_vote_cost'] is not None and data['total_active_validators'] is not None and data['total_active_validators'] != 0:
        try:
            data['avg_vote_cost'] = data['total_vote_cost'] / data['total_active_validators']
        except (ValueError, TypeError) as e:
            logging.error(f"Error calculating avg_vote_cost for epoch {epoch}: {e}")
            data['avg_vote_cost'] = None
    else:
        data['avg_vote_cost'] = None
        
    lamport_fields = [
        ('avg_mev_per_block', 7),
        ('avg_mev_to_validator', 5),
        ('avg_mev_to_jito_block_engine', 5),
        ('avg_mev_to_jito_tip_router', 5),
        ('avg_priority_fees_per_block', 7),
        ('avg_rewards_per_block', 7),
        ('avg_signature_fees_per_block', 7),
        ('avg_vote_cost_per_block', 7),
        ('avg_vote_cost', 7),
        ('median_rewards_per_block', 7),
        ('median_mev_per_block', 7),
        ('total_mev_earned', 5),
        ('total_mev_to_validator', 5),
        ('total_mev_to_jito_block_engine', 5),
        ('total_mev_to_jito_tip_router', 5),
        ('total_mev_to_stakers', 5),        
        ('total_validator_fees', 5),
        ('total_validator_priority_fees', 5),
        ('total_validator_signature_fees', 5),
        ('total_validator_inflation_rewards', 5),
        ('total_delegator_inflation_rewards', 5),
        ('total_block_rewards', 5),
        ('total_active_stake', 0),
        ('avg_active_stake', 0),
        ('median_active_stake', 0),
        ('total_active_validators', 0),
        ('total_vote_cost', 5)
    ]

    for field, precision in lamport_fields:
        if field in data and data[field] is not None:
            data[field] = format_lamports_to_sol(data[field], precision)

    number_fields = [
        ('avg_commission', 2),
        ('avg_credits', 0),
        ('median_credits', 0),
        ('avg_cu_per_block', 0),
        ('avg_mev_commission', 0),
        ('avg_tx_per_block', 0),
        ('avg_user_tx_per_block', 0),
        ('avg_vote_tx_per_block', 0),
        ('avg_votes_cast', 0),
        ('avg_stake_weighted_skip_rate', 2),
        ('avg_stake_weighted_leader_slots', 0),
        ('median_votes_cast', 0),
        ('total_blocks_produced', 0),
        ('total_credits', 0),
        ('total_cu', 0),
        ('total_signatures', 0),
        ('total_tx', 0),
        ('total_user_tx', 0),
        ('total_vote_tx', 0),
        ('total_votes_cast', 0),
        ('total_active_validators', 0),
        ('average_sol_per_4_slots', 0),
        ('median_sol_per_4_slots', 0),
        ('elapsed_time_per_epoch', 5),
        ('epochs_per_year', 2),
        ('avg_credit_per_voted_slot', 3),
        ('max_vote_latency', 3),
        ('mean_vote_latency', 3),
        ('median_vote_latency', 3),
        ('inflation_decay_rate',2),
        ('inflation_rate',2)
    ]
    for field, precision in number_fields:
        if field in data and data[field] is not None:
            data[field] = format_number(data[field], precision)

    if epoch > 0:   # was missing active stake prior to epoch 632 -- we have it now
        if data['total_active_stake'] is not None:
            try:
                avg_stake_per_leader_slot = int(float(data['total_active_stake']) / 432000)
                data['avg_stake_per_leader_slot'] = avg_stake_per_leader_slot
            except (ValueError, TypeError) as e:
                logging.error(f"Error calculating avg_stake_per_leader_slot for epoch {epoch}: {e}")
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

def write_epoch_aggregate_data_to_json(epoch, data):
    filename = f"epoch{epoch}_epoch_aggregate_data.json"
    data_with_attribution = add_trillium_attribution(data)
    with open(filename, 'w') as f:
        json.dump(data, f, indent=4, default=decimal_default)
    logger.info(f"*** file created - {filename}")

def get_min_max_epochs(engine):
    query = "SELECT MIN(epoch), MAX(epoch) FROM validator_stats"
    with engine.connect() as conn:
        result = conn.execute(text(query)).fetchone()
    return result[0], result[1]

def generate_last_ten_epochs_data(max_epoch, engine):
    last_ten_epochs = range(max_epoch - 9, max_epoch + 1)
    last_ten_epochs_data = []
    for epoch in last_ten_epochs:
        epoch_aggregate_data = get_epoch_aggregate_data(epoch, engine)
        if epoch_aggregate_data is not None:
            if epoch > 0 and epoch_aggregate_data['total_active_stake'] is not None:  # was missing active stake prior to epoch 632 -- we have it now
                try:
                    avg_stake_per_leader_slot = int(float(epoch_aggregate_data['total_active_stake']) / 432000)
                    epoch_aggregate_data['avg_stake_per_leader_slot'] = avg_stake_per_leader_slot
                except (ValueError, TypeError) as e:
                    logger.error(f"Error calculating avg_stake_per_leader_slot for epoch {epoch} in last ten epochs: {e}")
                    epoch_aggregate_data['avg_stake_per_leader_slot'] = None
            else:
                epoch_aggregate_data['avg_stake_per_leader_slot'] = None
            epoch_aggregate_data['_epoch'] = epoch
            last_ten_epochs_data.append(epoch_aggregate_data)

    last_ten_epochs_data.sort(key=lambda x: x['epoch'], reverse=True)
    last_ten_epochs_data = add_trillium_attribution(last_ten_epochs_data)
    filename = "last_ten_epoch_aggregate_data.json"
    with open(filename, 'w') as f:
        json.dump(last_ten_epochs_data, f, indent=4, default=decimal_default)
    logger.info(f"*** file created - {filename}")

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
            logger.debug(f"Debug - Name: {record['name']}, "
                         f"Identity Pubkey: {record['identity_pubkey']}, "
                         f"Commission: {record['commission']}, "
                         f"Validator Inflation Reward: {record['validator_inflation_reward']}")

        fields_to_prefix = [
            'activated_stake', 'blocks_produced', 'commission', 'cu', 'epoch_credits',
            'leader_slots', 'mev_commission', 'mev_earned', 'mev_to_validator', 'mev_to_jito_block_engine', 'mev_to_jito_tip_router',
            'rewards', 'signatures', 'stake_percentage', 'total_block_rewards_after_burn',
            'total_block_rewards_before_burn', 'tx_included_in_blocks',
            'user_tx_included_in_blocks', 'validator_priority_fees',
            'validator_signature_fees', 'validator_inflation_reward', 'delegator_inflation_reward', 'vote_cost', 'vote_tx_included_in_blocks', 'votes_cast',
            'vote_credits', 'voted_slots', 'max_vote_latency', 'mean_vote_latency', 'median_vote_latency', 'vote_credits_rank'
        ]

        for field in fields_to_prefix:
            if field in record:
                record[f"average_{field}"] = record.pop(field)

        lamport_fields = [
            ('average_activated_stake', 0),
            ('avg_mev_per_block', 7),
            ('avg_priority_fees_per_block', 7),
            ('avg_rewards_per_block', 7),
            ('avg_signature_fees_per_block', 7),
            ('average_mev_earned', 5),
            ('average_mev_to_validator', 5),
            ('average_mev_to_jito_block_engine', 5),
            ('average_mev_to_jito_tip_router', 5),
            ('average_rewards', 5),
            ('average_total_block_rewards_before_burn', 5),
            ('average_total_block_rewards_after_burn', 5),
            ('average_validator_priority_fees', 5),
            ('average_validator_signature_fees', 5),
            ('average_validator_inflation_reward', 5),
            ('average_delegator_inflation_reward', 5),
            ('average_vote_cost', 5),
        ]

        for field, precision in lamport_fields:
            if field in record and record[field] is not None:
                record[field] = format_lamports_to_sol(record[field], precision)

        number_fields = [
            ('average_blocks_produced', 0),
            ('average_commission', 0),
            ('average_cu', 0),
            ('average_epoch_credits', 0),
            ('median_credits', 0),
            ('average_leader_slots', 0),
            ('average_mev_commission', 0),
            ('average_signatures', 0),
            ('average_stake_percentage', 5),
            ('average_tx_included_in_blocks', 0),
            ('average_user_tx_included_in_blocks', 0),
            ('average_vote_tx_included_in_blocks', 0),
            ('average_jito_rank', 0),
            ('average_votes_cast', 0),
            ('avg_cu_per_block', 0),
            ('avg_skip_rate', 2),
            ('avg_tx_per_block', 2),
            ('avg_user_tx_per_block', 2),
            ('avg_vote_tx_per_block', 2),
            ('average_vote_credits', 0),
            ('average_voted_slots', 0),
            ('avg_credit_per_voted_slot', 3),
            ('average_max_vote_latency', 3),
            ('average_mean_vote_latency', 3),
            ('average_median_vote_latency', 3),
            ('average_vote_credits_rank', 0)
        ]

        for field, precision in number_fields:
            if field in record and record[field] is not None:
                record[field] = format_number(record[field], precision)

        if max_epoch > 0: # was missing active stake prior to epoch 632 -- we have it now
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
    filename = 'ten_epoch_validator_rewards.json'
    with open(filename, 'w') as f:
        json.dump(data, f, indent=4, default=decimal_default)
    logger.info(f"*** file created - {filename}")

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
            ('avg_mev_per_block', 7),
            ('avg_mev_to_validator', 5),
            ('avg_mev_to_jito_block_engine', 5),
            ('avg_mev_to_jito_tip_router', 5),
            ('avg_priority_fees_per_block', 7),
            ('avg_rewards_per_block', 7),
            ('avg_signature_fees_per_block', 7),
            ('avg_vote_cost_per_block', 7),
            ('avg_vote_cost', 7),
            ('average_median_rewards_per_block', 7),
            ('average_median_mev_per_block', 7),
            ('average_total_active_stake', 0),
            ('avg_active_stake', 0),
            ('average_median_active_stake', 0),
            ('average_total_mev_earned', 5),
            ('average_total_mev_to_validator', 5),
            ('average_total_mev_to_jito_block_engine', 5),
            ('average_total_mev_to_jito_tip_router', 5),
            ('average_total_validator_fees', 5),
            ('average_total_validator_priority_fees', 5),
            ('average_total_validator_signature_fees', 5),
            ('average_total_validator_inflation_rewards', 5),
            ('average_total_delegator_inflation_rewards', 5),
            ('average_total_block_rewards', 5),
            ('average_total_active_validators', 0),
            ('average_total_vote_cost', 5)
        ]

        for field, precision in lamport_fields:
            if field in data and data[field] is not None:
                data[field] = format_lamports_to_sol(data[field], precision)

        number_fields = [
            ('avg_commission', 5),
            ('avg_credits', 0),
            ('median_credits', 0),
            ('avg_cu_per_block', 0),
            ('avg_mev_commission', 0),
            ('avg_tx_per_block', 0),
            ('avg_user_tx_per_block', 0),
            ('avg_vote_tx_per_block', 0),
            ('avg_votes_cast', 0),
            ('avg_stake_weighted_skip_rate', 2),
            ('avg_stake_weighted_leader_slots', 0),
            ('average_median_votes_cast', 0),
            ('average_total_blocks_produced', 0),
            ('average_total_credits', 0),
            ('average_total_cu', 0),
            ('average_total_signatures', 0),
            ('average_total_tx', 0),
            ('average_total_user_tx', 0),
            ('average_total_vote_tx', 0),
            ('average_total_votes_cast', 0),
            ('average_total_active_validators', 0),
            ('average_sol_per_4_slots', 0),
            ('average_median_sol_per_4_slots', 0),
            ('average_elapsed_time_per_epoch', 2),
            ('average_epochs_per_year', 2),
            ('avg_credit_per_voted_slot', 3),
            ('average_max_vote_latency', 3),
            ('average_mean_vote_latency', 3),
            ('average_median_vote_latency', 3)
        ]

        for field, precision in number_fields:
            if field in data and data[field] is not None:
                data[field] = format_number(data[field], precision)

        if max_epoch > 0: # was missing active stake prior to epoch 632 -- we have it now
            if data['average_total_active_stake'] is not None:
                try:
                    avg_stake_per_leader_slot = int(float(data['average_total_active_stake']) / 432000)
                    data['avg_stake_per_leader_slot'] = avg_stake_per_leader_slot
                except (ValueError, TypeError) as e:
                    print(f"Error calculating avg_stake_per_leader_slot for ten epoch aggregate: {e}")
                    data['avg_stake_per_leader_slot'] = None
            else:
                data['avg_stake_per_leader_slot'] = None
        else:
            data['avg_stake_per_leader_slot'] = None

        min_epoch = data.pop('min_epoch')
        max_epoch = data.pop('max_epoch')
        data['epoch_range'] = f"{min_epoch}-{max_epoch}"
        data["_epoch_range"] = f"{min_epoch}-{max_epoch}"
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
    filename = 'ten_epoch_aggregate_data.json'
    with open(filename, 'w') as f:
        json.dump(data, f, indent=4, default=decimal_default)
    logger.info(f"*** file created - {filename}")

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
            ('average_activated_stake', 0),
            ('average_mev_earned', 5),
            ('average_mev_to_validator', 5),
            ('average_mev_to_jito_block_engine', 5),
            ('average_mev_to_jito_tip_router', 5),
            ('average_rewards', 5),
            ('average_total_block_rewards_after_burn', 5),
            ('average_total_block_rewards_before_burn', 5),
            ('average_validator_priority_fees', 5),
            ('average_validator_signature_fees', 5),
            ('average_validator_inflation_reward', 5),
            ('average_delegator_inflation_reward', 5),
            ('average_vote_cost', 5),
            ('avg_mev_per_block', 7),
            ('avg_priority_fees_per_block', 7),
            ('avg_rewards_per_block', 7),
            ('avg_signature_fees_per_block', 7),
        ]
        for field, precision in lamport_fields:
            if field in avg_record and avg_record[field] is not None:
                avg_record[field] = format_lamports_to_sol(avg_record[field], precision)

        number_fields = [
            ('average_blocks_produced', 0),
            ('average_commission', 2),
            ('average_cu', 0),
            ('average_epoch_credits', 0),
            ('average_leader_slots', 0),
            ('average_mev_commission', 2),
            ('average_signatures', 0),
            ('average_stake_percentage', 5),
            ('average_tx_included_in_blocks', 0),
            ('average_user_tx_included_in_blocks', 0),
            ('average_vote_tx_included_in_blocks', 0),
            ('average_jito_rank', 0),
            ('average_votes_cast', 0),
            ('avg_cu_per_block', 0),
            ('avg_skip_rate', 2),
            ('avg_tx_per_block', 2),
            ('avg_user_tx_per_block', 2),
            ('avg_vote_tx_per_block', 2),
            ('average_vote_credits', 0),
            ('average_voted_slots', 0),
            ('avg_credit_per_voted_slot', 3),
            ('average_max_vote_latency', 3),
            ('average_mean_vote_latency', 3),
            ('average_median_vote_latency', 3),
            ('average_vote_credits_rank', 0)
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
    filename = 'recency_weighted_average_validator_rewards.json'
    with open(filename, 'w') as f:
        json.dump(weighted_avg_data, f, indent=4, default=decimal_default)
    logger.info(f"*** file created - {filename}")

def get_color_map(items):
    colors = [
        '#6A9F4F', '#4E89A7', '#A77A9F', '#66A7B2', '#FF00FF', '#00FFFF',
        '#FF4500', '#32CD32', '#4169E1', '#FFD700', '#FF1493', '#40E0D0',
        '#DC143C', '#7FFF00', '#1E90FF', '#FFA500', '#EE82EE', '#00CED1',
        '#FF6347', '#3CB371', '#6495ED', '#DA70D6', '#48D1CC', '#66CDAA',
        '#87CEFA', '#FF69B4', '#20B2AA', '#BA55D3', '#5F9EA0', '#CD5C5C',
        '#FFB6C1', '#9370DB', '#7FFFD4', '#FF7F50', '#8A2BE2', '#8B008B',
        '#B22222', '#228B22', '#FF8C00', '#8B4513', '#7B68EE', '#6A5ACD',
        '#4682B4', '#DDA0DD', '#D2691E', '#B0E0E6', '#32CD32', '#ADFF2F',
        '#FFA07A', '#87CEEB', '#9370DB', '#B0C4DE', '#FFDEAD', '#F4A460',
        '#DAA520', '#A0522D', '#A52A2A', '#708090', '#556B2F', '#8FBC8F',
        '#CD853F', '#BC8F8F', '#2F4F4F', '#D3D3D3', '#00BFFF', '#8A2BE2'
    ]
    color_cycle = (colors * (len(items) // len(colors) + 1))[:len(items)]
    return dict(zip(sorted(items), color_cycle))

def get_persistent_color_map(items, filename='country_colors.json'):
    colors = [
        '#6A9F4F', '#4E89A7', '#A77A9F', '#66A7B2', '#FF00FF', '#00FFFF',
        '#FF4500', '#32CD32', '#4169E1', '#FFD700', '#FF1493', '#40E0D0',
        '#DC143C', '#7FFF00', '#1E90FF', '#FFA500', '#EE82EE', '#00CED1',
        '#FF6347', '#3CB371', '#6495ED', '#DA70D6', '#48D1CC', '#66CDAA',
        '#87CEFA', '#FF69B4', '#20B2AA', '#BA55D3', '#5F9EA0', '#CD5C5C',
        '#FFB6C1', '#9370DB', '#7FFFD4', '#FF7F50', '#8A2BE2', '#8B008B',
        '#B22222', '#228B22', '#FF8C00', '#8B4513', '#7B68EE', '#6A5ACD',
        '#4682B4', '#DDA0DD', '#D2691E', '#B0E0E6', '#32CD32', '#ADFF2F',
        '#FFA07A', '#87CEEB', '#9370DB', '#B0C4DE', '#FFDEAD', '#F4A460',
        '#DAA520', '#A0522D', '#A52A2A', '#708090', '#556B2F', '#8FBC8F',
        '#CD853F', '#BC8F8F', '#2F4F4F', '#D3D3D3', '#00BFFF', '#8A2BE2'
    ]

    color_map = {}
    if os.path.exists(filename):
        with open(filename, 'r') as f:
            color_map = json.load(f)
    else:
        color_map = {}

    for item in items:
        if item not in color_map:
            if item == 'Unknown':
                color_map[item] = '#CCCCCC'
            else:
                available_colors = [c for c in colors if c not in color_map.values()]
                if not available_colors:
                    available_colors = colors
                color_map[item] = random.choice(available_colors)

    with open(filename, 'w') as f:
        json.dump(color_map, f)
    return color_map

def calculate_stake_statistics(epoch, max_epoch, engine):
    try:
        query = """
        SELECT 
            identity_pubkey,
            activated_stake,
            city,
            country,
            continent,
            region
        FROM validator_stats
        WHERE epoch = :epoch
            AND activated_stake != 0;
        """

        slot_query = """
        SELECT identity_pubkey, COUNT(block_slot)
        FROM leader_schedule
        WHERE epoch = :epoch
        GROUP BY identity_pubkey;
        """

        with engine.connect() as conn:
            logging.info(f"validator_stats for epoch {epoch} query")
            validator_stats = conn.execute(text(query), {"epoch": epoch}).fetchall()
            logging.info(f"block_slot_results for epoch {epoch} slot_query")
            block_slot_results = conn.execute(text(slot_query), {"epoch": epoch}).fetchall()

        total_slots = sum(slot_count for _, slot_count in block_slot_results)
        logging.info(f"total_slots for epoch {epoch} {total_slots}")
        
        if total_slots == 0:
            logging.warning(f"total_slots is zero for epoch {epoch}. Setting to 1 to avoid division by zero.")
            total_slots = 1  # Avoid division by zero
            
        total_activated_stake = sum(stats[1] for stats in validator_stats if stats[1] is not None)
        logging.info(f"total_activated_stake for epoch {epoch} {total_activated_stake}")
        
        if total_activated_stake == 0:
            logging.warning(f"total_activated_stake is zero for epoch {epoch}. Setting to 1 to avoid division by zero.")
            total_activated_stake = 1  # Avoid division by zero
        
        slot_counts = dict(block_slot_results)

        country_results = defaultdict(lambda: {"count": 0, "total_activated_stake": 0.0, "total_slots": 0, "average_stake": 0.0, "percent_stake": 0.0, "percent_slots": 0.0})
        continent_results = defaultdict(lambda: {"count": 0, "total_activated_stake": 0.0, "total_slots": 0, "average_stake": 0.0, "percent_stake": 0.0, "percent_slots": 0.0})
        region_results = defaultdict(lambda: {"count": 0, "total_activated_stake": 0.0, "total_slots": 0, "average_stake": 0.0, "percent_stake": 0.0, "percent_slots": 0.0})

        for identity_pubkey, activated_stake, city, country, continent, region in validator_stats:
            slot_count = slot_counts.get(identity_pubkey, 0)
            if activated_stake is None:
                activated_stake = 0.0
            if country:
                country_results[country]["count"] += 1
                country_results[country]["total_activated_stake"] += activated_stake
                country_results[country]["total_slots"] += slot_count
            if continent:
                continent_results[continent]["count"] += 1
                continent_results[continent]["total_activated_stake"] += activated_stake
                continent_results[continent]["total_slots"] += slot_count
            if region:
                region_results[region]["count"] += 1
                region_results[region]["total_activated_stake"] += activated_stake
                region_results[region]["total_slots"] += slot_count

        for results in [country_results, continent_results, region_results]:
            for area in results:
                if results[area]["count"] > 0:
                    results[area]["average_stake"] = results[area]["total_activated_stake"] / results[area]["count"]
                else:
                    results[area]["average_stake"] = 0.0
                results[area]["percent_stake"] = (results[area]["total_activated_stake"] / total_activated_stake) * 100 if total_activated_stake > 0 else 0.0
                results[area]["percent_slots"] = (results[area]["total_slots"] / total_slots) * 100 if total_slots > 0 else 0.0

        country_df = pd.DataFrame.from_dict(country_results, orient='index')
        continent_df = pd.DataFrame.from_dict(continent_results, orient='index')
        region_df = pd.DataFrame.from_dict(region_results, orient='index')
        
        # Check if DataFrames are empty
        for df_name, df in [("country_df", country_df), ("continent_df", continent_df), ("region_df", region_df)]:
            if df.empty:
                logging.warning(f"{df_name} is empty for epoch {epoch}")
                # Create a minimal valid DataFrame with required columns
                df = pd.DataFrame({
                    'count': [0], 
                    'total_activated_stake': [0.0], 
                    'total_slots': [0], 
                    'average_stake': [0.0], 
                    'percent_stake': [0.0], 
                    'percent_slots': [0.0]
                }, index=['Unknown'])
                if df_name == "country_df":
                    country_df = df
                elif df_name == "continent_df":
                    continent_df = df
                else:
                    region_df = df

        for df in [country_df, continent_df, region_df]:
            df.index.name = 'Area'
            # Check if 'total_slots' exists in columns
            if 'total_slots' in df.columns:
                df = df.sort_values(by="total_slots", ascending=False)
            else:
                logging.warning(f"'total_slots' column not found in DataFrame for epoch {epoch}")
            
            # Safely apply transformations with error checking
            if 'total_activated_stake' in df.columns:
                df['total_activated_stake'] = df['total_activated_stake'].apply(
                    lambda x: format_lamports_to_sol(x, 0) if x is not None else 0)
            
            if 'total_slots' in df.columns:
                df['total_slots'] = df['total_slots'].apply(
                    lambda x: f"{x:,}" if x is not None else "0")
            
            if 'average_stake' in df.columns:
                df['average_stake'] = df['average_stake'].apply(
                    lambda x: format_lamports_to_sol(x, 0) if x is not None else 0)
            
            if 'count' in df.columns:
                df['count'] = df['count'].apply(
                    lambda x: f"{x:,}" if x is not None else "0")
            
            if 'percent_stake' in df.columns:
                df['percent_stake'] = df['percent_stake'].apply(
                    lambda x: round(x, 2) if x is not None else 0.0)
            
            if 'percent_slots' in df.columns:
                df['percent_slots'] = df['percent_slots'].apply(
                    lambda x: round(x, 2) if x is not None else 0.0)

        countries = sorted(set(country_df.index))
        continents = sorted(set(continent_df.index))
        regions = sorted(set(region_df.index))

        try:
            country_color_map = get_persistent_color_map(countries)
            continent_color_map = get_color_map(continents)
            region_color_map = get_color_map(regions)
        except Exception as e:
            logging.error(f"Error creating color maps for epoch {epoch}: {str(e)}")
            # Create default color maps if there's an error
            country_color_map = {'Unknown': '#CCCCCC'}
            continent_color_map = {'Unknown': '#CCCCCC'}
            region_color_map = {'Unknown': '#CCCCCC'}

        def create_pie_chart(df, title, color_map, subplot_col):
            try:
                if 'percent_stake' not in df.columns:
                    logging.warning(f"'percent_stake' column not found in DataFrame for {title}")
                    return
                
                df_for_pie = df[df['percent_stake'] >= 0.0].sort_values(by='percent_stake', ascending=False)
                if df_for_pie.empty:
                    logging.warning(f"No data with percent_stake >= 0.0 for {title}")
                    return
                
                labels = df_for_pie.index
                sizes = df_for_pie['percent_stake']
                
                # Ensure all labels exist in color_map
                for label in labels:
                    if label not in color_map:
                        color_map[label] = '#CCCCCC'  # Default gray
                
                colors = [color_map[label] for label in labels]
                custom_labels = [f"{label} ({size:.2f}%)" for label, size in zip(labels, sizes)]

                text_position = 'inside' if subplot_col == 1 else 'outside'
                text_font = dict(size=10, color='white' if subplot_col == 1 else 'black')

                fig.add_trace(go.Pie(
                    values=sizes,
                    labels=labels,
                    text=custom_labels,
                    textposition=text_position,
                    textinfo='text',
                    hovertemplate='<b>%{label}</b><br>Percent Stake: %{value:.5f}%<br>Total Stake: %{customdata[0]:.0f} SOL<br>Total Slots: %{customdata[1]}',
                    customdata=df_for_pie[['total_activated_stake', 'total_slots']],
                    pull=[0.05] * len(labels),
                    marker=dict(colors=colors),
                    textfont=text_font
                ), row=1, col=subplot_col)
            except Exception as e:
                logging.error(f"Error creating pie chart for {title}: {str(e)}")
                import traceback
                logging.error(f"Pie chart traceback: {traceback.format_exc()}")

        try:
            fig = make_subplots(rows=1, cols=2, 
                                specs=[[{'type': 'domain'}, {'type': 'domain'}]],
                                subplot_titles=(f"Stake by Continent - Epoch {epoch}", f"Stake by Country - Epoch {epoch}"),
                                horizontal_spacing=0.1)

            create_pie_chart(continent_df, f"Stake by Continent - Epoch {epoch}", continent_color_map, 1)
            create_pie_chart(country_df, f"Stake by Country - Epoch {epoch}", country_color_map, 2)

            # Update layout with HTML title
            fig.update_layout(
                showlegend=False,
                title=dict(
                    text=f"Stake Distribution Overview - Epoch {epoch}",
                    y=0.95, 
                    x=0.5, 
                    xanchor='center', 
                    font=dict(size=16, weight='bold')
                ),
                images=[
                    dict(
                        source="https://trillium.so/images/fueled-by-trillium.png",
                        xref="paper", yref="paper",
                        x=0.5, y=-0.15,
                        sizex=0.2, sizey=0.1,  # Adjust size to fit, roughly matching previous text+icons
                        xanchor="center", yanchor="middle"
                    )
                ],
                height=900,
                width=1200,
                barmode='group',
                legend=dict(x=0.01, y=1.1, xanchor='left', yanchor='top', orientation='h', font=dict(size=10)),
                margin=dict(t=150, b=250, l=50, r=50)
            )

            filename = f'epoch{epoch}_stake_distribution_charts.png'
            fig.write_image(f"{filename}", scale=2)
            logger.info(f"*** file created - {filename}")

            if epoch == max_epoch:
                filename = 'stake_distribution_charts.png'
                fig.write_image(filename, scale=2)
                logger.info(f"*** file created - {filename}")
        except Exception as e:
            logging.error(f"Error creating or saving figure for epoch {epoch}: {str(e)}")
            import traceback
            logging.error(f"Figure traceback: {traceback.format_exc()}")

        # Safely write CSV files
        try:
            country_df.to_csv(f'epoch{epoch}_country_stats.csv', index=True)
            logging.info(f"country_df epoch{epoch}_country_stats.csv")
        except Exception as e:
            logging.error(f"Error writing country_df to CSV for epoch {epoch}: {e}")
            # Create an empty CSV file to avoid further errors
            pd.DataFrame().to_csv(f'epoch{epoch}_country_stats.csv')

        try:
            continent_df.to_csv(f'epoch{epoch}_continent_stats.csv', index=True)
            logging.info(f"continent_df epoch{epoch}_continent_stats.csv")
        except Exception as e:
            logging.error(f"Error writing continent_df to CSV for epoch {epoch}: {e}")
            pd.DataFrame().to_csv(f'epoch{epoch}_continent_stats.csv')

        try:
            region_df.to_csv(f'epoch{epoch}_region_stats.csv', index=True)
            logging.info(f"region_df epoch{epoch}_region_stats.csv")
        except Exception as e:
            logging.error(f"Error writing region_df to CSV for epoch {epoch}: {e}")
            pd.DataFrame().to_csv(f'epoch{epoch}_region_stats.csv')

        return country_df, continent_df, region_df
    
    except Exception as e:
        logging.error(f"Error in calculate_stake_statistics for epoch {epoch}: {str(e)}")
        logging.error(f"Exception details: {type(e).__name__}")
        import traceback
        logging.error(f"Traceback: {traceback.format_exc()}")
        
        # Create empty DataFrames
        empty_df = pd.DataFrame({
            'count': [0], 
            'total_activated_stake': [0], 
            'total_slots': [0], 
            'average_stake': [0], 
            'percent_stake': [0], 
            'percent_slots': [0]
        }, index=['Unknown'])
        empty_df.index.name = 'Area'
        
        # Write empty CSVs to prevent downstream errors
        empty_df.to_csv(f'epoch{epoch}_country_stats.csv', index=True)
        empty_df.to_csv(f'epoch{epoch}_continent_stats.csv', index=True)
        empty_df.to_csv(f'epoch{epoch}_region_stats.csv', index=True)
        
        return empty_df.copy(), empty_df.copy(), empty_df.copy()

def calculate_stake_statistics_metro(epoch, max_epoch, engine):
    try:
        query = """
        SELECT 
            identity_pubkey,
            activated_stake,
            metro,
            country,
            continent,
            region
        FROM validator_stats
        WHERE epoch = :epoch
            AND activated_stake != 0;
        """

        slot_query = """
        SELECT identity_pubkey, COUNT(block_slot)
        FROM leader_schedule
        WHERE epoch = :epoch
        GROUP BY identity_pubkey;
        """

        with engine.connect() as conn:
            logging.info(f"validator_stats for epoch {epoch} query")
            validator_stats = conn.execute(text(query), {"epoch": epoch}).fetchall()
            logging.info(f"block_slot_results for epoch {epoch} slot_query")
            block_slot_results = conn.execute(text(slot_query), {"epoch": epoch}).fetchall()

        total_slots = sum(slot_count for _, slot_count in block_slot_results)
        logging.info(f"total_slots for epoch {epoch} {total_slots}")
        
        if total_slots == 0:
            logging.warning(f"total_slots is zero for epoch {epoch}. Setting to 1 to avoid division by zero.")
            total_slots = 1
            
        total_activated_stake = sum(stats[1] for stats in validator_stats if stats[1] is not None)
        logging.info(f"total_activated_stake for epoch {epoch} {total_activated_stake}")
        
        if total_activated_stake == 0:
            logging.warning(f"total_activated_stake is zero for epoch {epoch}. Setting to 1 to avoid division by zero.")
            total_activated_stake = 1
        
        slot_counts = dict(block_slot_results)

        country_results = defaultdict(lambda: {"count": 0, "total_activated_stake": 0.0, "total_slots": 0, "average_stake": 0.0, "percent_stake": 0.0, "percent_slots": 0.0})
        metro_results = defaultdict(lambda: {"count": 0, "total_activated_stake": 0.0, "total_slots": 0, "average_stake": 0.0, "percent_stake": 0.0, "percent_slots": 0.0})

        for identity_pubkey, activated_stake, metro, country, continent, region in validator_stats:
            slot_count = slot_counts.get(identity_pubkey, 0)
            if activated_stake is None:
                activated_stake = 0.0
            if country:
                country_results[country]["count"] += 1
                country_results[country]["total_activated_stake"] += activated_stake
                country_results[country]["total_slots"] += slot_count
            if metro:
                metro_results[metro]["count"] += 1
                metro_results[metro]["total_activated_stake"] += activated_stake
                metro_results[metro]["total_slots"] += slot_count

        for results in [country_results, metro_results]:
            for area in results:
                if results[area]["count"] > 0:
                    results[area]["average_stake"] = results[area]["total_activated_stake"] / results[area]["count"]
                else:
                    results[area]["average_stake"] = 0.0
                results[area]["percent_stake"] = (results[area]["total_activated_stake"] / total_activated_stake) * 100 if total_activated_stake > 0 else 0.0
                results[area]["percent_slots"] = (results[area]["total_slots"] / total_slots) * 100 if total_slots > 0 else 0.0

        country_df = pd.DataFrame.from_dict(country_results, orient='index')
        metro_df = pd.DataFrame.from_dict(metro_results, orient='index')
        
        for df_name, df in [("country_df", country_df), ("metro_df", metro_df)]:
            if df.empty:
                logging.warning(f"{df_name} is empty for epoch {epoch}")
                df = pd.DataFrame({
                    'count': [0], 
                    'total_activated_stake': [0.0], 
                    'total_slots': [0], 
                    'average_stake': [0.0], 
                    'percent_stake': [0.0], 
                    'percent_slots': [0.0]
                }, index=['Unknown'])
                if df_name == "country_df":
                    country_df = df
                else:
                    metro_df = df

        for df in [country_df, metro_df]:
            df.index.name = 'Area'
            if 'total_slots' in df.columns:
                df = df.sort_values(by="total_slots", ascending=False)
            else:
                logging.warning(f"'total_slots' column not found in DataFrame for epoch {epoch}")
            
            if 'total_activated_stake' in df.columns:
                df['total_activated_stake'] = df['total_activated_stake'].apply(
                    lambda x: format_lamports_to_sol(x, 0) if x is not None else 0)
            if 'total_slots' in df.columns:
                df['total_slots'] = df['total_slots'].apply(
                    lambda x: f"{x:,}" if x is not None else "0")
            if 'average_stake' in df.columns:
                df['average_stake'] = df['average_stake'].apply(
                    lambda x: format_lamports_to_sol(x, 0) if x is not None else 0)
            if 'count' in df.columns:
                df['count'] = df['count'].apply(
                    lambda x: f"{x:,}" if x is not None else "0")
            if 'percent_stake' in df.columns:
                df['percent_stake'] = df['percent_stake'].apply(
                    lambda x: round(x, 2) if x is not None else 0.0)
            if 'percent_slots' in df.columns:
                df['percent_slots'] = df['percent_slots'].apply(
                    lambda x: round(x, 2) if x is not None else 0.0)

        countries = sorted(set(country_df.index))
        metros = sorted(set(metro_df.index))

        try:
            country_color_map = get_persistent_color_map(countries)
            metro_color_map = get_color_map(metros)
        except Exception as e:
            logging.error(f"Error creating color maps for epoch {epoch}: {str(e)}")
            country_color_map = {'Unknown': '#CCCCCC'}
            metro_color_map = {'Unknown': '#CCCCCC'}

        def create_pie_chart(df, title, color_map, subplot_col, limit=None):
            try:
                if 'percent_stake' not in df.columns:
                    logging.warning(f"'percent_stake' column not found in DataFrame for {title}")
                    return
                
                df_for_pie = df[df['percent_stake'] >= 0.5].sort_values(by='percent_stake', ascending=False) if subplot_col == 1 else df.sort_values(by='percent_stake', ascending=False).head(30)
                if df_for_pie.empty:
                    logging.warning(f"No data with percent_stake >= 0.5 for {title}")
                    return
                
                labels = df_for_pie.index
                sizes = df_for_pie['percent_stake']
                
                for label in labels:
                    if label not in color_map:
                        color_map[label] = '#CCCCCC'
                
                colors = [color_map[label] for label in labels]
                custom_labels = [f"{label} ({size:.1f}%)" for label, size in zip(labels, sizes)]

                text_position = 'inside' if subplot_col == 1 else 'outside'
                text_font = dict(size=10, color='white' if subplot_col == 1 else 'black')

                fig.add_trace(go.Pie(
                    values=sizes,
                    labels=labels,
                    text=custom_labels,
                    textposition=text_position,
                    textinfo='text',
                    hovertemplate='<b>%{label}</b><br>Percent Stake: %{value:.1f}%<br>Total Stake: %{customdata[0]:.0f} SOL<br>Total Slots: %{customdata[1]}',
                    customdata=df_for_pie[['total_activated_stake', 'total_slots']],
                    pull=[0.05] * len(labels),
                    marker=dict(colors=colors),
                    textfont=text_font
                ), row=1, col=subplot_col)
            except Exception as e:
                logging.error(f"Error creating pie chart for {title}: {str(e)}")
                import traceback
                logging.error(f"Pie chart traceback: {traceback.format_exc()}")

        try:
            fig = make_subplots(rows=1, cols=2, 
                                specs=[[{'type': 'domain'}, {'type': 'domain'}]],
                                subplot_titles=(f"Stake by Country - Epoch {epoch}", f"Stake by Metro (Top 30) - Epoch {epoch}"),
                                horizontal_spacing=0.1)

            create_pie_chart(country_df, f"Stake by Country - Epoch {epoch}", country_color_map, 1)
            create_pie_chart(metro_df, f"Stake by Metro - Epoch {epoch}", metro_color_map, 2, limit=30)

            fig.update_layout(
                showlegend=False,
                title=dict(
                    text=f"Stake Distribution Overview - Epoch {epoch}",
                    y=0.95, 
                    x=0.5, 
                    xanchor='center', 
                    font=dict(size=16, weight='bold')
                ),
                images=[
                    dict(
                        source="https://trillium.so/images/fueled-by-trillium.png",
                        xref="paper", yref="paper",
                        x=0.5, y=-0.15,
                        sizex=0.2, sizey=0.1,
                        xanchor="center", yanchor="middle"
                    )
                ],
                height=900,
                width=1200,
                barmode='group',
                legend=dict(x=0.01, y=1.1, xanchor='left', yanchor='top', orientation='h', font=dict(size=10)),
                margin=dict(t=150, b=250, l=50, r=50)
            )

            filename = f'epoch{epoch}_stake_distribution_charts_metro.png'
            fig.write_image(f"{filename}", scale=2)
            logger.info(f"*** file created - {filename}")

            if epoch == max_epoch:
                filename = 'stake_distribution_charts_metro.png'
                fig.write_image(filename, scale=2)
                logger.info(f"*** file created - {filename}")
        except Exception as e:
            logging.error(f"Error creating or saving figure for epoch {epoch}: {str(e)}")
            import traceback
            logging.error(f"Figure traceback: {traceback.format_exc()}")

        try:
            country_df.to_csv(f'epoch{epoch}_country_stats_metro.csv', index=True)
            logging.info(f"country_df epoch{epoch}_country_stats_metro.csv")
            metro_df.to_csv(f'epoch{epoch}_metro_stats_metro.csv', index=True)
            logging.info(f"metro_df epoch{epoch}_metro_stats_metro.csv")
        except Exception as e:
            logging.error(f"Error writing CSVs for epoch {epoch}: {e}")
            pd.DataFrame().to_csv(f'epoch{epoch}_country_stats_metro.csv')
            pd.DataFrame().to_csv(f'epoch{epoch}_metro_stats_metro.csv')

        return country_df, metro_df
    
    except Exception as e:
        logging.error(f"Error in calculate_stake_statistics_metro for epoch {epoch}: {str(e)}")
        logging.error(f"Exception details: {type(e).__name__}")
        import traceback
        logging.error(f"Traceback: {traceback.format_exc()}")
        
        empty_df = pd.DataFrame({
            'count': [0], 
            'total_activated_stake': [0], 
            'total_slots': [0], 
            'average_stake': [0], 
            'percent_stake': [0], 
            'percent_slots': [0]
        }, index=['Unknown'])
        empty_df.index.name = 'Area'
        
        empty_df.to_csv(f'epoch{epoch}_country_stats_metro.csv', index=True)
        empty_df.to_csv(f'epoch{epoch}_metro_stats_metro.csv', index=True)
        
        return empty_df.copy(), empty_df.copy()

def plot_votes_cast_metrics():
    file_path = "last_ten_epoch_aggregate_data.json"
    try:
        with open(file_path, 'r') as f:
            data = json.load(f)

        latest_epochs = data[::-1]
        epochs = [epoch['epoch'] for epoch in latest_epochs]
        avg_votes_cast = [epoch['avg_votes_cast'] for epoch in latest_epochs]
        median_votes_cast = [epoch['median_votes_cast'] for epoch in latest_epochs]

        fig = go.Figure()
        fig.add_trace(go.Bar(
            x=epochs, y=avg_votes_cast, name="Average Votes Cast",
            marker_color='#6A9F4F', width=0.35, offset=-0.175,
            text=[f"{v:,.0f}" for v in avg_votes_cast],
            textposition='inside',
            textfont=dict(color="white"),
            textangle=-90
        ))
        fig.add_trace(go.Bar(
            x=epochs, y=median_votes_cast, name="Median Votes Cast",
            marker_color='#4E89A7', width=0.35, offset=0.175,
            text=[f"{v:,.0f}" for v in median_votes_cast],
            textposition='inside',
            textfont=dict(color="white"),
            textangle=-90
        ))

        # Update layout with HTML title
        fig.update_layout(
            title=dict(
                text="Solana Epoch Votes Cast Metrics Overview",
                y=0.95, 
                x=0.5, 
                xanchor='center', 
                font=dict(size=16, weight='bold')
            ),xaxis=dict(
                title=dict(
                    text="Epoch",
                    font=dict(size=16, weight='bold')
                ),
                title_standoff=3 # Moves title up; increase for more distance
            ),
            images=[
                dict(
                    source="https://trillium.so/images/fueled-by-trillium.png",
                    xref="paper", yref="paper",
                    x=0.5, y=-0.1,
                    sizex=0.2, sizey=0.1,  # Adjust size to fit, roughly matching previous text+icons
                    xanchor="center", yanchor="middle"
                )
            ],
            height=900,
            width=1200,
            barmode='group',
            legend=dict(x=0.01, y=1.1, xanchor='left', yanchor='top', orientation='h', font=dict(size=10)),
            margin=dict(t=150, b=250, l=50, r=50)
        )

        filename = "votes_cast_metrics_chart.png"
        fig.write_image(filename, scale=2)
        logger.info(f"*** file created - {filename}")

    except FileNotFoundError:
        print(f"Error: The file {file_path} was not found.")
    except json.JSONDecodeError:
        print("Error: Could not decode JSON from the file.")

import pandas as pd
import re
from sqlalchemy import text
import plotly.graph_objects as go
from plotly.subplots import make_subplots

def plot_latency_and_consensus_charts(start_epoch, end_epoch, engine=None):
    if start_epoch == end_epoch:
        epoch_query = "SELECT DISTINCT epoch FROM validator_stats WHERE epoch <= :end_epoch ORDER BY epoch DESC LIMIT 10"
        with engine.connect() as conn:
            recent_epochs = pd.read_sql(text(epoch_query), engine, params={"end_epoch": end_epoch})
        epoch_range = recent_epochs['epoch'].tolist()
    else:
        epoch_range = list(range(start_epoch, end_epoch + 1))

    query = """
    SELECT 
        vs.epoch, 
        vs.version, 
        vs.activated_stake,
        vx.average_vl, 
        vx.average_llv, 
        vx.average_cv,
        vt.mean_vote_latency,
        vt.median_vote_latency,
        vt.vote_credits_rank,
        vt.avg_credit_per_voted_slot
    FROM validator_stats vs
    JOIN validator_xshin vx 
        ON vs.vote_account_pubkey = vx.vote_account_pubkey 
        AND vs.epoch = vx.epoch
    JOIN votes_table vt 
        ON vs.vote_account_pubkey = vt.vote_account_pubkey 
        AND vs.epoch = vt.epoch
    WHERE vs.epoch IN :epochs
    """
    with engine.connect() as conn:
        data = pd.read_sql(text(query), engine, params={"epochs": tuple(epoch_range)})

    data['version_digit'] = data['version'].apply(
        lambda x: re.match(r'[0-2]', str(x)).group() if re.match(r'[0-2]', str(x)) else None
    )
    data = data.dropna(subset=['version_digit'])
    data = data[data['version_digit'].isin(['0', '2'])]

    # Calculate top 30% activated stake per epoch for v2
    data['rank'] = data.groupby('epoch')['activated_stake'].rank(method='dense', ascending=False)
    data['is_top30'] = data.groupby('epoch')['rank'].transform(lambda x: x <= x.quantile(0.3))
    top30_data = data[(data['version_digit'] == '2') & (data['is_top30'])]

    v0_color = '#6A9F4F'
    v2_color = '#4E89A7'
    v2_top30_color = '#66B2FF'

    fig = make_subplots(rows=3, cols=1, vertical_spacing=0.1)

    # Subplot 1: Vote Latency
    latency_data = data.dropna(subset=['mean_vote_latency'])
    # Filter to include only positive values
    latency_data = latency_data[latency_data['mean_vote_latency'] > 0]
    grouped_latency_data = latency_data.groupby(['epoch', 'version_digit'])[['mean_vote_latency']].mean().reset_index()
    # Apply same filter to top30_data for consistency
    top30_latency = top30_data.dropna(subset=['mean_vote_latency'])
    top30_latency = top30_latency[top30_latency['mean_vote_latency'] > 0]
    top30_latency_data = top30_latency.groupby('epoch')[['mean_vote_latency']].mean().reset_index()
    for version in [('0', v0_color), ('2', v2_color), ('v2-Top30', v2_top30_color)]:
        version_digit, color = version
        subset = grouped_latency_data[grouped_latency_data['version_digit'] == version_digit] if version_digit != 'v2-Top30' else top30_latency_data
        if not subset.empty:
            offset = -0.25 if version_digit == '0' else (0 if version_digit == '2' else 0.25)
            fig.add_trace(go.Bar(
                x=subset['epoch'],
                y=subset['mean_vote_latency'],
                name="FrankenDancer 'FD' (v0)" if version_digit == '0' else "Agave (v2)" if version_digit == '2' else "Agave Top 30% by Stake (v2-Top30)",
                marker_color=color,
                width=0.25,
                offset=offset,
                text=[f"{round(val, 2):,.2f}" for val in subset['mean_vote_latency']],
                textposition='inside',
                textfont=dict(color="white", size=10),
                textangle=-90
            ), row=1, col=1)

    # Subplot 2: Vote Credits Rank
    llv_data = data.dropna(subset=['vote_credits_rank'])
    # Filter to include only positive values
    llv_data = llv_data[llv_data['vote_credits_rank'] > 0]
    grouped_llv_data = llv_data.groupby(['epoch', 'version_digit'])[['vote_credits_rank']].mean().reset_index()
    # Apply same filter to top30_data for consistency
    top30_llv = top30_data.dropna(subset=['vote_credits_rank'])
    top30_llv = top30_llv[top30_llv['vote_credits_rank'] > 0]
    top30_llv_data = top30_llv.groupby('epoch')[['vote_credits_rank']].mean().reset_index()
    for version in [('0', v0_color), ('2', v2_color), ('v2-Top30', v2_top30_color)]:
        version_digit, color = version
        subset = grouped_llv_data[grouped_llv_data['version_digit'] == version_digit] if version_digit != 'v2-Top30' else top30_llv_data
        if not subset.empty:
            offset = -0.25 if version_digit == '0' else (0 if version_digit == '2' else 0.25)
            fig.add_trace(go.Bar(
                x=subset['epoch'],
                y=subset['vote_credits_rank'],
                name="FrankenDancer 'FD' (v0)" if version_digit == '0' else "Agave (v2)" if version_digit == '2' else "Agave Top 30% by Stake (v2-Top30)",
                marker_color=color,
                width=0.25,
                offset=offset,
                showlegend=False,
                text=[f"{round(val):,}" for val in subset['vote_credits_rank']],
                textfont=dict(color="white", size=10),
                textangle=-90
            ), row=2, col=1)

    # Subplot 3: Consensus Votes
    cv_data = data.dropna(subset=['average_cv'])
    # Filter to include only positive values
    cv_data = cv_data[cv_data['average_cv'] > 0]
    grouped_cv_data = cv_data.groupby(['epoch', 'version_digit'])[['average_cv']].mean().reset_index()
    # Apply same filter to top30_data for consistency
    top30_cv = top30_data.dropna(subset=['average_cv'])
    top30_cv = top30_cv[top30_cv['average_cv'] > 0]
    top30_cv_data = top30_cv.groupby('epoch')[['average_cv']].mean().reset_index()
    for version in [('0', v0_color), ('2', v2_color), ('v2-Top30', v2_top30_color)]:
        version_digit, color = version
        subset = grouped_cv_data[grouped_cv_data['version_digit'] == version_digit] if version_digit != 'v2-Top30' else top30_cv_data
        if not subset.empty:
            offset = -0.25 if version_digit == '0' else (0 if version_digit == '2' else 0.25)
            fig.add_trace(go.Bar(
                x=subset['epoch'],
                y=subset['average_cv'],
                name="FrankenDancer 'FD' (v0)" if version_digit == '0' else "Agave (v2)" if version_digit == '2' else "Agave Top 30% by Stake (v2-Top30)",
                marker_color=color,
                width=0.25,
                offset=offset,
                showlegend=False,
                text=[f"{round(val * 100)}%" for val in subset['average_cv']],
                textposition='inside',
                textfont=dict(color="white", size=10),
                textangle=-90
            ), row=3, col=1)

    # Update layout without subtitle
    fig.update_layout(
        title=dict(
            text="Solana Validator Client Version - Latency & Consensus",
            y=0.95,
            x=0.5,
            xanchor='center',
            font=dict(size=16, weight='bold')
        ),
        images=[
            dict(
                source="https://trillium.so/images/fueled-by-trillium.png",
                xref="paper", yref="paper",
                x=0.5, y=-0.15,
                sizex=0.2, sizey=0.1,
                xanchor="center", yanchor="middle"
            )
        ],
        height=900,
        width=1200,
        barmode='group',
        legend=dict(x=0.01, y=1.1, xanchor='left', yanchor='top', orientation='h', font=dict(size=10)),
        margin=dict(t=150, b=250, l=50, r=50)
    )

    fig.update_xaxes(title_text="(smaller = better)", row=1, col=1)
    fig.update_xaxes(title_text="(smaller = better)", row=2, col=1)
    fig.update_xaxes(title_text="", row=3, col=1)
    fig.add_annotation(
        text="(larger = better)<br><b>Epoch</b>",
        xref="paper", yref="paper",
        x=0.5, y=-0.1,
        showarrow=False,
        font=dict(size=14),
        align="center"
    )
    
    fig.update_yaxes(title_text="Vote Latency (slots)", row=1, col=1)
    fig.update_yaxes(title_text="Vote Credits Rank", tickformat=",", row=2, col=1)
    fig.update_yaxes(title_text="Consensus Votes", tickformat=".0%", row=3, col=1)

    filename = "latency_and_consensus_charts.png"
    fig.write_image(filename, scale=2)
    logger.info(f"*** file created - {filename}")

def plot_epoch_comparison_charts(start_epoch, end_epoch, engine=None):
    if start_epoch == end_epoch:
        epoch_query = "SELECT DISTINCT epoch FROM validator_stats WHERE epoch <= :end_epoch ORDER BY epoch DESC LIMIT 10"
        with engine.connect() as conn:
            recent_epochs = pd.read_sql(text(epoch_query), engine, params={"end_epoch": end_epoch})
        epoch_range = recent_epochs['epoch'].tolist()
    else:
        epoch_range = list(range(start_epoch, end_epoch + 1))

    query = """
        SELECT epoch, version, activated_stake,
            avg_priority_fees_per_block, avg_mev_per_block, avg_signature_fees_per_block,
            avg_cu_per_block, avg_user_tx_per_block, avg_vote_tx_per_block
        FROM validator_stats
        WHERE epoch IN :epochs
        """

    with engine.connect() as conn:
        data = pd.read_sql(text(query), engine, params={"epochs": tuple(epoch_range)})

    data['version_digit'] = data['version'].apply(
        lambda x: re.match(r'[0-2]', str(x)).group() if re.match(r'[0-2]', str(x)) else None
    )
    data = data.dropna(subset=['version_digit'])
    sample_size = data.groupby(['epoch', 'version_digit']).size().unstack(fill_value=0)

    if DEBUG:
        print("The sample sizes by epoch are (Epoch, v0, v2):")
        for epoch in sample_size.index:
            v0 = sample_size.loc[epoch, '0'] if '0' in sample_size.columns else 0
            v2 = sample_size.loc[epoch, '2'] if '2' in sample_size.columns else 0
            print(f"{epoch}, {v0}, {v2}")

    data = data[data['version_digit'].isin(['0', '2'])]
    LAMPORTS_PER_SOL = 1_000_000_000

    # Define consistent colors
    v0_color = '#6A9F4F'  
    v2_color = '#4E89A7'  
    v2_top30_color = '#66B2FF'

    # Create subplots: 3 rows, 1 column
    fig = make_subplots(rows=3, cols=1, vertical_spacing=0.1)

    # Calculate top 30% activated stake per epoch for v2
    data['rank'] = data.groupby('epoch')['activated_stake'].rank(method='dense', ascending=False)
    data['is_top30'] = data.groupby('epoch')['rank'].transform(lambda x: x <= x.quantile(0.3))
    top30_data = data[(data['version_digit'] == '2') & (data['is_top30'])]
    
    # Subplot 1: Priority Fees + MEV (Row 1)
    chart1_data = data.dropna(subset=['avg_priority_fees_per_block', 'avg_mev_per_block'])
    # Filter to include only positive values
    chart1_data = chart1_data[
        (chart1_data['avg_priority_fees_per_block'] > 0) & 
        (chart1_data['avg_mev_per_block'] > 0)
    ]
    grouped_data1 = chart1_data.groupby(['epoch', 'version_digit'])[['avg_priority_fees_per_block', 'avg_mev_per_block']].mean().reset_index()
    # Apply same filter to top30_data for consistency
    top30_chart1 = top30_data.dropna(subset=['avg_priority_fees_per_block', 'avg_mev_per_block'])
    top30_chart1 = top30_chart1[
        (top30_chart1['avg_priority_fees_per_block'] > 0) & 
        (top30_chart1['avg_mev_per_block'] > 0)
    ]
    top30_grouped1 = top30_chart1.groupby('epoch')[['avg_priority_fees_per_block', 'avg_mev_per_block']].mean().reset_index()
    for version in [('0', v0_color), ('2', v2_color), ('v2-Top30', v2_top30_color)]: 
        version_digit, color = version
        subset = grouped_data1[grouped_data1['version_digit'] == version_digit] if version_digit != 'v2-Top30' else top30_grouped1
        if not subset.empty:
            priority_values = (subset['avg_priority_fees_per_block'] / LAMPORTS_PER_SOL).round(7)
            mev_values = (subset['avg_mev_per_block'] / LAMPORTS_PER_SOL).round(7)
            offset = -0.25 if version_digit == '0' else (0 if version_digit == '2' else 0.25)
            fig.add_trace(go.Bar(
                x=subset['epoch'], 
                y=priority_values, 
                name="FrankenDancer 'FD' (v0)" if version_digit == '0' else "Agave (v2)" if version_digit == '2' else "Agave Top 30% by Stake (v2-Top30)",
                marker_color=color,
                width=0.25, 
                offset=offset,
                text=[f"P: {val:,.3f}" for val in priority_values],
                textposition='inside',
                textfont=dict(color="white", size=10),
                textangle=-90
            ), row=1, col=1)

            fig.add_trace(go.Bar(
                x=subset['epoch'], 
                y=mev_values, 
                name="FrankenDancer 'FD' (v0)" if version_digit == '0' else "Agave (v2)" if version_digit == '2' else "Agave Top 30% by Stake (v2-Top30)",
                marker_color=color,
                base=priority_values,
                width=0.25, 
                offset=offset,
                showlegend=False,
                text=[f"M: {val:,.3f}" for val in mev_values],
                textposition='inside',
                textfont=dict(color="white", size=10),
                textangle=-90
            ), row=1, col=1)

    # Subplot 2: Transactions (User + Vote) (Row 2)
    chart2_data = data.dropna(subset=['avg_user_tx_per_block', 'avg_vote_tx_per_block'])
    # Filter to include only positive values
    chart2_data = chart2_data[
        (chart2_data['avg_user_tx_per_block'] > 0) & 
        (chart2_data['avg_vote_tx_per_block'] > 0)
    ]
    grouped_data2 = chart2_data.groupby(['epoch', 'version_digit'])[['avg_user_tx_per_block', 'avg_vote_tx_per_block']].mean().reset_index()
    # Apply same filter to top30_data for consistency
    top30_chart2 = top30_data.dropna(subset=['avg_user_tx_per_block', 'avg_vote_tx_per_block'])
    top30_chart2 = top30_chart2[
        (top30_chart2['avg_user_tx_per_block'] > 0) & 
        (top30_chart2['avg_vote_tx_per_block'] > 0)
    ]
    top30_grouped2 = top30_chart2.groupby('epoch')[['avg_user_tx_per_block', 'avg_vote_tx_per_block']].mean().reset_index()
    for version in [('0', v0_color), ('2', v2_color), ('v2-Top30', v2_top30_color)]: 
        version_digit, color = version
        subset = grouped_data2[grouped_data2['version_digit'] == version_digit] if version_digit != 'v2-Top30' else top30_grouped2
        if not subset.empty:
            offset = -0.25 if version_digit == '0' else (0 if version_digit == '2' else 0.25)
            fig.add_trace(go.Bar(
                x=subset['epoch'], 
                y=subset['avg_user_tx_per_block'], 
                name="FrankenDancer 'FD' (v0)" if version_digit == '0' else "Agave (v2)" if version_digit == '2' else "Agave Top 30% by Stake (v2-Top30)",
                marker_color=color,
                width=0.25, 
                offset=offset,
                showlegend=False,
                text=[f"U: {val:,.0f}" for val in subset['avg_user_tx_per_block']],
                textposition='inside',
                textfont=dict(color="white", size=10),
                textangle=-90
            ), row=2, col=1)

            fig.add_trace(go.Bar(
                x=subset['epoch'], 
                y=subset['avg_vote_tx_per_block'], 
                name="FrankenDancer 'FD' (v0)" if version_digit == '0' else "Agave (v2)" if version_digit == '2' else "Agave Top 30% by Stake (v2-Top30)",
                marker_color=color,
                base=subset['avg_user_tx_per_block'],
                width=0.25, 
                offset=offset,
                showlegend=False,
                text=[f"V: {val:,.0f}" for val in subset['avg_vote_tx_per_block']],
                textposition='inside',
                textfont=dict(color="white", size=10),
                textangle=-90
            ), row=2, col=1)

    # Subplot 3: Compute Units (Row 3)
    chart3_data = data.dropna(subset=['avg_cu_per_block'])
    # Filter to include only positive values
    chart3_data = chart3_data[chart3_data['avg_cu_per_block'] > 0]
    grouped_data3 = chart3_data.groupby(['epoch', 'version_digit'])[['avg_cu_per_block']].mean().reset_index()
    # Apply same filter to top30_data for consistency
    top30_chart3 = top30_data.dropna(subset=['avg_cu_per_block'])
    top30_chart3 = top30_chart3[top30_chart3['avg_cu_per_block'] > 0]
    top30_grouped3 = top30_chart3.groupby('epoch')[['avg_cu_per_block']].mean().reset_index()
    for version in [('0', v0_color), ('2', v2_color), ('v2-Top30', v2_top30_color)]: 
        version_digit, color = version
        subset = grouped_data3[grouped_data3['version_digit'] == version_digit] if version_digit != 'v2-Top30' else top30_grouped3
        if not subset.empty:
            offset = -0.25 if version_digit == '0' else (0 if version_digit == '2' else 0.25)
            fig.add_trace(go.Bar(
                x=subset['epoch'], 
                y=subset['avg_cu_per_block'], 
                name="FrankenDancer 'FD' (v0)" if version_digit == '0' else "Agave (v2)" if version_digit == '2' else "Agave Top 30% by Stake (v2-Top30)",
                marker_color=color,
                width=0.25, 
                offset=offset,
                showlegend=False,
                text=[f"{val:,.0f}" for val in subset['avg_cu_per_block']],
                textposition='inside',
                textfont=dict(color="white", size=10),
                textangle=-90
            ), row=3, col=1)

    # Update layout without subtitle
    fig.update_layout(
        title=dict(
            text="Solana Validator Client Version - Average Per-Block Metrics",
            y=0.95, 
            x=0.5, 
            xanchor='center', 
            font=dict(size=16, weight='bold')
        ),
        images=[
            dict(
                source="https://trillium.so/images/fueled-by-trillium.png",
                xref="paper", yref="paper",
                x=0.5, y=-0.15,
                sizex=0.2, sizey=0.1,
                xanchor="center", yanchor="middle"
            )
        ],
        height=900,
        width=1200,
        barmode='group',
        legend=dict(x=0.01, y=1.1, xanchor='left', yanchor='top', orientation='h', font=dict(size=10)),
        margin=dict(t=150, b=250, l=50, r=50)
    )

    fig.update_xaxes(title_text="<b>Epoch</b>", row=3, col=1)
    fig.update_yaxes(title_text="Prio Fees + MEV", row=1, col=1)
    fig.update_yaxes(title_text="TX Count", row=2, col=1)
    fig.update_yaxes(title_text="Compute Units", row=3, col=1)

    filename = "epoch_comparison_charts.png"
    fig.write_image(filename, scale=2)
    logger.info(f"*** file created - {filename}")

def plot_epoch_metrics_with_stake_colors():
    file_path = "last_ten_epoch_aggregate_data.json"
    try:
        with open(file_path, 'r') as f:
            data = json.load(f)

        latest_epochs = data
        epochs = [epoch['epoch'] for epoch in latest_epochs]
        priority_fees = [epoch['total_validator_priority_fees'] or 0 for epoch in latest_epochs]  # Replace None with 0
        signature_fees = [epoch['total_validator_signature_fees'] or 0 for epoch in latest_epochs]  # Replace None with 0
        # Combine validator and delegator inflation rewards for total inflation rewards
        inflation_rewards = [
            (epoch['total_validator_inflation_rewards'] or 0) + (epoch['total_delegator_inflation_rewards'] or 0)
            for epoch in latest_epochs
        ]
        mev_earned = [epoch['total_mev_earned'] or 0 for epoch in latest_epochs]  # Replace None with 0

        colors = ['#6A9F4F', '#4E89A7', '#A77A9F', '#66A7B2'] 

        fig = go.Figure()
        fig.add_trace(go.Bar(
            x=epochs, y=priority_fees, name="Total Priority Fees",
            marker_color=colors[0], 
            text=[f"{v:,.0f}" for v in priority_fees], 
            textposition='inside',
            textfont=dict(color="white")  # Set label color to white
        ))
        fig.add_trace(go.Bar(
            x=epochs, y=signature_fees, name="Total Signature Fees",
            marker_color=colors[1], 
            base=priority_fees, 
            text=[f"{v:,.0f}" for v in signature_fees], 
            textposition='inside',
            textfont=dict(color="white")  # Set label color to white
        ))
        fig.add_trace(go.Bar(
            x=epochs, y=mev_earned, name="Total MEV Earned",
            marker_color=colors[2], 
            base=[i+j for i,j in zip(priority_fees, signature_fees)],
            text=[f"{v:,.0f}" for v in mev_earned], 
            textposition='inside',
            textfont=dict(color="white")  # Set label color to white
        ))
        fig.add_trace(go.Bar(
            x=epochs, y=inflation_rewards, name="Total Inflation Rewards",
            marker_color=colors[3], 
            base=[i+j+k for i,j,k in zip(priority_fees, signature_fees, mev_earned)],
            text=[f"{v:,.0f}" for v in inflation_rewards], 
            textposition='inside',
            textfont=dict(color="white")  # Set label color to white
        ))

        # Calculate grand totals for each bar
        totals = [p + s + m + i for p, s, m, i in zip(priority_fees, signature_fees, mev_earned, inflation_rewards)]
        
        # Create a list to hold all annotations
        annotations = []

        # Add annotations for the grand totals above each bar
        for i, total in enumerate(totals):
            annotations.append(
                dict(
                    x=epochs[i], 
                    y=total, 
                    text=f"{total:,.0f}", 
                    showarrow=False,
                    yshift=10,  # Adjust this value to position the text just above the bar
                    font=dict(size=12, color="black", family="Arial", weight='bold'),
                    bgcolor="white",  # Optional: Adds a white background for better visibility
                    bordercolor="black",  # Optional: Adds a border around the text
                    borderwidth=1  # Optional: Width of the border
                )
            )

        # Update the layout with the combined annotations and image
        fig.update_layout(
            title=dict(
                text="Solana Epoch Metrics Overview",
                y=0.95, x=0.5, xanchor='center', font=dict(size=14, weight='bold', color="#333333")
            ),
            barmode='stack',
            xaxis_title="<b>Epoch</b>",
            yaxis_title="Value (SOL)",
            legend=dict(
                x=0.01, y=1.1, xanchor='left', yanchor='top', orientation='h', font=dict(size=10)
            ),
            margin=dict(t=150, b=250, l=50, r=50),
            height=900,
            width=1200,
            annotations=annotations,  # Use the combined annotations list
            images=[
                dict(
                    source="https://trillium.so/images/fueled-by-trillium.png",
                    xref="paper", yref="paper",
                    x=0.5, y=-0.15,
                    sizex=0.2, sizey=0.1,
                    xanchor="center", yanchor="middle"
                )
            ]
        )

        filename = "epoch_metrics_chart.png"
        fig.write_image(filename, scale=2)
        logger.info(f"*** file created - {filename}")

    except FileNotFoundError:
        print(f"Error: The file {file_path} was not found.")
    except json.JSONDecodeError:
        print("Error: Could not decode JSON from the file.")
        
def main(start_epoch=None, end_epoch=None):
    engine = create_engine(
        f"postgresql+psycopg2://{db_params['user']}@{db_params['host']}:{db_params['port']}/{db_params['database']}?sslmode={db_params['sslmode']}"
    )
    
    min_epoch, max_epoch = get_min_max_epochs(engine)
    if DEBUG:
        print("\nAvailable epoch range:")
        print(f"Minimum epoch: {min_epoch}")
        print(f"Maximum epoch: {max_epoch}")
        print()

    if start_epoch is not None and end_epoch is not None:
        if not (min_epoch <= start_epoch <= end_epoch <= max_epoch):
            print(f"Error: Epochs {start_epoch} and {end_epoch} must be between {min_epoch} and {max_epoch}, with start <= end.")
            sys.exit(1)
    else:
        if end_epoch is None:
            while True:
                end_epoch_input = input(f"Enter end epoch (default is {max_epoch}, press Enter for default): ").strip()
                if end_epoch_input == "":
                    end_epoch = max_epoch
                    break
                try:
                    end_epoch = int(end_epoch_input)
                    if min_epoch <= end_epoch <= max_epoch:
                        break
                    else:
                        print(f"Please enter a value between {min_epoch} and {max_epoch}.")
                except ValueError:
                    print("Please enter a valid integer.")

        default_start_epoch = end_epoch if start_epoch is None else start_epoch
        if start_epoch is None:
            while True:
                start_epoch_input = input(f"Enter start epoch (default is {default_start_epoch}, press Enter for default): ").strip()
                if start_epoch_input == "":
                    start_epoch = default_start_epoch
                    break
                try:
                    start_epoch = int(start_epoch_input)
                    if min_epoch <= start_epoch <= end_epoch:
                        break
                    else:
                        print(f"Please enter a value between {min_epoch} and {end_epoch}.")
                except ValueError:
                    print("Please enter a valid integer.")

    epochs = range(start_epoch, end_epoch + 1)
    missing_data_epochs = []

    for epoch in epochs:
        print(f"Processing epoch: {epoch}")
        try:
            logging.info(f" get_validator_stats for epoch {epoch}")
            validator_stats = get_validator_stats(epoch, engine)
            logging.info(f"write_validator_stats_to_json for epoch {epoch}")
            write_validator_stats_to_json(epoch, validator_stats)
            epoch_aggregate_data = get_epoch_aggregate_data(epoch, engine)
            if epoch_aggregate_data is not None:
                logging.info(f"write_epoch_aggregate_data_to_json for epoch {epoch}")
                write_epoch_aggregate_data_to_json(epoch, epoch_aggregate_data)
            else:
                logging.warning(f"Skipping epoch_aggregate_data for epoch {epoch} due to missing data")
                missing_data_epochs.append(epoch)

            logging.info(f"country_df - calculate_stake_statistics for epoch {epoch}")
            country_df, continent_df, region_df = calculate_stake_statistics(epoch, max_epoch, engine)
            logging.info(f"country_df epoch{epoch}_country_stats.csv")
            country_df.to_csv(f'epoch{epoch}_country_stats.csv', index=True)
            logging.info(f"country_df epoch{epoch}_continent_stats.csv")
            continent_df.to_csv(f'epoch{epoch}_continent_stats.csv', index=True)
            logging.info(f"country_df epoch{epoch}_region_stats.csv")
            region_df.to_csv(f'epoch{epoch}_region_stats.csv', index=True)

            logging.info(f"calculate_stake_statistics_metro for epoch {epoch}")
            country_df_metro, metro_df = calculate_stake_statistics_metro(epoch, max_epoch, engine)
            logging.info(f"country_df epoch{epoch}_country_stats_metro.csv")
            country_df_metro.to_csv(f'epoch{epoch}_country_stats_metro.csv', index=True)
            logging.info(f"metro_df epoch{epoch}_metro_stats_metro.csv")
            metro_df.to_csv(f'epoch{epoch}_metro_stats_metro.csv', index=True)

        except Exception as e:
            logging.error(f"93_build_leaderboard_json.py Failed to process epoch {epoch}: {str(e)}")

    if max_epoch not in epochs:
        try:
            print(f"Generating stake distribution chart for max epoch: {max_epoch}")
            country_df, continent_df, region_df = calculate_stake_statistics(max_epoch, max_epoch, engine)
            country_df_metro, metro_df = calculate_stake_statistics_metro(max_epoch, max_epoch, engine)
            print(f"Stake distribution chart for max epoch {max_epoch} has been generated.")
        except Exception as e:
            logging.error(f"Failed to generate stake distribution chart for max epoch {max_epoch}: {str(e)}")

    if missing_data_epochs:
        logging.warning(f"Missing epoch_aggregate_data for epochs: {missing_data_epochs}")

    generate_last_ten_epochs_data(end_epoch, engine)
    generate_ten_epoch_validator_rewards(end_epoch, engine)
    generate_ten_epoch_aggregate_data(end_epoch, engine)
    generate_weighted_average_validator_rewards(end_epoch, engine)

    plot_epoch_metrics_with_stake_colors()
    plot_epoch_comparison_charts(start_epoch, end_epoch, engine)
    plot_latency_and_consensus_charts(start_epoch, end_epoch, engine)
    plot_votes_cast_metrics()

    print("Processing complete.")

if __name__ == '__main__':
    if len(sys.argv) > 2:
        try:
            start_epoch = int(sys.argv[1])
            end_epoch = int(sys.argv[2])
            main(start_epoch, end_epoch)
        except ValueError:
            print("Error: Please provide valid integer epochs as command-line arguments (start_epoch end_epoch).")
            main()
    elif len(sys.argv) == 2:
        try:
            end_epoch = int(sys.argv[1])
            main(end_epoch=end_epoch)
        except ValueError:
            print("Error: Please provide a valid integer epoch as a command-line argument.")
            main()
    else:
        main()