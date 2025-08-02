def get_validator_stats(epoch, engine):
    # The query includes the slot duration statistics fields and metro/client_type from validator_stats
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

        if DEBUG and record['identity_pubkey'] == '5pPRHniefFjkiaArbGX3Y8NUysJmQ9tMZg3FrFGwHzSm':
            print(f"Debug - Epoch: {record['epoch']}, "
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

        # Format slot duration fields
        duration_fields = [
            'slot_duration_min',
            'slot_duration_max',
            'slot_duration_mean',
            'slot_duration_median',
            'slot_duration_stddev',
            'slot_duration_confidence_interval_lower_ms',
            'slot_duration_confidence_interval_upper_ms'
        ]
        for field in duration_fields:
            if field in record and record[field] is not None:
                # Convert to float to handle Decimal from numeric type, then to milliseconds
                milliseconds = float(record[field]) / 1000000.0 if field in ['slot_duration_min', 'slot_duration_max', 'slot_duration_mean', 'slot_duration_median', 'slot_duration_stddev'] else float(record[field])
                # Format to 5 decimal places and update the record
                record[field] = f"{milliseconds:.5f}"

        # Format p-value to 7 decimal places or "N/A"
        if 'slot_duration_p_value' in record and record['slot_duration_p_value'] is not None:
            record['slot_duration_p_value'] = f"{record['slot_duration_p_value']:.7f}"
        else:
            record['slot_duration_p_value'] = "N/A"

        # Map client_type to string
        if 'client_type' in record:
            record['client_type'] = CLIENT_TYPE_MAP.get(record['client_type'], 'Unknown')

        if epoch > 0:  # was missing active stake prior to epoch 632 -- we have it now
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