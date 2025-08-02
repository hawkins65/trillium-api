
                WITH validator_rewards AS (
                    SELECT
                        identity_pubkey,
                        epoch,
                        SUM(COALESCE(rewards, 0)) AS rewards_total,
                        AVG(COALESCE(rewards, 0)) AS rewards_average,
                        COUNT(*) AS blocks_produced,
                        SUM(total_user_tx) AS user_tx_included_in_blocks,
                        SUM(total_vote_tx) AS vote_tx_included_in_blocks,
                        SUM(total_cu) AS cu,
                        SUM(total_tx) AS tx_included_in_blocks,
                        SUM(total_signatures) AS signatures,
                        SUM(total_fees) AS total_block_rewards_before_burn,
                        SUM(total_validator_signature_fees) AS validator_signature_fees,
                        SUM(total_validator_priority_fees) AS validator_priority_fees,
                        SUM(total_validator_fees) AS total_block_rewards_after_burn
                    FROM validator_data
                    WHERE epoch = 752 AND reward_type = 'Fee'
                    GROUP BY identity_pubkey, epoch
                ),
                epoch_total_stake AS (
                    SELECT SUM(activated_stake) AS total_stake
                    FROM validator_stats
                    WHERE epoch = 752
                )
                INSERT INTO validator_stats (
                    identity_pubkey,
                    epoch,
                    rewards,
                    avg_rewards_per_block,
                    blocks_produced,
                    user_tx_included_in_blocks,
                    vote_tx_included_in_blocks,
                    cu,
                    tx_included_in_blocks,
                    signatures,
                    total_block_rewards_before_burn,
                    validator_signature_fees,
                    validator_priority_fees,
                    total_block_rewards_after_burn,
                    avg_mev_per_block,
                    mev_to_validator,
                    mev_to_jito_block_engine,
                    mev_to_jito_tip_router,
                    avg_cu_per_block,
                    avg_user_tx_per_block,
                    avg_vote_tx_per_block,
                    avg_priority_fees_per_block,
                    avg_signature_fees_per_block,
                    avg_tx_per_block,
                    vote_cost,
                    stake_percentage
                )
                SELECT
                    vr.identity_pubkey,
                    vr.epoch,
                    vr.rewards_total,
                    CASE WHEN vr.blocks_produced > 0 THEN vr.rewards_total / vr.blocks_produced ELSE 0 END,
                    vr.blocks_produced,
                    vr.user_tx_included_in_blocks,
                    vr.vote_tx_included_in_blocks,
                    vr.cu,
                    vr.tx_included_in_blocks,
                    vr.signatures,
                    vr.total_block_rewards_before_burn,
                    vr.validator_signature_fees,
                    vr.validator_priority_fees,
                    vr.total_block_rewards_after_burn,
                    CASE WHEN vr.blocks_produced > 0 THEN COALESCE(vs.mev_earned, 0) / vr.blocks_produced ELSE 0 END,
                    COALESCE(vs.mev_earned, 0) * COALESCE(vs.mev_commission, 0) / 10000,
                    CASE 
                        WHEN vr.epoch < 752 THEN COALESCE(vs.mev_earned, 0) / 0.95  -- Includes 751
                        WHEN vr.epoch > 751 THEN COALESCE(vs.mev_earned, 0) / 0.97
                        ELSE NULL
                    END,
                    CASE
                        WHEN vr.epoch > 751 THEN COALESCE(vs.mev_earned, 0) / 0.97  -- Excludes 751
                        ELSE NULL
                    END,
                    CASE WHEN vr.blocks_produced > 0 THEN vr.cu / vr.blocks_produced ELSE 0 END,
                    CASE WHEN vr.blocks_produced > 0 THEN vr.user_tx_included_in_blocks / vr.blocks_produced ELSE 0 END,
                    CASE WHEN vr.blocks_produced > 0 THEN vr.vote_tx_included_in_blocks / vr.blocks_produced ELSE 0 END,
                    CASE WHEN vr.blocks_produced > 0 THEN vr.validator_priority_fees / vr.blocks_produced ELSE 0 END,
                    CASE WHEN vr.blocks_produced > 0 THEN vr.validator_signature_fees / vr.blocks_produced ELSE 0 END,
                    vr.tx_included_in_blocks / NULLIF(vr.blocks_produced, 0),
                    COALESCE(vs.votes_cast, 0) * 5000,
                    (COALESCE(vs.activated_stake, 0)::float / NULLIF(ets.total_stake, 0)) * 100
                FROM validator_rewards AS vr
                CROSS JOIN epoch_total_stake AS ets
                LEFT JOIN validator_stats AS vs ON vr.identity_pubkey = vs.identity_pubkey AND vr.epoch = vs.epoch
                ON CONFLICT (identity_pubkey, epoch) DO UPDATE SET
                    rewards = EXCLUDED.rewards,
                    avg_rewards_per_block = EXCLUDED.avg_rewards_per_block,
                    blocks_produced = EXCLUDED.blocks_produced,
                    user_tx_included_in_blocks = EXCLUDED.user_tx_included_in_blocks,
                    vote_tx_included_in_blocks = EXCLUDED.vote_tx_included_in_blocks,
                    cu = EXCLUDED.cu,
                    tx_included_in_blocks = EXCLUDED.tx_included_in_blocks,
                    signatures = EXCLUDED.signatures,
                    total_block_rewards_before_burn = EXCLUDED.total_block_rewards_before_burn,
                    validator_signature_fees = EXCLUDED.validator_signature_fees,
                    validator_priority_fees = EXCLUDED.validator_priority_fees,
                    total_block_rewards_after_burn = EXCLUDED.total_block_rewards_after_burn,
                    avg_mev_per_block = EXCLUDED.avg_mev_per_block,
                    mev_to_validator = EXCLUDED.mev_to_validator,
                    mev_to_jito_block_engine = EXCLUDED.mev_to_jito_block_engine,
                    mev_to_jito_tip_router = EXCLUDED.mev_to_jito_tip_router,
                    avg_cu_per_block = EXCLUDED.avg_cu_per_block,
                    avg_user_tx_per_block = EXCLUDED.avg_user_tx_per_block,
                    avg_vote_tx_per_block = EXCLUDED.avg_vote_tx_per_block,
                    avg_priority_fees_per_block = EXCLUDED.avg_priority_fees_per_block,
                    avg_signature_fees_per_block = EXCLUDED.avg_signature_fees_per_block,
                    avg_tx_per_block = EXCLUDED.avg_tx_per_block,
                    vote_cost = EXCLUDED.vote_cost,
                    stake_percentage = EXCLUDED.stake_percentage;
                