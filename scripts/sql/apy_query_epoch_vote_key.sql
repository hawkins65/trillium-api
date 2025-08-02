\copy (
    SELECT 
        TO_CHAR(vs.activated_stake / 1000000000, 'FM999,999,999,990.00000') AS activated_stake_sol,
        vs.epoch,
        TO_CHAR(ea.epochs_per_year, 'FM999,999,990.00000') AS epochs_per_year,
        TO_CHAR(vs.inflation_reward / 1000000000, 'FM999,999,999,990.00000') AS inflation_reward_sol,
        TO_CHAR(vs.mev_earned / 1000000000, 'FM999,999,999,990.00000') AS mev_earned_sol,
        TO_CHAR(vs.mev_to_validator / 1000000000, 'FM999,999,999,990.00000') AS mev_to_validator_sol,
        TO_CHAR(vs.rewards / 1000000000, 'FM999,999,999,990.00000') AS rewards_sol,
        vs.vote_account_pubkey,
        vs.commission,
        TO_CHAR(vs.current_total_apy, 'FM999,999,990.00000') AS current_total_apy,
        TO_CHAR(vs.compound_total_apy, 'FM999,999,990.00000') AS compound_total_apy,
        TO_CHAR(vs.validator_total_apy, 'FM999,999,990.00000') AS validator_total_apy,
        TO_CHAR(vs.validator_compound_total_apy, 'FM999,999,990.00000') AS validator_compound_total_apy,
        TO_CHAR(vs.current_inflation_apy, 'FM999,999,990.00000') AS current_inflation_apy,
        TO_CHAR(vs.compound_inflation_apy, 'FM999,999,990.00000') AS compound_inflation_apy,
        TO_CHAR(vs.validator_inflation_apy, 'FM999,999,990.00000') AS validator_inflation_apy,
        TO_CHAR(vs.validator_compound_inflation_apy, 'FM999,999,990.00000') AS validator_compound_inflation_apy,
        TO_CHAR(vs.current_mev_apy, 'FM999,999,990.00000') AS current_mev_apy,
        TO_CHAR(vs.compound_mev_apy, 'FM999,999,990.00000') AS compound_mev_apy,
        TO_CHAR(vs.validator_mev_apy, 'FM999,999,990.00000') AS validator_mev_apy,
        TO_CHAR(vs.validator_compound_mev_apy, 'FM999,999,990.00000') AS validator_compound_mev_apy,
        TO_CHAR(vs.current_block_rewards_apy, 'FM999,999,990.00000') AS current_block_rewards_apy,
        TO_CHAR(vs.compound_block_rewards_apy, 'FM999,999,990.00000') AS compound_block_rewards_apy,
        TO_CHAR(vs.validator_block_rewards_apy, 'FM999,999,990.00000') AS validator_block_rewards_apy,
        TO_CHAR(vs.validator_compound_block_rewards_apy, 'FM999,999,990.00000') AS validator_compound_block_rewards_apy
    FROM 
        validator_stats vs
    LEFT JOIN 
        epoch_aggregate_data ea
    ON 
        vs.epoch = ea.epoch
    WHERE 
        vs.vote_account_pubkey = 'DdCNGDpP7qMgoAy6paFzhhak2EeyCZcgjH7ak5u5v28m'
        AND vs.activated_stake >= 100 / 1e9
    ORDER BY 
        vs.epoch
) TO '/home/smilax/block-production/api/kiln1.csv' CSV HEADER;
