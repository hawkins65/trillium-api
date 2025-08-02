WITH previous_stake AS (
    SELECT 
        vs_curr.vote_account_pubkey,
        vs_curr.epoch,
        COALESCE((
            SELECT vs_prev.activated_stake 
            FROM validator_stats vs_prev 
            WHERE vs_prev.epoch = vs_curr.epoch - 1 
            AND vs_prev.vote_account_pubkey = vs_curr.vote_account_pubkey
        ), 0) AS previous_activated_stake
    FROM 
        validator_stats vs_curr
    WHERE 
        vs_curr.epoch = 784
        AND vs_curr.vote_account_pubkey = 'gaToR246dheK1DGAMEqxMdBJZwU4qFyt7DzhSwAHFWF'
)
SELECT 
    vs.epoch, 
    vs.vote_account_pubkey, 
    COALESCE(vs.activated_stake, 0) AS current_activated_stake,
    ps.previous_activated_stake,
    COALESCE(vs.delegator_inflation_reward, 0) AS delegator_inflation_reward,
    COALESCE(vs.validator_inflation_reward, 0) AS validator_inflation_reward,
    COALESCE(vs.mev_earned, 0) AS mev_earned,
    COALESCE(vs.mev_to_validator, 0) AS mev_to_validator,
    COALESCE(vs.mev_to_jito_tip_router, 0) AS mev_to_jito_tip_router,
    COALESCE(vs.rewards, 0) AS rewards,
    COALESCE(ead.epochs_per_year, 0) AS epochs_per_year,
    vs.delegator_inflation_apy,
    vs.delegator_compound_inflation_apy,
    vs.delegator_mev_apy,
    vs.delegator_compound_mev_apy,
    vs.delegator_total_apy,
    vs.delegator_compound_total_apy,
    vs.validator_inflation_apy,
    vs.validator_compound_inflation_apy,
    vs.validator_mev_apy,
    vs.validator_compound_mev_apy,
    vs.validator_block_rewards_apy,
    vs.validator_compound_block_rewards_apy,
    vs.validator_total_apy,
    vs.validator_compound_total_apy,
    vs.total_overall_apy,
    vs.compound_overall_apy
FROM 
    validator_stats vs
JOIN 
    epoch_aggregate_data ead
    ON vs.epoch = ead.epoch
JOIN 
    previous_stake ps
    ON vs.vote_account_pubkey = ps.vote_account_pubkey
    AND vs.epoch = ps.epoch
WHERE 
    vs.epoch = 783
    AND vs.vote_account_pubkey = 'gaToR246dheK1DGAMEqxMdBJZwU4qFyt7DzhSwAHFWF';