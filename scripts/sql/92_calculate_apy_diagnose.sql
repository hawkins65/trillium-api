WITH previous_stake AS (
    SELECT 
        vote_account_pubkey,
        epoch,
        COALESCE((
            SELECT MAX(activated_stake) 
            FROM validator_stats vs_prev 
            WHERE vs_prev.epoch = vs_curr.epoch - 1 
            AND vs_prev.vote_account_pubkey = vs_curr.vote_account_pubkey
        ), 0) AS previous_activated_stake
    FROM 
        validator_stats vs_curr
    WHERE 
        epoch = 785
)
SELECT 
    vs.epoch, 
    vs.vote_account_pubkey, 
    COALESCE(vs.activated_stake, 0) / 1000000000.0 AS current_activated_stake_sol,
    ps.previous_activated_stake / 1000000000.0 AS previous_activated_stake_sol,
    COALESCE(ead.epochs_per_year, 0) AS epochs_per_year,
    COALESCE(vs.commission, 0) AS commission,
    COALESCE(vs.mev_commission, 0) AS mev_commission,
    COALESCE(vs.delegator_inflation_reward, 0) / 1000000000.0 AS delegator_inflation_reward_sol,
    COALESCE(vs.validator_inflation_reward, 0) / 1000000000.0 AS validator_inflation_reward_sol,
    COALESCE(vs.mev_earned, 0) / 1000000000.0 AS mev_earned_sol,
    COALESCE(vs.mev_to_validator, 0) / 1000000000.0 AS mev_to_validator_sol,
    COALESCE(vs.mev_to_jito_tip_router, 0) / 1000000000.0 AS mev_to_jito_tip_router_sol,
    COALESCE(vs.rewards, 0) / 1000000000.0 AS rewards_sol,
    CASE 
        WHEN ps.previous_activated_stake >= 1000000000000 THEN 'Yes'
        ELSE 'No'
    END AS stake_threshold_met,
    CASE 
        WHEN ps.previous_activated_stake >= 1000000000000
        THEN ROUND((COALESCE(vs.delegator_inflation_reward, 0) / 1000000000.0 / 
                    (ps.previous_activated_stake / 1000000000.0)) * ead.epochs_per_year * 100, 2)
        ELSE NULL
    END AS delegator_inflation_apy,
    CASE 
        WHEN ps.previous_activated_stake >= 1000000000000
        THEN ROUND(((1 + COALESCE(vs.delegator_inflation_reward, 0) / 1000000000.0 / 
                    (ps.previous_activated_stake / 1000000000.0)) ^ ead.epochs_per_year - 1) * 100, 2)
        ELSE NULL
    END AS delegator_compound_inflation_apy,
    CASE 
        WHEN ps.previous_activated_stake >= 1000000000000
        THEN ROUND(((COALESCE(vs.mev_earned, 0) - COALESCE(vs.mev_to_validator, 0) - 
                    COALESCE(vs.mev_to_jito_tip_router, 0)) / 1000000000.0 / 
                    (ps.previous_activated_stake / 1000000000.0)) * ead.epochs_per_year * 100, 2)
        ELSE NULL
    END AS delegator_mev_apy,
    CASE 
        WHEN ps.previous_activated_stake >= 1000000000000
        THEN ROUND(((COALESCE(vs.mev_earned, 0) - COALESCE(vs.mev_to_validator, 0) - 
                    COALESCE(vs.mev_to_jito_tip_router, 0)) / 1000000000.0 / 
                    (ps.previous_activated_stake / 1000000000.0)) * ead.epochs_per_year * 100, 2)
        ELSE NULL
    END AS delegator_compound_mev_apy,
    CASE 
        WHEN ps.previous_activated_stake >= 1000000000000
        THEN ROUND(((COALESCE(vs.delegator_inflation_reward, 0) + 
                    (COALESCE(vs.mev_earned, 0) - COALESCE(vs.mev_to_validator, 0) - 
                    COALESCE(vs.mev_to_jito_tip_router, 0))) / 1000000000.0 / 
                    (ps.previous_activated_stake / 1000000000.0)) * ead.epochs_per_year * 100, 2)
        ELSE NULL
    END AS delegator_total_apy,
    CASE 
        WHEN ps.previous_activated_stake >= 1000000000000
        THEN ROUND((
            ((1 + COALESCE(vs.delegator_inflation_reward, 0) / 1000000000.0 / 
                 (ps.previous_activated_stake / 1000000000.0)) ^ ead.epochs_per_year - 1) +
            ((COALESCE(vs.mev_earned, 0) - COALESCE(vs.mev_to_validator, 0) - 
              COALESCE(vs.mev_to_jito_tip_router, 0)) / 1000000000.0 / 
             (ps.previous_activated_stake / 1000000000.0)) * ead.epochs_per_year
        ) * 100, 2)
        ELSE NULL
    END AS delegator_compound_total_apy,
    CASE 
        WHEN ps.previous_activated_stake >= 1000000000000
        THEN ROUND((COALESCE(vs.validator_inflation_reward, 0) / 1000000000.0 / 
                    (ps.previous_activated_stake / 1000000000.0)) * ead.epochs_per_year * 100, 2)
        ELSE NULL
    END AS validator_inflation_apy,
    CASE 
        WHEN ps.previous_activated_stake >= 1000000000000
        THEN ROUND((COALESCE(vs.mev_to_validator, 0) / 1000000000.0 / 
                    (ps.previous_activated_stake / 1000000000.0)) * ead.epochs_per_year * 100, 2)
        ELSE NULL
    END AS validator_mev_apy,
    CASE 
        WHEN ps.previous_activated_stake >= 1000000000000 
        AND COALESCE(vs.rewards, 0) > 0 
        THEN ROUND((COALESCE(vs.rewards, 0) / 1000000000.0 / 
                    (ps.previous_activated_stake / 1000000000.0)) * ead.epochs_per_year * 100, 2)
        ELSE NULL
    END AS validator_block_rewards_apy,
    CASE 
        WHEN ps.previous_activated_stake >= 1000000000000
        THEN ROUND((COALESCE(vs.validator_inflation_reward, 0) / 1000000000.0 / 
                    (ps.previous_activated_stake / 1000000000.0)) * ead.epochs_per_year * 100, 2)
        ELSE NULL
    END AS validator_compound_inflation_apy,
    CASE 
        WHEN ps.previous_activated_stake >= 1000000000000
        THEN ROUND((COALESCE(vs.mev_to_validator, 0) / 1000000000.0 / 
                    (ps.previous_activated_stake / 1000000000.0)) * ead.epochs_per_year * 100, 2)
        ELSE NULL
    END AS validator_compound_mev_apy,
    CASE 
        WHEN ps.previous_activated_stake >= 1000000000000 
        AND COALESCE(vs.rewards, 0) > 0 
        THEN ROUND((COALESCE(vs.rewards, 0) / 1000000000.0 / 
                    (ps.previous_activated_stake / 1000000000.0)) * ead.epochs_per_year * 100, 2)
        ELSE NULL
    END AS validator_compound_block_rewards_apy,
    CASE 
        WHEN ps.previous_activated_stake >= 1000000000000
        THEN ROUND((
            (COALESCE(vs.validator_inflation_reward, 0) + 
             COALESCE(vs.mev_to_validator, 0) + 
             COALESCE(vs.rewards, 0)) / 1000000000.0 / 
            (ps.previous_activated_stake / 1000000000.0)
        ) * ead.epochs_per_year * 100, 2)
        ELSE NULL
    END AS validator_total_apy,
    CASE 
        WHEN ps.previous_activated_stake >= 1000000000000
        THEN ROUND((
            ((1 + COALESCE(vs.validator_inflation_reward, 0) / 1000000000.0 / 
                 (ps.previous_activated_stake / 1000000000.0)) ^ ead.epochs_per_year - 1) +
            ((COALESCE(vs.mev_to_validator, 0) + COALESCE(vs.rewards, 0)) / 1000000000.0 / 
             (ps.previous_activated_stake / 1000000000.0)) * ead.epochs_per_year
        ) * 100, 2)
        ELSE NULL
    END AS validator_compound_total_apy,
    CASE 
        WHEN ps.previous_activated_stake >= 1000000000000
        THEN ROUND((
            (COALESCE(vs.delegator_inflation_reward, 0) + 
             COALESCE(vs.validator_inflation_reward, 0) + 
             COALESCE(vs.mev_earned, 0) - 
             COALESCE(vs.mev_to_jito_tip_router, 0) + 
             COALESCE(vs.rewards, 0)) / 1000000000.0 / 
            (ps.previous_activated_stake / 1000000000.0)
        ) * ead.epochs_per_year * 100, 2)
        ELSE NULL
    END AS total_overall_apy,
    CASE 
        WHEN ps.previous_activated_stake >= 1000000000000
        THEN ROUND((
            ((1 + (COALESCE(vs.delegator_inflation_reward, 0) + 
                   COALESCE(vs.validator_inflation_reward, 0)) / 1000000000.0 / 
                  (ps.previous_activated_stake / 1000000000.0)) ^ ead.epochs_per_year - 1) +
            ((COALESCE(vs.mev_earned, 0) - COALESCE(vs.mev_to_jito_tip_router, 0) + 
              COALESCE(vs.rewards, 0)) / 1000000000.0 / 
             (ps.previous_activated_stake / 1000000000.0)) * ead.epochs_per_year
        ) * 100, 2)
        ELSE NULL
    END AS compound_overall_apy
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
    vs.epoch = 785
    AND vs.vote_account_pubkey = 'chdv8H9fPfk2zFqSVaxRjsEo2qEDmswbju3BVgAHPNb'
    AND vs.activated_stake IS NOT NULL
    AND vs.activated_stake > 0;