SELECT 
    epoch,
    COUNT(DISTINCT CASE WHEN avg_mev_per_block > 0 THEN identity_pubkey END) AS mev_count,
    COUNT(DISTINCT CASE WHEN avg_rewards_per_block > 0 THEN identity_pubkey END) AS rewards_count,
    ROUND(
        COUNT(DISTINCT CASE WHEN avg_mev_per_block > 0 THEN identity_pubkey END)::numeric
        / NULLIF(COUNT(DISTINCT CASE WHEN avg_rewards_per_block > 0 THEN identity_pubkey END), 0) * 100,
        2
    ) AS mev_percentage
FROM 
    validator_stats
GROUP BY 
    epoch
ORDER BY 
    epoch;
