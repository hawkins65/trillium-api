\copy (
    WITH data AS (
        SELECT 
            epoch::text AS epoch,
            TO_CHAR(ROUND(SUM(total_mev_earned) / 1e9, 2), 'FM999,999,999,990.00') AS total_mev_earned_sol,
            '$' || TO_CHAR(ROUND(SUM(total_mev_earned) / 1e9 * 145, 2), 'FM999,999,999,990.00') AS total_mev_earned_usd,
            TO_CHAR(ROUND(SUM(total_mev_to_validator) / 1e9, 2), 'FM999,999,999,990.00') AS total_mev_to_validator_sol,
            '$' || TO_CHAR(ROUND(SUM(total_mev_to_validator) / 1e9 * 145, 2), 'FM999,999,999,990.00') AS total_mev_to_validator_usd,
            TO_CHAR(ROUND(AVG(avg_mev_to_validator) / 1e9, 2), 'FM999,999,999,990.00') AS avg_mev_to_validator_sol,
            '$' || TO_CHAR(ROUND(AVG(avg_mev_to_validator) / 1e9 * 145, 2), 'FM999,999,999,990.00') AS avg_mev_to_validator_usd,
            ROUND(AVG(avg_mev_commission)) AS avg_mev_commission
        FROM 
            epoch_aggregate_data
        WHERE 
            min_block_time >= EXTRACT(EPOCH FROM TIMESTAMP '2024-07-29 00:00:00') 
            AND max_block_time <= EXTRACT(EPOCH FROM TIMESTAMP '2024-08-27 23:59:59')
        GROUP BY 
            epoch
    ),
    totals AS (
        SELECT 
            'Total' AS epoch,
            TO_CHAR(ROUND(SUM(total_mev_earned) / 1e9, 2), 'FM999,999,999,990.00') AS total_mev_earned_sol,
            '$' || TO_CHAR(ROUND(SUM(total_mev_earned) / 1e9 * 145, 2), 'FM999,999,999,990.00') AS total_mev_earned_usd,
            TO_CHAR(ROUND(SUM(total_mev_to_validator) / 1e9, 2), 'FM999,999,999,990.00') AS total_mev_to_validator_sol,
            '$' || TO_CHAR(ROUND(SUM(total_mev_to_validator) / 1e9 * 145, 2), 'FM999,999,999,990.00') AS total_mev_to_validator_usd,
            TO_CHAR(ROUND(AVG(avg_mev_to_validator) / 1e9, 2), 'FM999,999,999,990.00') AS avg_mev_to_validator_sol,
            '$' || TO_CHAR(ROUND(AVG(avg_mev_to_validator) / 1e9 * 145, 2), 'FM999,999,999,990.00') AS avg_mev_to_validator_usd,
            ROUND(AVG(avg_mev_commission)) AS avg_mev_commission
        FROM 
            epoch_aggregate_data
        WHERE 
            min_block_time >= EXTRACT(EPOCH FROM TIMESTAMP '2024-07-29 00:00:00') 
            AND max_block_time <= EXTRACT(EPOCH FROM TIMESTAMP '2024-08-27 23:59:59')
    )
    SELECT * FROM data
    UNION ALL
    SELECT * FROM totals
) TO 'mev_past_month.csv' WITH CSV HEADER;
