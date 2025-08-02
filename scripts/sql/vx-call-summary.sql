-- Redirect output to a JSON file
\o output.json

-- Query to generate JSON summary
WITH aggregated_data AS (
    SELECT 
        epoch,
        MAX(avg_vote_latency)::NUMERIC(10,3) AS max_avg_vote_latency,
        ROUND(AVG(avg_vote_latency), 3) AS mean_avg_vote_latency,
        ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY avg_vote_latency)::NUMERIC, 3) AS median_avg_vote_latency,
        MAX(vote_credits)::INTEGER AS max_vote_credits,
        AVG(vote_credits)::INTEGER AS mean_vote_credits,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY vote_credits)::INTEGER AS median_vote_credits,
        MAX(voted_slots)::INTEGER AS max_voted_slots,
        AVG(voted_slots)::INTEGER AS mean_voted_slots,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY voted_slots)::INTEGER AS median_voted_slots,
        ROUND(MAX(avg_credit_per_voted_slot), 3) AS max_avg_credit_per_voted_slot,
        ROUND(AVG(avg_credit_per_voted_slot), 3) AS mean_avg_credit_per_voted_slot,
        ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY avg_credit_per_voted_slot)::NUMERIC, 3) AS median_avg_credit_per_voted_slot,
        ROUND(MAX(avg_latency_per_voted_slot), 3) AS max_avg_latency_per_voted_slot,
        ROUND(AVG(avg_latency_per_voted_slot), 3) AS mean_avg_latency_per_voted_slot,
        ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY avg_latency_per_voted_slot)::NUMERIC, 3) AS median_avg_latency_per_voted_slot,
        MAX(max_vote)::INTEGER AS max_max_vote,
        ROUND(AVG(max_vote)::NUMERIC, 3) AS mean_max_vote,
        ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY max_vote)::NUMERIC, 3) AS median_max_vote,
        MAX(mean_vote)::INTEGER AS max_mean_vote,
        ROUND(AVG(mean_vote)::NUMERIC, 3) AS mean_mean_vote,
        ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY mean_vote)::NUMERIC, 3) AS median_mean_vote,
        MAX(median_vote)::INTEGER AS max_median_vote,
        ROUND(AVG(median_vote)::NUMERIC, 3) AS mean_median_vote,
        ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY median_vote)::NUMERIC, 3) AS median_median_vote
    FROM votes_table
    GROUP BY epoch
    ORDER BY epoch
)
SELECT json_agg(
    json_build_object(
        'epoch', epoch,
        'max_avg_vote_latency', max_avg_vote_latency,
        'mean_avg_vote_latency', mean_avg_vote_latency,
        'median_avg_vote_latency', median_avg_vote_latency,
        'max_vote_credits', max_vote_credits,
        'mean_vote_credits', mean_vote_credits,
        'median_vote_credits', median_vote_credits,
        'max_voted_slots', max_voted_slots,
        'mean_voted_slots', mean_voted_slots,
        'median_voted_slots', median_voted_slots,
        'max_avg_credit_per_voted_slot', max_avg_credit_per_voted_slot,
        'mean_avg_credit_per_voted_slot', mean_avg_credit_per_voted_slot,
        'median_avg_credit_per_voted_slot', median_avg_credit_per_voted_slot,
        'max_avg_latency_per_voted_slot', max_avg_latency_per_voted_slot,
        'mean_avg_latency_per_voted_slot', mean_avg_latency_per_voted_slot,
        'median_avg_latency_per_voted_slot', median_avg_latency_per_voted_slot,
        'max_max_vote', max_max_vote,
        'mean_max_vote', mean_max_vote,
        'median_max_vote', median_max_vote,
        'max_mean_vote', max_mean_vote,
        'mean_mean_vote', mean_mean_vote,
        'median_mean_vote', median_mean_vote,
        'max_median_vote', max_median_vote,
        'mean_median_vote', mean_median_vote,
        'median_median_vote', median_median_vote
    )
) AS json_result
FROM aggregated_data;

-- Reset output back to terminal
\o
