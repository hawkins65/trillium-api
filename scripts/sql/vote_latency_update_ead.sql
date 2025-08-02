UPDATE epoch_aggregate_data
SET
    avg_vote_credits = subquery.avg_vote_credits,
    total_vote_credits = subquery.total_vote_credits,
    median_vote_credits = subquery.median_vote_credits,
    avg_votes = subquery.avg_votes,
    avg_credit_per_voted_slot = subquery.avg_credit_per_voted_slot,
    max_vote_latency = subquery.max_vote_latency,
    mean_vote_latency = subquery.mean_vote_latency,
    median_vote_latency = subquery.median_vote_latency
FROM (
    SELECT
        AVG(vt.vote_credits)::numeric AS avg_vote_credits,
        SUM(vt.vote_credits)::numeric AS total_vote_credits,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY vt.vote_credits)::numeric AS median_vote_credits,
        AVG(vt.voted_slots)::numeric AS avg_votes,
        AVG(vt.avg_credit_per_voted_slot)::numeric AS avg_credit_per_voted_slot,
        MAX(vt.max_vote_latency)::numeric AS max_vote_latency,
        AVG(vt.mean_vote_latency)::numeric AS mean_vote_latency,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY vt.mean_vote_latency)::numeric AS median_vote_latency
    FROM votes_table vt
    WHERE vt.epoch = :epoch
) AS subquery
WHERE epoch_aggregate_data.epoch = :epoch;
