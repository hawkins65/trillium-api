--
-- 92_validator_stats_duration.sql
--
-- This script calculates and inserts slot duration statistics for each validator
-- for a specific epoch. It joins the leader schedule with slot duration data,
-- filters for produced blocks with valid durations, and then calculates
-- min, max, mean, median, and standard deviation duration per validator.
--
-- The ':epoch' variable is set by the calling shell script.
--

-- Disable paging for this session to ensure clean output
\pset pager off

-- Delete existing stats for the given epoch to ensure idempotency.
\echo 'Deleting existing stats for epoch' :epoch 'from validator_stats_slot_duration...'
DELETE FROM validator_stats_slot_duration WHERE epoch = :epoch;

-- Calculate and insert new statistics into the validator_stats_slot_duration table.
-- All duration values are stored in nanoseconds, except slot_duration_stddev which is numeric.
\echo 'Calculating and inserting new stats for epoch' :epoch '...'
INSERT INTO validator_stats_slot_duration (
    identity_pubkey,
    epoch,
    slot_duration_min,
    slot_duration_max,
    slot_duration_mean,
    slot_duration_median,
    slot_duration_stddev
)
SELECT
    ls.identity_pubkey,
    ls.epoch,
    MIN(sd.duration) AS slot_duration_min,
    MAX(sd.duration) AS slot_duration_max,
    ROUND(AVG(sd.duration))::bigint AS slot_duration_mean,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY sd.duration)::bigint AS slot_duration_median,
    STDDEV(sd.duration)::numeric AS slot_duration_stddev
FROM
    leader_schedule ls
JOIN
    slot_duration sd ON ls.epoch = sd.epoch AND ls.block_slot = sd.block_slot
WHERE
    ls.epoch = :epoch
    AND ls.block_produced = true
    AND sd.duration > 0
GROUP BY
    ls.identity_pubkey,
    ls.epoch;

\echo '✅ Finished processing stats for epoch' :epoch'.'