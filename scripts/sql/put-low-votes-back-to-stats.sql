\echo 'Deduplicating validator_stats_low_votes and moving data to validator_stats'

BEGIN TRANSACTION;

-- Create a temporary table with deduplicated records from validator_stats_low_votes
CREATE TEMPORARY TABLE validator_stats_low_votes_dedup AS
SELECT DISTINCT ON (identity_pubkey, epoch) *
FROM validator_stats_low_votes
ORDER BY identity_pubkey, epoch, votes_cast DESC;

-- Update conflicting records in validator_stats
UPDATE validator_stats vs
SET votes_cast = vslv.votes_cast
FROM validator_stats_low_votes_dedup vslv
WHERE vs.identity_pubkey = vslv.identity_pubkey
    AND vs.epoch = vslv.epoch;

-- Insert non-conflicting records
INSERT INTO validator_stats
SELECT vslv.*
FROM validator_stats_low_votes_dedup vslv
LEFT JOIN validator_stats vs
    ON vslv.identity_pubkey = vs.identity_pubkey
    AND vslv.epoch = vs.epoch
WHERE vs.identity_pubkey IS NULL;

-- Drop the temporary table
DROP TABLE validator_stats_low_votes_dedup;

COMMIT;

\echo 'Data move complete'

-- Verify the operation
\echo 'Verifying data move'

-- Count records in both tables
SELECT 'validator_stats' AS table_name, COUNT(*) AS row_count
FROM validator_stats
UNION
SELECT 'validator_stats_low_votes' AS table_name, COUNT(*) AS row_count
FROM validator_stats_low_votes;

-- Check for remaining duplicates in validator_stats_low_votes
SELECT identity_pubkey, epoch, COUNT(*) AS count
FROM validator_stats_low_votes
GROUP BY identity_pubkey, epoch
HAVING COUNT(*) > 1
ORDER BY identity_pubkey, epoch;

-- Check for records in validator_stats_low_votes that were not moved
SELECT 
    vslv.identity_pubkey,
    vslv.epoch,
    vslv.votes_cast AS original_low_votes,
    vs.votes_cast AS validator_stats_votes
FROM validator_stats_low_votes vslv
LEFT JOIN validator_stats vs
    ON vslv.identity_pubkey = vs.identity_pubkey
    AND vslv.epoch = vs.epoch
WHERE vs.identity_pubkey IS NULL
ORDER BY vslv.identity_pubkey, vslv.epoch;

\echo 'Verification complete'