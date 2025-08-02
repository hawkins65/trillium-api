\echo 'Checking for conflicts between validator_stats_low_votes and validator_stats'

-- Step 1: Identify conflicting records
SELECT vslv.*
FROM validator_stats_low_votes vslv
INNER JOIN validator_stats vs
    ON vslv.identity_pubkey = vs.identity_pubkey
    AND vslv.epoch = vs.epoch;

-- Step 2: Count conflicting records
SELECT COUNT(*) AS conflict_count
FROM validator_stats_low_votes vslv
INNER JOIN validator_stats vs
    ON vslv.identity_pubkey = vs.identity_pubkey
    AND vslv.epoch = vs.epoch;

-- Step 3: Detailed conflict report
SELECT 
    vslv.identity_pubkey,
    vslv.epoch,
    vslv.votes_cast AS low_votes_vslv,
    vs.votes_cast AS validator_stats_votes
FROM validator_stats_low_votes vslv
INNER JOIN validator_stats vs
    ON vslv.identity_pubkey = vs.identity_pubkey
    AND vslv.epoch = vs.epoch
ORDER BY vslv.identity_pubkey, vslv.epoch;

\echo 'Conflict check complete'