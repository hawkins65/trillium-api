\echo 'Diagnosing conflicts for validator_stats data move'

-- Check for duplicate (identity_pubkey, epoch) values within validator_stats_low_votes
SELECT identity_pubkey, epoch, COUNT(*) AS count
FROM validator_stats_low_votes
GROUP BY identity_pubkey, epoch
HAVING COUNT(*) > 1
ORDER BY identity_pubkey, epoch;

-- Inspect the specific conflicting record in validator_stats_low_votes
SELECT *
FROM validator_stats_low_votes
WHERE identity_pubkey = 'HwcVgFSgmfeeF7zGFUBLoVA8Hpx8rtwyfCrJ1npBaSVC'
  AND epoch = 783;

-- Inspect the corresponding record in validator_stats
SELECT *
FROM validator_stats
WHERE identity_pubkey = 'HwcVgFSgmfeeF7zGFUBLoVA8Hpx8rtwyfCrJ1npBaSVC'
  AND epoch = 783;

-- Check for records in validator_stats_low_votes that should be inserted but conflict
SELECT vslv.identity_pubkey, vslv.epoch, vslv.votes_cast
FROM validator_stats_low_votes vslv
INNER JOIN validator_stats vs
    ON vslv.identity_pubkey = vs.identity_pubkey
    AND vslv.epoch = vs.epoch
WHERE NOT EXISTS (
    SELECT 1
    FROM validator_stats vs2
    WHERE vs2.identity_pubkey = vslv.identity_pubkey
      AND vs2.epoch = vslv.epoch
      AND vs2.votes_cast = vslv.votes_cast
)
ORDER BY vslv.identity_pubkey, vslv.epoch;

\echo 'Diagnosis complete'