-- Define variables using a WITH clause
WITH constants AS (
    SELECT 
        'HyperSPG8w4jgdHgmA8ExrhRL1L1BriRTHD9UFdXJUud' AS identity_pubkey,
        'DzQHN1oTdN85Sbku2bc9Fu9yEwrgRMiu2XbRcntZ31yb' AS vote_account_pubkey
)

-- Query using the variables with fully qualified column names
SELECT 'validator_data' AS table_name, epoch, COUNT(*) AS row_count
FROM validator_data, constants
WHERE validator_data.identity_pubkey = constants.identity_pubkey
GROUP BY epoch
UNION ALL
SELECT 'validator_data_to_inspect' AS table_name, epoch, COUNT(*)
FROM validator_data_to_inspect, constants
WHERE validator_data_to_inspect.identity_pubkey = constants.identity_pubkey
GROUP BY epoch
UNION ALL
SELECT 'validator_stats' AS table_name, epoch, COUNT(*)
FROM validator_stats, constants
WHERE validator_stats.vote_account_pubkey = constants.vote_account_pubkey
GROUP BY epoch
UNION ALL
SELECT 'validator_stats_low_votes' AS table_name, epoch, COUNT(*)
FROM validator_stats_low_votes, constants
WHERE validator_stats_low_votes.vote_account_pubkey = constants.vote_account_pubkey
GROUP BY epoch
UNION ALL
SELECT 'validator_stats_to_inspect' AS table_name, epoch, COUNT(*)
FROM validator_stats_to_inspect, constants
WHERE validator_stats_to_inspect.vote_account_pubkey = constants.vote_account_pubkey
GROUP BY epoch
UNION ALL
SELECT 'epoch_votes' AS table_name, epoch, COUNT(*)
FROM epoch_votes, constants
WHERE epoch_votes.vote_account_pubkey = constants.vote_account_pubkey
GROUP BY epoch
UNION ALL
SELECT 'leader_schedule' AS table_name, epoch, COUNT(*)
FROM leader_schedule, constants
WHERE leader_schedule.identity_pubkey = constants.identity_pubkey
GROUP BY epoch
UNION ALL
SELECT 'stake_accounts' AS table_name, epoch, COUNT(*)
FROM stake_accounts, constants
WHERE stake_accounts.vote_account_pubkey = constants.vote_account_pubkey
GROUP BY epoch
UNION ALL
SELECT 'validator_xshin' AS table_name, epoch, COUNT(*)
FROM validator_xshin, constants
WHERE validator_xshin.vote_account_pubkey = constants.vote_account_pubkey
GROUP BY epoch
UNION ALL
SELECT 'votes_table' AS table_name, epoch, COUNT(*)
FROM votes_table, constants
WHERE votes_table.vote_account_pubkey = constants.vote_account_pubkey
GROUP BY epoch
UNION ALL
SELECT 'xshin_data' AS table_name, epoch, COUNT(*)
FROM xshin_data, constants
WHERE xshin_data.vote_account_pubkey = constants.vote_account_pubkey
GROUP BY epoch
ORDER BY table_name, epoch;