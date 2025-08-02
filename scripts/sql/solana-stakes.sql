-- Query #1: Total stakes by vote_account_pubkey per epoch
WITH calc AS (
    SELECT epoch, vote_account_pubkey,
           ROUND(SUM(active_stake) / 1000000000.0, 5) AS total_active_stake,
           ROUND(SUM(activating_stake) / 1000000000.0, 5) AS total_activating_stake,
           ROUND(SUM(deactivating_stake) / 1000000000.0, 5) AS total_deactivating_stake,
           ROUND(SUM(activating_stake - deactivating_stake) / 1000000000.0, 5) AS activating_vs_deactivating_stake
    FROM stake_accounts
    GROUP BY epoch, vote_account_pubkey
)
\copy (SELECT * FROM calc ORDER BY total_active_stake DESC) TO '/home/smilax/block-production/api/solana-stakes/1_total_stake_by_vote_account.csv' CSV HEADER;

-- Query #2a: Total stakes by staker per epoch
WITH calc AS (
    SELECT epoch, staker,
           ROUND(SUM(active_stake) / 1000000000.0, 5) AS total_active_stake,
           ROUND(SUM(activating_stake) / 1000000000.0, 5) AS total_activating_stake,
           ROUND(SUM(deactivating_stake) / 1000000000.0, 5) AS total_deactivating_stake,
           ROUND(SUM(activating_stake - deactivating_stake) / 1000000000.0, 5) AS activating_vs_deactivating_stake
    FROM stake_accounts
    GROUP BY epoch, staker
)
\copy (SELECT * FROM calc ORDER BY total_active_stake DESC) TO '/home/smilax/block-production/api/solana-stakes/2a_total_stake_by_staker.csv' CSV HEADER;

-- Query #2b: Total stakes by withdrawer per epoch
WITH calc AS (
    SELECT epoch, withdrawer,
           ROUND(SUM(active_stake) / 1000000000.0, 5) AS total_active_stake,
           ROUND(SUM(activating_stake) / 1000000000.0, 5) AS total_activating_stake,
           ROUND(SUM(deactivating_stake) / 1000000000.0, 5) AS total_deactivating_stake,
           ROUND(SUM(activating_stake - deactivating_stake) / 1000000000.0, 5) AS activating_vs_deactivating_stake
    FROM stake_accounts
    GROUP BY epoch, withdrawer
)
\copy (SELECT * FROM calc ORDER BY total_active_stake DESC) TO '/home/smilax/block-production/api/solana-stakes/2b_total_stake_by_withdrawer.csv' CSV HEADER;

-- Query #2c: Total stakes by custodian per epoch
WITH calc AS (
    SELECT epoch, custodian,
           ROUND(SUM(active_stake) / 1000000000.0, 5) AS total_active_stake,
           ROUND(SUM(activating_stake) / 1000000000.0, 5) AS total_activating_stake,
           ROUND(SUM(deactivating_stake) / 1000000000.0, 5) AS total_deactivating_stake,
           ROUND(SUM(activating_stake - deactivating_stake) / 1000000000.0, 5) AS activating_vs_deactivating_stake
    FROM stake_accounts
    GROUP BY epoch, custodian
)
\copy (SELECT * FROM calc ORDER BY total_active_stake DESC) TO '/home/smilax/block-production/api/solana-stakes/2c_total_stake_by_custodian.csv' CSV HEADER;

-- Query #3: Aggregate totals for an epoch
WITH calc AS (
    SELECT epoch,
           ROUND(SUM(active_stake) / 1000000000.0, 5) AS total_active_stake,
           ROUND(SUM(activating_stake) / 1000000000.0, 5) AS total_activating_stake,
           ROUND(SUM(deactivating_stake) / 1000000000.0, 5) AS total_deactivating_stake,
           ROUND(SUM(activating_stake - deactivating_stake) / 1000000000.0, 5) AS activating_vs_deactivating_stake
    FROM stake_accounts
    GROUP BY epoch
)
\copy (SELECT * FROM calc ORDER BY total_active_stake DESC) TO '/home/smilax/block-production/api/solana-stakes/3_aggregate_totals.csv' CSV HEADER;

-- Query #4: Distribution of active_stake with median calculation
WITH calc AS (
    WITH stake_distribution AS (
        SELECT epoch,
               active_stake,
               ROW_NUMBER() OVER (PARTITION BY epoch ORDER BY active_stake) AS rn,
               COUNT(*) OVER (PARTITION BY epoch) AS total_count
        FROM stake_accounts
    )
    SELECT epoch,
           ROUND(MIN(active_stake) / 1000000000.0, 5) AS min_active_stake,
           ROUND(MAX(active_stake) / 1000000000.0, 5) AS max_active_stake,
           ROUND(AVG(active_stake) / 1000000000.0, 5) AS avg_active_stake,
           ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY active_stake) / 1000000000.0, 5) AS median_active_stake
    FROM stake_accounts
    GROUP BY epoch
)
\copy (SELECT * FROM calc ORDER BY avg_active_stake DESC) TO '/home/smilax/block-production/api/solana-stakes/4_active_stake_distribution.csv' CSV HEADER;

-- Query #5: Accounts with zero active_stake
\copy (SELECT epoch, stake_pubkey, vote_account_pubkey,
           ROUND(active_stake / 1000000000.0, 5) AS active_stake,
           ROUND(activating_stake / 1000000000.0, 5) AS activating_stake,
           ROUND(deactivating_stake / 1000000000.0, 5) AS deactivating_stake
    FROM stake_accounts
    WHERE active_stake = 0
    ORDER BY epoch, stake_pubkey
) TO '/home/smilax/block-production/api/solana-stakes/5_zero_active_stake_accounts.csv' CSV HEADER;

-- Query #6a: Total accounts per vote_account_pubkey
\copy (SELECT epoch, vote_account_pubkey,
           COUNT(*) AS account_count
    FROM stake_accounts
    GROUP BY epoch, vote_account_pubkey
    ORDER BY account_count DESC
) TO '/home/smilax/block-production/api/solana-stakes/6a_accounts_per_vote_account.csv' CSV HEADER;

-- Query #6b: Total accounts per custodian
\copy (SELECT epoch, custodian,
           COUNT(*) AS account_count
    FROM stake_accounts
    GROUP BY epoch, custodian
    ORDER BY account_count DESC
) TO '/home/smilax/block-production/api/solana-stakes/6b_accounts_per_custodian.csv' CSV HEADER;

-- Query #7: Changes in stakes over time for a specific vote_account_pubkey
WITH calc AS (
    SELECT epoch, vote_account_pubkey,
           ROUND(SUM(active_stake) / 1000000000.0, 5) AS total_active_stake
    FROM stake_accounts
    WHERE vote_account_pubkey = 'tri1cHBy47fPyhCvrCf6FnR7Mz6XdSoSBah2FsZVQeT'
    GROUP BY epoch, vote_account_pubkey
)
\copy (SELECT * FROM calc ORDER BY total_active_stake DESC) TO '/home/smilax/block-production/api/solana-stakes/7_stake_changes_for_specific_vote_account.csv' CSV HEADER;
