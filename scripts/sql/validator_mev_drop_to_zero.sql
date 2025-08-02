\copy (
    SELECT 
        c.epoch AS latest_epoch,
        c.avg_mev_per_block AS current_avg_mev,
        p.avg_mev_per_block AS n_1_epoch_avg_mev,
        p2.avg_mev_per_block AS n_2_epoch_avg_mev,
        p.vote_account_pubkey,
        p.version
    FROM 
        (SELECT 
            epoch,
            vote_account_pubkey,
            AVG(avg_mev_per_block) AS avg_mev_per_block,
            MAX(version) AS version
         FROM 
            validator_stats
         WHERE 
            epoch = 770
         GROUP BY 
            epoch, vote_account_pubkey) p
    JOIN 
        (SELECT 
            epoch,
            vote_account_pubkey,
            AVG(avg_mev_per_block) AS avg_mev_per_block
         FROM 
            validator_stats
         WHERE 
            epoch = 771
         GROUP BY 
            epoch, vote_account_pubkey) c
    ON 
        p.epoch = c.epoch - 1
        AND p.vote_account_pubkey = c.vote_account_pubkey
    LEFT JOIN 
        (SELECT 
            epoch,
            vote_account_pubkey,
            AVG(avg_mev_per_block) AS avg_mev_per_block
         FROM 
            validator_stats
         WHERE 
            epoch = 769
         GROUP BY 
            epoch, vote_account_pubkey) p2
    ON 
        p2.epoch = p.epoch - 1
        AND p2.vote_account_pubkey = p.vote_account_pubkey
    WHERE 
        p.avg_mev_per_block > 0
        AND c.avg_mev_per_block = 0
    ORDER BY 
        n_1_epoch_avg_mev ASC
) TO 'validator_mev_drop_to_zero.csv' WITH (FORMAT CSV, HEADER TRUE);