WITH leader_positions AS (
    SELECT 
        ls.block_slot,
        ls.block_produced,
        ls.identity_pubkey,
        LEAD(ls.identity_pubkey) OVER (ORDER BY block_slot) AS next_leader,
        ((ls.block_slot - (650 * 432000)) % 4) AS position_in_group,
        COALESCE(vs.stake_percentage, 0) AS stake_percentage,
        COALESCE(vs.skip_rate, 0) AS skip_rate,
        COALESCE(vs.tx_included_in_blocks, 0) AS tx_included_in_blocks,
        COALESCE(vs.votes_cast, 0) AS votes_cast,
        COALESCE(vs.city, 'unknown') AS city,
        COALESCE(vs.country, 'unknown') AS country,
        COALESCE(vs.continent, 'unknown') AS continent,
        COALESCE(vs.asn, -1) AS asn,
        COALESCE(vs.skip_rate, 0) AS previous_skip_rate,
        ls.epoch
    FROM leader_schedule ls
    LEFT JOIN validator_stats vs
        ON ls.identity_pubkey = vs.identity_pubkey 
       AND ls.epoch = 650
    WHERE ls.epoch = 650
    ORDER BY ls.block_slot
),
fourth_slots AS (
    SELECT
        lp.*,
        CASE WHEN position_in_group = 3 THEN 1 ELSE 0 END AS is_4th_slot
    FROM leader_positions lp
),
subsequent_skips AS (
    SELECT
        fs.block_slot AS fourth_slot,
        fs.identity_pubkey,
        fs.block_produced,
        fs.next_leader,
        fs.stake_percentage,
        fs.skip_rate,
        fs.tx_included_in_blocks,
        fs.votes_cast,
        fs.city,
        fs.country,
        fs.continent,
        fs.asn,
        fs.previous_skip_rate,
        fs.position_in_group,
        (SELECT COUNT(*)
         FROM leader_positions lp2
         WHERE lp2.block_slot > fs.block_slot
           AND lp2.block_produced = FALSE
           AND lp2.identity_pubkey != fs.identity_pubkey
           AND lp2.epoch = 650
           AND lp2.block_slot < COALESCE(
                 (SELECT lp3.block_slot FROM leader_positions lp3
                  WHERE lp3.block_slot > fs.block_slot
                    AND lp3.block_produced = TRUE
                    AND lp3.epoch = 650
                  ORDER BY lp3.block_slot ASC LIMIT 1),
                 (SELECT MAX(block_slot) FROM leader_positions)
               )
        ) AS subsequent_skips_count,
        (SELECT COALESCE(
           (SELECT lp3.block_slot - fs.block_slot
            FROM leader_positions lp3
            WHERE lp3.block_slot > fs.block_slot
              AND lp3.block_produced = TRUE
              AND lp3.epoch = 650
            ORDER BY lp3.block_slot ASC LIMIT 1),
           0
        )) AS subsequent_skips_duration
    FROM fourth_slots fs
)
SELECT
    ss.fourth_slot AS block_slot,
    ss.block_produced,
    ss.identity_pubkey,
    ss.next_leader,
    ss.stake_percentage,
    ss.skip_rate,
    ss.tx_included_in_blocks,
    ss.votes_cast,
    ss.city,
    ss.country,
    ss.continent,
    ss.asn,
    ss.previous_skip_rate,
    ss.position_in_group,
    ss.subsequent_skips_count,
    ss.subsequent_skips_duration
FROM subsequent_skips ss
ORDER BY block_slot;
