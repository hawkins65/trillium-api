-- Disable paging for this session
\pset pager off

-- Import CSV data using temp table approach
-- Create temporary table for import
CREATE TEMP TABLE temp_slot_import (
    block_slot bigint,
    duration bigint
);

-- Import CSV to temp table
-- PLACEHOLDER_CSV_FILE will be replaced by the bash script
\copy temp_slot_import (block_slot, duration) FROM 'PLACEHOLDER_CSV_FILE' WITH (FORMAT csv, HEADER true);

-- Delete existing data for this epoch before inserting new data
DELETE FROM slot_duration WHERE epoch = :epoch;

-- Identify slots to exclude (skipped slots and the next produced slot after each)
WITH skipped_slots AS (
    SELECT block_slot
    FROM leader_schedule
    WHERE epoch = :epoch AND block_produced = false
),
next_produced AS (
    SELECT ls.block_slot
    FROM leader_schedule ls
    WHERE ls.epoch = :epoch AND ls.block_produced = true
    AND ls.block_slot > ANY (SELECT block_slot FROM skipped_slots)
    AND ls.block_slot = (
        SELECT MIN(block_slot)
        FROM leader_schedule ls2
        WHERE ls2.epoch = ls.epoch
        AND ls2.block_produced = true
        AND ls2.block_slot > ls.block_slot
    )
),
excluded_slots AS (
    SELECT block_slot FROM skipped_slots
    UNION
    SELECT block_slot FROM next_produced
)

-- Insert into main table with epoch, excluding skipped slots and their next produced slots
INSERT INTO slot_duration (epoch, block_slot, duration)
SELECT :epoch as epoch, tsi.block_slot, tsi.duration
FROM temp_slot_import tsi
LEFT JOIN excluded_slots es ON tsi.block_slot = es.block_slot
WHERE es.block_slot IS NULL;

-- Drop temp table
DROP TABLE temp_slot_import;

-- Query to find block_slots with duration > 0 but block_produced = false
-- These should be empty due to the filtering above
\echo 'Slots with duration > 0 but block not produced:'
SELECT 
    ls.epoch,
    ls.block_slot,
    ls.identity_pubkey,
    sd.duration,
    ls.block_produced
FROM leader_schedule ls
JOIN slot_duration sd ON ls.epoch = sd.epoch AND ls.block_slot = sd.block_slot
WHERE sd.duration > 0 
  AND ls.block_produced = false
  AND ls.epoch = :epoch
ORDER BY ls.epoch, ls.block_slot;

-- Query to find block_slots with block_produced = true but duration = 0 or NULL
\echo 'Slots with block produced but duration = 0 or NULL:'
SELECT 
    ls.epoch,
    ls.block_slot,
    ls.identity_pubkey,
    COALESCE(sd.duration, 0) as duration,
    ls.block_produced
FROM leader_schedule ls
LEFT JOIN slot_duration sd ON ls.epoch = sd.epoch AND ls.block_slot = sd.block_slot
WHERE ls.block_produced = true 
  AND (sd.duration IS NULL OR sd.duration = 0)
  AND ls.epoch = :epoch
ORDER BY ls.epoch, ls.block_slot;

-- Combined report showing both anomalies
\echo 'Combined anomaly report:'
SELECT 
    'Duration > 0, Block Not Produced' as anomaly_type,
    ls.epoch,
    ls.block_slot,
    ls.identity_pubkey,
    sd.duration,
    ls.block_produced
FROM leader_schedule ls
JOIN slot_duration sd ON ls.epoch = sd.epoch AND ls.block_slot = sd.block_slot
WHERE sd.duration > 0 AND ls.block_produced = false AND ls.epoch = :epoch

UNION ALL

SELECT 
    'Block Produced, Duration = 0/NULL' as anomaly_type,
    ls.epoch,
    ls.block_slot,
    ls.identity_pubkey,
    COALESCE(sd.duration, 0) as duration,
    ls.block_produced
FROM leader_schedule ls
LEFT JOIN slot_duration sd ON ls.epoch = sd.epoch AND ls.block_slot = sd.block_slot
WHERE ls.block_produced = true AND (sd.duration IS NULL OR sd.duration = 0) AND ls.epoch = :epoch

ORDER BY anomaly_type, epoch, block_slot;

-- Summary statistics
\echo 'Summary statistics:'
SELECT 
    'Total slots in leader_schedule' as metric,
    COUNT(*) as count
FROM leader_schedule
WHERE epoch = :epoch

UNION ALL

SELECT 
    'Total slots with duration data' as metric,
    COUNT(*) as count
FROM slot_duration
WHERE epoch = :epoch

UNION ALL

SELECT 
    'Blocks produced with duration data' as metric,
    COUNT(*) as count
FROM leader_schedule ls
JOIN slot_duration sd ON ls.epoch = sd.epoch AND ls.block_slot = sd.block_slot
WHERE ls.epoch = :epoch AND ls.block_produced = true AND sd.duration > 0

UNION ALL

SELECT 
    'Anomaly: Duration > 0, Block Not Produced' as metric,
    COUNT(*) as count
FROM leader_schedule ls
JOIN slot_duration sd ON ls.epoch = sd.epoch AND ls.block_slot = sd.block_slot
WHERE ls.epoch = :epoch AND sd.duration > 0 AND ls.block_produced = false

UNION ALL

SELECT 
    'Anomaly: Block Produced, Duration = 0/NULL' as metric,
    COUNT(*) as count
FROM leader_schedule ls
LEFT JOIN slot_duration sd ON ls.epoch = sd.epoch AND ls.block_slot = sd.block_slot
WHERE ls.epoch = :epoch AND ls.block_produced = true AND (sd.duration IS NULL OR sd.duration = 0);