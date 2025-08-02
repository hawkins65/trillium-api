\copy (
    SELECT parent_slot
    FROM validator_data
    WHERE parent_slot IS NOT NULL
    AND parent_slot NOT IN (SELECT block_slot FROM validator_data)
) TO 'missing-slots.csv' CSV HEADER;
