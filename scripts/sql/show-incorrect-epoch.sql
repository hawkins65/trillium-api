-- First, create a temporary table to store the results
CREATE TEMPORARY TABLE incorrect_epochs AS
WITH epoch_calculation AS (
  SELECT 
    block_slot,
    epoch,
    ((block_slot - 259200000) / 432000) + 600 AS calculated_epoch
  FROM temp_validator_data
  WHERE block_slot >= 258768000  -- Starting from epoch 599
)
SELECT 
  block_slot,
  epoch AS stored_epoch,
  calculated_epoch,
  'Incorrect' AS status
FROM epoch_calculation
WHERE epoch != calculated_epoch
ORDER BY block_slot;

-- Then, copy the data from the temporary table to a CSV file
COPY incorrect_epochs TO '/tmp/incorrect_epochs.csv' WITH CSV HEADER;

-- Optionally, drop the temporary table if you don't need it anymore
DROP TABLE incorrect_epochs;