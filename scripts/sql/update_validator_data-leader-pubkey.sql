-- Update query
UPDATE validator_data AS vd
SET identity_pubkey = ls.identity_pubkey
FROM leader_schedule AS ls
WHERE vd.block_slot = ls.block_slot
  AND vd.epoch = :epoch
  AND vd.identity_pubkey IS DISTINCT FROM ls.identity_pubkey;
