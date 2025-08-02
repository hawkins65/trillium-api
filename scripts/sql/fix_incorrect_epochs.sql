UPDATE temp_epoch_votes
SET epoch = (block_slot - 287712000) / 432000
WHERE epoch != (block_slot - 287712000) / 432000;
