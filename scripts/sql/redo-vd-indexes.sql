BEGIN;

-- Drop existing indexes
DROP INDEX IF EXISTS public.idx_validator_data_epoch_identity_pubkey;
DROP INDEX IF EXISTS public.validator_data_epoch_identity_pubkey_idx;
DROP INDEX IF EXISTS public.validator_data_block_slot_idx;
DROP INDEX IF EXISTS public.validator_data_parent_slot_idx;

-- Create four indexes
CREATE INDEX idx_validator_data_epoch_identity_pubkey
ON public.validator_data USING btree (epoch, identity_pubkey);

CREATE INDEX idx_validator_data_block_slot
ON public.validator_data USING btree (block_slot);

CREATE INDEX idx_validator_data_parent_slot
ON public.validator_data USING btree (parent_slot);

CREATE INDEX idx_validator_data_identity_pubkey
ON public.validator_data USING btree (identity_pubkey);

COMMIT;