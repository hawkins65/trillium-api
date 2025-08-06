import importlib.util

# Setup unified logging
spec = importlib.util.spec_from_file_location("logging_config", "999_logging_config.py")
logging_config = importlib.util.module_from_spec(spec)
spec.loader.exec_module(logging_config)
logger = logging_config.setup_logging(os.path.basename(__file__).replace('.py', ''))
# Logger setup moved to unified configuration

def extract_vote_data(slot, block_data, epoch_number):
    vote_data_list = []
    transactions = block_data.get('transactions', [])

    for tx in transactions:
        tx_message = tx['transaction']['message']
        account_keys = tx_message['accountKeys']
        if "Vote111111111111111111111111111111111111111" in account_keys:
            vote_program_index = account_keys.index("Vote111111111111111111111111111111111111111")
            if vote_program_index >= 2:
                vote_authority = account_keys[vote_program_index - 2]
                vote_account = account_keys[vote_program_index - 1]
                vote_data_list.append({
                    "epoch": epoch_number,
                    "block_slot": slot,
                    "block_hash": block_data['blockhash'],
                    "identity_pubkey": vote_authority,
                    "vote_account_pubkey": vote_account
                })
    if not vote_data_list:
        logger.debug(f"Slot {slot}: No vote data extracted.")
    return vote_data_list
