import math
import logging
logger = logging.getLogger(__name__)

def extract_slot_data(slot, block_data, epoch_number):
    if not block_data.get('rewards'):
        logger.warning(f"Slot {slot}: No rewards found.")
        return None

    block_time = block_data['blockTime']
    block_hash = block_data['blockhash']
    block_height = block_data['blockHeight']
    parent_slot = block_data['parentSlot']
    previous_blockhash = block_data['previousBlockhash']
    transactions = block_data.get('transactions', [])

    total_fees = sum(tx['meta']['fee'] for tx in transactions)
    total_validator_fees = math.ceil(total_fees)
    total_tx = len(transactions)
    total_signatures = sum(len(tx['transaction']['signatures']) for tx in transactions)
    total_signature_fees = total_signatures * 10000
    total_priority_fees = total_fees - total_signature_fees
    total_validator_signature_fees = total_signatures * 10000
    total_validator_priority_fees = total_validator_fees - total_validator_signature_fees
    total_vote_tx = sum(1 for tx in transactions if any(
        pk == "Vote111111111111111111111111111111111111111" for pk in
        tx['transaction']['message']['accountKeys']))
    total_user_tx = total_tx - total_vote_tx
    total_cu = sum(tx['meta']['computeUnitsConsumed'] for tx in transactions)

    reward = block_data['rewards'][0]
    slot_info = {
        "pubkey": reward['pubkey'],
        "epoch": epoch_number,
        "block_slot": slot,
        "block_hash": block_hash,
        "block_time": block_time,
        "rewards": reward['lamports'],
        "post_balance": reward['postBalance'],
        "reward_type": reward['rewardType'],
        "commission": reward['commission'],
        "total_user_tx": total_user_tx,
        "total_vote_tx": total_vote_tx,
        "total_cu": total_cu,
        "total_signature_fees": total_signature_fees,
        "total_priority_fees": total_priority_fees,
        "total_fees": total_fees,
        "total_tx": total_tx,
        "total_signatures": total_signatures,
        "total_validator_fees": total_validator_fees,
        "total_validator_signature_fees": total_validator_signature_fees,
        "total_validator_priority_fees": total_validator_priority_fees,
        "block_height": block_height,
        "parent_slot": parent_slot,
        "previous_block_hash": previous_blockhash
    }
    return slot_info
