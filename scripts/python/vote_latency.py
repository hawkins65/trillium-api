import base58
import struct
import importlib.util
import os

# Setup unified logging
script_dir = os.path.dirname(os.path.abspath(__file__))
logging_config_path = os.path.join(script_dir, "999_logging_config.py")
spec = importlib.util.spec_from_file_location("logging_config", logging_config_path)
logging_config = importlib.util.module_from_spec(spec)
spec.loader.exec_module(logging_config)
logger = logging_config.setup_logging(os.path.basename(__file__).replace('.py', ''))
# Logger setup moved to unified configuration

def decode_varint(data, offset):
    """Decode a VarInt starting from the given offset."""
    value = 0
    shift = 0
    while True:
        byte = data[offset]
        offset += 1
        value |= (byte & 0x7F) << shift
        if not (byte & 0x80):
            break
        shift += 7
    return value, offset

def decode_lockouts(data, root_slot):
    offset = 12  # Starting offset for lockouts
    current_slot = root_slot
    confirmation_count_1_slot = None

    # Step 1: Number of lockouts (1 byte)
    n_lockouts = struct.unpack_from("<B", data, offset)[0]
    offset += 1

    # Step 2: First slot increment (VarInt)
    first_slot_increment, offset = decode_varint(data, offset)
    current_slot += first_slot_increment

    # Step 3: Largest confirmation count (1 byte) - skip it
    offset += 1

    # Step 4: Decode remaining lockouts
    for _ in range(n_lockouts - 1):
        try:
            slot_increment, offset = decode_varint(data, offset)
            current_slot += slot_increment
            confirmation_count = struct.unpack_from("<B", data, offset)[0]
            offset += 1
            logger.debug(f"Decoded lockout: slot increment {slot_increment}, confirmation count {confirmation_count}")
            if confirmation_count == 1:
                confirmation_count_1_slot = current_slot
        except Exception as e:
            logger.debug(f"Error decoding lockouts: {e}")
            break

    return confirmation_count_1_slot

def decode_vote_state(data: bytes):
    # Extract the root slot (offset 4)
    root_offset = 4
    root = struct.unpack_from("<Q", data, root_offset)[0]
    logger.debug(f"Found root slot: {root} at offset {root_offset}")
    # Extract hash (offset 75)
    try:
        hash_offset = 75
        hash_bytes = data[hash_offset:hash_offset + 32]
        vote_hash = base58.b58encode(hash_bytes).decode()
        logger.debug(f"Extracted hash at offset {hash_offset}: {vote_hash}")
    except IndexError:
        vote_hash = "Error extracting hash"
        logger.error("Error extracting vote hash due to IndexError.")
    # Decode lockouts to determine the block voted on
    confirmation_count_1_slot = decode_lockouts(data, root)
    return {
        "base_slot": root,
        "hash": vote_hash,
        "block_voted_on": confirmation_count_1_slot,
    }

def extract_vote_latency_data(slot, block_data, epoch_number):
    vote_latency_list = []
    transactions = block_data.get('transactions', [])
    
    for tx_index, tx in enumerate(transactions):
        tx_message = tx['transaction']['message']
        account_keys = tx_message['accountKeys']
        signature = tx['transaction']['signatures'][0]
        
        if "Vote111111111111111111111111111111111111111" in account_keys:
            vote_program_index = account_keys.index("Vote111111111111111111111111111111111111111")
            if vote_program_index >= 2:
                vote_authority = account_keys[vote_program_index - 2]
                vote_account = account_keys[vote_program_index - 1]
                instructions = tx_message.get('instructions', [])
                for instr_index, instr in enumerate(instructions):
                    if instr['programIdIndex'] == vote_program_index:
                        try:
                            encoded_data = instr['data']
                            decoded_data = base58.b58decode(encoded_data)
                            result = decode_vote_state(decoded_data)
                            block_voted_on = result.get("block_voted_on")
                            if block_voted_on is None:
                                logger.debug(f"Transaction {tx_index}, instruction {instr_index}: No block voted on found.")
                                continue
                            vote_latency_list.append({
                                "epoch": epoch_number,
                                "block_slot": slot,
                                "block_hash": block_data['blockhash'],
                                "identity_pubkey": vote_authority,
                                "vote_account_pubkey": vote_account,
                                "block_voted_on": block_voted_on,
                                "signature": signature,
                                "latency": slot - block_voted_on
                            })
                        except Exception as e:
                            logger.warning(f"Error parsing vote latency data in transaction {tx_index}, instruction {instr_index}: {e}")
                            continue
    if not vote_latency_list:
        logger.debug(f"Slot {slot}: No vote latency data extracted.")
    return vote_latency_list
