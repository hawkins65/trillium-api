import base58
# Typing and Data Structures
from typing import List, Optional
from dataclasses import dataclass

# Set up logging
logging.basicConfig(level=logging.WARNING)
logger = logging.getLogger(__name__)

# Get the script name without the extension
script_name = os.path.splitext(os.path.basename(__file__))[0]

# Configure logging
logging.basicConfig(
    level=logging.DEBUG,  # Set the logging level
    filename=f'{script_name}.log',  # Use the script name for the log file
    filemode='w',  # Overwrite the log file each time the script runs
    format='%(asctime)s - %(levelname)s - %(message)s'  # Define the log message format
)

def decode_varint(data, offset):
    """Decode a VarInt starting from the given offset."""
    value = 0
    shift = 0
    while True:
        byte = data[offset]
        offset += 1
        value |= (byte & 0x7F) << shift
        if not (byte & 0x80):  # Stop when the MSB is not set
            break
        shift += 7
    return value, offset

def decode_lockouts(data, root_slot):
    offset = 12  # Start decoding at offset 12
    current_slot = root_slot
    confirmation_count_1_slot = None

    logger.debug("\n=== Decoding Lockouts ===")

    # Step 1: Extract the number of lockouts (1 byte)
    n_lockouts = struct.unpack_from("<B", data, offset)[0]
    offset += 1
    logger.debug(f"Number of Lockouts: {n_lockouts}")

    # Step 2: Extract the first slot increment (VarInt)
    first_slot_increment, offset = decode_varint(data, offset)
    current_slot += first_slot_increment
    logger.debug(f"First Slot Increment: {first_slot_increment} -> First Slot: {current_slot}")

    # Step 3: Extract the largest confirmation count (1 byte)
    largest_confirmation_count = struct.unpack_from("<B", data, offset)[0]
    offset += 1
    logger.debug(f"Largest Confirmation Count: {largest_confirmation_count}")

    # Step 4: Decode remaining lockouts
    for _ in range(n_lockouts - 1):  # Remaining lockouts
        try:
            # Extract slot increment (VarInt)
            slot_increment, offset = decode_varint(data, offset)
            current_slot += slot_increment

            # Extract confirmation count (1 byte)
            confirmation_count = struct.unpack_from("<B", data, offset)[0]
            offset += 1

            logger.debug(f"Decoded Lockout: Slot Increment = {slot_increment}, Confirmation Count = {confirmation_count}")

            # Capture the slot for confirmation count 1
            if confirmation_count == 1:
                confirmation_count_1_slot = current_slot

        except Exception as e:
            logger.debug(f"Error decoding lockouts: {e}")
            break

    return confirmation_count_1_slot

def decode_vote_state(data: bytes):
    # Step 1: Extract root slot
    root_offset = 4  # Hardcoded offset for the root slot
    root = struct.unpack_from("<Q", data, root_offset)[0]
    logger.debug(f"Found Root Slot: {root} at offset {root_offset}")

    # Step 2: Extract hash at offset 75
    try:
        hash_offset = 75
        hash_bytes = data[hash_offset:hash_offset + 32]
        vote_hash = base58.b58encode(hash_bytes).decode()
        logger.debug(f"Hash at offset {hash_offset}: {vote_hash}")
    except IndexError:
        vote_hash = "Error extracting hash"

    # Step 3: Decode lockouts
    confirmation_count_1_slot = decode_lockouts(data, root)

    # Output only the requested values
    return {
        "base_slot": root,
        "hash": vote_hash,
        "block_voted_on": confirmation_count_1_slot,
    }

def extract_vote_data(slot, block_data, epoch_number):
    vote_data_list = []
    transactions = block_data.get('transactions', [])

    for tx_index, tx in enumerate(transactions):
        tx_message = tx['transaction']['message']
        account_keys = tx_message['accountKeys']
        signature = tx['transaction']['signatures'][0]  # Extract signature

        if "Vote111111111111111111111111111111111111111" in account_keys:
            vote_program_index = account_keys.index("Vote111111111111111111111111111111111111111")

            if vote_program_index >= 2:
                vote_authority = account_keys[vote_program_index - 2]
                vote_account = account_keys[vote_program_index - 1]

                instructions = tx_message['instructions']
                vote_instruction_count = 0  # Counter for vote instructions in this transaction

                for instr_index, instr in enumerate(instructions):
                    if instr['programIdIndex'] == vote_program_index:
                        vote_instruction_count += 1
                        try:
                            encoded_data = instr['data']
                            decoded_data = base58.b58decode(encoded_data)
                            result = decode_vote_state(decoded_data)

                            # Extract the block_voted_on correctly
                            block_voted_on = result.get("block_voted_on")

                            # Append parsed data
                            vote_data_list.append({
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
                            logging.warning(f"Error parsing vote data in transaction {tx_index}, instruction {instr_index}: {e}")
                            continue

                # Log a warning if there are more than one vote instruction in the transaction
                if vote_instruction_count > 1:
                    logging.warning(
                        f"Transaction {tx_index} (signature: {signature}) contains {vote_instruction_count} vote-related instructions."
                    )

    return vote_data_list

