from solana.rpc.api import Client
from solana.rpc.types import TxOpts
from solders.pubkey import Pubkey
from solders.system_program import TransferParams, transfer
import time
import json
from typing import Optional, List
import sys
import tenacity

# Default values
DEFAULT_FROM_KEY = "9oL4TA6kDKaZLAb6tA84JYZf4xkX8Ba25hs1BThFVKFK"
DEFAULT_TO_KEY = "tri1cHBy47fPyhCvrCf6FnR7Mz6XdSoSBah2FsZVQeT"  # Likely invalid
DEFAULT_RPC_ENDPOINT = "https://api.mainnet-beta.solana.com"

def validate_pubkey(pubkey_str: str) -> Optional[Pubkey]:
    """Validate and convert a string to a Solana Pubkey."""
    try:
        return Pubkey.from_string(pubkey_str)
    except ValueError:
        print(f"Invalid public key: {pubkey_str}")
        return None

def get_epoch_for_slot(client: Client, slot: int) -> int:
    """Calculate the epoch for a given slot."""
    epoch_info = client.get_epoch_info().value
    if not epoch_info:
        raise ValueError("Failed to fetch epoch info")
    slots_per_epoch = epoch_info.slots_in_epoch
    return slot // slots_per_epoch

def prompt_for_parameters(
    from_key: Optional[str],
    to_key: Optional[str],
    epoch: Optional[int],
    rpc_endpoint: Optional[str]
) -> tuple[str, str, int, str]:
    """Prompt user for missing parameters or accept defaults."""
    if from_key is None:
        user_input = input(f"Enter from-key (default: {DEFAULT_FROM_KEY}): ").strip()
        from_key = user_input if user_input else DEFAULT_FROM_KEY

    if to_key is None:
        print(f"Warning: Default to-key ({DEFAULT_TO_KEY}) appears invalid. Please provide a valid Solana public key.")
        user_input = input(f"Enter to-key (default: {DEFAULT_TO_KEY}): ").strip()
        to_key = user_input if user_input else DEFAULT_TO_KEY

    if epoch is None:
        while True:
            user_input = input("Enter epoch (required, no default): ").strip()
            try:
                epoch = int(user_input)
                if epoch < 0:
                    print("Epoch must be non-negative.")
                    continue
                break
            except ValueError:
                print("Invalid epoch. Please enter a valid number.")

    if rpc_endpoint is None:
        user_input = input(f"Enter RPC endpoint (default: {DEFAULT_RPC_ENDPOINT}): ").strip()
        rpc_endpoint = user_input if user_input else DEFAULT_RPC_ENDPOINT

    return from_key, to_key, epoch, rpc_endpoint

@tenacity.retry(
    stop=tenacity.stop_after_attempt(3),
    wait=tenacity.wait_fixed(1),
    retry=tenacity.retry_if_exception_type(Exception),
    reraise=True
)
def get_signatures_with_retry(client: Client, pubkey: Pubkey, before: Optional[str], limit: int = 1000) -> List[dict]:
    """Fetch signatures with retry logic."""
    return client.get_signatures_for_address(pubkey, limit=limit, before=before).value or []

@tenacity.retry(
    stop=tenacity.stop_after_attempt(3),
    wait=tenacity.wait_fixed(1),
    retry=tenacity.retry_if_exception_type(Exception),
    reraise=True
)
def get_transaction_with_retry(client: Client, signature: str) -> Optional[dict]:
    """Fetch transaction with retry logic."""
    return client.get_transaction(signature, encoding="jsonParsed", max_supported_transaction_version=0).value

def find_sol_transfers(
    from_key: Optional[str] = None,
    to_key: Optional[str] = None,
    epoch: Optional[int] = None,
    rpc_endpoint: Optional[str] = None
) -> None:
    """Find SOL transfers from from_key to to_key in the specified epoch."""
    # Prompt for missing parameters
    from_key, to_key, epoch, rpc_endpoint = prompt_for_parameters(from_key, to_key, epoch, rpc_endpoint)

    # Validate public keys
    from_pubkey = validate_pubkey(from_key)
    to_pubkey = validate_pubkey(to_key)
    if not from_pubkey or not to_pubkey:
        print("Invalid public key(s). Exiting.")
        return

    # Initialize Solana client
    try:
        client = Client(rpc_endpoint)
        if not client.is_connected():
            print("Failed to connect to RPC endpoint.")
            return
    except Exception as e:
        print(f"Error connecting to RPC: {e}")
        return

    # Fetch transaction signatures for the from_key
    signatures: List[dict] = []
    before = None
    while True:
        try:
            response = get_signatures_with_retry(client, from_pubkey, before)
            if not response:
                break
            signatures.extend(response)
            before = response[-1].signature if response else None
        except Exception as e:
            print(f"Error fetching signatures: {e}")
            break
        if not before:
            break

    print(f"Found {len(signatures)} transactions for from-key {from_key}")

    # Process transactions
    for sig_info in signatures:
        slot = sig_info.slot
        signature = sig_info.signature
        try:
            # Check if transaction is in the target epoch
            transaction_epoch = get_epoch_for_slot(client, slot)
            if transaction_epoch != epoch:
                continue

            # Fetch transaction details
            tx = get_transaction_with_retry(client, str(signature))
            if not tx:
                print(f"Transaction {signature} not found or unsupported.")
                continue

            # Check for SOL transfers
            instructions = tx.transaction.message.instructions
            for instruction in instructions:
                if (
                    instruction.program_id == Pubkey.from_string("11111111111111111111111111111111")  # System Program
                    and "parsed" in instruction
                    and instruction["parsed"]["type"] == "transfer"
                ):
                    info = instruction["parsed"]["info"]
                    if (
                        info["source"] == str(from_pubkey)
                        and info["destination"] == str(to_pubkey)
                    ):
                        lamports = info["lamports"]
                        sol = lamports / 1_000_000_000
                        timestamp = tx.block_time
                        timestamp_str = (
                            time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(timestamp))
                            if timestamp
                            else "Unknown"
                        )
                        print(json.dumps({
                            "signature": str(signature),
                            "slot": slot,
                            "epoch": transaction_epoch,
                            "timestamp": timestamp_str,
                            "sol_transferred": sol
                        }, indent=2))
        except Exception as e:
            print(f"Error processing transaction {signature}: {str(e)}")

if __name__ == "__main__":
    # Parse command-line arguments
    from_key = None
    to_key = None
    epoch = None
    rpc_endpoint = None

    # Handle command-line arguments
    args = sys.argv[1:]
    if len(args) >= 1:
        from_key = args[0] if len(args) > 0 else None
        to_key = args[1] if len(args) > 1 else None
        if len(args) > 2:
            try:
                epoch = int(args[2])
            except ValueError:
                print("Invalid epoch provided. Will prompt for epoch.")
                epoch = None
        rpc_endpoint = args[3] if len(args) > 3 else None

    find_sol_transfers(from_key, to_key, epoch, rpc_endpoint)