import argparse
import base58
from solders.pubkey import Pubkey
from solana.rpc.api import Client
from anchorpy import Program, Provider, Wallet
from anchorpy.program.common import borsh as borsh_types
from dataclasses import dataclass
from typing import Optional
from construct import Struct, Int8ul, Int64ul, Int16ul, Flag, Bytes, Optional as ConstructOptional

# Define Borsh serialization structures (equivalent to Rust structs)
Config = Struct(
    "authority" / Bytes(32),
    "expired_funds_account" / Bytes(32),
    "num_epochs_valid" / Int64ul,
    "max_validator_commission_bps" / Int16ul,
    "bump" / Int8ul
)

MerkleRoot = Struct(
    "root" / Bytes(32),
    "max_total_claim" / Int64ul,
    "max_num_nodes" / Int64ul,
    "total_funds_claimed" / Int64ul,
    "num_nodes_claimed" / Int64ul
)

TipDistributionAccount = Struct(
    "validator_vote_account" / Bytes(32),
    "merkle_root_upload_authority" / Bytes(32),
    "epoch_created_at" / Int64ul,
    "validator_commission_bps" / Int16ul,
    "expires_at" / Int64ul,
    "bump" / Int8ul,
    "merkle_root" / ConstructOptional(MerkleRoot)
)

ClaimStatus = Struct(
    "is_claimed" / Flag,
    "claimant" / Bytes(32),
    "claim_status_payer" / Bytes(32),
    "slot_claimed_at" / Int64ul,
    "amount" / Int64ul,
    "expires_at" / Int64ul,
    "bump" / Int8ul
)

@dataclass
class UpdateConfigArgs:
    new_config: dict

@dataclass
class UpdateConfigAccounts:
    config: Pubkey
    authority: Pubkey

def derive_config_account_address(program_id: Pubkey) -> tuple[Pubkey, int]:
    return Pubkey.find_program_address([b"config"], program_id)

def derive_tip_distribution_account_address(program_id: Pubkey, vote_account: Pubkey, epoch: int) -> tuple[Pubkey, int]:
    return Pubkey.find_program_address(
        [b"tip_distribution", bytes(vote_account), epoch.to_bytes(8, "little")],
        program_id
    )

def update_config_ix(program_id: Pubkey, args: UpdateConfigArgs, accounts: UpdateConfigAccounts):
    # Simplified instruction creation (actual implementation would depend on program IDL)
    data = Config.build({
        "authority": bytes(args.new_config["authority"]),
        "expired_funds_account": bytes(args.new_config["expired_funds_account"]),
        "num_epochs_valid": args.new_config["num_epochs_valid"],
        "max_validator_commission_bps": args.new_config["max_validator_commission_bps"],
        "bump": args.new_config["bump"]
    })
    return {"program_id": program_id, "data": data, "accounts": accounts}

def main():
    parser = argparse.ArgumentParser(description="Solana Tip Distribution CLI")
    parser.add_argument("--rpc-url", default="http://localhost:8899", help="RPC URL for the Solana cluster")
    parser.add_argument("--program-id", default="4R3gSG8BpU4t19KYj8CfnbtRpnT8gtk4dvTHxVRwc2r7",
                        help="Tip Distribution program ID")
    subparsers = parser.add_subparsers(dest="command", required=True)

    # GetConfig command
    subparsers.add_parser("get-config", help="Get the config account information")

    # GetTipDistributionAccount command
    tda_parser = subparsers.add_parser("get-tip-distribution-account",
                                     help="Get tip distribution account information")
    tda_parser.add_argument("--vote-account", required=True, help="Validator vote account pubkey")
    tda_parser.add_argument("--epoch", type=int, required=True, help="Epoch for the tip distribution account")

    # GetClaimStatus command
    cs_parser = subparsers.add_parser("get-claim-status",
                                    help="Get claim status for a specific validator, epoch and claimant")
    cs_parser.add_argument("--vote-account", required=True, help="Validator vote account pubkey")
    cs_parser.add_argument("--epoch", type=int, required=True, help="Epoch for the tip distribution account")
    cs_parser.add_argument("--claimant", required=True, help="Claimant pubkey")

    # UpdateConfig command
    uc_parser = subparsers.add_parser("update-config", help="Update the config account information")
    uc_parser.add_argument("--authority", required=True, help="Authority pubkey")
    uc_parser.add_argument("--expired-funds-account", required=True, help="Expired funds account pubkey")
    uc_parser.add_argument("--num-epochs-valid", type=int, required=True, help="Number of epochs valid")
    uc_parser.add_argument("--max-validator-commission-bps", type=int, required=True,
                         help="Max validator commission BPS")
    uc_parser.add_argument("--bump", type=int, required=True, help="Bump")

    args = parser.parse_args()

    try:
        program_id = Pubkey.from_string(args.program_id)
    except ValueError as e:
        print(f"Invalid program ID: {e}")
        return

    client = Client(args.rpc_url)

    if args.command == "get-config":
        config_pda, _ = derive_config_account_address(program_id)
        print(f"Config Account Address: {config_pda}")

        try:
            account = client.get_account_info(config_pda).value
            if account is None:
                print("Account not found")
                return
            config = Config.parse(account.data)
            print("Config Account Data:")
            print(f"  Authority: {Pubkey(config.authority)}")
            print(f"  Expired Funds Account: {Pubkey(config.expired_funds_account)}")
            print(f"  Num Epochs Valid: {config.num_epochs_valid}")
            print(f"  Max Validator Commission BPS: {config.max_validator_commission_bps}")
            print(f"  Bump: {config.bump}")
        except Exception as e:
            print(f"Error fetching config: {e}")

    elif args.command == "get-tip-distribution-account":
        try:
            vote_pubkey = Pubkey.from_string(args.vote_account)
            tip_dist_pda, _ = derive_tip_distribution_account_address(program_id, vote_pubkey, args.epoch)
            print(f"Tip Distribution Account Address: {tip_dist_pda}")

            account = client.get_account_info(tip_dist_pda).value
            if account is None:
                print("Account not found")
                return
            tip_dist = TipDistributionAccount.parse(account.data)
            print("Tip Distribution Account Data:")
            print(f"  Vote Account: {Pubkey(tip_dist.validator_vote_account)}")
            print(f"  Merkle Root Upload Authority: {Pubkey(tip_dist.merkle_root_upload_authority)}")
            print(f"  Epoch Created At: {tip_dist.epoch_created_at}")
            print(f"  Validator Commission BPS: {tip_dist.validator_commission_bps}")
            print(f"  Expires At: {tip_dist.expires_at}")
            print(f"  Bump: {tip_dist.bump}")

            if tip_dist.merkle_root:
                print("  Merkle Root:")
                print(f"    Root: {tip_dist.merkle_root.root.hex()}")
                print(f"    Max Total Claim: {tip_dist.merkle_root.max_total_claim}")
                print(f"    Max Num Nodes: {tip_dist.merkle_root.max_num_nodes}")
                print(f"    Total Funds Claimed: {tip_dist.merkle_root.total_funds_claimed}")
                print(f"    Num Nodes Claimed: {tip_dist.merkle_root.num_nodes_claimed}")
            else:
                print("  Merkle Root: None")
        except Exception as e:
            print(f"Error fetching tip distribution account: {e}")

    elif args.command == "get-claim-status":
        try:
            vote_pubkey = Pubkey.from_string(args.vote_account)
            claimant_pubkey = Pubkey.from_string(args.claimant)
            tip_dist_pda, _ = derive_tip_distribution_account_address(program_id, vote_pubkey, args.epoch)
            claim_status_pda, _ = Pubkey.find_program_address(
                [b"claim_status", bytes(claimant_pubkey), bytes(tip_dist_pda)],
                program_id
            )
            print(f"Claim Status Account Address: {claim_status_pda}")

            account = client.get_account_info(claim_status_pda).value
            if account is None:
                print("Account not found")
                return
            claim_status = ClaimStatus.parse(account.data)
            print("Claim Status Data:")
            print(f"  Is Claimed: {claim_status.is_claimed}")
            print(f"  Claimant: {Pubkey(claim_status.claimant)}")
            print(f"  Claim Status Payer: {Pubkey(claim_status.claim_status_payer)}")
            print(f"  Slot Claimed At: {claim_status.slot_claimed_at}")
            print(f"  Amount: {claim_status.amount}")
            print(f"  Expires At: {claim_status.expires_at}")
            print(f"  Bump: {claim_status.bump}")
        except Exception as e:
            print(f"Error fetching claim status: {e}")

    elif args.command == "update-config":
        try:
            authority_pubkey = Pubkey.from_string(args.authority)
            expired_funds_account_pubkey = Pubkey.from_string(args.expired_funds_account)

            config = {
                "authority": authority_pubkey,
                "expired_funds_account": expired_funds_account_pubkey,
                "num_epochs_valid": args.num_epochs_valid,
                "max_validator_commission_bps": args.max_validator_commission_bps,
                "bump": args.bump
            }

            accounts = UpdateConfigAccounts(
                config=Pubkey.from_bytes(b"\x00" * 32),  # Default pubkey
                authority=authority_pubkey
            )

            instruction = update_config_ix(program_id, UpdateConfigArgs(config), accounts)
            serialized_data = instruction["data"]
            base58_data = base58.b58encode(serialized_data).decode()
            print(f"Base58 Serialized Data: {base58_data}")
        except Exception as e:
            print(f"Error updating config: {e}")

if __name__ == "__main__":
    main()