import psycopg2
import sys
import json
from db_config import db_params
import logging
from time import perf_counter
from collections import defaultdict, deque
from decimal import Decimal
from typing import Dict, List, Tuple, Any
from tqdm import tqdm
from concurrent.futures import ProcessPoolExecutor, as_completed
import os

# Logging setup
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)

def get_db_connection_string(db_params: Dict[str, str]) -> str:
    return f"postgresql://{db_params['user']}@{db_params['host']}:{db_params['port']}/{db_params['database']}?sslmode={db_params['sslmode']}"

def convert_decimal(value: Any) -> Any:
    if isinstance(value, Decimal):
        return float(value)
    return value

def execute_query(cur, query: str, params: tuple = None) -> List[tuple]:
    try:
        start_time = perf_counter()
        cur.execute(query, params or ())
        result = cur.fetchall()
        end_time = perf_counter()
        logging.info(f"Query completed in {end_time - start_time:.4f} seconds")
        return result
    except Exception as e:
        logging.error(f"Error executing query: {str(e)}")
        return []

class DecimalEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            return float(obj)
        return super().default(obj)

class LeaderScheduleData:
    def __init__(self, epoch: int):
        self.epoch = epoch
        self.epoch_start_slot = epoch * 432000
        self.skipped_slots: set = set()  # Use set for O(1) lookups
        self.validator_slots: Dict[str, List[int]] = defaultdict(list)  # pubkey -> slots
        self.validator_names: Dict[str, str] = {}
        self.slot_to_validator: Dict[int, str] = {}  # For correlation analysis
        self.skipped_by_position: Dict[int, int] = defaultdict(int)  # Precomputed skips by position
        self.validator_group_stats: Dict[str, Dict[str, int]] = {}  # Precomputed group stats
        self.slot1_to_slot4_correlations: Dict[str, List[dict]] = defaultdict(list)  # Precomputed correlations
        self.validator_skipped_slots: Dict[str, List[Tuple[int, int]]] = defaultdict(list)  # Precomputed skipped slots with positions
        self.validator_prior_stats: Dict[str, Dict[str, float]] = {}  # Precomputed prior skip stats

    def load_data(self, cur) -> None:
        """Load data efficiently and precompute aggregates to minimize per-validator work"""
        logging.info("Loading leader schedule data...")
        start_time = perf_counter()

        # Query 1: Get only skipped slots
        skip_query = """
        SELECT ls.identity_pubkey, ls.block_slot
        FROM leader_schedule ls
        WHERE ls.epoch = %s AND ls.block_produced = FALSE
        ORDER BY ls.block_slot
        """
        skipped_results = execute_query(cur, skip_query, (self.epoch,))
        self.skipped_slots = {row[1] for row in skipped_results}  # Use set for O(1) lookups

        # Query 2: Get all slot assignments and validator names
        main_query = """
        SELECT ls.identity_pubkey, ls.block_slot, vi.name
        FROM leader_schedule ls
        LEFT JOIN validator_info vi ON ls.identity_pubkey = vi.identity_pubkey
        WHERE ls.epoch = %s
        ORDER BY ls.block_slot
        """
        results = execute_query(cur, main_query, (self.epoch,))

        # Process results and precompute aggregates
        for pubkey, slot, name in results:
            self.validator_slots[pubkey].append(slot)
            self.slot_to_validator[slot] = pubkey
            if name and pubkey not in self.validator_names:
                self.validator_names[pubkey] = name
            # Precompute skipped_by_position and validator_skipped_slots
            if slot in self.skipped_slots:
                pos = (slot - self.epoch_start_slot) % 4
                self.skipped_by_position[pos] += 1
                self.validator_skipped_slots[pubkey].append((slot, pos))
                # Precompute slot4-to-slot1 correlations
                if pos == 0:  # Slot 1
                    prev_slot = slot - 1
                    if prev_slot in self.skipped_slots:
                        prev_pubkey = self.slot_to_validator[prev_slot]
                        self.slot1_to_slot4_correlations[pubkey].append({
                            "validator_name": self.validator_names.get(prev_pubkey, prev_pubkey),
                            "validator_pubkey": prev_pubkey,
                            "their_skipped_slot4": prev_slot,
                            "our_skipped_slot1": slot
                        })

        # Precompute group stats and prior skip stats for each validator
        for pubkey in self.validator_slots:
            slots = self.validator_slots[pubkey]
            self.validator_group_stats[pubkey] = self._calculate_group_stats(pubkey, slots)
            self.validator_prior_stats[pubkey] = self._calculate_prior_skip_stats(pubkey, slots)

        end_time = perf_counter()
        logging.info(f"Data loading completed in {end_time - start_time:.4f} seconds")

    def analyze_validator(self, pubkey: str) -> dict:
        """Analyze a validator's performance using precomputed data, minimizing computation"""
        slots = self.validator_slots.get(pubkey, [])
        if not slots:
            return self._create_empty_result(pubkey)

        total_slots = len(slots)
        total_groups = total_slots // 4
        skipped_slots = self.validator_skipped_slots.get(pubkey, [])

        if not skipped_slots:
            return self._create_empty_result(pubkey)

        skipped_by_position = defaultdict(int)
        for _, pos in skipped_slots:
            skipped_by_position[pos] += 1

        return {
            "epoch": self.epoch,
            "identity_pubkey": pubkey,
            "total_slots": total_slots,
            "total_leader_groups_4_slots": total_groups,
            "total_skipped_slots": len(skipped_slots),
            "skip_percentage": round(len(skipped_slots) / total_slots * 100, 2),
            "skipped_slots": {f"slot_{i+1}": skipped_by_position[i] for i in range(4)},
            "groups_with_skipped": self.validator_group_stats.get(pubkey, {f"{i}_slots_skipped": 0 for i in range(1, 5)}),
            "skipped_slot_numbers": [slot for slot, _ in skipped_slots],
            "prior_slot4_skip_to_our_slot1_skip": self.slot1_to_slot4_correlations.get(pubkey, []),
            "prior_skipped_stats": self.validator_prior_stats.get(pubkey, {"avg_prior_skipped": 0.0, "min_prior_skipped": 0, "max_prior_skipped": 0})
        }

    def _create_empty_result(self, pubkey: str) -> dict:
        total_slots = len(self.validator_slots.get(pubkey, []))
        return {
            "epoch": self.epoch,
            "identity_pubkey": pubkey,
            "total_slots": total_slots,
            "total_leader_groups_4_slots": total_slots // 4,
            "total_skipped_slots": 0,
            "skip_percentage": 0.0,
            "skipped_slots": {f"slot_{i+1}": 0 for i in range(4)},
            "groups_with_skipped": {f"{i}_slots_skipped": 0 for i in range(1, 5)},
            "skipped_slot_numbers": [],
            "prior_slot4_skip_to_our_slot1_skip": [],
            "prior_skipped_stats": {"avg_prior_skipped": 0.0, "min_prior_skipped": 0, "max_prior_skipped": 0}
        }

    def _calculate_group_stats(self, pubkey: str, slots: List[int]) -> Dict[str, int]:
        groups_dict = defaultdict(int)
        for i in range(0, len(slots), 4):
            group = slots[i:i+4]
            if len(group) == 4:
                skipped_count = sum(1 for slot in group if slot in self.skipped_slots)
                if skipped_count > 0:
                    groups_dict[f"{skipped_count}_slots_skipped"] += 1
        return {f"{i}_slots_skipped": groups_dict[f"{i}_slots_skipped"] for i in range(1, 5)}

    def _calculate_prior_skip_stats(self, pubkey: str, slots: List[int]) -> Dict[str, float]:
        if not slots:
            return {"avg_prior_skipped": 0.0, "min_prior_skipped": 0, "max_prior_skipped": 0}

        prior_skipped_counts = []
        look_back_window = 4  # Fixed window size
        window_skips = deque(maxlen=look_back_window)  # Efficient sliding window

        for i, slot in enumerate(slots):
            if slot in self.skipped_slots:
                prior_skips = sum(window_skips)  # Sum of skips in the window
                prior_skipped_counts.append(prior_skips)
            window_skips.append(1 if slot in self.skipped_slots else 0)  # Add current slot to window

        if not prior_skipped_counts:
            return {"avg_prior_skipped": 0.0, "min_prior_skipped": 0, "max_prior_skipped": 0}
        return {
            "avg_prior_skipped": round(sum(prior_skipped_counts) / len(prior_skipped_counts), 2),
            "min_prior_skipped": min(prior_skipped_counts),
            "max_prior_skipped": max(prior_skipped_counts)
        }

def process_validator_batch(pubkeys: List[str], data_manager: 'LeaderScheduleData') -> List[dict]:
    """Process a batch of validators in a single process to reduce overhead"""
    return [data_manager.analyze_validator(pubkey) for pubkey in pubkeys]

def main():
    conn = psycopg2.connect(get_db_connection_string(db_params))
    cur = conn.cursor()

    # Get epoch (unchanged from original)
    if len(sys.argv) > 1:
        try:
            epoch = int(sys.argv[1])
        except ValueError:
            print(f"Error: Invalid epoch number '{sys.argv[1]}'")
            sys.exit(1)
    else:
        epoch_result = execute_query(cur, "SELECT MAX(epoch) FROM leader_schedule")
        latest_epoch = epoch_result[0][0] if epoch_result else None
        if not latest_epoch:
            print("Could not determine latest epoch")
            return
        print(f"\nLatest epoch: {latest_epoch}")
        while True:
            try:
                epoch_input = input(f"Enter epoch to analyze (press Enter for {latest_epoch}): ")
                epoch = int(epoch_input) if epoch_input else latest_epoch
                if epoch <= 0:
                    print("Please enter a valid epoch number greater than 0")
                    continue
                break
            except ValueError:
                print("Please enter a valid epoch number")

    print(f"\nProcessing epoch: {epoch}")
    data_manager = LeaderScheduleData(epoch)
    data_manager.load_data(cur)

    identity_pubkeys = list(data_manager.validator_slots.keys())
    if not identity_pubkeys:
        print(f"No validators found for epoch {epoch}")
        return

    print(f"\nAnalyzing {len(identity_pubkeys)} validators...")
    total_slots = sum(len(slots) for slots in data_manager.validator_slots.values())
    total_skipped = len(data_manager.skipped_slots)
    skip_percentage = (total_skipped / total_slots * 100) if total_slots > 0 else 0

    print(f"\nEpoch {epoch} Overview:")
    print(f"Total slots: {total_slots:,}")
    print(f"Total produced: {total_slots - total_skipped:,}")
    print(f"Total missed: {total_skipped:,}")
    print(f"Total validators: {len(identity_pubkeys):,}")
    print(f"Overall skip percentage: {skip_percentage:.2f}%")

    # Parallel processing in batches, optimized for 32-core CPU
    results = []
    batch_size = max(1, len(identity_pubkeys) // 32)  # Ensure each core gets at least one batch
    pubkey_batches = [identity_pubkeys[i:i + batch_size] for i in range(0, len(identity_pubkeys), batch_size)]
    optimal_workers = min(len(pubkey_batches), 32)  # Use all 32 cores, but cap at number of batches
    print(f"\nUsing {optimal_workers} workers with batch size {batch_size}")
    with ProcessPoolExecutor(max_workers=optimal_workers) as executor:
        futures = {executor.submit(process_validator_batch, batch, data_manager): batch 
                   for batch in pubkey_batches}
        for future in tqdm(as_completed(futures), total=len(pubkey_batches), 
                          desc="Processing validator batches", unit="batch"):
            results.extend(future.result())

    # Aggregate totals
    total_by_position = defaultdict(int)
    total_groups_with_skips = defaultdict(int)
    for result in results:
        for pos in range(4):  # Use 0-based indexing (0, 1, 2, 3)
            total_by_position[pos] += result["skipped_slots"][f"slot_{pos+1}"]
        for i in range(1, 5):
            total_groups_with_skips[i] += result["groups_with_skipped"][f"{i}_slots_skipped"]

    epoch_summary = {
        "epoch": epoch,
        "total_validators": len(identity_pubkeys),
        "total_slots": total_slots,
        "total_leader_groups_4_slots": total_slots // 4,
        "total_blocks_produced": total_slots - total_skipped,
        "total_slots_skipped": total_skipped,
        "overall_slot_skip_percentage": round(skip_percentage, 2),
        "total_slot_1_skipped": total_by_position[0],  # Position 0 = slot 1
        "total_slot_2_skipped": total_by_position[1],  # Position 1 = slot 2
        "total_slot_3_skipped": total_by_position[2],  # Position 2 = slot 3
        "total_slot_4_skipped": total_by_position[3],  # Position 3 = slot 4
        "total_groups_with_1_skip": total_groups_with_skips[1],
        "total_groups_with_2_skips": total_groups_with_skips[2],
        "total_groups_with_3_skips": total_groups_with_skips[3],
        "total_groups_with_4_skips": total_groups_with_skips[4]
    }

    filename = f'skip_analysis_epoch_{epoch}.json'
    with open(filename, 'w') as f:
        json.dump({"summary": epoch_summary, "validators": results}, f, cls=DecimalEncoder, indent=2)
    print(f"\nResults saved to {filename}")

    cur.close()
    conn.close()

if __name__ == "__main__":
    main()