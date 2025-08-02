import matplotlib.pyplot as plt
import json
import psycopg2
import sys
import logging
from time import perf_counter
from typing import Dict, List, Tuple
from collections import defaultdict
import re

PROCESS_HISTORICAL_SUMMARY = False

# Define the threshold for top skip blame scores
SKIP_BLAME_TOP = 20
MIN_BLAME_SCORE = 0
DISTRIBUTION_SPLIT = 5

# Use a default font with wide glyph support
plt.rcParams['font.family'] = 'DejaVu Sans'

def strip_emojis(text):
    emoji_pattern = re.compile("["
        u"\U0001F600-\U0001F64F"  # emoticons
        u"\U0001F300-\U0001F5FF"  # symbols & pictographs
        u"\U0001F680-\U0001F6FF"  # transport & map symbols
        u"\U0001F1E0-\U0001F1FF"  # flags (iOS)
        u"\U00002702-\U000027B0"
        u"\U000024C2-\U0001F251"
        u"\U0001F300-\U0001F9FF"  # Extended symbols and pictographs
        u"\U0001FAE7"            # Missing glyph 129767
        u"\U0001FA90"            # Missing glyph 129680 (RINGED PLANET)
        "]+", flags=re.UNICODE)
    return emoji_pattern.sub(r'', text).strip()

def get_db_connection_string(db_params: Dict[str, str]) -> str:
    return f"postgresql://{db_params['user']}@{db_params['host']}:{db_params['port']}/{db_params['database']}?sslmode={db_params['sslmode']}"

def execute_query(cur, query: str, params: tuple = None) -> List[tuple]:
    try:
        start_time = perf_counter()
        if params:
            cur.execute(query, params)
        else:
            cur.execute(query)
        result = cur.fetchall()
        end_time = perf_counter()
        logging.info(f"Query completed in {end_time - start_time:.4f} seconds")
        return result
    except Exception as e:
        logging.error(f"Error executing query: {str(e)}")
        logging.error(f"Query: {query}")
        logging.error(f"Params: {params}")
        raise

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)

class SkipBlameCalculator:
    def __init__(self, epoch: int):
        self.epoch = epoch
        self.epoch_start_slot = epoch * 432000
        self.blame_scores = defaultdict(int)
        self.validator_names: Dict[str, str] = {}
        self.skip_details: Dict[str, List[Tuple[int, int]]] = defaultdict(list)
        self.total_validators = 0
        self.validators_with_blame = 0
        self.slot_four_counts = defaultdict(int)
        
    def load_and_analyze_data(self, cur) -> None:
        logging.info("Loading leader schedule data...")
        start_time = perf_counter()
        
        query = """
        WITH ordered_slots AS (
            SELECT 
                ls.identity_pubkey,
                ls.block_slot,
                ls.block_produced,
                vi.name as validator_name,
                (ls.block_slot - %s * 432000) %% 4 as position_in_group
            FROM leader_schedule ls
            LEFT JOIN validator_info vi ON ls.identity_pubkey = vi.identity_pubkey
            WHERE ls.epoch = %s
            ORDER BY ls.block_slot
        )
        SELECT 
            identity_pubkey,
            block_slot,
            block_produced,
            validator_name,
            position_in_group
        FROM ordered_slots;
        """
        
        verify_query = "SELECT COUNT(*) FROM leader_schedule WHERE epoch = %s"
        count_result = execute_query(cur, verify_query, (self.epoch,))
        if not count_result or count_result[0][0] == 0:
            logging.error(f"No data found for epoch {self.epoch}")
            return
            
        logging.info(f"Found {count_result[0][0]} slots for epoch {self.epoch}")
        
        results = execute_query(cur, query, (self.epoch, self.epoch))
        if not results:
            logging.error("No results returned from main query")
            return
            
        logging.info(f"Processing {len(results)} slots...")
        
        validator_set = set()

        for i in range(len(results) - 1):
            current_slot = results[i]

            validator_set.add(current_slot[0])

            # If this is slot 4 (position_in_group == 3)
            if current_slot[4] == 3:  # position_in_group = 3 (slot 4)
                current_pubkey = current_slot[0]
                self.slot_four_counts[current_pubkey] += 1  # Count slot 4s

                if current_slot[3]:
                    self.validator_names[current_pubkey] = current_slot[3]

                score = 0
                skipped_slots = 0

                # Check all subsequent slots for skips
                for j in range(i + 1, len(results)):
                    next_slot = results[j]
                    if not next_slot[2]:  # If the next slot is skipped
                        score += 1
                        skipped_slots += 1
                    else:
                        break  # Stop counting if a slot is produced

                # Only assign blame if at least one slot is skipped
                if score > 0:
                    self.blame_scores[current_pubkey] += score
                    self.skip_details[current_pubkey].append((current_slot[1], skipped_slots))
                
                self.total_validators = len(validator_set)
                self.validators_with_blame = len([v for v in self.blame_scores if self.blame_scores[v] > 0])
        
        end_time = perf_counter()
        logging.info(f"Analysis completed in {end_time - start_time:.4f} seconds")
    
    def get_results(self) -> List[Dict]:
        results = []
        for pubkey, score in self.blame_scores.items():
            slot_four_count = self.slot_four_counts.get(pubkey, 1)  # Default to 1 to avoid division by zero
            stake_weighted_skip_blame_score = score / slot_four_count if slot_four_count > 0 else 0
            
            results.append({
                "identity_pubkey": pubkey,
                "validator_name": self.validator_names.get(pubkey, "Unknown"),
                "skip_blame_score": score,
                "stake_weighted_skip_blame_score": stake_weighted_skip_blame_score
            })
        
        return sorted(results, key=lambda x: x["stake_weighted_skip_blame_score"], reverse=True)
    
    def plot_top_validators(self) -> None:
        results = [r for r in self.get_results() if r["skip_blame_score"] > SKIP_BLAME_TOP]
        if not results:
            logging.info(f"No validators with skip blame score greater than {SKIP_BLAME_TOP}.")
            return

        top_validators = results
        pubkeys = [f"{r['identity_pubkey'][:7]} - {strip_emojis(r['validator_name'])}" if r['validator_name'] != "Unknown" else r['identity_pubkey'] for r in top_validators]
        scores = [r["stake_weighted_skip_blame_score"] for r in top_validators]

        validator_count = len(top_validators)
        plt.figure(figsize=(12, max(8, validator_count * 0.3)))  # Dynamic height based on count
        plt.subplots_adjust(left=0.4)  # Increase space for labels
        bars = plt.barh(pubkeys, scores, color='skyblue')
        plt.xlabel('Stake Weighted Skip Blame Score')
        plt.title(f'Top {validator_count} Validators by Stake Weighted Skip Blame Score (Score > {SKIP_BLAME_TOP}) for Epoch: {self.epoch}')
        plt.gca().invert_yaxis()

        for bar, score in zip(bars, scores):
            plt.text(bar.get_width() + 0.1, bar.get_y() + bar.get_height() / 2, f"{score:.2f}", va='center')

        plt.tight_layout()
        plt.savefig(f'stake_weighted_skip_blame_top_validators_epoch_{self.epoch}.png')
        plt.close()

        print("")
        logging.info(f"Chart saved as 'stake_weighted_skip_blame_top_validators_epoch_{self.epoch}.png'")

    def plot_blame_distributions(self) -> None:
        lower_scores = [r["stake_weighted_skip_blame_score"] for r in self.get_results() if MIN_BLAME_SCORE <= r["stake_weighted_skip_blame_score"] <= DISTRIBUTION_SPLIT]
        higher_scores = [r["stake_weighted_skip_blame_score"] for r in self.get_results() if r["stake_weighted_skip_blame_score"] > DISTRIBUTION_SPLIT]

        # Plot low scores
        plt.figure(figsize=(10, 6))
        plt.hist(lower_scores, bins=DISTRIBUTION_SPLIT-MIN_BLAME_SCORE, color='skyblue', edgecolor='black')
        plt.xlabel('Stake Weighted Skip Blame Score')
        plt.ylabel('Number of Validators') 
        plt.title(f'Distribution of Stake Weighted Skip Blame Scores for {len(lower_scores)} Validators ({MIN_BLAME_SCORE}-{DISTRIBUTION_SPLIT}) - Epoch {self.epoch}')
        plt.grid(True, alpha=0.3)
        plt.savefig(f'stake_weighted_skip_blame_distribution_low_epoch_{self.epoch}.png')
        plt.close()

        # Plot high scores
        if higher_scores:
            plt.figure(figsize=(10, 6))
            plt.hist(higher_scores, bins=min(len(set(higher_scores)), 50), color='skyblue', edgecolor='black')
            plt.xlabel('Stake Weighted Skip Blame Score')
            plt.ylabel('Number of Validators')
            plt.title(f'Distribution of Stake Weighted Skip Blame Scores for {len(higher_scores)} Validators (>{DISTRIBUTION_SPLIT}) - Epoch {self.epoch}')
            plt.grid(True, alpha=0.3)
            plt.savefig(f'stake_weighted_skip_blame_distribution_high_epoch_{self.epoch}.png')
            plt.close()

        logging.info(f"Stake Weighted Blame score distribution charts saved as 'stake_weighted_skip_blame_distribution_low_epoch_{self.epoch}.png' and 'stake_weighted_skip_blame_distribution_high_epoch_{self.epoch}.png'")

def save_json_file(filename: str, data: Dict) -> None:
    try:
        with open(filename, 'w') as f:
            json.dump(data, f, indent=2)
        logging.info(f"Data saved to '{filename}'")
    except Exception as e:
        logging.error(f"Failed to save data to {filename}: {str(e)}")

def main():
    from db_config import db_params

    try:
        conn = psycopg2.connect(get_db_connection_string(db_params))
        cur = conn.cursor()

        if len(sys.argv) > 1:
            try:
                epoch = int(sys.argv[1])
            except ValueError:
                print(f"Error: Invalid epoch number '{sys.argv[1]}'")
                sys.exit(1)
        else:
            epoch_query = "SELECT MAX(epoch) FROM leader_schedule"
            epoch_result = execute_query(cur, epoch_query)
            if not epoch_result or epoch_result[0][0] is None:
                print("Could not determine latest epoch")
                return

            latest_epoch = epoch_result[0][0]
            print(f"\nLatest epoch: {latest_epoch}")

            while True:
                try:
                    epoch_input = input(f"Enter epoch to analyze (press Enter for latest epoch {latest_epoch}): ")
                    epoch = int(epoch_input) if epoch_input else latest_epoch

                    if epoch <= 616:
                        print("Please enter a valid epoch number greater than 615")
                        continue
                    break
                except ValueError:
                    print("Please enter a valid epoch number")

        print(f"\nProcessing epoch: {epoch}")

        # Process current epoch
        calculator = SkipBlameCalculator(epoch)
        calculator.load_and_analyze_data(cur)
        results = calculator.get_results()

        # Create and save current epoch analysis
        total_blame_score = sum(r["skip_blame_score"] for r in results)
        validators_with_blame = len([r for r in results if r["skip_blame_score"] > 0])
        
        blame_scores = [r["skip_blame_score"] for r in results if r["skip_blame_score"] > 0]
        median_score = 0
        if blame_scores:
            blame_scores.sort()
            mid = len(blame_scores) // 2
            median_score = blame_scores[mid] if len(blame_scores) % 2 != 0 else (blame_scores[mid-1] + blame_scores[mid]) / 2
        
        current_summary = {
            "epoch": epoch,
            "total_blame_score": total_blame_score,
            "validators_with_blame": validators_with_blame,
            "average_blame_score": round(total_blame_score / validators_with_blame if validators_with_blame > 0 else 0, 2),
            "median_blame_score": round(median_score, 2)
        }

        # Save current epoch analysis
        save_json_file(
            f'skip_blame_analysis_epoch_{epoch}.json',
            {
                "summary": current_summary,
                "validators": [{"identity_pubkey": r["identity_pubkey"], 
                                "validator_name": r["validator_name"], 
                                "skip_blame_score": r["skip_blame_score"], 
                                "stake_weighted_skip_blame_score": r["stake_weighted_skip_blame_score"]} for r in results]
            }
        )

        # Save top validators
        top_validators = [r for r in results if r["stake_weighted_skip_blame_score"] > SKIP_BLAME_TOP]
        if top_validators:
            save_json_file(
                f'stake_weighted_skip_blame_top_validators_epoch_{epoch}.json',
                top_validators
            )

        # Process historical data
        historical_summaries = []
        start_epoch = max(0, epoch - 9)

        # historical data processing
        if PROCESS_HISTORICAL_SUMMARY:
            print(f"\nProcessing historical data from epoch {start_epoch} to {epoch}...")

            for hist_epoch in range(start_epoch, epoch + 1):
                print(f"Processing historical epoch: {hist_epoch}")
                hist_calculator = SkipBlameCalculator(hist_epoch)
                hist_calculator.load_and_analyze_data(cur)
                hist_results = hist_calculator.get_results()
                
                total_blame = sum(r["skip_blame_score"] for r in hist_results)
                validators_blame = len([r for r in hist_results if r["skip_blame_score"] > 0])
                
                blame_scores = [r["skip_blame_score"] for r in hist_results if r["skip_blame_score"] > 0]
                median = 0
                if blame_scores:
                    blame_scores.sort()
                    mid = len(blame_scores) // 2
                    median = blame_scores[mid] if len(blame_scores) % 2 != 0 else (blame_scores[mid-1] + blame_scores[mid]) / 2
                
                historical_summaries.append({
                    "epoch": hist_epoch,
                    "total_blame_score": total_blame,
                    "validators_with_blame": validators_blame,
                    "average_blame_score": round(total_blame / validators_blame if validators_blame > 0 else 0, 2),
                    "median_blame_score": round(median, 2)
                })

            # Calculate and save historical summary
            valid_epochs = [s for s in historical_summaries if s["validators_with_blame"] > 0]
            avg_total_blame = sum(s["total_blame_score"] for s in valid_epochs) / len(valid_epochs) if valid_epochs else 0
            avg_validators = sum(s["validators_with_blame"] for s in valid_epochs) / len(valid_epochs) if valid_epochs else 0
            avg_blame_score = sum(s["average_blame_score"] for s in valid_epochs) / len(valid_epochs) if valid_epochs else 0
            avg_median_score = sum(s["median_blame_score"] for s in valid_epochs) / len(valid_epochs) if valid_epochs else 0

            historical_summary = {
                "epochs_analyzed": len(historical_summaries),
                "epoch_range": {
                    "start": start_epoch,
                    "end": epoch
                },
                "aggregate_statistics": {
                    "average_total_blame_score": round(avg_total_blame, 2),
                    "average_validators_with_blame": round(avg_validators, 2),
                    "average_blame_score": round(avg_blame_score, 2),
                    "average_median_score": round(avg_median_score, 2)
                },
                "epoch_summaries": historical_summaries
            }

            save_json_file(
                f'skip_blame_historical_{start_epoch}_to_{epoch}.json',
                historical_summary
            )

        # Print summaries
        print(f"\nSummary for epoch {epoch}:")
        print(f"Total blame score: {total_blame_score}")
        print(f"Validators with blame: {validators_with_blame}")
        print(f"Average blame score: {current_summary['average_blame_score']}")
        print(f"Median blame score: {current_summary['median_blame_score']}")

        if PROCESS_HISTORICAL_SUMMARY:
            print(f"\nHistorical Summary (Epochs {start_epoch} to {epoch}):")
            print(f"Average total blame score: {historical_summary['aggregate_statistics']['average_total_blame_score']}")
            print(f"Average validators with blame: {historical_summary['aggregate_statistics']['average_validators_with_blame']}")
            print(f"Average blame score: {historical_summary['aggregate_statistics']['average_blame_score']}")
            print(f"Average median score: {historical_summary['aggregate_statistics']['average_median_score']}")
        
        # Analyze scores by divisibility
        div_by_4 = 0
        div_by_3 = 0
        div_by_2_only = 0  # Divisible by 2 but not 4
        other = 0

        for r in results:
            score = r["skip_blame_score"]
            if score > 0:
                if score % 4 == 0:
                    div_by_4 += 1
                elif score % 3 == 0:
                    div_by_3 += 1
                elif score % 2 == 0:
                    div_by_2_only += 1
                else:
                    other += 1

        total = div_by_4 + div_by_3 + div_by_2_only + other
        if total > 0:
            print(f"\nScore distribution analysis:")
            print(f"Divisible by 4: {div_by_4} ({(div_by_4/total*100):.1f}%)")
            print(f"Divisible by 3: {div_by_3} ({(div_by_3/total*100):.1f}%)")
            print(f"Divisible by 2 only: {div_by_2_only} ({(div_by_2_only/total*100):.1f}%)")
            print(f"Not divisible by 2,3,4: {other} ({(other/total*100):.1f}%)")
        
        if results:
            top_validators = [r for r in results if r["stake_weighted_skip_blame_score"] > SKIP_BLAME_TOP]
            # jrh print(f"\nTop {len(top_validators)} validators with stake weighted skip blame score > {SKIP_BLAME_TOP}:")
            for r in top_validators:
                display_name = f"{r['identity_pubkey'][:7]} - {strip_emojis(r['validator_name'])}" if r['validator_name'] != "Unknown" else r['identity_pubkey']
                # jrh print(f"{display_name}: {r['stake_weighted_skip_blame_score']:.2f}")

        # Optional: Generate visualization
        calculator.plot_top_validators()
        calculator.plot_blame_distributions()

    except Exception as e:
        logging.error(f"An error occurred: {str(e)}")
        sys.exit(1)
    finally:
        if 'cur' in locals():
            cur.close()
        if 'conn' in locals():
            conn.close()

if __name__ == "__main__":
    main()