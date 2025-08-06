import matplotlib.pyplot as plt
import json
import psycopg2
import sys
import importlib.util
import os

# Setup unified logging
script_dir = os.path.dirname(os.path.abspath(__file__))
logging_config_path = os.path.join(script_dir, "999_logging_config.py")
spec = importlib.util.spec_from_file_location("logging_config", logging_config_path)
logging_config = importlib.util.module_from_spec(spec)
spec.loader.exec_module(logging_config)
logger = logging_config.setup_logging(os.path.basename(__file__).replace('.py', ''))
from time import perf_counter
from typing import Dict, List, Tuple
from collections import defaultdict
import re

# Define the threshold for top skip blame scores
SKIP_BLAME_TOP = 60
MIN_BLAME_SCORE = 0
DISTRIBUTION_SPLIT = 24
EPOCH_RANGE = 10

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
        logger.info(f"Query completed in {end_time - start_time:.4f} seconds")
        return result
    except Exception as e:
        logger.error(f"Error executing query: {str(e)}")
        logger.error(f"Query: {query}")
        logger.error(f"Params: {params}")
        raise

# Logging config moved to unified configurations - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)

class SkipBlameCalculator:
    def __init__(self, epochs: List[int]):
        self.epochs = epochs
        self.blame_scores = defaultdict(int)
        self.validator_names: Dict[str, str] = {}
        self.skip_details: Dict[str, List[Tuple[int, int]]] = defaultdict(list)
        self.total_validators = 0
        self.validators_with_blame = 0
        
    def load_and_analyze_data(self, cur) -> None:
        logger.info(f"Loading leader schedule data for epochs {min(self.epochs)} to {max(self.epochs)}...")
        start_time = perf_counter()
        
        for epoch in self.epochs:
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
            count_result = execute_query(cur, verify_query, (epoch,))
            if not count_result or count_result[0][0] == 0:
                logger.error(f"No data found for epoch {epoch}")
                continue
                
            logger.info(f"Found {count_result[0][0]} slots for epoch {epoch}")
            
            results = execute_query(cur, query, (epoch, epoch))
            if not results:
                logger.error(f"No results returned from main query for epoch {epoch}")
                continue
                
            logger.info(f"Processing {len(results)} slots for epoch {epoch}...")
            
            validator_set = set()

            for i in range(len(results) - 1):
                current_slot = results[i]

                validator_set.add(current_slot[0])

                if current_slot[4] == 3:  # position_in_group = 3 (slot 4)
                    current_pubkey = current_slot[0]

                    if current_slot[3]:
                        self.validator_names[current_pubkey] = current_slot[3]

                    score = 0
                    skipped_slots = 0

                    for j in range(i + 1, len(results)):
                        next_slot = results[j]
                        if not next_slot[2]:  # If the next slot is skipped
                            score += 1
                            skipped_slots += 1
                        else:
                            break

                    if score > 0:
                        self.blame_scores[current_pubkey] += score
                        self.skip_details[current_pubkey].append((current_slot[1], skipped_slots))
                    
                    self.total_validators = len(validator_set)
                    self.validators_with_blame = len([v for v in self.blame_scores if self.blame_scores[v] > 0])
        
        end_time = perf_counter()
        logger.info(f"Analysis completed in {end_time - start_time:.4f} seconds")
    
    def get_results(self) -> List[Dict]:
        results = []
        for pubkey, score in self.blame_scores.items():
            averaged_score = round(score / EPOCH_RANGE)  # Average over 10 epochs
            if averaged_score > 0:  # Only include if there's a non-zero average
                results.append({
                    "identity_pubkey": pubkey,
                    "validator_name": self.validator_names.get(pubkey, "Unknown"),
                    "skip_blame_score": averaged_score
                })
        
        return sorted(results, key=lambda x: x["skip_blame_score"], reverse=True)
    
    def plot_top_validators(self) -> None:
        results = [r for r in self.get_results() if r["skip_blame_score"] > SKIP_BLAME_TOP]
        if not results:
            logger.info(f"No validators with skip blame score greater than {SKIP_BLAME_TOP}.")
            return

        top_validators = results
        pubkeys = [f"{r['identity_pubkey'][:7]} - {strip_emojis(r['validator_name'])}" if r['validator_name'] != "Unknown" else r['identity_pubkey'] for r in top_validators]
        scores = [r["skip_blame_score"] for r in top_validators]

        validator_count = len(top_validators)
        
        # Adjust figure size and margins
        plt.figure(figsize=(12, max(6, validator_count * 0.25)))
        plt.subplots_adjust(left=0.4, top=0.95, bottom=0.1)
        
        # Create horizontal bar chart
        bars = plt.barh(pubkeys, scores, color='skyblue')
        plt.xlabel('Skip Blame Score')
        plt.title(f'Top {validator_count} Validators by Skip Blame Score (Score > {SKIP_BLAME_TOP})\nEpochs {min(self.epochs)} to {max(self.epochs)}')
        plt.gca().invert_yaxis()
        
        # Add score labels
        for bar, score in zip(bars, scores):
            plt.text(bar.get_width() + 0.1, bar.get_y() + bar.get_height() / 2, str(score), va='center')

        output_file = f'skip_blame_top_validators_epoch_{max(self.epochs)}_10_epochs.png'
        plt.savefig(output_file, bbox_inches='tight', pad_inches=0.3)
        plt.close()
        
        logger.info(f"Generated visualization: {output_file}")

    def plot_blame_distributions(self) -> None:
        lower_scores = [score for score in self.blame_scores.values() if MIN_BLAME_SCORE <= score <= DISTRIBUTION_SPLIT]
        higher_scores = [score for score in self.blame_scores.values() if score > DISTRIBUTION_SPLIT]

        low_output = f'skip_blame_distribution_low_epoch_{max(self.epochs)}_10_epochs.png'
        plt.figure(figsize=(10, 6))
        plt.hist(lower_scores, bins=DISTRIBUTION_SPLIT-MIN_BLAME_SCORE, color='skyblue', edgecolor='black')
        plt.xlabel('Skip Blame Score')
        plt.ylabel('Number of Validators')
        plt.title(f'Distribution of Skip Blame Scores for {len(lower_scores)} Validators ({MIN_BLAME_SCORE}-{DISTRIBUTION_SPLIT})\nEpochs {min(self.epochs)} to {max(self.epochs)}')
        plt.grid(True, alpha=0.3)
        plt.savefig(low_output, bbox_inches='tight', pad_inches=0.3)
        plt.close()
        logger.info(f"Generated visualization: {low_output}")

        if higher_scores:
            high_output = f'skip_blame_distribution_high_epoch_{max(self.epochs)}_10_epochs.png'
            plt.figure(figsize=(10, 6))
            plt.hist(higher_scores, bins=min(len(set(higher_scores)), 50), color='skyblue', edgecolor='black')
            plt.xlabel('Skip Blame Score')
            plt.ylabel('Number of Validators')
            plt.title(f'Distribution of Skip Blame Scores for {len(higher_scores)} Validators (>{DISTRIBUTION_SPLIT})\nEpochs {min(self.epochs)} to {max(self.epochs)}')
            plt.grid(True, alpha=0.3)
            plt.savefig(high_output, bbox_inches='tight', pad_inches=0.3)
            plt.close()
            logger.info(f"Generated visualization: {high_output}")

def save_json_file(filename: str, data: Dict) -> None:
    try:
        with open(filename, 'w') as f:
            json.dump(data, f, indent=2)
        logger.info(f"Generated JSON file: {filename}")
    except Exception as e:
        logger.error(f"Failed to save data to {filename}: {str(e)}")

def main():
    from db_config import db_params

    try:
        conn = psycopg2.connect(get_db_connection_string(db_params))
        cur = conn.cursor()

        if len(sys.argv) > 1:
            try:
                latest_epoch = int(sys.argv[1])
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
                    latest_epoch = int(epoch_input) if epoch_input else latest_epoch

                    if latest_epoch <= 616:
                        print("Please enter a valid epoch number greater than 615")
                        continue
                    break
                except ValueError:
                    print("Please enter a valid epoch number")

        start_epoch = max(616, latest_epoch - EPOCH_RANGE + 1)
        epochs = list(range(start_epoch, latest_epoch + 1))
        print(f"\nProcessing epochs {start_epoch} to {latest_epoch}")

        calculator = SkipBlameCalculator(epochs)
        calculator.load_and_analyze_data(cur)
        results = calculator.get_results()

        # Create and save current analysis
        total_blame_score = sum(r["skip_blame_score"] for r in results)
        validators_with_blame = len([r for r in results if r["skip_blame_score"] > 0])
        
        blame_scores = [r["skip_blame_score"] for r in results if r["skip_blame_score"] > 0]
        median_score = 0
        if blame_scores:
            blame_scores.sort()
            mid = len(blame_scores) // 2
            median_score = blame_scores[mid] if len(blame_scores) % 2 != 0 else (blame_scores[mid-1] + blame_scores[mid]) / 2

        summary = {
            "epoch_range": {
                "start": start_epoch,
                "end": latest_epoch
            },
            "total_blame_score": total_blame_score,
            "validators_with_blame": validators_with_blame,
            "average_blame_score": round(total_blame_score / validators_with_blame if validators_with_blame > 0 else 0, 2),
            "median_blame_score": round(median_score, 2)
        }

        # Save analysis
        save_json_file(
            f'skip_blame_analysis_epoch_{latest_epoch}_10_epochs.json',
            {
                "summary": summary,
                "validators": results
            }
        )

        # Save top validators
        top_validators = [r for r in results if r["skip_blame_score"] > SKIP_BLAME_TOP]
        if top_validators:
            save_json_file(
                f'skip_blame_top_validators_epoch_{latest_epoch}_10_epochs.json',
                top_validators
            )
      
        # Print summary
        print(f"\nSummary for epochs {start_epoch} to {latest_epoch}:")
        print(f"Total blame score: {total_blame_score}")
        print(f"Validators with blame: {validators_with_blame}")
        print(f"Average blame score: {summary['average_blame_score']}")
        print(f"Median blame score: {summary['median_blame_score']}")
        
        if results:
            top_validators = [r for r in results if r["skip_blame_score"] > SKIP_BLAME_TOP]
            print(f"\nTop {len(top_validators)} validators with blame score > {SKIP_BLAME_TOP}:")
            for r in top_validators:
                display_name = f"{r['identity_pubkey'][:7]} - {strip_emojis(r['validator_name'])}" if r['validator_name'] != "Unknown" else r['identity_pubkey']
                print(f"{display_name}: {r['skip_blame_score']}")

        # Generate visualizations
        calculator.plot_top_validators()
        calculator.plot_blame_distributions()

    except Exception as e:
        logger.error(f"An error occurred: {str(e)}")
        sys.exit(1)
    finally:
        if 'cur' in locals():
            cur.close()
        if 'conn' in locals():
            conn.close()

if __name__ == "__main__":
    main()