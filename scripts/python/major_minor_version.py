import json
import subprocess
import importlib.util
import datetime
import os
import sys
import logging
from collections import defaultdict

# Setup unified logging
spec = importlib.util.spec_from_file_location("logging_config", "/home/smilax/trillium_api/scripts/python/999_logging_config.py")
logging_config = importlib.util.module_from_spec(spec)
spec.loader.exec_module(logging_config)
logger = logging_config.setup_logging(os.path.basename(__file__).replace('.py', ''))

from db_config import db_params


# ---------------------------
# Functions
# ---------------------------
def run_command_with_retry(cmd, max_retries=3, retry_delay=5):
    """Run a command with retry logic for better reliability."""
    import time
    
    for attempt in range(max_retries):
        try:
            logger.info(f"Executing command (attempt {attempt + 1}/{max_retries}): {' '.join(cmd)}")
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                check=True,
                timeout=60  # 60-second timeout
            )
            return result
        except subprocess.TimeoutExpired:
            logger.warning(f"Command timed out on attempt {attempt + 1}")
            if attempt < max_retries - 1:
                time.sleep(retry_delay)
                continue
            raise
        except subprocess.CalledProcessError as e:
            logger.warning(f"Command failed on attempt {attempt + 1}: {e}")
            if attempt < max_retries - 1:
                time.sleep(retry_delay)
                continue
            raise

def get_solana_epoch(solana_flag):
    """Run 'solana epoch-info' command with the given flag and return the epoch number."""
    logger.info("Running 'solana epoch-info' command to retrieve epoch info.")
    cmd = [
        "/home/smilax/agave/bin/solana",
        "epoch-info",
        "--url",
        solana_flag.split()[1],
        "--output",
        "json"
    ]
    
    try:
        result = run_command_with_retry(cmd)
        epoch_info = json.loads(result.stdout)
        epoch = int(epoch_info.get("epoch", 0))
        logger.info(f"Epoch retrieved: {epoch}")
        return epoch
    except (subprocess.CalledProcessError, subprocess.TimeoutExpired) as e:
        logger.error(f"Failed to retrieve epoch info: {e}", exc_info=True)
        raise
    except json.JSONDecodeError as e:
        logger.error(f"Failed to parse epoch info JSON: {e}", exc_info=True)
        raise

def get_solana_validators(solana_flag):
    """Run 'solana validators' command with the given flag and return parsed JSON output."""
    logger.info("Running 'solana validators' command to retrieve validators data.")
    cmd = [
        "/home/smilax/agave/bin/solana",
        "validators",
        "--url",
        solana_flag.split()[1],
        "--output",
        "json"
    ]
    
    try:
        result = run_command_with_retry(cmd)
        validators_data = json.loads(result.stdout)
        logger.info(f"Validators data retrieved successfully.")
        return validators_data
    except (subprocess.CalledProcessError, subprocess.TimeoutExpired) as e:
        logger.error(f"Failed to retrieve validators data: {e}", exc_info=True)
        raise
    except json.JSONDecodeError as e:
        logger.error(f"Failed to parse validators JSON: {e}", exc_info=True)
        raise

def get_jito_data_mainnet():
    """Generate and run a .sql file to fetch total Jito data for mainnet summary."""
    timestamp = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
    sql_filename = f"jito_data_{timestamp}.sql"
    output_filename = f"jito_data_{timestamp}.csv"
    
    query = (
        "\\COPY (SELECT "
        "SUM(vs.activated_stake) / 1000000000.0 AS jito_stake, COUNT(*) AS jito_validators "
        "FROM validator_stats vs WHERE vs.epoch = (SELECT MAX(epoch) FROM validator_stats) AND vs.mev_earned > 0) "
        f"TO '{output_filename}' WITH (FORMAT CSV, HEADER)"
    )
    
    try:
        with open(sql_filename, "w") as sql_file:
            sql_file.write(query)
        
        logger.info(f"Running SQL query from {sql_filename}")
        psql_command = [
            "psql",
            "-h", db_params["host"],
            "-p", db_params["port"],
            "-d", db_params["database"],
            "-U", db_params["user"],
            "-f", sql_filename
        ]
        
        result = subprocess.run(psql_command, capture_output=True, text=True, timeout=30)
        if result.returncode != 0:
            logger.error(f"Error running psql: {result.stderr}")
            return {"stake": 0, "validators": 0}
        
        jito_data = {"stake": 0, "validators": 0}
        try:
            with open(output_filename, "r") as csv_file:
                lines = csv_file.readlines()
                if len(lines) <= 1:
                    logger.warning("No Jito data returned from SQL query")
                    return jito_data
                stake, validators = lines[1].strip().split(",")
                jito_data = {
                    "stake": int(float(stake)) if stake else 0,
                    "validators": int(validators) if validators else 0
                }
            logger.info(f"Jito data parsed: {jito_data}")
        except Exception as e:
            logger.error(f"Error parsing Jito CSV: {e}", exc_info=True)
    except Exception as e:
        logger.error(f"Error in get_jito_data_mainnet: {e}", exc_info=True)
        jito_data = {"stake": 0, "validators": 0}
    finally:
        # Clean up temporary files
        for filename in [sql_filename, output_filename]:
            try:
                if os.path.exists(filename):
                    os.remove(filename)
            except OSError as e:
                logger.warning(f"Could not remove {filename}: {e}")
    
    return jito_data

def parse_stake_by_version(data, jito_data=None, network="mainnet"):
    """
    Parse stakeByVersion using full version numbers, grouping by equivalent Agave versions with subtotals.
    """
    stake_by_version = data.get("stakeByVersion", {})
    total_active_stake = int(data.get("totalActiveStake", 1) / 1e9)
    validators = data.get("validators", [])
    
    # Calculate total validator counts for percentages
    total_validators = len(validators)
    total_active_validators = sum(1 for v in validators if not v.get("delinquent", False))
    
    aggregated_stake = defaultdict(lambda: {"total": 0})
    validator_counts = defaultdict(lambda: {"total": 0, "active": 0})
    agave_groups = defaultdict(list)
    agave_subtotals = defaultdict(lambda: {"total": 0, "validators": 0, "active_validators": 0})
    unknown_versions = set()
    
    # Aggregate stake
    for version, stakes in stake_by_version.items():
        current_stake = int(stakes.get("currentActiveStake", 0) / 1e9)
        delinquent_stake = int(stakes.get("delinquentActiveStake", 0) / 1e9)
        total_stake = current_stake + delinquent_stake
        
        if not version or not all(c.isdigit() or c == '.' for c in version):
            unknown_versions.add(version or "unknown")
            aggregated_stake["unknown"]["total"] += total_stake
            agave_groups["unknown"].append(version or "unknown")
            agave_subtotals["unknown"]["total"] += total_stake
        else:
            aggregated_stake[version]["total"] += total_stake
            # Determine Agave version
            parts = version.split(".")
            if version.startswith("0.") and len(parts) >= 3 and len(parts[2]) >= 5:
                yyyyy = parts[2][-5:]
                try:
                    agave_minor = str(int(yyyyy[-2:]))
                    agave_version = f"2.2.{agave_minor}"
                    agave_groups[agave_version].append(version)
                    agave_subtotals[agave_version]["total"] += total_stake
                except ValueError:
                    logger.warning(f"Invalid yyyyy format in version {version}: {yyyyy}")
                    unknown_versions.add(version)
                    agave_groups["unknown"].append(version)
                    agave_subtotals["unknown"]["total"] += total_stake
            elif version.startswith("2.2."):
                agave_groups[version].append(version)
                agave_subtotals[version]["total"] += total_stake
            else:
                # Non-2.2.x, non-0.xxx.yyyyy versions
                agave_groups[version].append(version)
                agave_subtotals[version]["total"] += total_stake

    # Count validators
    for v in validators:
        version = v.get("version", "unknown")
        is_active = not v.get("delinquent", False)
        
        if not version or not all(c.isdigit() or c == '.' for c in version):
            unknown_versions.add(version or "unknown")
            validator_counts["unknown"]["total"] += 1
            if is_active:
                validator_counts["unknown"]["active"] += 1
            agave_subtotals["unknown"]["validators"] += 1
            if is_active:
                agave_subtotals["unknown"]["active_validators"] += 1
        else:
            validator_counts[version]["total"] += 1
            if is_active:
                validator_counts[version]["active"] += 1
            
            # Ensure version is in agave_groups
            parts = version.split(".")
            if version not in aggregated_stake:
                aggregated_stake[version]["total"] = 0
            if version.startswith("0.") and len(parts) >= 3 and len(parts[2]) >= 5:
                yyyyy = parts[2][-5:]
                try:
                    agave_minor = str(int(yyyyy[-2:]))
                    agave_version = f"2.2.{agave_minor}"
                    if version not in agave_groups[agave_version]:
                        agave_groups[agave_version].append(version)
                    agave_subtotals[agave_version]["validators"] += 1
                    if is_active:
                        agave_subtotals[agave_version]["active_validators"] += 1
                except ValueError:
                    logger.warning(f"Invalid yyyyy format in version {version}: {yyyyy}")
                    unknown_versions.add(version)
                    if version not in agave_groups["unknown"]:
                        agave_groups["unknown"].append(version)
                    agave_subtotals["unknown"]["validators"] += 1
                    if is_active:
                        agave_subtotals["unknown"]["active_validators"] += 1
            elif version.startswith("2.2."):
                if version not in agave_groups[version]:
                    agave_groups[version].append(version)
                agave_subtotals[version]["validators"] += 1
                if is_active:
                    agave_subtotals[version]["active_validators"] += 1
            else:
                if version not in agave_groups[version]:
                    agave_groups[version].append(version)
                agave_subtotals[version]["validators"] += 1
                if is_active:
                    agave_subtotals[version]["active_validators"] += 1
    
    # Calculate percentages for stake
    stake_percentages = {
        version: {
            "total": (info["total"] / total_active_stake) * 100 if total_active_stake > 0 else 0
        }
        for version, info in aggregated_stake.items()
    }
    
    # Calculate percentages for validators
    validator_percentages = {
        version: {
            "total": (info["total"] / total_validators) * 100 if total_validators > 0 else 0,
            "active": (info["active"] / total_active_validators) * 100 if total_active_validators > 0 else 0
        }
        for version, info in validator_counts.items()
    }
    
    # Calculate subtotal percentages
    subtotal_percentages = {
        agave_version: {
            "total": (info["total"] / total_active_stake) * 100 if total_active_stake > 0 else 0,
            "validators": (info["validators"] / total_validators) * 100 if total_validators > 0 else 0,
            "active_validators": (info["active_validators"] / total_active_validators) * 100 if total_active_validators > 0 else 0
        }
        for agave_version, info in agave_subtotals.items()
    }

    return (aggregated_stake, stake_percentages, total_active_stake, unknown_versions,
            validator_counts, agave_groups, agave_subtotals, subtotal_percentages, 
            validator_percentages, total_validators, total_active_validators)

def format_output(network, epoch, aggregated_stake, stake_percentages, total_active_stake,
                  agave_2_1_stake, firedancer_2_1_stake, agave_2_1_percentage,
                  firedancer_2_1_percentage, unknown_versions, active_validators,
                  inactive_validators, validator_counts, jito_data=None,
                  firedancer_jito_2_1_stake=0, firedancer_jito_2_1_percentage=0,
                  agave_groups=None, agave_subtotals=None, subtotal_percentages=None,
                  validator_percentages=None, total_validators=0):
    """Format output with validator percentages and improved formatting."""
    lines = []
    current_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    lines.append(f"Network: {network} | {current_time}")
    lines.append(f"Epoch {epoch}")
    
    # Calculate Jito and Non-Jito stake for mainnet summary
    total_jito_stake = jito_data.get("stake", 0) if jito_data else 0
    total_jito_validators = jito_data.get("validators", 0) if jito_data else 0
    total_non_jito_stake = total_active_stake - total_jito_stake
    jito_stake_percent = (total_jito_stake / total_active_stake) * 100 if total_active_stake > 0 else 0
    non_jito_stake_percent = (total_non_jito_stake / total_active_stake) * 100 if total_active_stake > 0 else 0
    jito_validator_percent = (total_jito_validators / total_validators) * 100 if total_validators > 0 else 0
    
    # Summary section
    lines.append(f"\tTotal Active Stake: {total_active_stake:,} SOL")
    if network == "mainnet" and jito_data:
        lines.append(f"\t\tActive Jito Stake: {total_jito_stake:,} SOL ({jito_stake_percent:.1f}%)")
        lines.append(f"\t\tActive Jito Validators: {total_jito_validators:,} ({jito_validator_percent:.1f}%)")
        lines.append(f"\t\tActive Non-Jito Stake: {total_non_jito_stake:,} SOL ({non_jito_stake_percent:.1f}%)")
    lines.append(f"\tActive Validators: {active_validators}")
    lines.append(f"\t(Inactive Validators: {inactive_validators})")
    lines.append(f"\tTotal Validators: {total_validators}")
    lines.append("")
    lines.append("Stake by Version (in SOL), Percentages, and Validator Count:")
    lines.append("")
    
    # Stake-by-version section with validator percentages
    for agave_version in sorted(agave_groups.keys(), key=lambda x: x if x != "unknown" else "zzz"):
        subtotal = agave_subtotals.get(agave_version, {"total": 0, "validators": 0, "active_validators": 0})
        subtotal_perc = subtotal_percentages.get(agave_version, {"total": 0, "validators": 0, "active_validators": 0})
        
        if agave_version == "unknown":
            versions_str = ", ".join(sorted(unknown_versions)) if unknown_versions else "unknown"
            if subtotal["total"] > 0 or subtotal["validators"] > 0:
                lines.append(
                    f"unknown subtotal: {subtotal['total']:,} SOL ({subtotal_perc['total']:.2f}%) - "
                    f"{subtotal['validators']} validators ({subtotal_perc['validators']:.1f}%)"
                )
            for version in sorted(agave_groups[agave_version]):
                info = aggregated_stake[version]
                counts = validator_counts[version]
                stake_perc = stake_percentages[version]
                val_perc = validator_percentages.get(version, {"total": 0})
                lines.append(
                    f"   {version}: {info['total']:,} SOL ({stake_perc['total']:.2f}%) - "
                    f"{counts['total']} validators ({val_perc['total']:.1f}%)"
                )
        else:
            if subtotal["total"] > 0 or subtotal["validators"] > 0:
                lines.append(
                    f"{agave_version} subtotal: {subtotal['total']:,} SOL ({subtotal_perc['total']:.2f}%) - "
                    f"{subtotal['validators']} validators ({subtotal_perc['validators']:.1f}%)"
                )
            for version in sorted(agave_groups[agave_version]):
                info = aggregated_stake[version]
                counts = validator_counts[version]
                stake_perc = stake_percentages[version]
                val_perc = validator_percentages.get(version, {"total": 0})
                lines.append(
                    f"   {version}: {info['total']:,} SOL ({stake_perc['total']:.2f}%) - "
                    f"{counts['total']} validators ({val_perc['total']:.1f}%)"
                )
        lines.append("")
    
    return "\n".join(lines)

def main():
    output_filename = "major_minor_version.txt"
    logger.info(f"Writing output to {output_filename}.")
    
    try:
        with open(output_filename, "w") as outfile:
            outfile.write("-" * 80 + "\n")
            
            # Get Jito data once for mainnet
            jito_data = get_jito_data_mainnet()
            logger.info(f"Retrieved Jito data: {jito_data}")
            
            networks = [
                ("mainnet", "--url https://side-silent-county.solana-mainnet.quiknode.pro/2ffa9d32adcd0102e7b78a8ba107f5c49b9420d8/"),
                ("testnet", "--url https://shy-yolo-sheet.solana-testnet.quiknode.pro/696dfc16996feaa4bc5f97cf207aafa02bcbdb9c/")
            ]
            
            for network_name, solana_flag in networks:
                try:
                    logger.info(f"Processing {network_name} network...")
                    current_epoch = get_solana_epoch(solana_flag)
                    data = get_solana_validators(solana_flag)
                except (subprocess.CalledProcessError, subprocess.TimeoutExpired) as e:
                    logger.error(f"Error running solana commands for {network_name}: {e}", exc_info=True)
                    outfile.write(f"ERROR: Could not retrieve data for {network_name} network\n")
                    outfile.write("-" * 80 + "\n\n")
                    continue
                
                active_validators = sum(1 for v in data["validators"] if not v.get("delinquent", False))
                inactive_validators = len(data["validators"]) - active_validators
                
                jito_data_arg = jito_data if network_name == "mainnet" else None
                (aggregated_stake, stake_percentages, total_active_stake, unknown_versions,
                 validator_counts, agave_groups, agave_subtotals, subtotal_percentages,
                 validator_percentages, total_validators, total_active_validators) = parse_stake_by_version(
                    data, jito_data_arg, network_name
                )
                
                network_output = format_output(
                    network_name, current_epoch, aggregated_stake, stake_percentages, total_active_stake,
                    0, 0, 0, 0, unknown_versions, active_validators,
                    inactive_validators, validator_counts, jito_data_arg,
                    0, 0, agave_groups=agave_groups, agave_subtotals=agave_subtotals,
                    subtotal_percentages=subtotal_percentages, validator_percentages=validator_percentages,
                    total_validators=total_validators
                )
                outfile.write(network_output)
                outfile.write("\n" + ("-" * 80) + "\n\n")
                logger.info(f"Successfully processed data for {network_name} network.")
        
        logger.info(f"Processing completed successfully. Output written to {output_filename}.")
        
    except Exception as e:
        logger.error(f"Fatal error in main(): {e}", exc_info=True)
        sys.exit(1)

if __name__ == "__main__":
    main()