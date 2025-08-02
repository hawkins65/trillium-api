import psycopg2
import requests
import struct
import base58
import json
import logging
from typing import Dict, Any

logging.basicConfig(
    filename='shinobi_parse_errors.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

from db_config import db_params

def fetch_newest_pool_id():
    url = "https://xshin.fi/data/pool/newest"
    response = requests.get(url)
    return response.text.strip()

def fetch_bin_data(pool_id, file_type):
    url = f"https://xshin.fi/data/pool/{pool_id}/{file_type}.bin"
    response = requests.get(url)
    with open(f"{file_type}_{pool_id}.bin", "wb") as f:
        f.write(response.content)
    return response.content

class BincodeParser:
    def __init__(self, data):
        self.buffer = data
        self.offset = 0
        self.current_field = None
        self.current_validator = None

    def advance(self, count):
        if self.offset + count > len(self.buffer):
            raise ValueError(f"Attempt to read past buffer end at offset {self.offset}, requested {count} bytes")
        self.offset += count
        return self.buffer[self.offset-count:self.offset]

    def peek(self):
        if self.offset >= len(self.buffer):
            raise ValueError(f"Peek past buffer end at offset {self.offset}")
        return self.buffer[self.offset]

    def read_bool(self):
        value = self.peek()
        if value in (0, 1):
            self.advance(1)
            return value == 1
        else:
            logging.error(
                f"failed to parse bool at offset {self.offset}, found value {value}, "
                f"field: {self.current_field}, validator: {self.current_validator}, "
                f"nearby data: {self.buffer[max(0, self.offset-10):min(len(self.buffer), self.offset+10)].hex()}"
            )
            return False

    def read_u8(self):
        return struct.unpack('<B', self.advance(1))[0]

    def read_u16(self):
        if self.peek() == 251:
            self.advance(1)
            return struct.unpack('<H', self.advance(2))[0]
        else:
            return self.read_u8()

    def read_u32(self):
        if self.peek() == 251:
            self.advance(1)
            return struct.unpack('<H', self.advance(2))[0]
        elif self.peek() == 252:
            self.advance(1)
            return struct.unpack('<I', self.advance(4))[0]
        else:
            return self.read_u8()

    def read_u64(self):
        if self.peek() == 251:
            self.advance(1)
            return struct.unpack('<H', self.advance(2))[0]
        elif self.peek() == 252:
            self.advance(1)
            return struct.unpack('<I', self.advance(4))[0]
        elif self.peek() == 253:
            self.advance(1)
            return struct.unpack('<Q', self.advance(8))[0]
        else:
            return self.read_u8()

    def read_f32(self):
        try:
            return struct.unpack('<f', self.advance(4))[0]
        except struct.error:
            return 0.0

    def read_f64(self):
        try:
            return struct.unpack('<d', self.advance(8))[0]
        except struct.error:
            return 0.0

    def read_string(self, max_bytes=256):
        length = self.read_u32()
        if length > max_bytes:
            logging.warning(f"String length {length} exceeds max {max_bytes} at offset {self.offset}")
            length = max_bytes
        if length > len(self.buffer) - self.offset:
            logging.warning(f"String length {length} too high at offset {self.offset}")
            return None
        data = self.advance(length)
        try:
            decoded = data.decode('utf-8')
            encoded = decoded.encode('utf-8')
            if len(encoded) > max_bytes:
                decoded = encoded[:max_bytes].decode('utf-8', errors='ignore')
                logging.warning(f"Truncated string to {max_bytes} bytes at offset {self.offset - length}: {decoded}")
            elif len(encoded) != length:
                logging.warning(f"String byte length mismatch at offset {self.offset - length}: {decoded}")
            return decoded
        except UnicodeDecodeError as e:
            logging.error(f"Invalid UTF-8 string at offset {self.offset - length}: {e}")
            return None

    def skip_string(self):
        length = self.read_u32()
        if length > len(self.buffer) - self.offset:
            logging.warning(f"String length {length} too high at offset {self.offset}")
            return None
        logging.info(f"Skipping string of length {length} at offset {self.offset} for validator {self.current_validator}, field {self.current_field}")
        self.advance(length)
        return None

    def read_pubkey(self):
        return base58.b58encode(self.advance(32)).decode()

def bincode_overview_details(parser):
    parser.current_field = "version"
    version = parser.read_u8()
    if version != 0:
        raise ValueError(f"unexpected version: {version}")
    
    parser.current_field = "price"
    price = parser.read_f32()
    parser.current_field = "epoch"
    epoch = parser.read_u64()
    parser.current_field = "epoch_start"
    epoch_start = parser.read_u64()
    parser.current_field = "epoch_duration"
    epoch_duration = parser.read_u64()
    parser.current_field = "pool_stake"
    pool_stake = bincode_stake(parser)
    parser.current_field = "reserve"
    reserve = parser.read_u64()
    parser.current_field = "apy"
    apy = parser.read_f32()

    parser.current_field = None
    return {
        "price": price,
        "epoch": epoch,
        "epoch_start": epoch_start,
        "epoch_duration": epoch_duration,
        "pool_stake": pool_stake,
        "reserve": reserve,
        "apy": apy
    }

def bincode_pool_details(parser):
    parser.current_field = "version"
    try:
        version = parser.read_u8()
        print(f"Pool version: {version}")
        logging.info(f"Pool version: {version}")
        if version not in (0, 1, 2):
            raise ValueError(f"unexpected version: {version}")
    except Exception as e:
        logging.error(f"Error parsing version at offset {parser.offset}: {e}")
        raise

    parser.current_field = "pool_validator_count"
    try:
        pool_validator_count = parser.read_u64()
        print(f"Pool validator count: {pool_validator_count}")
        logging.info(f"Pool validator count: {pool_validator_count}")
    except Exception as e:
        logging.error(f"Error parsing pool_validator_count at offset {parser.offset}: {e}")
        raise

    # Parse best metrics (9 fields)
    for i in range(9):
        parser.current_field = f"best_metric_{i}"
        try:
            count = parser.read_u64()
            print(f"Best metric {i} count: {count}")
            logging.info(f"Best metric {i} count: {count}")
            if count * (32 + 8 + 8) > len(parser.buffer) - parser.offset:
                logging.warning(f"Best metric {i} count {count} too high at offset {parser.offset}")
                count = min(count, (len(parser.buffer) - parser.offset) // (32 + 8 + 8))
            for _ in range(count):
                parser.read_pubkey()
                parser.read_f64()
                parser.read_u64()
        except Exception as e:
            logging.error(f"Error parsing best_metric_{i} at offset {parser.offset}: {e}")
            raise

    if version >= 1:
        parser.current_field = "best_vote_inclusion"
        try:
            count = parser.read_u64()
            print(f"Best vote inclusion count: {count}")
            logging.info(f"Best vote inclusion count: {count}")
            if count * (32 + 8 + 8) > len(parser.buffer) - parser.offset:
                logging.warning(f"Best vote inclusion count {count} too high at offset {parser.offset}")
                count = min(count, (len(parser.buffer) - parser.offset) // (32 + 8 + 8))
            for _ in range(count):
                parser.read_pubkey()
                parser.read_f64()
                parser.read_u64()
        except Exception as e:
            logging.error(f"Error parsing best_vote_inclusion at offset {parser.offset}: {e}")
            raise

    parser.current_field = "best_apy"
    try:
        count = parser.read_u64()
        print(f"Best APY count: {count}")
        logging.info(f"Best APY count: {count}")
        if count * (32 + 4 + 8) > len(parser.buffer) - parser.offset:
            logging.warning(f"Best APY count {count} too high at offset {parser.offset}")
            count = min(count, (len(parser.buffer) - parser.offset) // (32 + 4 + 8))
        for _ in range(count):
            parser.read_pubkey()
            parser.read_f32()
            parser.read_u64()
    except Exception as e:
        logging.error(f"Error parsing best_apy at offset {parser.offset}: {e}")
        raise

    for i in range(2):
        parser.current_field = f"best_concentration_{i}"
        try:
            count = parser.read_u64()
            print(f"Best concentration {i} count: {count}")
            logging.info(f"Best concentration {i} count: {count}")
            if count * (32 + 8 + 8) > len(parser.buffer) - parser.offset:
                logging.warning(f"Best concentration {i} count {count} too high at offset {parser.offset}")
                count = min(count, (len(parser.buffer) - parser.offset) // (32 + 8 + 8))
            for _ in range(count):
                parser.read_pubkey()
                parser.read_f64()
                parser.read_u64()
        except Exception as e:
            logging.error(f"Error parsing best_concentration_{i} at offset {parser.offset}: {e}")
            raise

    # Read pool voters
    parser.current_field = "voter_count"
    try:
        count = parser.read_u64()
        print(f"Pool voter count: {count}")
        logging.info(f"Pool voter count: {count}")
        remaining_bytes = len(parser.buffer) - parser.offset
        if count * 32 > remaining_bytes:
            logging.warning(f"Pool voter count {count} too high for {remaining_bytes} bytes")
            count = min(count, remaining_bytes // 32)
    except Exception as e:
        logging.error(f"Error parsing voter_count at offset {parser.offset}: {e}")
        raise

    pool_voters = {}
    for i in range(count):
        parser.current_field = "pubkey"
        try:
            pubkey = parser.read_pubkey()
        except Exception as e:
            logging.error(f"Error reading pubkey for pool voter {i+1}/{count}: {e}")
            print(f"Error reading pubkey for pool voter {i+1}/{count}: {e}")
            break
        parser.current_validator = pubkey
        start_offset = parser.offset
        try:
            pool_voters[pubkey] = bincode_pool_voter_details(parser, version)
            print(f"Parsed pool validator {pubkey}, bytes consumed: {parser.offset - start_offset}")
            logging.info(f"Parsed pool validator {pubkey}, bytes consumed: {parser.offset - start_offset}")
        except Exception as e:
            logging.error(f"Skipping pool validator {pubkey} due to parsing error at offset {parser.offset}: {e}")
            print(f"Skipping pool validator {pubkey} due to parsing error: {e}")
            continue
        finally:
            parser.current_validator = None

    parser.current_field = None
    return {'pool_voters': pool_voters, 'version': version}

def bincode_non_pool_voters(parser):
    parser.current_field = "version"
    try:
        version = parser.read_u8()
        print(f"Non-pool voters version: {version}")
        logging.info(f"Non-pool voters version: {version}")
        if version not in (0, 1, 2):
            raise ValueError(f"unexpected version: {version}")
    except Exception as e:
        logging.error(f"Error parsing version at offset {parser.offset}: {e}")
        raise

    parser.current_field = "voter_count"
    try:
        count = parser.read_u64()
        print(f"Non-pool voter count: {count}")
        logging.info(f"Non-pool voter count: {count}")
        remaining_bytes = len(parser.buffer) - parser.offset
        if count * 32 > remaining_bytes:
            logging.warning(f"Non-pool voter count {count} too high for {remaining_bytes} bytes")
            count = min(count, remaining_bytes // 32)
    except Exception as e:
        logging.error(f"Error parsing voter_count at offset {parser.offset}: {e}")
        raise

    voters = {}
    for i in range(count):
        parser.current_field = "pubkey"
        try:
            pubkey = parser.read_pubkey()
        except Exception as e:
            logging.error(f"Error reading pubkey for non-pool voter {i+1}/{count}: {e}")
            print(f"Error reading pubkey for non-pool voter {i+1}/{count}: {e}")
            break
        parser.current_validator = pubkey
        start_offset = parser.offset
        try:
            voters[pubkey] = bincode_non_pool_voter_details(parser, version)
            print(f"Parsed non-pool validator {pubkey}, bytes consumed: {parser.offset - start_offset}")
            logging.info(f"Parsed non-pool validator {pubkey}, bytes consumed: {parser.offset - start_offset}")
        except Exception as e:
            logging.error(f"Skipping non-pool validator {pubkey} due to parsing error at offset {parser.offset}: {e}")
            print(f"Skipping non-pool validator {pubkey} due to parsing error: {e}")
            continue
        finally:
            parser.current_validator = None

    parser.current_field = None
    return {'voters': voters, 'version': version}

def bincode_pool_voter_details(parser, version):
    parser.current_field = "voter_details"
    logging.info(f"Parsing voter_details at offset {parser.offset} for validator {parser.current_validator}")
    try:
        details = bincode_voter_details(parser, version, is_pool=True)
    except Exception as e:
        logging.error(f"Error parsing voter_details for validator {parser.current_validator} at offset {parser.offset}: {e}")
        return None
    parser.current_field = "pool_stake"
    try:
        pool_stake = bincode_stake(parser)
    except Exception as e:
        logging.error(f"Error parsing pool_stake for validator {parser.current_validator} at offset {parser.offset}: {e}")
        pool_stake = {'active': 0, 'activating': 0, 'deactivating': 0}
    parser.current_field = "noneligibility_count"
    try:
        count = parser.read_u64()
        if count * 8 > len(parser.buffer) - parser.offset:
            logging.warning(f"Noneligibility count {count} too high for validator {parser.current_validator}")
            count = 0
    except Exception as e:
        logging.error(f"Error parsing noneligibility_count at offset {parser.offset}: {e}")
        count = 0
    parser.current_field = "noneligibility_reasons"
    noneligibility_reasons = []
    for _ in range(count):
        try:
            noneligibility_reasons.append(bincode_noneligibility_reason(parser))
        except Exception as e:
            logging.error(f"Error parsing noneligibility reason for validator {parser.current_validator} at offset {parser.offset}: {e}")
            break
    parser.current_field = None
    return {
        'details': details,
        'pool_stake': pool_stake,
        'noneligibility_reasons': noneligibility_reasons
    }

def bincode_non_pool_voter_details(parser, version):
    parser.current_field = "voter_details"
    logging.info(f"Parsing voter_details at offset {parser.offset} for validator {parser.current_validator}")
    try:
        details = bincode_voter_details(parser, version, is_pool=False)
    except Exception as e:
        logging.error(f"Error parsing voter_details for validator {parser.current_validator} at offset {parser.offset}: {e}")
        return None
    parser.current_field = "noneligibility_count"
    try:
        count = parser.read_u64()
        if count * 8 > len(parser.buffer) - parser.offset:
            logging.warning(f"Noneligibility count {count} too high for validator {parser.current_validator}")
            count = 0
    except Exception as e:
        logging.error(f"Error parsing noneligibility_count at offset {parser.offset}: {e}")
        count = 0
    parser.current_field = "noneligibility_reasons"
    noneligibility_reasons = []
    for _ in range(count):
        try:
            noneligibility_reasons.append(bincode_noneligibility_reason(parser))
        except Exception as e:
            logging.error(f"Error parsing noneligibility reason for validator {parser.current_validator} at offset {parser.offset}: {e}")
            break
    parser.current_field = None
    return {
        'details': details,
        'noneligibility_reasons': noneligibility_reasons
    }

def bincode_voter_details(parser, version, is_pool):
    result = {'raw_score': {}, 'normalized_score': {}}
    # Skip string fields (covered by validator_info)
    for field in ['name', 'icon_url', 'details', 'website_url', 'city', 'country']:
        parser.current_field = field
        logging.info(f"Parsing {field} at offset {parser.offset}")
        try:
            if parser.read_bool():
                result[field] = parser.skip_string()
            else:
                result[field] = None
        except Exception as e:
            logging.error(f"Parse failed for {field}, setting to None at offset {parser.offset}: {e}")
            print(f"Parse failed for {field}, setting to None: {e}")
            result[field] = None
    
    parser.current_field = "stake"
    logging.info(f"Parsing stake at offset {parser.offset}")
    try:
        result['stake'] = bincode_stake(parser)
    except Exception as e:
        logging.error(f"Error parsing stake at offset {parser.offset}: {e}")
        result['stake'] = {'active': 0, 'activating': 0, 'deactivating': 0}

    parser.current_field = "target_pool_stake"
    logging.info(f"Parsing target_pool_stake at offset {parser.offset}")
    try:
        result['target_pool_stake'] = parser.read_u64()
    except Exception as e:
        logging.error(f"Error parsing target_pool_stake at offset {parser.offset}: {e}")
        result['target_pool_stake'] = 0

    parser.current_field = "raw_score"
    logging.info(f"Parsing raw_score at offset {parser.offset}")
    try:
        result['raw_score'] = bincode_score(parser, version, is_pool)
    except Exception as e:
        logging.error(f"Error parsing raw_score at offset {parser.offset}: {e}")
        result['raw_score'] = {
            'skip_rate': 0.0, 'prior_skip_rate': 0.0, 'subsequent_skip_rate': 0.0,
            'cu': 0.0, 'latency': 0.0, 'llv': 0.0, 'cv': 0.0, 'vote_inclusion': 0.0,
            'apy': 0.0, 'pool_extra_lamports': 0.0, 'city_concentration': 0.0,
            'country_concentration': 0.0
        }

    parser.current_field = "normalized_score"
    logging.info(f"Parsing normalized_score at offset {parser.offset}")
    try:
        if version >= 2:
            result['normalized_score'] = bincode_score(parser, version, is_pool)
        else:
            result['normalized_score'] = {k: 0.0 for k in result['raw_score'].keys()}
    except Exception as e:
        logging.error(f"Error parsing normalized_score at offset {parser.offset}: {e}")
        result['normalized_score'] = {k: 0.0 for k in result['raw_score'].keys()}

    parser.current_field = "total_score"
    logging.info(f"Parsing total_score at offset {parser.offset}")
    try:
        result['total_score'] = parser.read_f64()
    except Exception as e:
        logging.error(f"Error parsing total_score at offset {parser.offset}: {e}")
        result['total_score'] = 0.0

    parser.current_field = None
    return result

def bincode_stake(parser):
    parser.current_field = "active"
    try:
        active = parser.read_u64()
    except Exception as e:
        logging.error(f"Error parsing stake.active at offset {parser.offset}: {e}")
        active = 0
    parser.current_field = "activating"
    try:
        activating = parser.read_u64()
    except Exception as e:
        logging.error(f"Error parsing stake.activating at offset {parser.offset}: {e}")
        activating = 0
    parser.current_field = "deactivating"
    try:
        deactivating = parser.read_u64()
    except Exception as e:
        logging.error(f"Error parsing stake.deactivating at offset {parser.offset}: {e}")
        deactivating = 0
    parser.current_field = None
    return {
        'active': active,
        'activating': activating,
        'deactivating': deactivating
    }

def bincode_score(parser, version, is_pool):
    score = {}
    for field in ['skip_rate', 'prior_skip_rate', 'subsequent_skip_rate', 'cu', 'latency', 'llv', 'cv']:
        parser.current_field = field
        try:
            score[field] = parser.read_f64()
        except Exception as e:
            logging.error(f"Error parsing score.{field} at offset {parser.offset}: {e}")
            score[field] = 0.0
    parser.current_field = "vote_inclusion"
    try:
        score['vote_inclusion'] = parser.read_f64() if version >= 1 else 0.0
    except Exception as e:
        logging.error(f"Error parsing score.vote_inclusion at offset {parser.offset}: {e}")
        score['vote_inclusion'] = 0.0
    parser.current_field = "apy"
    try:
        score['apy'] = parser.read_f32() if is_pool else 0.0
        if not is_pool:
            parser.advance(4)  # Skip apy for non-pool
    except Exception as e:
        logging.error(f"Error parsing score.apy at offset {parser.offset}: {e}")
        score['apy'] = 0.0
    for field in ['pool_extra_lamports', 'city_concentration', 'country_concentration']:
        parser.current_field = field
        try:
            score[field] = parser.read_f64()
        except Exception as e:
            logging.error(f"Error parsing score.{field} at offset {parser.offset}: {e}")
            score[field] = 0.0
    parser.current_field = None
    return score

def bincode_noneligibility_reason(parser):
    parser.current_field = "noneligibility_index"
    try:
        index = parser.read_u64()
    except Exception as e:
        logging.error(f"Error parsing noneligibility_index at offset {parser.offset}: {e}")
        return None
    if index == 0:
        parser.current_field = "blacklist_reason"
        try:
            return f"blacklisted ({parser.read_string(256)})"
        except Exception as e:
            logging.error(f"Error parsing blacklist_reason at offset {parser.offset}: {e}")
            return None
    elif index == 1:
        return "in superminority"
    elif index == 2:
        parser.current_field = "recent_epochs_leader"
        try:
            count = parser.read_u64()
            epochs = [str(parser.read_u64()) for _ in range(count)]
            return f"not leader in recent epochs ({', '.join(epochs)})"
        except Exception as e:
            logging.error(f"Error parsing recent_epochs_leader at offset {parser.offset}: {e}")
            return None
    elif index == 3:
        parser.current_field = "recent_epochs_credits"
        try:
            count = parser.read_u64()
            epochs = [str(parser.read_u64()) for _ in range(count)]
            return f"low credits in recent epochs ({', '.join(epochs)})"
        except Exception as e:
            logging.error(f"Error parsing recent_epochs_credits at offset {parser.offset}: {e}")
            return None
    elif index == 4:
        parser.current_field = "recent_epochs_delinquency"
        try:
            count = parser.read_u64()
            epochs = [str(parser.read_u64()) for _ in range(count)]
            return f"excessive delinquency in recent epochs ({', '.join(epochs)})"
        except Exception as e:
            logging.error(f"Error parsing recent_epochs_delinquency at offset {parser.offset}: {e}")
            return None
    elif index == 5:
        return "shared vote accounts"
    elif index == 6:
        parser.current_field = "commission"
        try:
            return f"commission too high: {parser.read_u8()}"
        except Exception as e:
            logging.error(f"Error parsing commission at offset {parser.offset}: {e}")
            return None
    elif index == 7:
        parser.current_field = "recent_epochs_apy"
        try:
            count = parser.read_u64()
            epochs = [str(parser.read_u64()) for _ in range(count)]
            return f"APY too low in recent epochs ({', '.join(epochs)})"
        except Exception as e:
            logging.error(f"Error parsing recent_epochs_apy at offset {parser.offset}: {e}")
            return None
    elif index == 8:
        return "insufficient branding"
    elif index == 9:
        return "insufficient non-pool stake"
    else:
        logging.error(f"Invalid non eligibility reason: {index} at offset {parser.offset}")
        return None

def insert_validator_data(vote_account_pubkey, validator_data, epoch, in_pool):
    if validator_data is None or validator_data['details'] is None:
        logging.warning(f"Skipping insertion for validator {vote_account_pubkey} due to incomplete data")
        return
    logging.info(f"Attempting to insert validator {vote_account_pubkey}, in_pool={in_pool}")
    conn = psycopg2.connect(**db_params)
    cur = conn.cursor()
    try:
        cur.execute("""
            INSERT INTO shinobi_pool (
                vote_account_pubkey, epoch, total_score,
                raw_skip_rate, raw_prior_skip_rate, raw_subsequent_skip_rate, raw_cu,
                raw_latency, raw_llv, raw_cv, raw_apy, raw_pool_extra_lamports,
                raw_city_concentration, raw_country_concentration, raw_vote_inclusion,
                normalized_skip_rate, normalized_prior_skip_rate, normalized_subsequent_skip_rate,
                normalized_cu, normalized_latency, normalized_llv, normalized_cv, normalized_apy,
                normalized_pool_extra_lamports, normalized_city_concentration,
                normalized_country_concentration, normalized_vote_inclusion,
                pool_stake_active, stake_active, target_pool_stake, noneligibility_reasons,
                in_pool
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (vote_account_pubkey, epoch) DO UPDATE SET
                total_score = EXCLUDED.total_score,
                raw_skip_rate = EXCLUDED.raw_skip_rate,
                raw_prior_skip_rate = EXCLUDED.raw_prior_skip_rate,
                raw_subsequent_skip_rate = EXCLUDED.raw_subsequent_skip_rate,
                raw_cu = EXCLUDED.raw_cu,
                raw_latency = EXCLUDED.raw_latency,
                raw_llv = EXCLUDED.raw_llv,
                raw_cv = EXCLUDED.raw_cv,
                raw_apy = EXCLUDED.raw_apy,
                raw_pool_extra_lamports = EXCLUDED.raw_pool_extra_lamports,
                raw_city_concentration = EXCLUDED.raw_city_concentration,
                raw_country_concentration = EXCLUDED.raw_country_concentration,
                raw_vote_inclusion = EXCLUDED.raw_vote_inclusion,
                normalized_skip_rate = EXCLUDED.normalized_skip_rate,
                normalized_prior_skip_rate = EXCLUDED.normalized_prior_skip_rate,
                normalized_subsequent_skip_rate = EXCLUDED.normalized_subsequent_skip_rate,
                normalized_cu = EXCLUDED.normalized_cu,
                normalized_latency = EXCLUDED.normalized_latency,
                normalized_llv = EXCLUDED.normalized_llv,
                normalized_cv = EXCLUDED.normalized_cv,
                normalized_apy = EXCLUDED.normalized_apy,
                normalized_pool_extra_lamports = EXCLUDED.normalized_pool_extra_lamports,
                normalized_city_concentration = EXCLUDED.normalized_city_concentration,
                normalized_country_concentration = EXCLUDED.normalized_country_concentration,
                normalized_vote_inclusion = EXCLUDED.normalized_vote_inclusion,
                pool_stake_active = EXCLUDED.pool_stake_active,
                stake_active = EXCLUDED.stake_active,
                target_pool_stake = EXCLUDED.target_pool_stake,
                noneligibility_reasons = EXCLUDED.noneligibility_reasons,
                in_pool = EXCLUDED.in_pool
        """, (
            vote_account_pubkey,
            epoch,
            validator_data['details']['total_score'],
            validator_data['details']['raw_score']['skip_rate'],
            validator_data['details']['raw_score']['prior_skip_rate'],
            validator_data['details']['raw_score']['subsequent_skip_rate'],
            validator_data['details']['raw_score']['cu'],
            validator_data['details']['raw_score']['latency'],
            validator_data['details']['raw_score']['llv'],
            validator_data['details']['raw_score']['cv'],
            validator_data['details']['raw_score']['apy'],
            validator_data['details']['raw_score']['pool_extra_lamports'],
            validator_data['details']['raw_score']['city_concentration'],
            validator_data['details']['raw_score']['country_concentration'],
            validator_data['details']['raw_score'].get('vote_inclusion', 0.0),
            validator_data['details']['normalized_score']['skip_rate'],
            validator_data['details']['normalized_score']['prior_skip_rate'],
            validator_data['details']['normalized_score']['subsequent_skip_rate'],
            validator_data['details']['normalized_score']['cu'],
            validator_data['details']['normalized_score']['latency'],
            validator_data['details']['normalized_score']['llv'],
            validator_data['details']['normalized_score']['cv'],
            validator_data['details']['normalized_score']['apy'],
            validator_data['details']['normalized_score']['pool_extra_lamports'],
            validator_data['details']['normalized_score']['city_concentration'],
            validator_data['details']['normalized_score']['country_concentration'],
            validator_data['details']['normalized_score'].get('vote_inclusion', 0.0),
            validator_data.get('pool_stake', {}).get('active', 0),
            validator_data['details']['stake']['active'],
            validator_data['details']['target_pool_stake'],
            json.dumps(validator_data['noneligibility_reasons']),
            in_pool
        ))
        logging.info(f"Successfully inserted validator {vote_account_pubkey}, in_pool={in_pool}")
        conn.commit()
    except Exception as e:
        logging.error(f"Error inserting data for validator {vote_account_pubkey}: {e}")
        print(f"Error inserting data for validator {vote_account_pubkey}: {e}")
    finally:
        cur.close()
        conn.close()

def main():
    pool_id = fetch_newest_pool_id()
    print(f"Pool ID: {pool_id}")
    logging.info(f"Pool ID: {pool_id}")
    overview_data = fetch_bin_data(pool_id, "overview")
    pool_data = fetch_bin_data(pool_id, "pool")
    non_pool_data = fetch_bin_data(pool_id, "non_pool_voters")
    
    try:
        overview_parser = BincodeParser(overview_data)
        parsed_overview = bincode_overview_details(overview_parser)
        epoch = parsed_overview['epoch']
        print(f"Epoch: {epoch}")
        logging.info(f"Epoch: {epoch}")
        
        pool_parser = BincodeParser(pool_data)
        parsed_pool = bincode_pool_details(pool_parser)
        for vote_account_pubkey, validator_data in parsed_pool['pool_voters'].items():
            insert_validator_data(vote_account_pubkey, validator_data, epoch, in_pool=True)
        
        non_pool_parser = BincodeParser(non_pool_data)
        parsed_non_pool = bincode_non_pool_voters(non_pool_parser)
        for vote_account_pubkey, validator_data in parsed_non_pool['voters'].items():
            insert_validator_data(vote_account_pubkey, validator_data, epoch, in_pool=False)
            
    except Exception as e:
        logging.error(f"Error parsing data: {e}")
        print(f"Error parsing data: {e}")

if __name__ == "__main__":
    main()