from flask import Flask, jsonify, request
import psycopg2
from datetime import datetime, timedelta
import logging
import json
import os

app = Flask(__name__)

# PostgreSQL database connection parameters
db_params = {
    "host": "private-dbaas-db-9382663-do-user-15771670-0.c.db.ondigitalocean.com",
    "port": "25060",
    "database": "sol_blocks",
    "user": "smilax",
    "sslmode": "require"
}

JSON_DIR = 'static/json'

# Establish a connection to the database
def get_db_connection():
    conn = psycopg2.connect(**db_params)
    return conn

# API endpoint: Get Min and Max Block Slot
@app.route('/api/rewards/block-slot-range', methods=['GET'])
def get_block_slot_range():
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("SELECT MIN(block_slot) AS min_block_slot, MAX(block_slot) AS max_block_slot FROM validator_data")
    result = cur.fetchone()
    cur.close()
    conn.close()

    min_block_slot = result[0]
    max_block_slot = result[1]
    
    return jsonify({
        'min_block_slot': min_block_slot,
        'max_block_slot': max_block_slot
    })

# API endpoint: Get Oldest and Newest Block Time
@app.route('/api/rewards/block-time-range', methods=['GET'])
def get_block_time_range():
    logging.debug("Executing get_block_time_range function")

    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("SELECT MIN(block_time) AS oldest_block_time, MAX(block_time) AS newest_block_time from validator_data")
    result = cur.fetchone()
    cur.close()
    conn.close()

    logging.debug(f"Fetched result from database: {result}")

    oldest_block_time = datetime.utcfromtimestamp(result[0]).strftime("%Y-%m-%dT%H:%M:%SZ")
    newest_block_time = datetime.utcfromtimestamp(result[1]).strftime("%Y-%m-%dT%H:%M:%SZ")

    logging.debug(f"Formatted oldest_block_time: {oldest_block_time}")
    logging.debug(f"Formatted newest_block_time: {newest_block_time}")

    response_data = {
        'oldest_block_time': oldest_block_time,
        'newest_block_time': newest_block_time
    }

    logging.debug(f"Response data: {response_data}")

    return jsonify(response_data)

# API endpoint: Get Rewards by Time Range
@app.route('/api/rewards/by-time-range', methods=['GET'])
def get_rewards_by_time_range():
    start_time = request.args.get('start_time')
    logging.debug(f"*** 1 start_time: {start_time}")
    if start_time == 'current':
            end_time = datetime.utcnow()
            start_time = end_time - timedelta(minutes=10)
    else:
        try:
            start_time = datetime.strptime(start_time, "%Y-%m-%dT%H:%M:%SZ")
            logging.debug(f"*** 2 start_time: {start_time}")
            end_time = start_time + timedelta(minutes=10)  # Results will be from start time given to 10 minutes later
            
        except ValueError:
            logging.debug(f"*** error")
            return jsonify({'error': 'Invalid time format. Please use "yyyy-mm-ddThh:mm:ssZ" or "current".'}), 400

    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("""
        SELECT pubkey, block_slot, block_hash, block_time, rewards, post_balance, reward_type, commission
        from validator_data
        WHERE block_time BETWEEN %s AND %s
    """, (start_time.timestamp(), end_time.timestamp()))
    results = cur.fetchall()
    cur.close()
    conn.close()

    rewards = []
    for row in results:
        reward = {
            'pubkey': row[0],
            'block_slot': row[1],
            'block_hash': row[2],
            'block_time': datetime.utcfromtimestamp(row[3]).strftime("%Y-%m-%dT%H:%M:%SZ"),
            'rewards': row[4],
            'post_balance': row[5],
            'reward_type': row[6],
            'commission': row[7]
        }
        rewards.append(reward)

    response_data = {'rewards': rewards}

    return jsonify(response_data)

# API endpoint: Get Rewards by Slot Range
@app.route('/api/rewards/by-slot-range', methods=['GET'])
def get_rewards_by_slot_range():
    start_slot = request.args.get('start_slot', type=int)
    end_slot = request.args.get('end_slot', type=int)

    if end_slot is None or start_slot is None:
        return jsonify({'error': 'Both start_slot and end_slot are required.'}), 400

    slot_difference = end_slot - start_slot
    if slot_difference < 0 or slot_difference > 500:  # The maximum allowed difference between start_slot and end_slot is 500
        return jsonify({'error': 'The difference between end_slot and start_slot cannot exceed 500.'}), 400

    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("""
        SELECT pubkey, block_slot, block_hash, block_time, rewards, post_balance, reward_type, commission
        from validator_data
        WHERE block_slot BETWEEN %s AND %s
    """, (start_slot, end_slot))
    results = cur.fetchall()
    cur.close()
    conn.close()

    rewards = []
    for row in results:
        reward = {
            'pubkey': row[0],
            'block_slot': row[1],
            'block_hash': row[2],
            'block_time': datetime.utcfromtimestamp(row[3]).strftime("%Y-%m-%dT%H:%M:%SZ"),
            'rewards': row[4],
            'post_balance': row[5],
            'reward_type': row[6],
            'commission': row[7]
        }
        rewards.append(reward)

    return jsonify({'rewards': rewards})

# API endpoint: Get Rewards by Validator
@app.route('/api/rewards/by-validator', methods=['GET'])
def get_rewards_by_validator():
    validator = request.args.get('validator')
    if not validator or not (4 <= len(validator) <= 44):
        return jsonify({'error': 'Validator string length should be between 4 and 44 characters.'}), 400

    conn = get_db_connection()
    cur = conn.cursor()

    sql_query = f"""
        SELECT pubkey, block_slot, block_hash, block_time, rewards, post_balance, reward_type, commission
        from validator_data
        WHERE pubkey LIKE '%{validator}%'
        LIMIT 500
    """

    cur.execute(sql_query)
    results = cur.fetchall()
    cur.close()
    conn.close()

    rewards = []
    for row in results:
        reward = {
            'pubkey': row[0],
            'block_slot': row[1],
            'block_hash': row[2],
            'block_time': datetime.utcfromtimestamp(row[3]).strftime("%Y-%m-%dT%H:%M:%SZ"),
            'rewards': row[4],
            'post_balance': row[5],
            'reward_type': row[6],
            'commission': row[7]
        }
        rewards.append(reward)
    return jsonify({'rewards': rewards})

# API endpoint: Get Rewards by Block Hash
@app.route('/api/rewards/by-block-hash', methods=['GET'])
def get_rewards_by_block_hash():
    block_hash = request.args.get('block_hash')
    if not block_hash or not (4 <= len(block_hash) <= 44):
        return jsonify({'error': 'Block hash string length should be between 4 and 44 characters.'}), 400

    conn = get_db_connection()
    cur = conn.cursor()

    sql_query = f"""
        SELECT pubkey, block_slot, block_hash, block_time, rewards, post_balance, reward_type, commission
        from validator_data
        WHERE block_hash LIKE '%{block_hash}%'
        LIMIT 500
    """

    cur.execute(sql_query)
    results = cur.fetchall()
    cur.close()
    conn.close()

    rewards = []
    for row in results:
        reward = {
            'pubkey': row[0],
            'block_slot': row[1],
            'block_hash': row[2],
            'block_time': datetime.utcfromtimestamp(row[3]).strftime("%Y-%m-%dT%H:%M:%SZ"),
            'rewards': row[4],
            'post_balance': row[5],
            'reward_type': row[6],
            'commission': row[7]
        }
        rewards.append(reward)
    return jsonify({'rewards': rewards})

# API endpoint: Get Epoch Average Rewards
@app.route('/api/epoch_avg_rewards')
def get_epoch_avg_rewards():
    json_files = [f for f in os.listdir(JSON_DIR) if f.startswith('validator_rewards_epoch_') and f.endswith('.json')]
    epochs = sorted(set([int(f.split('_')[-1].split('.')[0]) for f in json_files]), reverse=True)

    epoch_avg_rewards = []

    for epoch in epochs:
        json_file = f'validator_rewards_epoch_{epoch}.json'
        json_path = os.path.join(JSON_DIR, json_file)

        with open(json_path) as file:
            data = json.load(file)

        if data:
            epoch_data = data[0]
            epoch_avg_rewards.append({
                'epoch': epoch_data['epoch'],
                'epoch_average_rewards': epoch_data['epoch_average_rewards'],
                'epoch_block_count': epoch_data['epoch_block_count']
            })

    return jsonify(epoch_avg_rewards)

# API endpoint: Get Epoch Average Rewards by Vote Account Pubkey
@app.route('/api/epoch_avg_rewards/vote_account_pubkey')
def get_epoch_avg_rewards_by_vote_account_pubkey():
    vote_account_pubkey = request.args.get('vote_account_pubkey')
    if not vote_account_pubkey:
        return jsonify({'error': 'Vote account pubkey is required.'}), 400

    json_files = [f for f in os.listdir(JSON_DIR) if f.startswith('validator_rewards_epoch_') and f.endswith('.json')]
    epochs = sorted(set([int(f.split('_')[-1].split('.')[0]) for f in json_files]), reverse=True)

    epoch_avg_rewards = []

    for epoch in epochs:
        json_file = f'validator_rewards_epoch_{epoch}.json'
        json_path = os.path.join(JSON_DIR, json_file)

        with open(json_path) as file:
            data = json.load(file)

        validator_data = next((validator for validator in data if validator['vote_account_pubkey'] == vote_account_pubkey), None)
        if validator_data:
            epoch_avg_rewards.append(validator_data)

    return jsonify(epoch_avg_rewards)

# API endpoint: Get Epoch Average Rewards by Identity Pubkey
@app.route('/api/epoch_avg_rewards/identity_pubkey')
def get_epoch_avg_rewards_by_identity_pubkey():
    identity_pubkey = request.args.get('identity_pubkey')
    if not identity_pubkey:
        return jsonify({'error': 'Identity pubkey is required.'}), 400

    json_files = [f for f in os.listdir(JSON_DIR) if f.startswith('validator_rewards_epoch_') and f.endswith('.json')]
    epochs = sorted(set([int(f.split('_')[-1].split('.')[0]) for f in json_files]), reverse=True)

    epoch_avg_rewards = []

    for epoch in epochs:
        json_file = f'validator_rewards_epoch_{epoch}.json'
        json_path = os.path.join(JSON_DIR, json_file)

        with open(json_path) as file:
            data = json.load(file)

        validator_data = next((validator for validator in data if validator['pubkey'] == identity_pubkey), None)
        if validator_data:
            epoch_avg_rewards.append(validator_data)

    return jsonify(epoch_avg_rewards)

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO) # levels are listed in order: DEBUG, INFO, WARNING, ERROR, CRITICAL  
    app.run(host='0.0.0.0', port=5000)
