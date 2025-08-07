from flask import Flask, render_template, request, jsonify, send_file
from flask_cors import CORS
import json
import os

app = Flask(__name__)
CORS(app, resources={r"/api/*": {"origins": "https://leaderboard.trillium.so"}})

JSON_DIR = 'static/json'
VALIDATOR_REWARDS_DIR = 'validator_rewards/static/json'

def find_identity_pubkeys(pubkeys):
    """
    Helper function to find identity_pubkeys from a list of pubkeys
    (either identity_pubkey or vote_account_pubkey)
    """
    identity_pubkeys = set()
    json_files = sorted([f for f in os.listdir(VALIDATOR_REWARDS_DIR) if f.startswith('epoch') and f.endswith('_validator_rewards.json')], reverse=True)
    
    for json_file in json_files[:10]:  # Check last 10 epochs
        json_path = os.path.join(VALIDATOR_REWARDS_DIR, json_file)
        with open(json_path) as file:
            data = json.load(file)
            for item in data:
                if item['identity_pubkey'] in pubkeys or item['vote_account_pubkey'] in pubkeys:
                    identity_pubkeys.add(item['identity_pubkey'])
                    if len(identity_pubkeys) == len(pubkeys):
                        return list(identity_pubkeys)
    
    return list(identity_pubkeys)

@app.route('/overall_epoch_averages')
def overall_epoch_averages():
    json_files = [f for f in os.listdir(JSON_DIR) if f.startswith('validator_rewards_epoch_') and f.endswith('.json')]
    latest_epoch_file = max(json_files, key=lambda f: int(f.split('_')[-1].split('.')[0]))
    json_path = os.path.join(JSON_DIR, latest_epoch_file)

    with open(json_path) as file:
        data = json.load(file)

    return jsonify(data)

@app.route('/<path:path>')
def catch_all(path):
    print(f"Debug: Catch-all route hit. Path: {path}")
    return f"Caught unhandled path: {path}", 404

@app.route('/')
@app.route('/multi_validator_graph/')
@app.route('/multi_validator_credits_graph/')
@app.route('/multi_validator_mev_graph/')
def multi_validator_graphs():
    print(f"Debug: Full request URL: {request.url}")
    print(f"Debug: Request method: {request.method}")
    print(f"Debug: Request headers: {request.headers}")
    print(f"Debug: Requested path: {request.path}")

    template_name = 'multi_validator_graph.html'
    if 'credits' in request.path:
        template_name = 'multi_validator_credits_graph.html'
    elif 'mev' in request.path:
        template_name = 'multi_validator_mev_graph.html'

    print(f"Debug: Selected template: {template_name}")
    
    default_pubkeys = [
        'A4hyMd3FyvUJSRafDUSwtLLaQcxRP4r1BRC9w2AJ1to2',
        'CMPSSdrTnRQBiBGTyFpdCc3VMNuLWYWaSkE8Zh5z6gbd',
        'CXPeim1wQMkcTvEHx9QdhgKREYYJD8bnaCCqPRwJ1to1',
        'Cogent51kHgGLHr7zpkpRjGYFXM57LgjHjDdqXd4ypdA',
        'DB7DNWMVQASMFxcjkwdr4w4eg3NmfjWTk2rqFMMbrPLA',
        'Diman2GphWLwECE3swjrAEAJniezpYLxK1edUydiDZau',
        'EdGevanA2MZsDpxDXK6b36FH7RCcTuDZZRcc6MEyE9hy',
        'HEL1USMZKAL2odpNBj2oCjffnFGaYwmbGmyewGv1e2TU',
        'LA1NEzryoih6CQW3gwQqJQffK2mKgnXcjSQZSRpM3wc',
        'SLNDCSGTEsA6KHpgR32MBt9UAurZnVSJGUtW2tRpdU2',
        'Tri1F8B6YtjkBztGCwBNSLEZib1EAqMUEUM7dTT7ZG3',
        'ETcW7iuVraMKLMJayNCCsr9bLvKrJPDczy1CMVMPmXTc',
        'Ninja1spj6n9t5hVYgF3PdnYz2PLnkt7rvaw3firmjs',
    ]
    
    if pubkeys is None:
        pubkey_list = default_pubkeys
    else:
        pubkey_list = pubkeys.split(',')

    identity_pubkeys = find_identity_pubkeys(pubkey_list)
    pubkeys_str = ','.join(identity_pubkeys)

    return render_template(template_name, pubkeys=pubkeys_str)

@app.route('/api/data')
def get_data():
    pubkey = request.args.get('identity_pubkey')
    epoch = request.args.get('epoch')

    if epoch:
        json_file = f'validator_rewards_epoch_{epoch}.json'
    else:
        json_files = [f for f in os.listdir(JSON_DIR) if f.startswith('validator_rewards_epoch_') and f.endswith('.json')]
        latest_epoch = max([int(f.split('_')[-1].split('.')[0]) for f in json_files])
        json_file = f'validator_rewards_epoch_{latest_epoch}.json'

    json_path = os.path.join(JSON_DIR, json_file)

    with open(json_path) as file:
        data = json.load(file)

    if pubkey:
        # Return all data when a pubkey is provided
        data = [item for item in data if item['pubkey'] == pubkey]
    else:
        data = sorted(data, key=lambda x: float(x['rewards_per_block']) if x['rewards_per_block'] is not None else float('-inf'), reverse=True)

    print(f"Debug (get_data): Returning data for epoch {epoch} and pubkey {pubkey}")
    print(f"Debug (get_data): Number of records: {len(data)}")

    return jsonify(data)

@app.route('/api/pubkeys')
def get_pubkeys():
    json_files = sorted([f for f in os.listdir(VALIDATOR_REWARDS_DIR) if f.startswith('epoch') and f.endswith('_validator_rewards.json')], reverse=True)
    if json_files:
        latest_epoch_file = json_files[0]
        latest_epoch = int(latest_epoch_file.split('_')[0][5:])
        json_file = latest_epoch_file
        json_path = os.path.join(VALIDATOR_REWARDS_DIR, json_file)
    else:
        return jsonify([])

    with open(json_path) as file:
        data = json.load(file)

    pubkeys = [
        {
            'pubkey': item['identity_pubkey'],
            'name': item['name'] if item['name'] else item['identity_pubkey'],
            'vote_account_pubkey': item['vote_account_pubkey']
        }
        for item in data
    ]

    # Sort the pubkeys list by name
    pubkeys.sort(key=lambda x: x['name'])

    print(f"Debug (get_pubkeys): Returning {len(pubkeys)} pubkeys for epoch {latest_epoch}")
    return jsonify(pubkeys)

@app.route('/api/validators_data/<pubkeys>')
def get_validators_data(pubkeys):
    pubkey_list = pubkeys.split(',')
    print(f"Debug (get_validators_data): Received pubkeys: {pubkey_list}")

    if pubkey_list:
        historical_data = []
        json_files = sorted([f for f in os.listdir(VALIDATOR_REWARDS_DIR) if f.startswith('epoch') and f.endswith('_validator_rewards.json')], reverse=True)
        latest_epochs = [int(f.split('_')[0][5:]) for f in json_files[:20]]

        print(f"Debug (get_validators_data): Latest epochs: {latest_epochs}")

        for epoch_num in latest_epochs:
            epoch_data = []
            json_file = f'epoch{epoch_num}_validator_rewards.json'
            json_path = os.path.join(VALIDATOR_REWARDS_DIR, json_file)

            with open(json_path) as file:
                data = json.load(file)
                for item in data:
                    if item['identity_pubkey'] in pubkey_list or item['vote_account_pubkey'] in pubkey_list:
                        epoch_data.append(item)

            historical_data.extend(epoch_data)

        print(f"Debug (get_validators_data): Returning historical data for pubkeys")
        print(f"Debug (get_validators_data): Number of historical records: {len(historical_data)}")
        return jsonify(historical_data)

    print("Debug (get_validators_data): No pubkeys provided")
    return jsonify([])

@app.route('/validator_rewards/', defaults={'pubkey': None})
@app.route('/validator_rewards/<pubkey>')
def get_validator_rewards(pubkey):
    if pubkey is None:
        # Use the default pubkey when none is provided
        pubkey = 'Tri1F8B6YtjkBztGCwBNSLEZib1EAqMUEUM7dTT7ZG3'

    json_files = sorted([f for f in os.listdir(VALIDATOR_REWARDS_DIR) if f.startswith('epoch') and f.endswith('_validator_rewards.json')], reverse=True)
    latest_epochs = [int(f.split('_')[0][5:]) for f in json_files[:10]]

    validator_data = []
    for epoch in latest_epochs:
        json_file = f'epoch{epoch}_validator_rewards.json'
        json_path = os.path.join(VALIDATOR_REWARDS_DIR, json_file)
        
        if os.path.exists(json_path):
            with open(json_path, 'r') as file:
                epoch_data = json.load(file)
                validator_info = next((item for item in epoch_data if item['identity_pubkey'] == pubkey or item['vote_account_pubkey'] == pubkey), None)
                if validator_info:
                    validator_data.append(validator_info)

    if validator_data:
        response = jsonify(validator_data)
        response.headers.add('Access-Control-Allow-Origin', '*')
        return response
    else:
        return jsonify({'error': f'Validator with pubkey {pubkey} not found in the last 10 epochs'}), 404


@app.route('/epoch_data/', defaults={'epoch': None})
@app.route('/epoch_data/<int:epoch>')
def get_epoch_data(epoch):
    if epoch is None:
        json_file = 'last_ten_epoch_aggregate_data.json'
        json_path = os.path.join(VALIDATOR_REWARDS_DIR, json_file)
    else:
        json_file = f'epoch{epoch}_epoch_aggregate_data.json'
        json_path = os.path.join(VALIDATOR_REWARDS_DIR, json_file)

    if os.path.exists(json_path):
        with open(json_path) as file:
            data = json.load(file)

        response = jsonify(data)
        response.headers.add('Access-Control-Allow-Origin', 'https://leaderboard.trillium.so')
        return response
        
    else:
        return jsonify({'error': 'Epoch not found'}), 404

@app.route('/ten_epoch_validator_rewards')
def ten_epoch_validator_rewards():
    json_path = os.path.join(VALIDATOR_REWARDS_DIR, 'ten_epoch_validator_rewards.json')
    if os.path.exists(json_path):
        with open(json_path, 'r') as file:
            json_data = json.load(file)
        response = jsonify(json_data)
        response.headers.add('Access-Control-Allow-Origin', '*')
        return response
    else:
        return jsonify({'error': 'Ten epoch validator rewards data not found'}), 404

@app.route('/ten_epoch_aggregate_data')
def ten_epoch_aggregate_data():
    json_path = os.path.join(VALIDATOR_REWARDS_DIR, 'ten_epoch_aggregate_data.json')
    if os.path.exists(json_path):
        with open(json_path, 'r') as file:
            json_data = json.load(file)
        response = jsonify(json_data)
        response.headers.add('Access-Control-Allow-Origin', '*')
        return response
    else:
        return jsonify({'error': 'Ten epoch aggregate data not found'}), 404

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5001, debug=True)