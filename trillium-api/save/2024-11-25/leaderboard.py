from flask import Flask, render_template, request, jsonify, send_file, redirect, url_for, abort, send_from_directory
from flask_cors import CORS
from werkzeug.routing import BaseConverter
import json
import os

JSON_DIR = 'static/json'
VALIDATOR_REWARDS_DIR = 'validator_rewards/static/json'

app = Flask(__name__, static_folder='/home/smilax/block-production/leaderboard/production/static')
#CORS(app, resources={r"/*": {"origins": "https://leaderboard.trillium.so"}})

from flask import redirect, current_app, jsonify
import logging

# Ensure logging is set up (place this near the top of your file)
logging.basicConfig(level=logging.INFO)

@app.route('/combined_slot_production')
def combined_slot_production():
    return render_template('combined_slot_production.html')

@app.route('/stake_distribution_chart/', defaults={'epoch': None})
@app.route('/stake_distribution_chart/<int:epoch>')
def stake_distribution_chart(epoch):
    # Log that the route was accessed
    current_app.logger.info(f"stake_distribution_chart route accessed with epoch: {epoch}")

    if epoch is None:
        url = 'https://trillium.so/images/stake_distribution_charts.png'
    else:
        url = f'https://trillium.so/images/epoch{epoch}_stake_distribution_charts.png'
    
    # Log the constructed URL
    current_app.logger.info(f"Constructed URL: {url}")

    # If in debug mode, return debug info instead of redirecting
    if current_app.debug:
        debug_info = {
            "message": "Debug info for stake_distribution_chart",
            "epoch": epoch,
            "constructed_url": url,
            "request_path": current_app.request.path,
            "request_args": dict(current_app.request.args),
            "request_headers": dict(current_app.request.headers)
        }
        return jsonify(debug_info), 200

    # If not in debug mode, perform the redirect
    return redirect(url, code=302)

@app.route('/epoch-metrics')
def serve_react_app():
    return send_from_directory(app.static_folder, 'index.html')

@app.route('/epoch-metrics/<path:path>')
def serve_react_assets(path):
    return send_from_directory(app.static_folder, path)


default_pubkeys = [
    '1KXvrkPXwkGF6NK1zyzVuJqbXfpenPVPP6hoiK9bsK3',
    '3V2xaccDpFib4DbTksdiveNDmiwpXBqSWyjSof3w1Bg7',
    '61QB1Evn9E3noQtpJm4auFYyHSXS5FPgqKtPgwJJfEQk',
    'AsMpvJ3DZ2Ydu1WTRMAyMH4QjSLiUG39rKzfzvtE1bWr',
    'BitokuDHQiAhpUKrwx1VssAAoW5Rst8zB6gpfoaxM3Kh',
    'Cogent51kHgGLHr7zpkpRjGYFXM57LgjHjDdqXd4ypdA',
    'DB7DNWMVQASMFxcjkwdr4w4eg3NmfjWTk2rqFMMbrPLA',
    'Diman2GphWLwECE3swjrAEAJniezpYLxK1edUydiDZau',
    'EdGevanA2MZsDpxDXK6b36FH7RCcTuDZZRcc6MEyE9hy',
    'ETcW7iuVraMKLMJayNCCsr9bLvKrJPDczy1CMVMPmXTc',
    'HyperSPG8w4jgdHgmA8ExrhRL1L1BriRTHD9UFdXJUud',
    'HEL1USMZKAL2odpNBj2oCjffnFGaYwmbGmyewGv1e2TU',
    'juigBT2qetpYpf1iwgjaiWTjryKkY3uUTVAnRFKkqY6',
    'LA1NEzryoih6CQW3gwQqJQffK2mKgnXcjSQZSRpM3wc',
    'mastWEbKEMjvBCd1uaUBpNjWcfSPhXMWnH9tTrgzn1g',
    'Ninja1spj6n9t5hVYgF3PdnYz2PLnkt7rvaw3firmjs',
    'PUmpKiNnSVAZ3w4KaFX6jKSjXUNHFShGkXbERo54xjb',
    'Tri1F8B6YtjkBztGCwBNSLEZib1EAqMUEUM7dTT7ZG3'
    ]

class ListConverter(BaseConverter):
    regex = r'[^/]+(,[^/]+)*'

    def to_python(self, value):
        return value.split(',')

    def to_url(self, values):
        return ','.join(BaseConverter.to_url(value) for value in values)

app.url_map.converters['list'] = ListConverter

def get_latest_epoch_file():
    json_files = sorted([f for f in os.listdir(VALIDATOR_REWARDS_DIR) if f.startswith('epoch') and f.endswith('_validator_rewards.json')], reverse=True)
    return json_files[0] if json_files else None

def load_validator_data():
    latest_file = get_latest_epoch_file()
    if not latest_file:
        return {}
    
    file_path = os.path.join(VALIDATOR_REWARDS_DIR, latest_file)
    with open(file_path, 'r') as file:
        data = json.load(file)
    
    return {validator['vote_account_pubkey']: validator['identity_pubkey'] for validator in data}

validator_data = load_validator_data()

def lookup_identity_pubkey(pubkey):
    return validator_data.get(pubkey, pubkey)

@app.route('/')
def home():
    print("Debug: Home route accessed")
    return redirect(url_for('multi_validator_graph'))

# Define the path to leaderboard_graph React app's build folder
REACT_BUILD_FOLDER = '/home/smilax/block-production/leaderboard/production/solana-leaderboard/build'

@app.route('/leaderboard_graph', defaults={'path': ''})
@app.route('/leaderboard_graph/<path:path>')
def serve_leaderboard_graph(path):
    if path != "" and os.path.exists(os.path.join(REACT_BUILD_FOLDER, path)):
        return send_from_directory(REACT_BUILD_FOLDER, path)
    elif os.path.exists(os.path.join(REACT_BUILD_FOLDER, 'index.html')):
        return send_from_directory(REACT_BUILD_FOLDER, 'index.html')
    else:
        abort(404)

@app.route('/multi_validator_graph')
@app.route('/multi_validator_graph/<list:pubkeys>')
@app.route('/multi_validator_rewards_graph')
@app.route('/multi_validator_rewards_graph/<list:pubkeys>')
def multi_validator_graph(pubkeys=None):
    print(f"Debug: multi_validator_rewards route accessed with pubkeys: {pubkeys}")
    return combined_validator_graph('rewards', pubkeys)

@app.route('/validator_charts')
@app.route('/validator_charts/<pubkey>')
def validator_charts(pubkey=None):
    if pubkey is None:
        pubkey = 'Tri1F8B6YtjkBztGCwBNSLEZib1EAqMUEUM7dTT7ZG3'  # Default pubkey
    return render_template('validator_charts.html', pubkey=pubkey)

@app.route('/multi_validator_credits_graph')
@app.route('/multi_validator_credits_graph/<list:pubkeys>')
def multi_validator_credits_graph(pubkeys=None):
    print(f"Debug: multi_validator_credits_graph route accessed with pubkeys: {pubkeys}")
    return combined_validator_graph('credits', pubkeys)

@app.route('/multi_validator_mev_graph')
@app.route('/multi_validator_mev_graph/<list:pubkeys>')
def multi_validator_mev_graph(pubkeys=None):
    print(f"Debug: multi_validator_mev_graph route accessed with pubkeys: {pubkeys}")
    return combined_validator_graph('mev', pubkeys)

def combined_validator_graph(graph_type, pubkeys=None):
    print(f"Debug: combined_validator_graph called with type: {graph_type}, pubkeys: {pubkeys}")
    valid_graph_types = ['rewards', 'credits', 'mev']
    if graph_type not in valid_graph_types:
        abort(404)
    
    if not pubkeys:
        pubkeys = default_pubkeys
        print(f"Debug: Using default pubkeys for {graph_type} graph")
    else:
        print(f"Debug: Received pubkeys for {graph_type} graph: {pubkeys}")
    
    # Lookup identity_pubkeys
    identity_pubkeys = [lookup_identity_pubkey(pubkey.strip()) for pubkey in pubkeys]
    
    # Join the identity_pubkeys into a comma-separated string
    identity_pubkeys_str = ','.join(identity_pubkeys)
    
    print(f"Debug: Passing pubkeys to template for {graph_type} graph: {identity_pubkeys_str}")
    
    template_name = f'multi_validator_{graph_type}_graph.html'
    return render_template(template_name, pubkeys=identity_pubkeys_str, graph_type=graph_type)

# *** ??? ***
# which one of below are still needed?
# ???
  
@app.route('/validator_graph/<pubkey>')
def validator_graph(pubkey):
    if not pubkey:
        default_pubkey = 'Tri1F8B6YtjkBztGCwBNSLEZib1EAqMUEUM7dTT7ZG3'
        pubkey = default_pubkey
    return redirect(url_for('multi_validator_graph', pubkeys=pubkey))  # Redirect with a default pubkey

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

@app.route('/api/validator_data')
def get_validator_data():
    pubkey = request.args.get('pubkey')
    print(f"Debug (get_validator_data): Received pubkey: {pubkey}")

    if pubkey:
        historical_data = []
        json_files = sorted([f for f in os.listdir(VALIDATOR_REWARDS_DIR) if f.startswith('epoch') and f.endswith('_validator_rewards.json')], reverse=True)
        latest_epochs = [int(f.split('_')[0][5:]) for f in json_files[:10]]
        latest_epochs = list(range(max(latest_epochs) - 9, max(latest_epochs) + 1))

        print(f"Debug (get_validator_data) Latest epochs: {latest_epochs}")

        for epoch_num in latest_epochs:
            json_file = f'epoch{epoch_num}_validator_rewards.json'
            json_path = os.path.join(VALIDATOR_REWARDS_DIR, json_file)

            print(f"Debug (get_validator_data): Processing file: {json_file}")

            with open(json_path) as file:
                data = json.load(file)
                validator_data = next((item for item in data if item['identity_pubkey'] == pubkey), None)
                if validator_data:
                    historical_data.append(validator_data)
                else:
                    print(f"Debug (get_validator_data) No data found for pubkey {pubkey} in epoch {epoch_num}")

        historical_data.sort(key=lambda x: x['epoch'], reverse=True)
        print(f"Debug (get_validator_data): Returning historical data for pubkey {pubkey}")
        print(f"Debug (get_validator_data): Number of historical records: {len(historical_data)}")
        return jsonify(historical_data)

    print("Debug  (get_validator_data): No pubkey provided")
    return jsonify([])

@app.route('/api/validators_data')
def get_validators_data():
    pubkeys_str = request.args.get('pubkeys')
    if pubkeys_str:
        pubkeys = pubkeys_str.split(',')
    else:
        pubkeys = []
    print(f"Debug (get_validators_data): Received pubkeys: {pubkeys}")

    if pubkeys:
        historical_data = []
        json_files = sorted([f for f in os.listdir(VALIDATOR_REWARDS_DIR) if f.startswith('epoch') and f.endswith('_validator_rewards.json')], reverse=True)
        latest_epochs = [int(f.split('_')[0][5:]) for f in json_files[:10]]
        latest_epochs = list(range(max(latest_epochs) - 9, max(latest_epochs) + 1))

        print(f"Debug (get_validators_data): Latest epochs: {latest_epochs}")

        for epoch_num in latest_epochs:
            epoch_data = []
            json_file = f'epoch{epoch_num}_validator_rewards.json'
            json_path = os.path.join(VALIDATOR_REWARDS_DIR, json_file)

            with open(json_path) as file:
                data = json.load(file)
                for pubkey in pubkeys:
                    validator_data = next((item for item in data if item['identity_pubkey'] == pubkey), None)
                    if validator_data:
                        epoch_data.append(validator_data)
                    else:
                        print(f"Debug (get_validators_data): No data found for pubkey {pubkey} in epoch {epoch_num}")

            historical_data.extend(epoch_data)

        print(f"Debug (get_validators_data): Returning historical data for pubkeys {pubkeys}")
        print(f"Debug (get_validators_data): Number of historical records: {len(historical_data)}")
        return jsonify(historical_data)

    print("Debug (get_validators_data): No pubkeys provided")
    return jsonify([])

@app.route('/validator_rewards/', defaults={'epoch_or_pubkey': None})
@app.route('/validator_rewards/<epoch_or_pubkey>')
def get_validator_rewards(epoch_or_pubkey):
    if epoch_or_pubkey is None or epoch_or_pubkey.isdigit():
        # Existing logic for null or integer epoch
        if epoch_or_pubkey is None:
            json_files = sorted([f for f in os.listdir(VALIDATOR_REWARDS_DIR) if f.startswith('epoch') and f.endswith('_validator_rewards.json')], reverse=True)
            if json_files:
                latest_epoch_file = json_files[0]
                json_file = latest_epoch_file
                json_path = os.path.join(VALIDATOR_REWARDS_DIR, json_file)
            else:
                return jsonify({'error': 'No validator rewards files found'}), 404
        else:
            epoch = int(epoch_or_pubkey)
            json_file = f'epoch{epoch}_validator_rewards.json'
            json_path = os.path.join(VALIDATOR_REWARDS_DIR, json_file)

        if os.path.exists(json_path):
            with open(json_path, 'r') as file:
                json_data = json.load(file)
            response = jsonify(json_data)
            response.headers.add('Access-Control-Allow-Origin', '*')
            return response
        else:
            return jsonify({'error': 'Epoch not found'}), 404
    else:
        # Logic for identity_pubkey or vote_account_pubkey
        pubkey = epoch_or_pubkey
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
            return jsonify({'error': 'Validator not found in the last 10 epochs'}), 404

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

@app.route('/ten_epoch_validator_rewards', defaults={'pubkey': None})
@app.route('/ten_epoch_validator_rewards/<pubkey>')
def ten_epoch_validator_rewards(pubkey):
    json_path = os.path.join(VALIDATOR_REWARDS_DIR, 'ten_epoch_validator_rewards.json')
    if os.path.exists(json_path):
        with open(json_path, 'r') as file:
            json_data = json.load(file)
        
        if pubkey:
            # Find the object with matching identity_pubkey or vote_account_pubkey
            validator_data = next((item for item in json_data if item['identity_pubkey'] == pubkey or item['vote_account_pubkey'] == pubkey), None)
            
            if validator_data:
                response = jsonify(validator_data)
            else:
                return jsonify({'error': 'Validator not found'}), 404
        else:
            # If no pubkey is provided, return the entire JSON data
            response = jsonify(json_data)
        
        response.headers.add('Access-Control-Allow-Origin', '*')
        return response
    else:
        return jsonify({'error': 'Ten epoch validator rewards data not found'}), 404

@app.route('/xshin_weighted_average_validator_rewards', defaults={'pubkey': None})
@app.route('/xshin_weighted_average_validator_rewards/<pubkey>')
def xshin_weighted_average_validator_rewards(pubkey):
    json_path = os.path.join(VALIDATOR_REWARDS_DIR, 'xshin_weighted_average_validator_rewards.json')
    if os.path.exists(json_path):
        with open(json_path, 'r') as file:
            json_data = json.load(file)
        
        if pubkey:
            # Find the object with matching identity_pubkey or vote_account_pubkey
            validator_data = next((item for item in json_data if item['identity_pubkey'] == pubkey or item['vote_account_pubkey'] == pubkey), None)
            
            if validator_data:
                return jsonify(validator_data)
            else:
                return jsonify({'error': 'Validator not found'}), 404
        else:
            # If no pubkey is provided, return the entire JSON data
            return jsonify(json_data)
    else:
        return jsonify({'error': 'xshin weighted average validator rewards data not found'}), 404


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
    app.run(host='0.0.0.0', port=5001, debug=False)