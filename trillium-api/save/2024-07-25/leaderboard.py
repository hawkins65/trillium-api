from flask import Flask, render_template, request, jsonify, send_file
from flask_cors import CORS
import json
import os

app = Flask(__name__)
CORS(app, resources={r"/api/*": {"origins": "https://leaderboard.trillium.so"}})

JSON_DIR = 'static/json'
VALIDATOR_REWARDS_DIR = 'validator_rewards/static/json'

@app.route('/')
def home():
    return leaderboard_graph()

@app.route('/overall_epoch_averages')
def overall_epoch_averages():
    json_files = [f for f in os.listdir(JSON_DIR) if f.startswith('validator_rewards_epoch_') and f.endswith('.json')]
    latest_epoch_file = max(json_files, key=lambda f: int(f.split('_')[-1].split('.')[0]))
    json_path = os.path.join(JSON_DIR, latest_epoch_file)

    with open(json_path) as file:
        data = json.load(file)

    return jsonify(data)
    
@app.route('/leaderboard_graph')
def leaderboard_graph():
    json_files = [f for f in os.listdir(JSON_DIR) if f.startswith('validator_rewards_epoch_') and f.endswith('.json')]
    epochs = sorted(set([int(f.split('_')[-1].split('.')[0]) for f in json_files]), reverse=True)
    return render_template('leaderboard_graph.html', epochs=epochs)

@app.route('/leaderboard_table')
def leaderboard_table():
    json_files = [f for f in os.listdir(JSON_DIR) if f.startswith('validator_rewards_epoch_') and f.endswith('.json')]
    epochs = sorted(set([int(f.split('_')[-1].split('.')[0]) for f in json_files]), reverse=True)
    return render_template('leaderboard_table.html', epochs=epochs)

@app.route('/sig_prio_rewards')
def sig_prio_rewards():
    return render_template('sig_prio_rewards.html')

@app.route('/validator_graph/<pubkey>')
def validator_graph(pubkey):
    if not pubkey:
        default_pubkey = 'Tri1F8B6YtjkBztGCwBNSLEZib1EAqMUEUM7dTT7ZG3'
        pubkey = default_pubkey
    return redirect(url_for('multi_validator_graph', pubkeys=pubkey))  # Redirect with a default pubkey

@app.route('/multi_validator_graph')
def multi_validator_graph():
    pubkeys = request.args.getlist('pubkeys')
    if not pubkeys:
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
        pubkeys = default_pubkeys
    pubkeys_str = ','.join(pubkeys)
    return render_template('multi_validator_graph.html', pubkeys=pubkeys_str)

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
        latest_epochs = [int(f.split('_')[0][5:]) for f in json_files[:20]]

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
        latest_epochs = [int(f.split('_')[0][5:]) for f in json_files[:20]]

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

@app.route('/multi_validator_credits_graph')
def multi_validator_credits_graph():
    pubkeys = request.args.getlist('pubkeys')
    if not pubkeys:
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
            'ETcW7iuVraMKLMJayNCCsr9bLvKrJPDczy1CMVMPmXTc',
            'Tri1F8B6YtjkBztGCwBNSLEZib1EAqMUEUM7dTT7ZG3',
            'ETcW7iuVraMKLMJayNCCsr9bLvKrJPDczy1CMVMPmXTc',
            'Ninja1spj6n9t5hVYgF3PdnYz2PLnkt7rvaw3firmjs',
        ]
        pubkeys = default_pubkeys
    pubkeys_str = ','.join(pubkeys)
    return render_template('multi_validator_credits_graph.html', pubkeys=pubkeys_str)

@app.route('/multi_validator_mev_graph')
def multi_validator_mev_graph():
    pubkeys = request.args.getlist('pubkeys')
    if not pubkeys:
        default_pubkeys = [
            'A4hyMd3FyvUJSRafDUSwtLLaQcxRP4r1BRC9w2AJ1to2',
            'CMPSSdrTnRQBiBGTyFpdCc3VMNuLWYWaSkE8Zh5z6gbd',
            'CXPeim1wQMkcTvEHx9QdhgKREYYJD8bnaCCqPRwJ1to1',
            'Cogent51kHgGLHr7zpkpRjGYFXM57LgjHjDdqXd4ypdA',
            'DB7DNWMVQASMFxcjkwdr4w4eg3NmfjWTk2rqFMMbrPLA',
            'PUmpKiNnSVAZ3w4KaFX6jKSjXUNHFShGkXbERo54xjb',
            'EdGevanA2MZsDpxDXK6b36FH7RCcTuDZZRcc6MEyE9hy',
            'HEL1USMZKAL2odpNBj2oCjffnFGaYwmbGmyewGv1e2TU',
            'LA1NEzryoih6CQW3gwQqJQffK2mKgnXcjSQZSRpM3wc',
            'SLNDCSGTEsA6KHpgR32MBt9UAurZnVSJGUtW2tRpdU2',
            'Tri1F8B6YtjkBztGCwBNSLEZib1EAqMUEUM7dTT7ZG3',
            'ETcW7iuVraMKLMJayNCCsr9bLvKrJPDczy1CMVMPmXTc',
            'Ninja1spj6n9t5hVYgF3PdnYz2PLnkt7rvaw3firmjs',
        ]
        pubkeys = default_pubkeys
    pubkeys_str = ','.join(pubkeys)
    return render_template('multi_validator_mev_graph.html', pubkeys=pubkeys_str)

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