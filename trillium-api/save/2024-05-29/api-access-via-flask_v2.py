from flask import Flask, jsonify
import json
import os

app = Flask(__name__)

JSON_DIR = 'static/json'

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

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)