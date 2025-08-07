import json
from flask import Flask, render_template, request, jsonify
import psycopg2
from datetime import datetime, timedelta
import plotly
import plotly.graph_objs as go

app = Flask(__name__, static_url_path='/var/www/html/leaderboard/static', static_folder='static', template_folder='/var/www/html/leaderboard/templates')

# PostgreSQL database connection parameters
db_params = {
    "host": "private-dbaas-db-9382663-do-user-15771670-0.c.db.ondigitalocean.com",
    "port": "25060",
    "database": "sol_blocks",
    "user": "smilax",
    "sslmode": "require"
}

def get_db_connection():
    conn = psycopg2.connect(**db_params)
    return conn

@app.route('/')
def index():
    print("Rendering index.html")
    return render_template('leaderboard.html')

@app.route('/leaderboard')
def leaderboard():
    print("Rendering leaderboard.html")
    return render_template('leaderboard.html')
        
@app.route('/leaderboard_viz')
def leaderboard_viz():
    print("Received request for /leaderboard_viz")
    filter_type = request.args.get('filter-type', '')
    pubkeys = request.args.get('pubkeys', '').split(',')
    start_time = request.args.get('start_time', '')
    start_slot = request.args.get('start_slot', '')
    end_slot = request.args.get('end_slot', '')
    epoch1 = request.args.get('epoch1', '')
    epoch2 = request.args.get('epoch2', '')

    print(f"Filter type: {filter_type}")
    print(f"Pubkeys: {pubkeys}")
    print(f"Start time: {start_time}")
    print(f"Start slot: {start_slot}")
    print(f"End slot: {end_slot}")
    print(f"Epoch 1: {epoch1}")
    print(f"Epoch 2: {epoch2}")

    conn = get_db_connection()
    cur = conn.cursor()

    # Retrieve the min and max slot, epoch, and time values from the database
    cur.execute("SELECT MIN(block_slot), MAX(block_slot), MIN(epoch), MAX(epoch), MIN(block_time), MAX(block_time) FROM solana_data")
    min_slot, max_slot, min_epoch, max_epoch, min_time, max_time = cur.fetchone()

    print(f"Min slot: {min_slot}, Max slot: {max_slot}")
    print(f"Min epoch: {min_epoch}, Max epoch: {max_epoch}")
    print(f"Min time: {min_time}, Max time: {max_time}")

    # Apply filters based on the provided parameters
    filters = []
    if filter_type == 'pubkey' and pubkeys:
        valid_pubkeys = [pubkey for pubkey in pubkeys if is_valid_pubkey(cur, pubkey)]
        if valid_pubkeys:
            filters.append(f"pubkey IN ({','.join(['%s'] * len(valid_pubkeys))})")
            params = valid_pubkeys
        else:
            return render_template('leaderboard.html', error='No valid pubkeys provided.')
    elif filter_type == 'time' and start_time:
        if is_valid_time(min_time, max_time, start_time):
            end_time = min(max_time, int(start_time) + timedelta(days=7).total_seconds())
            filters.append("block_time BETWEEN %s AND %s")
            params = [start_time, end_time]
        else:
            return render_template('leaderboard.html', error='Invalid start time provided.')
    elif filter_type == 'slot' and start_slot and end_slot:
        if is_valid_slot_range(min_slot, max_slot, start_slot, end_slot):
            filters.append("block_slot BETWEEN %s AND %s")
            params = [start_slot, end_slot]
        else:
            return render_template('leaderboard.html', error='Invalid slot range provided.')
    elif filter_type == 'epoch' and epoch1 and epoch2:
        if is_valid_epoch_range(min_epoch, max_epoch, epoch1, epoch2):
            epoch_diff = abs(int(epoch2) - int(epoch1))
            if epoch_diff > 3:
                epoch2 = int(epoch1) + 3
            filters.append("epoch BETWEEN %s AND %s")
            params = [epoch1, epoch2]
        else:
            return render_template('leaderboard.html', error='Invalid epoch range provided.')
    else:
        filters.append("epoch = %s")
        params = [max_epoch]  # Display data for the latest epoch by default

    # Construct the SQL query with filters
    query = f'''
        SELECT pubkey, AVG(rewards) AS avg_rewards, SUM(rewards) AS total_rewards
        FROM solana_data
        {" WHERE " + " AND ".join(filters) if filters else ""}
        GROUP BY pubkey
        ORDER BY avg_rewards DESC
        LIMIT 25;
    '''

    cur.execute(query, params)
    results = cur.fetchall()

    # Prepare the Plotly visualization data
    pubkeys = [result[0] for result in results]
    avg_rewards = [result[1] / 1e9 for result in results]  # Convert lamports to SOL
    total_rewards = [result[2] / 1e9 for result in results]  # Convert lamports to SOL

    trace = go.Bar(
        x=pubkeys,
        y=avg_rewards,
        text=[f"Total Rewards: {total:.9f} SOL" for total in total_rewards],
        marker=dict(color='rgba(226, 145, 33, 0.8)')
    )

    layout = go.Layout(
        title='Solana Rewards Leaderboard',
        xaxis=dict(tickangle=-45),
        yaxis=dict(title='Average Rewards (SOL)')
    )

    fig = go.Figure(data=[trace], layout=layout)
    div = plotly.offline.plot(fig, output_type='div', include_plotlyjs=False)

    # Convert min_time to a formatted string
    min_time_str = datetime.fromtimestamp(min_time).strftime('%Y-%m-%d %H:%M:%S')

    cur.close()
    conn.close()

    return render_template('leaderboard.html', plot_div=div, min_time=min_time_str, min_slot=min_slot, max_slot=max_slot, min_epoch=min_epoch, max_epoch=max_epoch)

@app.route('/pubkeys')
def get_pubkeys():
    conn = get_db_connection()
    cur = conn.cursor()

    cur.execute("SELECT DISTINCT pubkey FROM solana_data")
    pubkeys = [row[0] for row in cur.fetchall()]

    print(f"Pubkeys: {pubkeys}")

    cur.close()
    conn.close()

    return jsonify({'pubkeys': pubkeys})

def is_valid_pubkey(cur, pubkey):
    cur.execute("SELECT COUNT(*) FROM solana_data WHERE pubkey = %s", (pubkey,))
    count = cur.fetchone()[0]
    return count > 0

def is_valid_time(min_time, max_time, start_time):
    try:
        start_time = int(start_time)
        return min_time <= start_time <= max_time
    except ValueError:
        return False

def is_valid_slot_range(min_slot, max_slot, start_slot, end_slot):
    try:
        start_slot = int(start_slot)
        end_slot = int(end_slot)
        return min_slot <= start_slot <= end_slot <= max_slot
    except ValueError:
        return False

def is_valid_epoch_range(min_epoch, max_epoch, epoch1, epoch2):
    try:
        epoch1 = int(epoch1)
        epoch2 = int(epoch2)
        return min_epoch <= epoch1 <= epoch2 <= max_epoch
    except ValueError:
        return False

if __name__ == '__main__':
    app.run(debug=True, port=5001)