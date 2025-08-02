import websocket
import json
import threading
import time
import logging
from collections import defaultdict

# WebSocket endpoint
WSS_ENDPOINT = "wss://fd.trillium.so/websocket"

# Dictionary to count metric occurrences
metric_counts = defaultdict(int)

# Configure logging
logging.basicConfig(filename="metrics_log.jsonl", level=logging.INFO, format="%(asctime)s %(message)s")

# Handle WebSocket connection open
def on_open(ws):
    print("Connected to WebSocket stream")
    logging.info("Connected to WebSocket stream")

# Handle incoming messages
def on_message(ws, message):
    try:
        # Parse JSON message
        data = json.loads(message)
        logging.info(json.dumps(data))

        # Extract top-level topic and key
        topic = data.get("topic", "unknown")
        key = data.get("key", "unknown")
        metric_name = f"{topic}.{key}"
        metric_counts[metric_name] += 1

        # Extract nested keys from value if it's a dict
        value = data.get("value")
        if isinstance(value, dict):
            for subkey in value.keys():
                nested_metric = f"{metric_name}.{subkey}"
                metric_counts[nested_metric] += 1

        # Print current counts periodically
        print(f"Metric counts so far: {dict(sorted(metric_counts.items(), key=lambda x: x[1], reverse=True))}")
    except json.JSONDecodeError as e:
        print(f"Error parsing message: {e}")
        logging.error(f"Error parsing message: {e}")
    except Exception as e:
        print(f"Unexpected error: {e}")
        logging.error(f"Unexpected error: {e}")

# Handle errors
def on_error(ws, error):
    print(f"WebSocket error: {error}")
    logging.error(f"WebSocket error: {error}")

# Handle connection close
def on_close(ws, close_status_code, close_msg):
    print(f"WebSocket connection closed: {close_status_code}, {close_msg}")
    logging.info(f"WebSocket connection closed: {close_status_code}, {close_msg}")
    # Save metric counts to JSON
    with open("metric_counts.json", "w") as f:
        json.dump(dict(metric_counts), f, indent=2)
    print(f"Final metric counts saved to metric_counts.json")
    # Attempt to reconnect
    print("Attempting to reconnect in 5 seconds...")
    time.sleep(5)
    run_websocket()

# Run WebSocket client
def run_websocket():
    # Increase max message size to 6MB
    websocket.WebSocketApp.max_message_size = 6291456
    
    ws = websocket.WebSocketApp(
        WSS_ENDPOINT,
        on_open=on_open,
        on_message=on_message,
        on_error=on_error,
        on_close=on_close
    )

    # Run WebSocket in a separate thread
    ws_thread = threading.Thread(target=ws.run_forever, kwargs={"ping_interval": 30, "ping_timeout": 10})
    ws_thread.daemon = True
    ws_thread.start()

    # Run for 5 minutes, then close
    time.sleep(300)
    ws.close()

if __name__ == "__main__":
    try:
        run_websocket()
    except KeyboardInterrupt:
        print("Script interrupted by user")
        logging.info("Script interrupted by user")
        with open("metric_counts.json", "w") as f:
            json.dump(dict(metric_counts), f, indent=2)
    except Exception as e:
        print(f"Error running script: {e}")
        logging.error(f"Error running script: {e}")