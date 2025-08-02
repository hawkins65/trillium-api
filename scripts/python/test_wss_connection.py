#!/usr/bin/env python3
import websocket
import json
import argparse
import os
import logging
from datetime import datetime
import sys

# Configuration
JSON_FILE = "/home/smilax/api/92_slot_duration_server_list.json"
LOG_DIR = os.path.expanduser("~/log")
os.makedirs(LOG_DIR, exist_ok=True)

# Set up logging
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
log_file = os.path.join(LOG_DIR, f"test_websocket_{timestamp}.log")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(message)s",
    handlers=[
        logging.FileHandler(log_file),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def load_servers():
    """Load servers from JSON file."""
    try:
        with open(JSON_FILE, "r") as f:
            data = json.load(f)
        return data.get("servers", [])
    except Exception as e:
        logger.error(f"Failed to load JSON file {JSON_FILE}: {e}")
        sys.exit(1)

def display_menu(servers):
    """Display a menu of available servers and return the selected server's name."""
    print("\nAvailable servers:")
    for i, server in enumerate(servers, 1):
        print(f"{i}. {server['name']} ({server['location']}, {server['endpoint']})")
    while True:
        try:
            choice = input("\nEnter the number of the server to test (or 'q' to quit): ")
            if choice.lower() == 'q':
                sys.exit(0)
            index = int(choice) - 1
            if 0 <= index < len(servers):
                return servers[index]["name"]
            else:
                print("Invalid choice. Please select a valid number.")
        except ValueError:
            print("Invalid input. Please enter a number or 'q'.")

def get_server_by_name(servers, name):
    """Find a server by name."""
    for server in servers:
        if server["name"] == name:
            return server
    return None

def test_websocket(server):
    """Test WebSocket connection for the given server."""
    endpoint = server["endpoint"]
    server_name = server["name"]
    location = server["location"]
    
    logger.info(f"Testing WebSocket for {server_name} ({location}) at {endpoint}")

    def on_open(ws):
        logger.info(f"Connected to {endpoint}")
        # Send a test ping message matching the main script's format
        ws.send(json.dumps({"topic": "summary", "key": "ping", "id": 1}))
        logger.info("Sent ping message: {'topic': 'summary', 'key': 'ping', 'id': 1}")

    def on_message(ws, message):
        logger.info(f"Received: {message}")
        try:
            data = json.loads(message)
            logger.info(f"Parsed message: {data}")
        except json.JSONDecodeError:
            logger.error(f"Failed to parse message: {message}")

    def on_error(ws, error):
        logger.error(f"WebSocket error: {error}")

    def on_close(ws, close_status_code, close_msg):
        logger.info(f"WebSocket closed: code={close_status_code}, reason={close_msg}")

    # Set a reasonable timeout
    websocket.setdefaulttimeout(120)
    
    try:
        ws = websocket.WebSocketApp(
            endpoint,
            on_open=on_open,
            on_message=on_message,
            on_error=on_error,
            on_close=on_close
        )
        # Run for 30 seconds to capture initial messages, then close
        ws.run_forever(ping_interval=60, ping_timeout=10, ping_payload="ping")
    except Exception as e:
        logger.error(f"Failed to connect to {endpoint}: {e}")
    finally:
        if 'ws' in locals():
            ws.close()

def main():
    # Parse command-line arguments
    parser = argparse.ArgumentParser(description="Test WebSocket connection for a server from JSON config.")
    parser.add_argument("--name", type=str, help="Name of the server to test (e.g., dtelecom_hattersheim)")
    args = parser.parse_args()

    # Load servers from JSON
    servers = load_servers()
    if not servers:
        logger.error("No servers found in JSON file.")
        sys.exit(1)

    # Select server
    if args.name:
        server = get_server_by_name(servers, args.name)
        if not server:
            logger.error(f"Server '{args.name}' not found in JSON file.")
            sys.exit(1)
    else:
        server_name = display_menu(servers)
        server = get_server_by_name(servers, server_name)
        if not server:
            logger.error(f"Server '{server_name}' not found in JSON file.")
            sys.exit(1)

    # Test the WebSocket
    test_websocket(server)

if __name__ == "__main__":
    main()