#!/usr/bin/env python3
import websocket
import json
import csv
import time
import os
import threading
import logging
import requests
import subprocess
import sys
import argparse
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
import signal

# Global settings
CONFIG_FILE = "/home/smilax/api/92_slot_duration_server_list.json"
LEADER_SCHEDULE_DIR = "/home/smilax/block-production/leaderboard/leader_schedules"
VALIDATOR_REWARDS_URL = "https://api.trillium.so/validator_rewards/"
LOG_DIR = os.path.expanduser("~/log")
OUTPUT_DIR = "/home/smilax/api/wss_slot_duration"
SOLANA_CMD = "/home/smilax/.local/share/solana/install/active_release/bin/solana"

# Global shared data
validator_mapping = {}
current_epoch = None
leader_schedule = {}
next_leader_schedule = {}
should_run = True
SERVERS = {}

# Thread management
server_threads = {}
shutdown_event = threading.Event()

class ServerWorker:
    def __init__(self, server_name, server_config):
        self.server_name = server_name
        self.server_config = server_config
        self.endpoint = server_config["endpoint"]
        self.location = server_config["location"]
        
        # Server-specific state
        self.current_epoch = None
        self.next_epoch = None
        self.leader_schedule = {}
        self.next_leader_schedule = {}
        self.current_hour = None
        self.sequence_number = 1
        self.csv_file = None
        self.latest_slot = 0
        self.first_slot_processed = None
        self.last_message_time = time.time()
        
        # Connection management
        self.ws_instance = None
        self.keep_alive_thread = None
        self.connection_lock = threading.Lock()
        
        # Logging
        self.logger = logging.getLogger(f"server_{server_name}")
        self.logger.setLevel(logging.INFO)
        self.log_handler = None

    def initialize_log(self, epoch):
        """Initialize log file for the current epoch"""
        log_file = os.path.join(LOG_DIR, f"{self.server_name}_wss_epoch{epoch}.log")
        
        # Remove existing handler if present
        if self.log_handler:
            self.logger.removeHandler(self.log_handler)
        
        # Set up new file handler
        self.log_handler = logging.FileHandler(log_file)
        self.log_handler.setLevel(logging.INFO)
        self.log_handler.setFormatter(logging.Formatter(f"[{self.server_name}] %(asctime)s - %(message)s"))
        self.logger.addHandler(self.log_handler)
        
        self.log(f"Logging to: {log_file}")
        return log_file

    def initialize_csv(self, epoch, hour):
        """Initialize CSV file for the current hour"""
        epoch_dir = os.path.join(OUTPUT_DIR, f"epoch{epoch}")
        os.makedirs(epoch_dir, exist_ok=True)
        
        while True:
            self.csv_file = os.path.join(epoch_dir, f"{self.server_name}_wss_epoch{epoch}_{self.sequence_number}.csv")
            if not os.path.exists(self.csv_file):
                with open(self.csv_file, mode="w", newline="") as f:
                    writer = csv.writer(f)
                    writer.writerow(["timestamp", "slot", "duration_nanos", "identity_pubkey", "name", "vote_account_pubkey"])
                break
            self.sequence_number += 1
        
        self.log(f"Started new CSV: {self.csv_file}")

    def log(self, message):
        """Log message with server identification"""
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        formatted_msg = f"[{self.server_name}] {timestamp} - {message}"
        print(formatted_msg)
        if self.logger and self.log_handler:
            self.logger.info(message)

    def keep_alive(self, ws):
        """Keep-alive function with health monitoring"""
        ping_interval = 30
        health_check_interval = 120
        max_silence_duration = 300
        
        last_ping_time = time.time()
        last_health_check = time.time()
        
        while not shutdown_event.is_set() and should_run:
            try:
                current_time = time.time()
                
                if current_time - last_ping_time >= ping_interval:
                    ws.send(json.dumps({"topic": "summary", "key": "ping", "id": int(current_time)}))
                    last_ping_time = current_time
                    self.log(f"Sent ping")
                
                if current_time - last_health_check >= health_check_interval:
                    silence_duration = current_time - self.last_message_time
                    self.log(f"Health check: {silence_duration:.1f}s since last message")
                    
                    if silence_duration > max_silence_duration:
                        self.log(f"Connection stale ({silence_duration:.1f}s), forcing reconnect")
                        ws.close()
                        break
                        
                    last_health_check = current_time
                
                time.sleep(10)
                
            except Exception as e:
                self.log(f"Keep-alive error: {e}")
                break

    def on_open(self, ws):
        """Handle WebSocket connection open"""
        self.log(f"Connected to {self.endpoint}")
        self.last_message_time = time.time()
        
        ws.send(json.dumps({"topic": "summary", "key": "ping", "id": 1}))
        
        global current_epoch, leader_schedule
        if current_epoch:
            self.current_epoch = current_epoch
            self.leader_schedule = leader_schedule.copy()
            self.log(f"Using epoch {self.current_epoch} with {len(self.leader_schedule)} slots")
            self.initialize_log(self.current_epoch)
        else:
            self.log("Warning: No current epoch available")
        
        self.keep_alive_thread = threading.Thread(target=self.keep_alive, args=(ws,), daemon=True)
        self.keep_alive_thread.start()

    def on_message(self, ws, message):
        """Handle incoming messages"""
        global current_epoch, leader_schedule, next_leader_schedule, validator_mapping
        
        self.last_message_time = time.time()
        
        try:
            data = json.loads(message)
            
            if data.get("topic") == "epoch" and data.get("key") == "new":
                epoch = data.get("value", {}).get("epoch", None)
                if epoch:
                    self.log(f"Received epoch.new for epoch {epoch}")
                    if epoch == current_epoch + 1:
                        self.next_epoch = epoch
                        self.next_leader_schedule = next_leader_schedule.copy()
                        self.log(f"Pre-loaded leader schedule for NEXT epoch {self.next_epoch}")
            
            if data.get("topic") == "slot" and data.get("key") == "update":
                publish = data.get("value", {}).get("publish", {})
                slot = publish.get("slot", None)
                level = publish.get("level", "N/A")
                
                if slot is not None:
                    self.latest_slot = max(self.latest_slot, slot)
                    
                if level == "optimistically_confirmed" and slot is not None:
                    # Check epoch transition
                    if (self.next_epoch and self.next_leader_schedule and 
                        slot in self.next_leader_schedule and slot not in self.leader_schedule):
                        self.log(f"Slot {slot} belongs to next epoch {self.next_epoch} - transitioning!")
                        
                        self.current_epoch = self.next_epoch
                        self.leader_schedule = self.next_leader_schedule
                        self.log(f"Now using leader schedule for epoch {self.current_epoch}")
                        
                        self.initialize_log(self.current_epoch)
                        self.current_hour = None
                        self.sequence_number = 1
                        
                        self.next_epoch = self.current_epoch + 1
                        self.next_leader_schedule = next_leader_schedule.copy()
                    
                    if self.first_slot_processed is None:
                        self.first_slot_processed = slot
                        if self.current_epoch is None:
                            self.log(f"Error: No epoch set when processing first slot {slot}")
                            return
                    
                    timestamp = datetime.now().isoformat()
                    duration_nanos = publish.get("duration_nanos", "N/A")
                    
                    current_time = datetime.now()
                    hour = current_time.strftime("%Y%m%d%H")
                    if self.current_hour != hour:
                        self.current_hour = hour
                        self.sequence_number += 1
                        self.initialize_csv(self.current_epoch, self.current_hour)
                    
                    identity_pubkey = self.leader_schedule.get(slot, "Unknown")
                    validator_info = validator_mapping.get(identity_pubkey, {"name": None, "vote_account_pubkey": "Unknown"})
                    
                    row = [
                        timestamp,
                        slot,
                        duration_nanos,
                        identity_pubkey,
                        "",
                        validator_info["vote_account_pubkey"]
                    ]
                    
                    if self.csv_file:
                        with open(self.csv_file, mode="a", newline="") as f:
                            writer = csv.writer(f)
                            writer.writerow(row)
                    
        except json.JSONDecodeError as e:
            self.log(f"Error parsing message: {e}")
        except Exception as e:
            self.log(f"Unexpected error: {e}")

    def on_error(self, ws, error):
        self.log(f"WebSocket error: {error}")

    def on_close(self, ws, close_status_code, close_msg):
        self.log(f"WebSocket closed: {close_status_code}, {close_msg}")

    def create_websocket_connection(self):
        websocket.setdefaulttimeout(60)
        return websocket.WebSocketApp(
            self.endpoint,
            on_open=self.on_open,
            on_message=self.on_message,
            on_error=self.on_error,
            on_close=self.on_close
        )

    def run(self):
        """Main run loop"""
        reconnect_delay = 5
        max_reconnect_delay = 300
        
        self.log(f"Starting worker for {self.location}")
        
        while not shutdown_event.is_set() and should_run:
            try:
                self.log(f"Connecting to {self.endpoint}")
                
                with self.connection_lock:
                    self.ws_instance = self.create_websocket_connection()
                
                self.ws_instance.run_forever(
                    ping_interval=60,
                    ping_timeout=10,
                    ping_payload="ping"
                )
                
                if not shutdown_event.is_set() and should_run:
                    self.log(f"Reconnecting in {reconnect_delay} seconds...")
                    time.sleep(reconnect_delay)
                    reconnect_delay = min(reconnect_delay * 1.5, max_reconnect_delay)
                    
            except Exception as e:
                self.log(f"Error in run loop: {e}")
                if not shutdown_event.is_set() and should_run:
                    time.sleep(reconnect_delay)
                    reconnect_delay = min(reconnect_delay * 1.5, max_reconnect_delay)
            finally:
                if self.ws_instance:
                    self.ws_instance.close()
        
        self.log("Worker shutting down")

def load_servers(group_id):
    """Load servers from JSON config file for the specified group"""
    global SERVERS
    try:
        with open(CONFIG_FILE, "r") as f:
            data = json.load(f)
        SERVERS = {server["name"]: server for server in data["servers"] if server["group"] == group_id}
        print(f"Loaded {len(SERVERS)} servers for group {group_id}")
    except Exception as e:
        print(f"Error loading servers from {CONFIG_FILE}: {e}")
        sys.exit(1)

def get_current_epoch():
    try:
        result = subprocess.run([SOLANA_CMD, "epoch"], capture_output=True, text=True, check=True)
        return int(result.stdout.strip())
    except Exception as e:
        print(f"Error getting current epoch: {e}")
        return None

def fetch_validator_rewards():
    try:
        response = requests.get(VALIDATOR_REWARDS_URL, timeout=10)
        response.raise_for_status()
        data = response.json()
        return {item["identity_pubkey"]: {"name": item["name"], "vote_account_pubkey": item["vote_account_pubkey"]} for item in data}
    except Exception as e:
        print(f"Error fetching validator rewards: {e}")
        return {}

def load_leader_schedule(epoch):
    file_path = os.path.join(LEADER_SCHEDULE_DIR, f"epoch{epoch}-leaderschedule.json")
    try:
        with open(file_path, "r") as f:
            data = json.load(f)
        return {entry["slot"]: entry["leader"] for entry in data["leaderScheduleEntries"]}
    except Exception as e:
        print(f"Error loading leader schedule for epoch {epoch}: {e}")
        return {}

def initialize_global_data():
    global validator_mapping, current_epoch, leader_schedule, next_leader_schedule
    
    print("Initializing global data...")
    
    validator_mapping = fetch_validator_rewards()
    print(f"Loaded validator mapping for {len(validator_mapping)} validators")
    
    current_epoch = get_current_epoch()
    if current_epoch:
        leader_schedule = load_leader_schedule(current_epoch)
        next_leader_schedule = load_leader_schedule(current_epoch + 1)
        print(f"Loaded leader schedule for epoch {current_epoch} with {len(leader_schedule)} slots")
        print(f"Pre-loaded leader schedule for epoch {current_epoch + 1} with {len(next_leader_schedule)} slots")
        return True
    else:
        print("Warning: Could not determine current epoch")
        return False

def signal_handler(signum, frame):
    global should_run
    print(f"\nReceived signal {signum}. Shutting down Group {args.group}...")
    should_run = False
    shutdown_event.set()
    
    for server_name, worker in server_threads.items():
        if hasattr(worker, 'ws_instance') and worker.ws_instance:
            worker.ws_instance.close()

def main():
    global should_run, args
    
    # Parse command-line arguments
    parser = argparse.ArgumentParser(description="WebSocket slot duration parser")
    parser.add_argument("--group", type=int, choices=[1, 2], required=True, help="Server group to process (1 or 2)")
    args = parser.parse_args()
    
    # Load servers for the specified group
    load_servers(args.group)
    if not SERVERS:
        print(f"No servers found for group {args.group}. Exiting.")
        return
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    if not initialize_global_data():
        print("Failed to initialize global data. Exiting.")
        return
    
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    os.makedirs(LOG_DIR, exist_ok=True)
    
    print(f"Starting WebSocket Group {args.group} - {len(SERVERS)} servers...")
    
    with ThreadPoolExecutor(max_workers=len(SERVERS)) as executor:
        futures = {}
        
        for server_name, server_config in SERVERS.items():
            worker = ServerWorker(server_name, server_config)
            server_threads[server_name] = worker
            future = executor.submit(worker.run)
            futures[future] = server_name
            print(f"Started {server_name} ({server_config['location']})")
        
        try:
            while should_run and not shutdown_event.is_set():
                time.sleep(1)
        except KeyboardInterrupt:
            print("\nShutdown requested")
        finally:
            print(f"Shutting down Group {args.group} workers...")
            should_run = False
            shutdown_event.set()
            
            for future in futures:
                try:
                    future.result(timeout=5)
                except Exception as e:
                    print(f"Worker {futures[future]} shutdown error: {e}")
    
    print(f"Group {args.group} workers shut down.")

if __name__ == "__main__":
    main()