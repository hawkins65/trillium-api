#!/usr/bin/env python3
"""
WebSocket Services Health Monitor
Monitors both websocket groups and their individual server connections
"""

import subprocess
import json
import os
import time
import requests
from datetime import datetime, timedelta, timezone
import glob

# Configuration
CONFIG_FILE = "/home/smilax/trillium_api/data/configs/92_slot_duration_server_list.json"
LOG_DIR = os.path.expanduser("~/log")
OUTPUT_DIR = "/home/smilax/trillium_api/data/monitoring/wss_slot_duration"
CHECK_INTERVAL = 60  # seconds
MAX_LOG_AGE = 300    # 5 minutes without new log entries is concerning
ALERT_COOLDOWN = 3600  # 1 hour in seconds for Discord alert suppression

# Discord webhook for critical alerts
DISCORD_WEBHOOK_URL = "https://discord.com/api/webhooks/1394780058895384638/AHAOfYGfstdfqcdkW3hK60hFEON9miILFwcXGf13AsXY0efX16Ukwbz02_SliUnK45Nm"

# Track last alert time for each server
last_alert_times = {}

def send_discord_alert(title, message, color=0xff0000):
    """Send a Discord webhook alert"""
    try:
        embed = {
            "title": title,
            "description": message,
            "color": color,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "footer": {
                "text": "WebSocket Monitor"
            }
        }
        
        payload = {
            "embeds": [embed]
        }
        
        response = requests.post(DISCORD_WEBHOOK_URL, json=payload, timeout=10)
        if response.status_code == 204:
            print(f"Discord alert sent: {title}")
        else:
            print(f"Failed to send Discord alert: {response.status_code}")
            
    except Exception as e:
        print(f"Error sending Discord alert: {e}")

def run_command(cmd):
    """Run a shell command and return the result"""
    try:
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True, timeout=10)
        return result.returncode == 0, result.stdout.strip(), result.stderr.strip()
    except subprocess.TimeoutExpired:
        return False, "", "Command timed out"
    except Exception as e:
        return False, "", str(e)

def check_service_status(service_name):
    """Check if a systemd service is running"""
    success, stdout, stderr = run_command(f"systemctl is-active {service_name}")
    return success and stdout == "active"

def get_service_uptime(service_name):
    """Get service uptime"""
    success, stdout, stderr = run_command(f"systemctl show {service_name} --property=ActiveEnterTimestamp")
    if success and stdout:
        try:
            timestamp_str = stdout.split("=", 1)[1]
            if timestamp_str and timestamp_str != "0":
                # Parse systemd timestamp format
                start_time = datetime.strptime(timestamp_str.split()[0] + " " + timestamp_str.split()[1], "%Y-%m-%d %H:%M:%S")
                uptime = datetime.now() - start_time
                return uptime
        except:
            pass
    return None

def get_running_services():
    """Dynamically discover running websocket services"""
    try:
        # Get all active systemd services
        success, stdout, stderr = run_command("systemctl list-units --type=service --state=active --no-pager --plain")
        if not success:
            return []
        
        services = []
        for line in stdout.split('\n'):
            if '92_wss_parse_slot_duration-' in line:
                # Extract service name (remove .service suffix)
                service_name = line.split()[0].replace('.service', '')
                services.append(service_name)
        
        return sorted(services)
    except Exception as e:
        print(f"Error discovering services: {e}")
        return []

def get_servers_from_json(group_id):
    """Load server data from JSON config file for the specified group"""
    try:
        with open(CONFIG_FILE, "r") as f:
            data = json.load(f)
        servers = [server for server in data["servers"] if server["group"] == group_id]
        return servers
    except Exception as e:
        print(f"Error reading servers from {CONFIG_FILE}: {e}")
        return []

def extract_server_name_from_url(endpoint):
    """Extract server name portion from WebSocket endpoint"""
    try:
        # Remove protocol and common prefixes/suffixes
        url_part = endpoint.replace("wss://", "").replace("ws://", "")
        
        # Split by / and take the first part (domain)
        domain = url_part.split('/')[0]
        
        # Split by : and take the first part (remove port)
        server_part = domain.split(':')[0]
        
        return server_part
    except Exception as e:
        return endpoint  # Return original endpoint if parsing fails

def discover_all_servers():
    """Discover all servers from JSON config and map to their services"""
    services = get_running_services()
    servers_by_service = {}
    server_info = {}  # Map server name to full server data including URL
    
    for service in services:
        # Extract group ID from service name (e.g., 92_wss_parse_slot_duration-1 -> 1)
        try:
            group_id = int(service.split('-')[-1])
            servers_data = get_servers_from_json(group_id)
            if servers_data:
                server_names = []
                for server_data in servers_data:
                    server_name = server_data["name"]
                    server_names.append(server_name)
                    # Store server info including URL
                    server_info[server_name] = server_data
                servers_by_service[service] = server_names
        except ValueError:
            print(f"Invalid group ID in service name: {service}")
            continue
    
    # Flatten the list of servers for reporting all servers
    all_servers = []
    for servers in servers_by_service.values():
        all_servers.extend(servers)
    
    return list(set(all_servers)), servers_by_service, server_info

def check_recent_logs(server_name, max_age_seconds=MAX_LOG_AGE):
    """Check if server has recent log activity"""
    try:
        # Find the most recent log file for this server
        log_pattern = os.path.join(LOG_DIR, f"{server_name}_wss_epoch*.log")
        log_files = glob.glob(log_pattern)
        
        if not log_files:
            return False, "No log files found"
        
        # Get the most recent log file
        latest_log = max(log_files, key=os.path.getmtime)
        
        # Check modification time
        mod_time = os.path.getmtime(latest_log)
        age_seconds = time.time() - mod_time
        
        if age_seconds > max_age_seconds:
            return False, f"Last log activity {age_seconds:.0f}s ago"
        else:
            return True, f"Active (last update {age_seconds:.0f}s ago)"
            
    except Exception as e:
        return False, f"Error checking logs: {e}"

def check_recent_data(server_name, max_age_seconds=MAX_LOG_AGE):
    """Check if server has recent CSV data"""
    try:
        # Find the most recent CSV file for this server across all epoch directories
        csv_files = []
        for epoch_dir in glob.glob(os.path.join(OUTPUT_DIR, "epoch*")):
            if os.path.isdir(epoch_dir):
                csv_pattern = os.path.join(epoch_dir, f"{server_name}_wss_epoch*.csv")
                csv_files.extend(glob.glob(csv_pattern))
        
        if not csv_files:
            return False, "No CSV files found"
        
        # Get the most recent CSV file
        latest_csv = max(csv_files, key=os.path.getmtime)
        
        # Check modification time
        mod_time = os.path.getmtime(latest_csv)
        age_seconds = time.time() - mod_time
        
        if age_seconds > max_age_seconds:
            return False, f"Last data {age_seconds:.0f}s ago"
        else:
            return True, f"Active (last data {age_seconds:.0f}s ago)"
            
    except Exception as e:
        return False, f"Error checking data: {e}"

def get_latest_slot_data():
    """Get the latest slot from any CSV file to check overall progress"""
    try:
        csv_files = []
        for epoch_dir in glob.glob(os.path.join(OUTPUT_DIR, "epoch*")):
            if os.path.isdir(epoch_dir):
                csv_pattern = os.path.join(epoch_dir, "*_wss_epoch*.csv")
                csv_files.extend(glob.glob(csv_pattern))
        
        if not csv_files:
            return None, "No CSV files found"
        
        latest_slots = {}
        
        for csv_file in csv_files:
            try:
                server_name = os.path.basename(csv_file).split('_wss_epoch')[0]
                
                # Read last few lines to get latest slot
                with open(csv_file, 'r') as f:
                    lines = f.readlines()
                    if len(lines) > 1:  # Skip header
                        last_line = lines[-1].strip()
                        if last_line:
                            parts = last_line.split(',')
                            if len(parts) >= 2:
                                try:
                                    slot = int(parts[1])
                                    timestamp = parts[0]
                                    latest_slots[server_name] = {
                                        'slot': slot,
                                        'timestamp': timestamp
                                    }
                                except ValueError:
                                    pass
            except Exception as e:
                continue
        
        return latest_slots, None
        
    except Exception as e:
        return None, f"Error reading slot data: {e}"

def format_uptime(uptime):
    """Format uptime duration nicely"""
    if uptime is None:
        return "Unknown"
    
    total_seconds = int(uptime.total_seconds())
    days = total_seconds // 86400
    hours = (total_seconds % 86400) // 3600
    minutes = (total_seconds % 3600) // 60
    
    if days > 0:
        return f"{days}d {hours}h {minutes}m"
    elif hours > 0:
        return f"{hours}h {minutes}m"
    else:
        return f"{minutes}m"

def print_status_report():
    """Print a comprehensive status report"""
    print("=" * 80)
    print(f"WebSocket Services Health Report - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 80)
    
    # Discover running services and servers
    services = get_running_services()
    all_servers, servers_by_service, server_info = discover_all_servers()
    
    # Initialize issues list
    issues = []
    critical_issues = []
    
    # Check service status
    print("\n📊 SERVICE STATUS:")
    if not services:
        print("  ❌ No websocket services found running")
        critical_issues.append("No websocket services running")
    else:
        for i, service_name in enumerate(services, 1):
            is_active = check_service_status(service_name)
            uptime = get_service_uptime(service_name)
            status_icon = "✅" if is_active else "❌"
            group_id = service_name.split('-')[-1]
            print(f"  {status_icon} Group {group_id:1} ({service_name:25}) - {'ACTIVE' if is_active else 'INACTIVE':8} - Uptime: {format_uptime(uptime)}")
            
            if not is_active:
                critical_issues.append(f"Service {service_name} is not active")
    
    # Check individual server health
    print("\n🔗 SERVER CONNECTION STATUS:")
    
    if not all_servers:
        print("  ❌ No servers discovered from JSON config")
        if services:  # Services exist but no servers found
            critical_issues.append("Services running but no servers discovered")
    else:
        for i, (service, servers) in enumerate(servers_by_service.items(), 1):
            group_id = service.split('-')[-1]
            print(f"\n  Group {group_id} ({service}):")
            for idx, server in enumerate(servers, 1):
                log_ok, log_msg = check_recent_logs(server)
                data_ok, data_msg = check_recent_data(server)
                
                status_icon = "✅" if (log_ok and data_ok) else "⚠️" if (log_ok or data_ok) else "❌"
                
                # Get endpoint info for display
                endpoint_part = ""
                if server in server_info and "endpoint" in server_info[server]:
                    server_endpoint_part = extract_server_name_from_url(server_info[server]["endpoint"])
                    endpoint_part = f" - {server_endpoint_part}"
                
                print(f"    {status_icon} {group_id}.{idx} {server:25}{endpoint_part} - Logs: {log_msg:25} | Data: {data_msg}")
                
                if not (log_ok and data_ok):
                    # Get endpoint info for the issue
                    endpoint_info = ""
                    if server in server_info and "endpoint" in server_info[server]:
                        server_endpoint_part = extract_server_name_from_url(server_info[server]["endpoint"])
                        endpoint_info = f" - {server_endpoint_part}"
                    
                    issue_msg = f"{group_id}.{idx} {server}{endpoint_info} : Logs={log_msg}, Data={data_msg}"
                    issues.append(issue_msg)
                    
                    # Consider it critical if both logs and data are failing
                    if not log_ok and not data_ok:
                        # Check if an alert for this server was sent within the last hour
                        current_time = time.time()
                        last_alert_time = last_alert_times.get(server, 0)
                        if current_time - last_alert_time >= ALERT_COOLDOWN:
                            critical_issues.append(f"{group_id}.{idx} {server}{endpoint_info} completely down")
                            last_alert_times[server] = current_time
    
    # Check latest slot progress
    print("\n📈 LATEST SLOT DATA:")
    slot_data, slot_error = get_latest_slot_data()
    
    if slot_error:
        print(f"  ❌ Error: {slot_error}")
        critical_issues.append(f"Slot data error: {slot_error}")
    elif slot_data:
        # Group by latest slot to see if servers are in sync
        slots_by_value = {}
        for server, data in slot_data.items():
            slot = data['slot']
            if slot not in slots_by_value:
                slots_by_value[slot] = []
            slots_by_value[slot].append(server)
        
        # Show latest few slots
        sorted_slots = sorted(slots_by_value.keys(), reverse=True)[:3]
        for slot in sorted_slots:
            servers = slots_by_value[slot]
            print(f"  Slot {slot:,}: {len(servers)} servers - {', '.join(servers[:3])}")
            if len(servers) > 3:
                print(f"    + {len(servers)-3} more servers")
    
    # Summary
    print(f"\n📋 SUMMARY:")
    active_services = [s for s in services if check_service_status(s)]
    
    if len(active_services) == len(services) and not issues:
        print("  ✅ All systems operational")
    else:
        print(f"  ⚠️ Issues detected:")
        if len(active_services) < len(services):
            inactive_services = [s for s in services if not check_service_status(s)]
            for service in inactive_services:
                print(f"    - Service {service} is not active")
        for issue in issues[:5]:  # Show first 5 issues
            print(f"    - {issue}")
        if len(issues) > 5:
            print(f"    - ... and {len(issues)-5} more server issues")
    
    print("=" * 80)
    
    # Send Discord alerts for critical issues
    if critical_issues:
        alert_message = "\n".join([f"• {issue}" for issue in critical_issues[:10]])
        if len(critical_issues) > 10:
            alert_message += f"\n• ... and {len(critical_issues)-10} more issues"
        
        send_discord_alert(
            "🚨 WebSocket Monitor - Critical Issues Detected",
            alert_message,
            color=0xff0000  # Red
        )

def monitor_loop():
    """Continuous monitoring loop"""
    print("Starting WebSocket services monitor...")
    print(f"Checking every {CHECK_INTERVAL} seconds")
    print("Press Ctrl+C to stop")
    
    try:
        while True:
            print_status_report()
            time.sleep(CHECK_INTERVAL)
    except KeyboardInterrupt:
        print("\nMonitoring stopped.")

def main():
    """Main function with command line options"""
    import sys
    
    script_name = os.path.basename(sys.argv[0])
    
    if len(sys.argv) > 1:
        if sys.argv[1] == "--once":
            print_status_report()
            return
        elif sys.argv[1] == "--help":
            print("Usage:")
            print(f"  python3 {script_name}          # Continuous monitoring")
            print(f"  python3 {script_name} --once   # Single status check")
            print(f"  python3 {script_name} --help   # Show this help")
            return
    
    monitor_loop()

if __name__ == "__main__":
    main()