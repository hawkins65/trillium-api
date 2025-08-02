import requests
import csv
from ipaddress import ip_address
import time
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry

def requests_retry_session(
    retries=3,
    backoff_factor=0.3,
    status_forcelist=(500, 502, 504),
    session=None,
):
    session = session or requests.Session()
    retry = Retry(
        total=retries,
        read=retries,
        connect=retries,
        backoff_factor=backoff_factor,
        status_forcelist=status_forcelist,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    return session

def get_net_range(net_blocks):
    if isinstance(net_blocks, dict):
        net_blocks = [net_blocks]
    
    if not net_blocks:
        return 'N/A - N/A'
    
    start_addresses = [block.get('startAddress', {}).get('$', 'N/A') for block in net_blocks]
    end_addresses = [block.get('endAddress', {}).get('$', 'N/A') for block in net_blocks]
    
    return f"{min(start_addresses)} - {max(end_addresses)}"

def arin_lookup(ip):
    url = f"https://whois.arin.net/rest/ip/{ip}"
    headers = {"Accept": "application/json"}
    
    try:
        response = requests_retry_session().get(url, headers=headers, timeout=10)
        response.raise_for_status()
        
        data = response.json()
        net = data.get('net', {})
        org = net.get('orgRef', {})
        net_blocks = net.get('netBlocks', {}).get('netBlock', [])
        
        return {
            'IP': ip,
            'Name': net.get('name', {}).get('$', 'N/A'),
            'Organization': org.get('$', 'N/A'),
            'City': net.get('city', {}).get('$', 'N/A'),
            'State': net.get('stateProvince', {}).get('$', 'N/A'),
            'Country': net.get('country', {}).get('$', 'N/A'),
            'NetRange': get_net_range(net_blocks)
        }
    except requests.exceptions.RequestException as e:
        print(f"Error looking up {ip}: {str(e)}")
        return {'IP': ip, 'Error': str(e)}

def process_ips(ip_list):
    results = []
    for ip in ip_list:
        try:
            ip_address(ip)  # Validate IP address
            result = arin_lookup(ip)
            results.append(result)
            time.sleep(1)  # Add a small delay between requests to avoid overwhelming the server
        except ValueError:
            results.append({'IP': ip, 'Error': 'Invalid IP address'})
    return results

# List of IPs to look up
ips = [
    "104.148.56.26", "157.52.220.194", "104.148.56.58", "157.52.220.242",
    "157.52.220.226", "104.148.56.50", "69.197.20.30", "157.52.220.202",
    "134.73.71.130", "134.73.117.130", "104.148.56.42", "157.52.220.218",
    "134.73.117.130", "157.52.220.234", "104.148.56.146", "157.52.220.210"
]

results = process_ips(ips)

# Write results to CSV
with open('arin_lookup_results.csv', 'w', newline='') as csvfile:
    fieldnames = ['IP', 'Name', 'Organization', 'City', 'State', 'Country', 'NetRange', 'Error']
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
    writer.writeheader()
    for result in results:
        writer.writerow(result)

print("ARIN lookup completed. Results saved in 'arin_lookup_results.csv'")