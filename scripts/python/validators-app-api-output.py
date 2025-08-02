import requests
import time
import json
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry

# API configuration
TOKEN = "MA4j9m8X31iaQoWPcGkZTWdg"
BASE_URL = "https://www.validators.app/api/v1/"

# Retry configuration
MAX_RETRIES = 5
BACKOFF_FACTOR = 2

# Create a session with retry logic
def create_session():
    session = requests.Session()
    retries = Retry(
        total=MAX_RETRIES,
        backoff_factor=BACKOFF_FACTOR,
        status_forcelist=[429],
        allowed_methods=["GET", "POST"]
    )
    adapter = HTTPAdapter(max_retries=retries)
    session.mount("https://", adapter)
    return session

# Updated GET request function
def make_get_request(endpoint, params=None):
    session = create_session()
    headers = {"Token": TOKEN}
    url = BASE_URL + endpoint
    # Construct curl command
    curl_cmd = ["curl -X GET"]
    curl_cmd.append(f'-H "Token: {TOKEN}"')
    if params:
        param_str = "&".join(f"{k}={v}" for k, v in params.items())
        url_with_params = f"{url}?{param_str}"
        curl_cmd.append(f'"{url_with_params}"')
    else:
        curl_cmd.append(f'"{url}"')
    print(f"Equivalent curl command: {' '.join(curl_cmd)}")
    try:
        response = session.get(url, headers=headers, params=params)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RetryError as e:
        print(f"Max retries exceeded for {url}: {e}")
        raise
    except requests.exceptions.HTTPError as e:
        print(f"HTTP error for {url}: {e}")
        raise

# Updated POST request function
def make_post_request(endpoint, data):
    session = create_session()
    headers = {"Token": TOKEN, "Content-Type": "application/json"}
    url = BASE_URL + endpoint
    # Construct curl command
    curl_cmd = ["curl -X POST"]
    curl_cmd.append(f'-H "Token: {TOKEN}"')
    curl_cmd.append('-H "Content-Type: application/json"')
    curl_cmd.append(f'-d \'{json.dumps(data)}\'')
    curl_cmd.append(f'"{url}"')
    print(f"Equivalent curl command: {' '.join(curl_cmd)}")
    try:
        response = session.post(url, headers=headers, data=json.dumps(data))
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RetryError as e:
        print(f"Max retries exceeded for {url}: {e}")
        raise
    except requests.exceptions.HTTPError as e:
        print(f"HTTP error for {url}: {e}")
        raise

# Helper function to print response (full if ≤10 lines, sample if >10 lines)
def print_response(test_name, response):
    print(f"\nOutput from {test_name}:")
    response_str = json.dumps(response, indent=2)
    lines = response_str.splitlines()
    if len(lines) <= 10:
        print(response_str)
    else:
        print("\n".join(lines[:5] + ["..."] + lines[-5:]))
    print("-" * 50)

# Test functions with output display

def test_ping():
    response = make_get_request("ping.json")
    assert response == {"answer": "pong"}
    print_response("test_ping", response)

def test_ping_times():
    response = make_get_request("ping-times/mainnet.json", params={"limit": 10})
    assert isinstance(response, list)
    print_response("test_ping_times", response)

def test_ping_time_stats():
    response = make_get_request("ping-time-stats/mainnet.json", params={"limit": 10})
    assert isinstance(response, list)
    print_response("test_ping_time_stats", response)

def test_validators_list():
    response = make_get_request("validators/mainnet.json", params={"limit": 10, "order": "score"})
    assert isinstance(response, list)
    print_response("test_validators_list", response)

def test_validator_details():
    validators = make_get_request("validators/mainnet.json", params={"limit": 1})
    if validators:
        account = validators[0]["account"]
        response = make_get_request(f"validators/mainnet/{account}.json", params={"with_history": "true"})
        assert "account" in response
        print_response("test_validator_details", response)

def test_validator_block_history():
    validators = make_get_request("validators/mainnet.json", params={"limit": 1})
    if validators:
        account = validators[0]["account"]
        response = make_get_request(f"validator-block-history/mainnet/{account}.json", params={"limit": 100})
        assert isinstance(response, list)
        print_response("test_validator_block_history", response)

def test_epochs():
    response = make_get_request("epochs/mainnet.json", params={"per": 50, "page": 1})
    assert "epochs" in response
    print_response("test_epochs", response)

def test_commission_changes():
    response = make_get_request("commission-changes/mainnet.json", params={"per": 25, "page": 1})
    assert isinstance(response, dict) and "commission_histories" in response
    print_response("test_commission_changes", response)

def test_stake_pools():
    response = make_get_request("stake-pools/mainnet.json")
    assert "stake_pools" in response
    print_response("test_stake_pools", response)

def test_stake_accounts():
    response = make_get_request("stake-accounts/mainnet.json", params={"per": 25, "page": 1})
    assert "stake_accounts" in response
    print_response("test_stake_accounts", response)

def test_stake_explorer():
    response = make_get_request("stake-explorer/mainnet.json", 
                              params={"staker": "mpa4abUkjQoAvPzREkh5Mo75hZhPFQ2FSH6w7dWKuQ5", "per": 25, "page": 1})
    assert "explorer_stake_accounts" in response
    print_response("test_stake_explorer", response)

def test_ping_thing_post():
    data = {
        "application": "mango",
        "commitment_level": "finalized",
        "signature": "signature",
        "success": True,
        "time": "NN",
        "transaction_type": "transfer",
        "slot_sent": "NN",
        "slot_landed": "NN",
        "reported_at": "2021-07-01T11:02:12",
        "priority_fee_percentile": "NN",
        "priority_fee_micro_lamports": "NN",
        "pinger_region": "reg"
    }
    response = make_post_request("ping-thing/mainnet.json", data)
    assert response == {"status": "created"}
    print_response("test_ping_thing_post", response)

def test_ping_thing_post_batch():
    data = {
        "transactions": [{
            "time": "NN",
            "signature": "signature",
            "transaction_type": "transfer",
            "slot_sent": "NN",
            "slot_landed": "NN",
            "reported_at": "2021-07-01T11:02:12",
            "priority_fee_percentile": "NN",
            "priority_fee_micro_lamports": "NN",
            "pinger_region": "reg"
        }]
    }
    response = make_post_request("ping-thing-batch/mainnet.json", data)
    assert response == {"status": "created"}
    print_response("test_ping_thing_post_batch", response)

def test_ping_thing_list():
    response = make_get_request("ping-thing/mainnet.json", params={"limit": 10, "page": 1})
    assert isinstance(response, list)
    print_response("test_ping_thing_list", response)

def test_ping_thing_stats():
    response = make_get_request("ping-thing-stats/mainnet.json", params={"interval": 1})
    assert isinstance(response, list)
    print_response("test_ping_thing_stats", response)

def test_sol_prices():
    response = make_get_request("sol-prices.json", 
                              params={"from": "2022-01-01T00:00:00", "to": "2022-01-02T00:00:00"})
    assert isinstance(response, list)
    print_response("test_sol_prices", response)

def test_gossip_nodes():
    response = make_get_request("gossip-nodes/mainnet.json", params={"staked": "true", "per": 50, "page": 1})
    assert isinstance(response, list)
    print_response("test_gossip_nodes", response)

def test_authorities_changes():
    response = make_get_request("account-authorities/mainnet.json", params={"per": 50, "page": 1})
    assert isinstance(response, dict) and "authority_changes" in response
    print_response("test_authorities_changes", response)

# Menu-driven test execution
def run_test_menu():
    tests = [
        ("Run all tests", lambda: [test() for test in [
            test_ping, test_ping_times, test_ping_time_stats, test_validators_list,
            test_validator_details, test_validator_block_history, test_epochs,
            test_commission_changes, test_stake_pools, test_stake_accounts,
            test_stake_explorer, test_ping_thing_post, test_ping_thing_post_batch,
            test_ping_thing_list, test_ping_thing_stats, test_sol_prices,
            test_gossip_nodes, test_authorities_changes
        ]]),
        ("Test ping", test_ping),
        ("Test ping times", test_ping_times),
        ("Test ping time stats", test_ping_time_stats),
        ("Test validators list", test_validators_list),
        ("Test validator details", test_validator_details),
        ("Test validator block history", test_validator_block_history),
        ("Test epochs", test_epochs),
        ("Test commission changes", test_commission_changes),
        ("Test stake pools", test_stake_pools),
        ("Test stake accounts", test_stake_accounts),
        ("Test stake explorer", test_stake_explorer),
        ("Test ping thing post", test_ping_thing_post),
        ("Test ping thing post batch", test_ping_thing_post_batch),
        ("Test ping thing list", test_ping_thing_list),
        ("Test ping thing stats", test_ping_thing_stats),
        ("Test SOL prices", test_sol_prices),
        ("Test gossip nodes", test_gossip_nodes),
        ("Test authorities changes", test_authorities_changes)
    ]

    while True:
        print("\nAPI Test Menu:")
        for i, (test_name, _) in enumerate(tests):
            print(f"{i}. {test_name}")
        print(f"{len(tests)}. Exit")
        
        try:
            choice = int(input("Enter your choice (0-{}): ".format(len(tests))))
            if choice == len(tests):
                print("Exiting...")
                break
            if 0 <= choice < len(tests):
                test_name, test_func = tests[choice]
                try:
                    print(f"\nRunning {test_name}...")
                    test_func()
                    print(f"{test_name} passed")
                except AssertionError as e:
                    print(f"{test_name} failed: {e}")
                except requests.exceptions.HTTPError as e:
                    print(f"{test_name} HTTP error: {e}")
                except Exception as e:
                    print(f"{test_name} unexpected error: {e}")
                time.sleep(5)  # 5-second delay to respect rate limits
            else:
                print("Invalid choice. Please select a number between 0 and {}.".format(len(tests)))
        except ValueError:
            print("Invalid input. Please enter a number.")

# Execute the menu
if __name__ == "__main__":
    run_test_menu()