import requests
import json
import time

# API configuration
TOKEN = "MA4j9m8X31iaQoWPcGkZTWdg"
BASE_URL = "https://www.validators.app/api/v1/"

# Helper function for GET requests
def make_get_request(endpoint, params=None):
    headers = {"Token": TOKEN}
    url = BASE_URL + endpoint
    response = requests.get(url, headers=headers, params=params)
    response.raise_for_status()  # Raise an exception for HTTP errors
    return response.json()

# Helper function for POST requests
def make_post_request(endpoint, data):
    headers = {"Token": TOKEN, "Content-Type": "application/json"}
    url = BASE_URL + endpoint
    response = requests.post(url, headers=headers, data=json.dumps(data))
    response.raise_for_status()  # Raise an exception for HTTP errors
    return response.json()

# Test functions for each endpoint

def test_ping():
    """Test the Server Ping endpoint."""
    response = make_get_request("ping.json")
    assert response == {"answer": "pong"}, "Expected {'answer': 'pong'}"

def test_ping_times():
    """Test the Ping Times endpoint for mainnet."""
    response = make_get_request("ping-times/mainnet.json", params={"limit": 10})
    assert isinstance(response, list), "Expected a list of ping times"
    if response:
        assert "id" in response[0], "Expected 'id' in ping time entry"

def test_ping_time_stats():
    """Test the Ping Time Stats endpoint for mainnet."""
    response = make_get_request("ping-time-stats/mainnet.json", params={"limit": 10})
    assert isinstance(response, list), "Expected a list of ping time stats"
    if response:
        assert "batch_uuid" in response[0], "Expected 'batch_uuid' in stats entry"

def test_validators_list():
    """Test the Validators List endpoint for mainnet."""
    response = make_get_request("validators/mainnet.json", params={"limit": 10, "order": "score"})
    assert isinstance(response, list), "Expected a list of validators"
    if response:
        assert "account" in response[0], "Expected 'account' in validator entry"

def test_validator_details():
    """Test the Validator Details endpoint with a real account from validators list."""
    validators = make_get_request("validators/mainnet.json", params={"limit": 1})
    if validators:
        account = validators[0]["account"]
        response = make_get_request(f"validators/mainnet/{account}.json", params={"with_history": "true"})
        assert "account" in response, "Expected 'account' in response"
        assert response["account"] == account, f"Expected account {account}"
    else:
        print("No validators found for test_validator_details")

def test_validator_block_history():
    """Test the Validator Block Production History endpoint with a real account."""
    validators = make_get_request("validators/mainnet.json", params={"limit": 1})
    if validators:
        account = validators[0]["account"]
        response = make_get_request(f"validator-block-history/mainnet/{account}.json", params={"limit": 100})
        assert isinstance(response, list), "Expected a list of block history entries"
        if response:
            assert "epoch" in response[0], "Expected 'epoch' in history entry"
    else:
        print("No validators found for test_validator_block_history")

def test_epochs():
    """Test the Epochs endpoint for mainnet."""
    response = make_get_request("epochs/mainnet.json", params={"per": 50, "page": 1})
    assert "epochs" in response, "Expected 'epochs' key"
    assert "epochs_count" in response, "Expected 'epochs_count' key"

def test_commission_changes():
    """Test the Commission Changes endpoint for mainnet."""
    response = make_get_request("commission-changes/mainnet.json", params={"per": 25, "page": 1})
    try:
        # Check that the response is a dictionary
        assert isinstance(response, dict), "Expected a dictionary response"
        
        # Check that the "commission_histories" key exists
        assert "commission_histories" in response, "Expected 'commission_histories' key"
        
        # Check that "commission_histories" is a list
        assert isinstance(response["commission_histories"], list), "Expected 'commission_histories' to be a list"
        
        # Check that the "total_count" key exists
        assert "total_count" in response, "Expected 'total_count' key"
        
        # Check that "total_count" is an integer
        assert isinstance(response["total_count"], int), "Expected 'total_count' to be an integer"
    except AssertionError as e:
        print(f"Response: {response}")
        raise e

def test_stake_pools():
    """Test the Stake Pools endpoint for mainnet."""
    response = make_get_request("stake-pools/mainnet.json")
    assert "stake_pools" in response, "Expected 'stake_pools' key"

def test_stake_accounts():
    """Test the Stake Accounts endpoint for mainnet."""
    response = make_get_request("stake-accounts/mainnet.json", params={"per": 25, "page": 1})
    assert "stake_accounts" in response, "Expected 'stake_accounts' key"

def test_stake_explorer():
    """Test the Stake Explorer endpoint with example staker."""
    response = make_get_request("stake-explorer/mainnet.json", 
                              params={"staker": "mpa4abUkjQoAvPzREkh5Mo75hZhPFQ2FSH6w7dWKuQ5", "per": 25, "page": 1})
    assert "explorer_stake_accounts" in response, "Expected 'explorer_stake_accounts' key"
    assert "total_count" in response, "Expected 'total_count' key"

def test_ping_thing_post():
    """Test the Ping Thing Post endpoint with example data."""
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
    assert response == {"status": "created"}, "Expected {'status': 'created'}"

def test_ping_thing_post_batch():
    """Test the Ping Thing Post Batch endpoint with example data."""
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
    assert response == {"status": "created"}, "Expected {'status': 'created'}"

def test_ping_thing_list():
    """Test the Ping Thing List endpoint for mainnet."""
    response = make_get_request("ping-thing/mainnet.json", params={"limit": 10, "page": 1})
    assert isinstance(response, list), "Expected a list of ping thing entries"
    if response:
        assert "application" in response[0], "Expected 'application' in entry"

def test_ping_thing_stats():
    """Test the Ping Thing Stats endpoint for mainnet."""
    response = make_get_request("ping-thing-stats/mainnet.json", params={"interval": 1})
    assert isinstance(response, list), "Expected a list of ping thing stats"
    if response:
        assert "interval" in response[0], "Expected 'interval' in entry"

def test_sol_prices():
    """Test the Sol Prices endpoint with example date range."""
    response = make_get_request("sol-prices.json", 
                              params={"from": "2022-01-01T00:00:00", "to": "2022-01-02T00:00:00"})
    assert isinstance(response, list), "Expected a list of price entries"
    if response:
        assert "id" in response[0], "Expected 'id' in price entry"

def test_gossip_nodes():
    """Test the Gossip Nodes endpoint for mainnet."""
    response = make_get_request("gossip-nodes/mainnet.json", params={"staked": "true", "per": 50, "page": 1})
    assert isinstance(response, list), "Expected a list of gossip nodes"
    if response:
        assert "account" in response[0], "Expected 'account' in node entry"

def test_authorities_changes():
    """Test the Authorities Changes endpoint for mainnet."""
    response = make_get_request("account-authorities/mainnet.json", params={"per": 50, "page": 1})
    try:
        # Check that the response is a dictionary
        assert isinstance(response, dict), "Expected a dictionary response"
        
        # Check that the "authority_changes" key exists
        assert "authority_changes" in response, "Expected 'authority_changes' key"
        
        # Check that "authority_changes" is a list
        assert isinstance(response["authority_changes"], list), "Expected 'authority_changes' to be a list"
        
        # Check that the "total_count" key exists
        assert "total_count" in response, "Expected 'total_count' key"
        
        # Check that "total_count" is an integer
        assert isinstance(response["total_count"], int), "Expected 'total_count' to be an integer"
    except AssertionError as e:
        print(f"Response: {response}")
        raise e

# Function to run all tests
# Test execution function
def run_tests():
    tests = [
        test_ping,
        test_ping_times,
        test_ping_time_stats,
        test_validators_list,
        test_validator_details,
        test_validator_block_history,
        test_epochs,
        test_commission_changes,
        test_stake_pools,
        test_stake_accounts,
        test_stake_explorer,
        test_ping_thing_post,
        test_ping_thing_post_batch,
        test_ping_thing_list,
        test_ping_thing_stats,
        test_sol_prices,
        test_gossip_nodes,
        test_authorities_changes
    ]
    for test in tests:
        try:
            test()
            print(f"{test.__name__} passed")
        except AssertionError as e:
            print(f"{test.__name__} failed: {e}")
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 429 and 'Retry-After' in e.response.headers:
                wait_time = int(e.response.headers['Retry-After'])
                print(f"Rate limited. Waiting {wait_time} seconds...")
                time.sleep(wait_time)
                return make_get_request(endpoint, params)  # Retry once
            print(f"HTTP error for {url}: {e}")
            raise            
            print(f"{test.__name__} HTTP error: {e}")
        except Exception as e:
            print(f"{test.__name__} unexpected error: {e}")
        time.sleep(5)  # Wait 5 seconds between tests

# Run the tests
if __name__ == "__main__":
    run_tests()