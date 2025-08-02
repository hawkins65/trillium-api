#!/usr/bin/env python3
"""
FIXED Diagnostic script for 401 authentication errors with Solana RPC endpoints
"""

import requests
import json
import time
import sys
import os
from datetime import datetime

# Add the directory containing rpc_config.py to sys.path
sys.path.append("/home/smilax/api")
from rpc_config import RPC_ENDPOINT

# RPC Configuration
RPC_ENDPOINTS = [
    ("PRIMARY (Alchemy)", RPC_ENDPOINT),
    ("SECONDARY (QuickNode)", "https://silent-frequent-firefly.solana-mainnet.quiknode.pro/2059a05165e13886cb8226c6b87081ad579860e3/"),
    ("TERTIARY (Alchemy)", RPC_ENDPOINT)  # Same as primary in your original code
]

headers = {'Content-Type': 'application/json'}

def get_rpc_url_prefix(url):
    """Extract meaningful prefix of RPC URL for logging (FIXED VERSION)"""
    if not url:
        return "unknown"
    
    # Remove protocol
    if url.startswith(('http://', 'https://')):
        clean_url = url.split('://', 1)[1]
    else:
        clean_url = url
    
    # For better readability, show domain + first part of path
    parts = clean_url.split('/')
    if len(parts) >= 2:
        domain = parts[0]
        if len(parts) > 1 and parts[1]:
            # Show first 8 chars of domain + first 8 chars of path
            return f"{domain[:12]}..{parts[1][:8]}"
        else:
            return domain[:15]
    else:
        return clean_url[:15]

def analyze_api_key_in_url(name, url):
    """FIXED: Properly analyze API key presence in URL"""
    print(f"\n=== API Key Analysis for {name} ===")
    print(f"Full URL: {url}")
    
    api_key_found = False
    
    # Check for query parameters (like ?apikey=xxx)
    if '?' in url:
        query_part = url.split('?', 1)[1]
        print(f"✅ Query parameters detected: {query_part[:20]}...")
        if 'apikey' in query_part.lower() or 'api_key' in query_part.lower():
            print(f"✅ API key detected in query parameters")
            api_key_found = True
        else:
            print(f"⚠️  Query parameters present but no obvious API key")
    
    # Check for API key in path (common for many RPC providers)
    if not api_key_found:
        path_parts = url.split('/')
        if len(path_parts) >= 4:  # https://domain.com/path/apikey or similar
            for i, part in enumerate(path_parts[3:], 3):  # Skip https, empty, domain
                if len(part) > 20:  # API keys are usually long
                    print(f"✅ Potential API key in path segment {i}: {part[:8]}...{part[-4:]} (length: {len(part)})")
                    api_key_found = True
                    
                    # Analyze the format
                    if part.startswith('v2'):
                        print(f"   🔍 Looks like Alchemy API key format (starts with v2)")
                    elif len(part) > 30 and not part.startswith('v'):
                        print(f"   🔍 Looks like QuickNode API key format (long alphanumeric)")
                    
                elif len(part) > 10:
                    print(f"⚠️  Medium-length path segment {i}: {part} (length: {len(part)})")
    
    if not api_key_found:
        print(f"❌ No API key detected in URL")
        print(f"   This could indicate:")
        print(f"   1. API key is missing (authentication will fail)")
        print(f"   2. API key is in a non-standard location")
        print(f"   3. Authentication uses a different method (headers, etc.)")
    
    return api_key_found

def test_endpoint_with_detailed_401_analysis(name, url):
    """Test endpoint with detailed 401 error analysis"""
    print(f"\n=== Testing {name} ({get_rpc_url_prefix(url)}) ===")
    
    # Test 1: Simple RPC call (getHealth)
    try:
        payload = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "getHealth"
        }
        response = requests.post(url, headers=headers, json=payload, timeout=10)
        print(f"getHealth: HTTP {response.status_code}")
        
        if response.status_code == 200:
            result = response.json()
            print(f"   ✅ Success: {result}")
        elif response.status_code == 401:
            print(f"   🚨 401 UNAUTHORIZED DETECTED")
            print(f"   Response headers: {dict(response.headers)}")
            print(f"   Response body: {response.text}")
            
            # Detailed 401 analysis
            response_text = response.text.lower() if response.text else ""
            auth_header = response.headers.get('WWW-Authenticate', 'Not provided')
            
            print(f"\n   🔍 DETAILED 401 ANALYSIS:")
            print(f"   WWW-Authenticate header: {auth_header}")
            
            if "unauthorized" in response_text:
                print(f"   ❌ Server explicitly returned UNAUTHORIZED")
            if "api key" in response_text or "apikey" in response_text:
                print(f"   🔑 Response mentions API key issues")
            if "quota" in response_text or "limit" in response_text:
                print(f"   📊 Possible quota/rate limit issue")
            if "expired" in response_text:
                print(f"   ⏰ Possible expired credentials")
            if "invalid" in response_text:
                print(f"   ❌ Possible invalid credentials")
            
            # Provider-specific analysis
            if "alchemy.com" in url:
                print(f"   🔍 ALCHEMY-SPECIFIC ANALYSIS:")
                if "/v2/" in url:
                    api_key = url.split("/v2/")[-1]
                    print(f"   API key format: {api_key[:8]}...{api_key[-4:]} (length: {len(api_key)})")
                    if len(api_key) < 30:
                        print(f"   ⚠️  API key seems short for Alchemy (expected 32+ chars)")
                else:
                    print(f"   ❌ Missing /v2/ in Alchemy URL structure")
                    
            elif "quiknode.pro" in url:
                print(f"   🔍 QUICKNODE-SPECIFIC ANALYSIS:")
                path_parts = url.rstrip('/').split('/')
                if len(path_parts) >= 4:
                    api_key = path_parts[-2] if path_parts[-1] == '' else path_parts[-1]
                    print(f"   API key format: {api_key[:8]}...{api_key[-4:]} (length: {len(api_key)})")
                    if len(api_key) < 32:
                        print(f"   ⚠️  API key seems short for QuickNode (expected 32+ chars)")
                else:
                    print(f"   ❌ Invalid QuickNode URL structure")
            
            return False
        else:
            print(f"   ⚠️  HTTP {response.status_code}: {response.reason}")
            print(f"   Response: {response.text[:200]}")
            
    except Exception as e:
        print(f"   ❌ Request failed: {e}")
        return False
    
    # Test 2: More complex call that might reveal different auth behavior
    try:
        payload = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "getEpochInfo"
        }
        response = requests.post(url, headers=headers, json=payload, timeout=10)
        print(f"getEpochInfo: HTTP {response.status_code}")
        
        if response.status_code == 401:
            print(f"   🚨 CONSISTENT 401 ERROR - Authentication definitely broken")
            return False
        elif response.status_code == 200:
            result = response.json()
            if "result" in result:
                print(f"   ✅ Success - Current epoch: {result['result'].get('epoch', 'unknown')}")
                return True
    except Exception as e:
        print(f"   ❌ getEpochInfo failed: {e}")
    
    return False

def test_concurrent_auth_behavior(name, url):
    """Test if concurrent requests affect authentication"""
    print(f"\n=== Concurrent Auth Test for {name} ===")
    
    import concurrent.futures
    
    def make_request(thread_id):
        payload = {
            "jsonrpc": "2.0",
            "id": thread_id,
            "method": "getHealth"
        }
        try:
            response = requests.post(url, headers=headers, json=payload, timeout=10)
            return {
                'thread_id': thread_id,
                'status_code': response.status_code,
                'response_text': response.text[:100] if response.text else ''
            }
        except Exception as e:
            return {
                'thread_id': thread_id,
                'status_code': None,
                'error': str(e)
            }
    
    # Test with 4 concurrent requests (matching your script)
    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
        futures = [executor.submit(make_request, i) for i in range(4)]
        results = [future.result() for future in concurrent.futures.as_completed(futures)]
    
    auth_errors = sum(1 for r in results if r.get('status_code') == 401)
    success_count = sum(1 for r in results if r.get('status_code') == 200)
    
    print(f"Results: {success_count}/4 success, {auth_errors}/4 auth errors")
    
    if auth_errors > 0:
        print(f"🚨 Concurrent requests causing auth failures!")
        for result in results:
            if result.get('status_code') == 401:
                print(f"  Thread {result['thread_id']}: 401 - {result.get('response_text', '')}")

def main():
    """Main diagnostic function"""
    print("🔍 FIXED Solana RPC 401 Authentication Diagnostics")
    print("=" * 60)
    print(f"Timestamp: {datetime.now().isoformat()}")
    
    working_endpoints = []
    failing_endpoints = []
    
    # Test each RPC endpoint
    for name, url in RPC_ENDPOINTS:
        if url:  # Skip if URL is None/empty
            print(f"\n{'='*60}")
            
            # Step 1: Analyze API key
            has_api_key = analyze_api_key_in_url(name, url)
            
            # Step 2: Test endpoint
            endpoint_works = test_endpoint_with_detailed_401_analysis(name, url)
            
            if endpoint_works:
                working_endpoints.append((name, url))
                print(f"✅ {name} is WORKING")
            else:
                failing_endpoints.append((name, url))
                print(f"❌ {name} is FAILING")
                
            # Step 3: Test concurrent behavior if endpoint works
            if endpoint_works:
                test_concurrent_auth_behavior(name, url)
        else:
            print(f"\n⚠️  Skipping {name} - No URL provided")
    
    # Final summary and recommendations
    print(f"\n{'='*60}")
    print("🎯 FINAL DIAGNOSIS:")
    print(f"Working endpoints: {len(working_endpoints)}")
    print(f"Failing endpoints: {len(failing_endpoints)}")
    
    if failing_endpoints:
        print(f"\n❌ FAILING ENDPOINTS:")
        for name, url in failing_endpoints:
            print(f"  {name}: {get_rpc_url_prefix(url)}")
            
        print(f"\n💡 RECOMMENDED ACTIONS:")
        
        for name, url in failing_endpoints:
            if "quiknode.pro" in url:
                print(f"  For {name} (QuickNode):")
                print(f"    1. Verify your QuickNode API key is valid")
                print(f"    2. Check if your IP is whitelisted in QuickNode dashboard")
                print(f"    3. Verify the endpoint URL format is correct")
                print(f"    4. Check QuickNode billing/usage limits")
                
            elif "alchemy.com" in url:
                print(f"  For {name} (Alchemy):")
                print(f"    1. Verify your Alchemy API key is valid")
                print(f"    2. Check Alchemy dashboard for usage limits")
                print(f"    3. Verify the endpoint URL format (/v2/API_KEY)")
                print(f"    4. Check if the app is enabled for Solana")
    
    if working_endpoints:
        print(f"\n✅ WORKING ENDPOINTS:")
        for name, url in working_endpoints:
            print(f"  {name}: {get_rpc_url_prefix(url)}")
        print(f"\n  You can configure your script to only use working endpoints")
    
    print(f"\n🔧 FOR YOUR SCRIPT:")
    print(f"  Update the process_slot_data function to skip failing endpoints")
    print(f"  Consider implementing endpoint health checks before processing")
    print(f"  Add retry logic that switches to working endpoints on 401 errors")

if __name__ == "__main__":
    main()