#!/usr/bin/env python3
"""
Debug script to test IAM authentication and permissions
"""
import os
import sys
import requests
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

def get_iam_token():
    """Get IAM token from API key"""
    api_key = os.getenv('IBM_CLOUD_API_KEY')
    if not api_key:
        print("ERROR: IBM_CLOUD_API_KEY not set in .env")
        sys.exit(1)
    
    print(f"Getting IAM token for API key: {api_key[:20]}...")
    
    response = requests.post(
        'https://iam.cloud.ibm.com/identity/token',
        headers={'Content-Type': 'application/x-www-form-urlencoded'},
        data={
            'grant_type': 'urn:ibm:params:oauth:grant-type:apikey',
            'apikey': api_key
        }
    )
    
    if response.status_code != 200:
        print(f"ERROR: Failed to get IAM token: {response.status_code}")
        print(response.text)
        sys.exit(1)
    
    token_data = response.json()
    print(f"✓ Got IAM token (expires in {token_data.get('expires_in')} seconds)")
    return token_data['access_token']

def test_presto_query(token, query):
    """Test a Presto query with IAM token"""
    host = os.getenv('PRESTO_HOST')
    port = os.getenv('PRESTO_PORT')
    user = os.getenv('PRESTO_USER')
    catalog = os.getenv('PRESTO_CATALOG')
    
    print(f"\nTesting query: {query}")
    print(f"Host: {host}:{port}")
    print(f"User: {user}")
    print(f"Catalog: {catalog}")
    
    url = f"https://{host}:{port}/v1/statement"
    headers = {
        'Authorization': f'Bearer {token}',
        'X-Presto-User': user,
        'X-Presto-Catalog': catalog,
        'X-Presto-Schema': 'default'
    }
    
    print(f"\nRequest headers:")
    for key, value in headers.items():
        if key == 'Authorization':
            print(f"  {key}: Bearer {value[:20]}...")
        else:
            print(f"  {key}: {value}")
    
    response = requests.post(
        url,
        headers=headers,
        data=query,
        verify=False
    )
    
    print(f"\nResponse status: {response.status_code}")
    print(f"Response headers:")
    for key, value in response.headers.items():
        print(f"  {key}: {value}")
    
    if response.status_code == 200:
        result = response.json()
        print(f"\n✓ Query submitted successfully")
        print(f"Query ID: {result.get('id')}")
        print(f"Info URI: {result.get('infoUri')}")
        
        # Check query status
        if 'nextUri' in result:
            print(f"\nChecking query status...")
            status_response = requests.get(
                result['nextUri'],
                headers=headers,
                verify=False
            )
            if status_response.status_code == 200:
                status_data = status_response.json()
                print(f"Query state: {status_data.get('stats', {}).get('state')}")
                if 'error' in status_data:
                    print(f"ERROR: {status_data['error']}")
                    return False
                return True
    else:
        print(f"\n✗ Query failed")
        print(f"Response body: {response.text}")
        return False
    
    return True

def main():
    print("=" * 80)
    print("IAM Authentication and Permissions Debug Script")
    print("=" * 80)
    
    # Get IAM token
    token = get_iam_token()
    
    # Test 1: Simple SHOW CATALOGS query
    print("\n" + "=" * 80)
    print("Test 1: SHOW CATALOGS")
    print("=" * 80)
    if not test_presto_query(token, "SHOW CATALOGS"):
        print("\n✗ Failed to list catalogs - this is a basic permission issue")
        sys.exit(1)
    
    # Test 2: USE catalog
    print("\n" + "=" * 80)
    print("Test 2: USE CATALOG")
    print("=" * 80)
    catalog = os.getenv('PRESTO_CATALOG')
    if not test_presto_query(token, f"USE {catalog}"):
        print(f"\n✗ Failed to USE catalog {catalog} - permission denied")
        print("\nThis means the API key doesn't have USE permission on the catalog.")
        print("You need to grant permissions in watsonx.data console:")
        print(f"1. Go to Access Control")
        print(f"2. Find catalog: {catalog}")
        print(f"3. Grant permissions to user: {os.getenv('PRESTO_USER')}")
        print(f"4. Required permissions: USE, CREATE, SELECT, INSERT, UPDATE, DELETE")
        sys.exit(1)
    
    # Test 3: SHOW SCHEMAS
    print("\n" + "=" * 80)
    print("Test 3: SHOW SCHEMAS")
    print("=" * 80)
    if not test_presto_query(token, f"SHOW SCHEMAS FROM {catalog}"):
        print(f"\n✗ Failed to list schemas in {catalog}")
        sys.exit(1)
    
    print("\n" + "=" * 80)
    print("✓ All tests passed! IAM authentication is working correctly.")
    print("=" * 80)

if __name__ == '__main__':
    main()

# Made with Bob
