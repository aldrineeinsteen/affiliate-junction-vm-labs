#!/usr/bin/env python3
"""Debug script to see what headers are being sent to Presto"""

import os
import sys
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Add current directory to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from affiliate_common.iam_token_manager import get_iam_token
import prestodb
import prestodb.auth

# Get IAM token
token = get_iam_token()
print(f"IAM Token (first 50 chars): {token[:50]}...")

# Create connection
connection = prestodb.dbapi.connect(
    host=os.getenv('PRESTO_HOST'),
    port=int(os.getenv('PRESTO_PORT')),
    user=os.getenv('PRESTO_USER'),
    catalog=os.getenv('PRESTO_CATALOG'),
    schema=os.getenv('PRESTO_SCHEMA'),
    http_scheme='https'
)

# Disable SSL verification
connection._http_session.verify = False

# Wrap the request method to log headers and capture response
original_request = connection._http_session.request
def debug_request(method, url, **kwargs):
    print(f"\n=== Request Debug ===")
    print(f"Method: {method}")
    print(f"URL: {url}")
    print(f"Headers: {kwargs.get('headers', {})}")
    print(f"Auth: {kwargs.get('auth', 'None')}")
    
    # Remove any existing auth
    kwargs.pop('auth', None)
    
    # Add Bearer token and required headers
    if 'headers' not in kwargs:
        kwargs['headers'] = {}
    kwargs['headers']['Authorization'] = f'Bearer {token}'
    
    # Ensure X-Presto-User is set
    if 'X-Presto-User' not in kwargs['headers']:
        kwargs['headers']['X-Presto-User'] = os.getenv('PRESTO_USER')
    
    print(f"\nModified Headers: {kwargs['headers']}")
    print("===================\n")
    
    response = original_request(method, url, **kwargs)
    
    print(f"\n=== Response Debug ===")
    print(f"Status Code: {response.status_code}")
    print(f"Response Headers: {dict(response.headers)}")
    print(f"Response Body: {response.text[:500]}")  # First 500 chars
    print("===================\n")
    
    return response

connection._http_session.request = debug_request

# Try to execute a query
try:
    cursor = connection.cursor()
    cursor.execute("SELECT 1")
    result = cursor.fetchall()
    print(f"Success! Result: {result}")
except Exception as e:
    print(f"Error: {e}")
    import traceback
    traceback.print_exc()

# Made with Bob
