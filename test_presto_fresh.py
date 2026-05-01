#!/usr/bin/env python3
"""Test Presto connection with fresh connection"""

import os
import sys
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

print(f"PRESTO_CATALOG={os.getenv('PRESTO_CATALOG')}")
print(f"PRESTO_SCHEMA={os.getenv('PRESTO_SCHEMA')}")
print(f"PRESTO_USER={os.getenv('PRESTO_USER')}")
print(f"PRESTO_USE_IAM={os.getenv('PRESTO_USE_IAM')}")

# Add current directory to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from affiliate_common.iam_token_manager import get_iam_token
import prestodb
import prestodb.auth

# Get IAM token
token = get_iam_token()
print(f"\nIAM Token obtained: {token[:50]}...")

# Create fresh connection with correct catalog
connection = prestodb.dbapi.connect(
    host=os.getenv('PRESTO_HOST'),
    port=int(os.getenv('PRESTO_PORT')),
    user=os.getenv('PRESTO_USER'),
    catalog=os.getenv('PRESTO_CATALOG'),  # affiliate_info
    schema=os.getenv('PRESTO_SCHEMA'),    # affiliate_junction
    http_scheme='https'
)

# Disable SSL verification
connection._http_session.verify = False

# Add Bearer token
original_request = connection._http_session.request
def request_with_bearer_token(method, url, **kwargs):
    kwargs.pop('auth', None)
    if 'headers' not in kwargs:
        kwargs['headers'] = {}
    kwargs['headers']['Authorization'] = f'Bearer {token}'
    if 'X-Presto-User' not in kwargs['headers']:
        kwargs['headers']['X-Presto-User'] = os.getenv('PRESTO_USER')
    return original_request(method, url, **kwargs)

connection._http_session.request = request_with_bearer_token

print(f"\nConnected to Presto at {os.getenv('PRESTO_HOST')}:{os.getenv('PRESTO_PORT')}")
print(f"Using catalog: {os.getenv('PRESTO_CATALOG')}")
print(f"Using schema: {os.getenv('PRESTO_SCHEMA')}")

# Try to execute a query
try:
    cursor = connection.cursor()
    print("\nExecuting: SHOW SCHEMAS")
    cursor.execute("SHOW SCHEMAS")
    result = cursor.fetchall()
    print(f"Success! Schemas: {result}")
except Exception as e:
    print(f"Error: {e}")
    import traceback
    traceback.print_exc()

# Made with Bob
