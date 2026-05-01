#!/usr/bin/env python3
"""
Automated watsonx.data Infrastructure Setup

This script automates the creation of:
1. IBM Cloud Object Storage bucket (if needed)
2. watsonx.data catalog registration
3. Presto engine association
4. Access permissions for API key user

Requires:
- IBM_CLOUD_API_KEY with admin permissions
- WATSONX_DATA_INSTANCE_ID
- WATSONX_DATA_REGION
"""

import os
import sys
import json
import time
import requests
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

class WatsonxDataSetup:
    def __init__(self):
        self.api_key = os.getenv('IBM_CLOUD_API_KEY')
        self.instance_id = os.getenv('WATSONX_DATA_INSTANCE_ID')
        self.region = os.getenv('WATSONX_DATA_REGION', 'eu-de')
        self.presto_user = os.getenv('PRESTO_USER')
        
        if not self.api_key:
            print("ERROR: IBM_CLOUD_API_KEY not set in .env")
            sys.exit(1)
        
        if not self.instance_id:
            print("ERROR: WATSONX_DATA_INSTANCE_ID not set in .env")
            print("Get this from: watsonx.data console → Instance details")
            sys.exit(1)
        
        self.iam_token = None
        self.base_url = f"https://api.{self.region}.lakehouse.cloud.ibm.com/lakehouse/api/v2"
        
    def get_iam_token(self):
        """Get IAM Bearer token"""
        print("Getting IAM token...")
        response = requests.post(
            'https://iam.cloud.ibm.com/identity/token',
            headers={'Content-Type': 'application/x-www-form-urlencoded'},
            data={
                'grant_type': 'urn:ibm:params:oauth:grant-type:apikey',
                'apikey': self.api_key
            }
        )
        
        if response.status_code != 200:
            print(f"ERROR: Failed to get IAM token: {response.status_code}")
            print(response.text)
            sys.exit(1)
        
        self.iam_token = response.json()['access_token']
        print("✓ IAM token obtained")
        return self.iam_token
    
    def get_headers(self):
        """Get request headers with auth"""
        if not self.iam_token:
            self.get_iam_token()
        
        return {
            'Authorization': f'Bearer {self.iam_token}',
            'Content-Type': 'application/json',
            'AuthInstanceId': self.instance_id
        }
    
    def list_buckets(self):
        """List registered storage buckets"""
        print("\nListing registered storage buckets...")
        response = requests.get(
            f"{self.base_url}/buckets",
            headers=self.get_headers()
        )
        
        if response.status_code == 200:
            buckets = response.json().get('buckets', [])
            print(f"✓ Found {len(buckets)} registered buckets")
            for bucket in buckets:
                print(f"  - {bucket.get('bucket_display_name')}: {bucket.get('bucket_id')}")
            return buckets
        else:
            print(f"✗ Failed to list buckets: {response.status_code}")
            print(response.text)
            return []
    
    def register_cos_bucket(self, bucket_name, cos_endpoint, access_key, secret_key):
        """Register IBM Cloud Object Storage bucket"""
        print(f"\nRegistering COS bucket: {bucket_name}...")
        
        payload = {
            "bucket_display_name": bucket_name,
            "bucket_type": "ibm_cos",
            "endpoint": cos_endpoint,
            "access_key": access_key,
            "secret_key": secret_key,
            "description": "Affiliate Junction Demo Storage"
        }
        
        response = requests.post(
            f"{self.base_url}/buckets",
            headers=self.get_headers(),
            json=payload
        )
        
        if response.status_code in [200, 201]:
            bucket_data = response.json()
            print(f"✓ Bucket registered: {bucket_data.get('bucket_id')}")
            return bucket_data
        else:
            print(f"✗ Failed to register bucket: {response.status_code}")
            print(response.text)
            return None
    
    def list_catalogs(self):
        """List registered catalogs"""
        print("\nListing registered catalogs...")
        response = requests.get(
            f"{self.base_url}/catalogs",
            headers=self.get_headers()
        )
        
        if response.status_code == 200:
            catalogs = response.json().get('catalogs', [])
            print(f"✓ Found {len(catalogs)} catalogs")
            for catalog in catalogs:
                print(f"  - {catalog.get('catalog_name')}: {catalog.get('catalog_type')}")
            return catalogs
        else:
            print(f"✗ Failed to list catalogs: {response.status_code}")
            print(response.text)
            return []
    
    def create_catalog(self, catalog_name, bucket_id, engine_id):
        """Create Iceberg catalog"""
        print(f"\nCreating catalog: {catalog_name}...")
        
        payload = {
            "catalog_name": catalog_name,
            "catalog_type": "iceberg",
            "bucket_id": bucket_id,
            "description": "Affiliate Junction Demo Catalog",
            "managed_by": "ibm",
            "catalog_tags": ["demo", "affiliate-junction"]
        }
        
        response = requests.post(
            f"{self.base_url}/catalogs",
            headers=self.get_headers(),
            json=payload
        )
        
        if response.status_code in [200, 201]:
            catalog_data = response.json()
            print(f"✓ Catalog created: {catalog_data.get('catalog_id')}")
            
            # Associate with engine
            if engine_id:
                self.associate_catalog_with_engine(catalog_data.get('catalog_id'), engine_id)
            
            return catalog_data
        else:
            print(f"✗ Failed to create catalog: {response.status_code}")
            print(response.text)
            return None
    
    def list_engines(self):
        """List Presto engines"""
        print("\nListing Presto engines...")
        response = requests.get(
            f"{self.base_url}/engines",
            headers=self.get_headers()
        )
        
        if response.status_code == 200:
            engines = response.json().get('engines', [])
            print(f"✓ Found {len(engines)} engines")
            for engine in engines:
                print(f"  - {engine.get('engine_display_name')}: {engine.get('engine_id')}")
            return engines
        else:
            print(f"✗ Failed to list engines: {response.status_code}")
            print(response.text)
            return []
    
    def associate_catalog_with_engine(self, catalog_id, engine_id):
        """Associate catalog with Presto engine"""
        print(f"\nAssociating catalog {catalog_id} with engine {engine_id}...")
        
        payload = {
            "catalog_id": catalog_id,
            "engine_id": engine_id
        }
        
        response = requests.post(
            f"{self.base_url}/catalogs/{catalog_id}/engines",
            headers=self.get_headers(),
            json=payload
        )
        
        if response.status_code in [200, 201, 204]:
            print("✓ Catalog associated with engine")
            return True
        else:
            print(f"✗ Failed to associate catalog: {response.status_code}")
            print(response.text)
            return False
    
    def grant_catalog_permissions(self, catalog_id, user_id):
        """Grant permissions to user on catalog"""
        print(f"\nGranting permissions to {user_id} on catalog {catalog_id}...")
        
        payload = {
            "catalog_id": catalog_id,
            "principal": user_id,
            "principal_type": "user",
            "permissions": ["USE", "CREATE", "SELECT", "INSERT", "UPDATE", "DELETE", "DROP"]
        }
        
        response = requests.post(
            f"{self.base_url}/access/catalogs/{catalog_id}/policies",
            headers=self.get_headers(),
            json=payload
        )
        
        if response.status_code in [200, 201, 204]:
            print("✓ Permissions granted")
            return True
        else:
            print(f"✗ Failed to grant permissions: {response.status_code}")
            print(response.text)
            return False
    
    def setup_infrastructure(self):
        """Main setup workflow"""
        print("=" * 80)
        print("watsonx.data Infrastructure Automation")
        print("=" * 80)
        
        # Get IAM token
        self.get_iam_token()
        
        # List existing resources
        buckets = self.list_buckets()
        catalogs = self.list_catalogs()
        engines = self.list_engines()
        
        if not engines:
            print("\n✗ No Presto engines found. Please provision an engine first.")
            print("Go to: watsonx.data console → Infrastructure → Engines → Add engine")
            return False
        
        engine_id = engines[0].get('engine_id')
        print(f"\nUsing engine: {engines[0].get('engine_display_name')} ({engine_id})")
        
        # Check if affiliate_info catalog exists
        affiliate_catalog = next((c for c in catalogs if c.get('catalog_name') == 'affiliate_info'), None)
        
        if affiliate_catalog:
            print(f"\n✓ Catalog 'affiliate_info' already exists")
            catalog_id = affiliate_catalog.get('catalog_id')
        else:
            # Need to create catalog
            if not buckets:
                print("\n✗ No storage buckets registered.")
                print("Please register a COS bucket first or provide credentials.")
                return False
            
            bucket_id = buckets[0].get('bucket_id')
            print(f"\nUsing bucket: {buckets[0].get('bucket_display_name')} ({bucket_id})")
            
            catalog_data = self.create_catalog('affiliate_info', bucket_id, engine_id)
            if not catalog_data:
                return False
            catalog_id = catalog_data.get('catalog_id')
        
        # Grant permissions to API key user
        if self.presto_user:
            self.grant_catalog_permissions(catalog_id, self.presto_user)
        
        print("\n" + "=" * 80)
        print("✓ Infrastructure setup complete!")
        print("=" * 80)
        print(f"\nCatalog: affiliate_info")
        print(f"Catalog ID: {catalog_id}")
        print(f"Engine: {engines[0].get('engine_display_name')}")
        print(f"\nYou can now run: sudo systemctl restart hcd_to_presto")
        
        return True

def main():
    setup = WatsonxDataSetup()
    success = setup.setup_infrastructure()
    sys.exit(0 if success else 1)

if __name__ == '__main__':
    main()

# Made with Bob
