#!/usr/bin/env python3
"""
Detect the S3 bucket location for a watsonx.data catalog.
This script queries the catalog metadata to find the default storage location.
"""

import os
import sys
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Add current directory to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

def detect_bucket_location():
    """Detect the S3 bucket location from the catalog"""
    try:
        # Import after adding to path
        from web.presto_wrapper import presto_wrapper
        
        catalog = os.getenv('PRESTO_CATALOG', 'iceberg_data')
        schema_name = os.getenv('PRESTO_SCHEMA', 'affiliate_junction')
        
        print(f"Detecting bucket location for catalog: {catalog}")
        
        # Query to get catalog properties
        # This will show us the default location configured for the catalog
        query = f"SHOW CREATE SCHEMA system.metadata"
        
        try:
            # Try to get any existing schema to see the location pattern
            result = presto_wrapper.execute_query_simple(f"SHOW SCHEMAS FROM {catalog}")
            print(f"Available schemas in {catalog}: {result}")
            
            # If there are existing schemas, try to get their location
            if result and len(result) > 0:
                for schema_row in result:
                    schema = schema_row[0]
                    if schema not in ['information_schema', 'system']:
                        try:
                            # Get the location of an existing schema
                            create_stmt = presto_wrapper.execute_query_simple(
                                f"SHOW CREATE SCHEMA {catalog}.{schema}"
                            )
                            if create_stmt and len(create_stmt) > 0:
                                create_sql = create_stmt[0][0]
                                # Extract location from CREATE SCHEMA statement
                                if 'location' in create_sql.lower():
                                    import re
                                    match = re.search(r"location\s*=\s*'([^']+)'", create_sql, re.IGNORECASE)
                                    if match:
                                        existing_location = match.group(1)
                                        # Extract bucket name and construct new path
                                        bucket_match = re.match(r'(s3a://[^/]+)/', existing_location)
                                        if bucket_match:
                                            bucket_base = bucket_match.group(1)
                                            new_location = f"{bucket_base}/{schema_name}"
                                            print(f"\nDetected bucket: {bucket_base}")
                                            print(f"Suggested schema location: {new_location}")
                                            return new_location
                        except Exception as e:
                            # Schema might not have location, continue
                            pass
            
            # If we couldn't detect from existing schemas, provide guidance
            print("\nCould not automatically detect bucket location.")
            print("Please check the watsonx.data UI:")
            print("1. Go to Infrastructure manager → Catalogs")
            print(f"2. Click on '{catalog}' catalog")
            print("3. Check the associated storage bucket")
            print(f"4. The location should be: s3a://<bucket-name>/{schema_name}")
            return None
            
        except Exception as e:
            print(f"Error querying catalog: {e}")
            print("\nPlease manually specify the schema location in .env:")
            print(f"PRESTO_SCHEMA_LOCATION=s3a://<your-bucket-name>/{schema_name}")
            return None
            
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
        return None

if __name__ == "__main__":
    location = detect_bucket_location()
    if location:
        print(f"\nTo use this location, add to your .env file:")
        print(f"PRESTO_SCHEMA_LOCATION={location}")
        sys.exit(0)
    else:
        sys.exit(1)

# Made with Bob
