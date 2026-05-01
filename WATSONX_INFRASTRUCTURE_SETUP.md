# watsonx.data Infrastructure Automation

This guide explains how to automatically set up watsonx.data infrastructure for the Affiliate Junction demo.

## Overview

The `setup_watsonx_infrastructure.py` script automates:
1. Listing existing storage buckets and catalogs
2. Creating the `affiliate_info` catalog (if it doesn't exist)
3. Associating the catalog with a Presto engine
4. Granting permissions to the API key user

## Prerequisites

### 1. IBM Cloud API Key with Admin Permissions

Your API key needs these permissions:
- **watsonx.data Administrator** role
- **Cloud Object Storage Writer** role (if creating buckets)

To create an API key with proper permissions:
1. Go to IBM Cloud console → Manage → Access (IAM)
2. Click **API keys** → **Create**
3. Name it (e.g., "watsonx-data-admin")
4. Assign these roles:
   - Service: watsonx.data → Role: Administrator
   - Service: Cloud Object Storage → Role: Writer
5. Copy the API key (you won't see it again!)

### 2. watsonx.data Instance ID

Get your instance ID:
1. Go to watsonx.data console
2. Click on your instance name
3. Go to **Instance details** or **Settings**
4. Copy the **Instance ID** (format: `xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx`)

### 3. Presto Engine

You must have at least one Presto engine provisioned:
1. Go to watsonx.data console → Infrastructure → Engines
2. If no engine exists, click **Add engine** → **Presto**
3. Choose size and configuration
4. Wait for provisioning to complete

## Configuration

Add these variables to your `.env` file:

```bash
# Required: Admin API key (different from the user API key)
IBM_CLOUD_API_KEY=your_admin_api_key_here

# Required: watsonx.data instance ID
WATSONX_DATA_INSTANCE_ID=xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx

# Optional: Region (default: eu-de)
WATSONX_DATA_REGION=eu-de

# Required: User that will access the catalog
PRESTO_USER=ibmlhapikey_student_xxx@techzone.ibm.com
```

## Usage

### Automated Setup (Recommended)

```bash
# On the server
cd /root/affiliate-junction-vm-labs
source .venv/bin/activate

# Run the infrastructure setup
python3 setup_watsonx_infrastructure.py
```

The script will:
1. ✓ Get IAM token
2. ✓ List existing resources (buckets, catalogs, engines)
3. ✓ Create `affiliate_info` catalog (if needed)
4. ✓ Associate catalog with Presto engine
5. ✓ Grant permissions to your API key user

### Manual Verification

After running the script, verify in the watsonx.data console:

1. **Infrastructure → Catalogs**
   - Should see `affiliate_info` catalog
   - Status should be "Active"

2. **Infrastructure → Engines**
   - Your Presto engine should show `affiliate_info` in associated catalogs

3. **Access Control**
   - User `ibmlhapikey_student_xxx@techzone.ibm.com` should have permissions on `affiliate_info`

### Test the Setup

```bash
# Test if the catalog is accessible
curl -k -X POST "https://${PRESTO_HOST}:${PRESTO_PORT}/v1/statement" \
  -H "Authorization: Bearer $(python3 -c 'from affiliate_common.iam_token_manager import get_iam_token; print(get_iam_token())')" \
  -H "X-Presto-User: ${PRESTO_USER}" \
  -d "SHOW CATALOGS"

# Should now include 'affiliate_info' in the results
```

## Troubleshooting

### Error: "IBM_CLOUD_API_KEY not set"
- Add `IBM_CLOUD_API_KEY` to your `.env` file
- Make sure it's an admin API key, not the user API key

### Error: "WATSONX_DATA_INSTANCE_ID not set"
- Get instance ID from watsonx.data console → Instance details
- Add to `.env` file

### Error: "No Presto engines found"
- Provision a Presto engine in the watsonx.data console first
- Go to: Infrastructure → Engines → Add engine

### Error: "Failed to create catalog: 403"
- Your API key doesn't have admin permissions
- Create a new API key with Administrator role

### Error: "Failed to grant permissions"
- The user ID might be incorrect
- Check `PRESTO_USER` in `.env` matches the API key username

## API Reference

The script uses the watsonx.data REST API:
- Base URL: `https://api.{region}.lakehouse.cloud.ibm.com/lakehouse/api/v2`
- Authentication: IBM Cloud IAM Bearer token
- Documentation: https://cloud.ibm.com/apidocs/watsonxdata

### Key Endpoints Used

- `GET /buckets` - List storage buckets
- `POST /buckets` - Register new bucket
- `GET /catalogs` - List catalogs
- `POST /catalogs` - Create catalog
- `GET /engines` - List Presto engines
- `POST /catalogs/{id}/engines` - Associate catalog with engine
- `POST /access/catalogs/{id}/policies` - Grant permissions

## Integration with Setup Script

To integrate with the main setup script, add to `setup.sh`:

```bash
# After configuring Presto connection
if [ "$PRESTO_USE_IAM" = "true" ] && [ -n "$WATSONX_DATA_INSTANCE_ID" ]; then
    echo "Setting up watsonx.data infrastructure..."
    python3 setup_watsonx_infrastructure.py
    if [ $? -eq 0 ]; then
        echo "✓ Infrastructure setup complete"
    else
        echo "✗ Infrastructure setup failed"
        exit 1
    fi
fi
```

## For Students

Once the infrastructure is set up by an administrator, students only need:
1. The user API key (not admin)
2. The Presto connection details
3. Run `./setup.sh` - everything else is automated!

The infrastructure setup is a **one-time operation** per watsonx.data instance.