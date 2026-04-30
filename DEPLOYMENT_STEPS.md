# Complete Deployment Steps

## Overview

This guide provides the exact order of operations for deploying the Affiliate Junction demo with watsonx.data Enterprise.

## Prerequisites

- IBM Cloud account with access to watsonx.data
- IBM Cloud CLI installed on your local machine
- SSH client

---

## Part 1: Provision Infrastructure (Local Machine)

### Step 1: Provision IBM Cloud VM

```bash
# On your local machine
cd /path/to/affiliate-junction-vm-labs
./setup-cloud.sh
```

**What it does:**
- Creates VPC, subnet, security group
- Provisions RHEL 9 VM (2 vCPU, 8GB RAM)
- Generates SSH key
- Assigns floating IP

**Output:** SSH command to connect to VM

---

## Part 2: Provision watsonx.data (IBM Cloud Console)

### Step 2: Create watsonx.data Instance

1. Go to IBM Cloud Console: https://cloud.ibm.com
2. Navigate to **Catalog → Databases → watsonx.data**
3. Create new instance:
   - **Region**: eu-de (same as VM)
   - **Plan**: Choose appropriate plan
   - **Resource Group**: Same as VM (itz-*)
4. Wait for provisioning (5-10 minutes)

### Step 3: Get Presto Connection Details

1. Open watsonx.data console: https://eu-de.lakehouse.cloud.ibm.com
2. Navigate to **Infrastructure → Engines**
3. Click on Presto engine
4. Click **Connection** or **Details** button
5. **Copy these values** (you'll need them soon):
   - ✅ Hostname (e.g., `747e742b-xxx.lakehouse.ibmappdomain.cloud`)
   - ✅ Port (e.g., `31138`)
   - ✅ Username (e.g., `ibmlhapikey_student_xxx@techzone.ibm.com`)
   - ✅ Password/API Key
   - ✅ SSL Certificate (full chain, including BEGIN/END lines)

**Save these to a text file on your local machine for easy copy/paste.**

---

## Part 3: Configure VM (On VM via SSH)

### Step 4: Connect to VM

```bash
# On your local machine
ssh -i ~/.ssh/affiliate-junction-key root@<PUBLIC_IP>
```

Replace `<PUBLIC_IP>` with the IP from setup-cloud.sh output.

### Step 5: Clone Repository

```bash
# On the VM
git clone https://github.com/aldrineeinsteen/affiliate-junction-vm-labs.git
cd affiliate-junction-vm-labs
```

### Step 6: Configure Presto Connection

**IMPORTANT: Do this BEFORE running setup.sh**

Create a file with your Presto configuration:

```bash
# On the VM
nano presto-config.txt
```

Paste this content (replace with YOUR values from Step 3):

```
HOSTNAME=747e742b-170a-4676-8bd7-8a0b400a9810.d4mn7ovf0r8q7913kbkg.lakehouse.ibmappdomain.cloud
PORT=31138
USERNAME=ibmlhapikey_student_69f34f752f@techzone.ibm.com
PASSWORD=your_api_key_here
CATALOG=iceberg_data
SCHEMA=affiliate_junction
```

Save and exit (Ctrl+X, Y, Enter).

Now create the certificate file:

```bash
# On the VM
sudo mkdir -p /certs
sudo nano /certs/presto.crt
```

Paste the **full SSL certificate chain** (all 3 certificates from Step 3).

Save and exit (Ctrl+X, Y, Enter).

### Step 7: Update .env File

**BEFORE running setup.sh**, manually update the .env file:

```bash
# On the VM
cp env-sample .env
nano .env
```

Update these lines with YOUR values from presto-config.txt:

```bash
# Find these lines and update them:
PRESTO_HOST=747e742b-170a-4676-8bd7-8a0b400a9810.d4mn7ovf0r8q7913kbkg.lakehouse.ibmappdomain.cloud
PRESTO_PORT=31138
PRESTO_USER=ibmlhapikey_student_69f34f752f@techzone.ibm.com
PRESTO_PASSWD=your_api_key_here
PRESTO_CATALOG=iceberg_data
PRESTO_SCHEMA=affiliate_junction
```

Save and exit (Ctrl+X, Y, Enter).

### Step 8: Run Setup Script

Now run the setup script:

```bash
# On the VM
./setup.sh
```

**What it does:**
- Installs HCD (Cassandra) locally
- Starts HCD with `-R` flag (allows root)
- Creates Python virtual environment
- Installs dependencies
- Initializes HCD schema
- Configures and starts systemd services
- **Uses the .env file you just configured**

**Duration:** 5-10 minutes

---

## Part 4: Verify Deployment

### Step 9: Check Services

```bash
# On the VM
sudo systemctl status generate_traffic
sudo systemctl status hcd_to_presto
sudo systemctl status uvicorn
```

All should show "active (running)".

### Step 10: Test Presto Connection

```bash
# On the VM
source .venv/bin/activate
python3 -c "from web.presto_wrapper import presto_wrapper; print(presto_wrapper.execute_query_simple('SELECT 1'))"
```

Should return: `[(1,)]`

### Step 11: Access Web UI

```bash
# Get your public IP
curl ifconfig.me
```

Open browser: `http://<PUBLIC_IP>:10000`

**Login:**
- Username: `watsonx`
- Password: `watsonx.data`

---

## Summary: Which Files to Configure

### Files You Must Configure (BEFORE setup.sh):

1. **`.env`** - Main configuration file
   - Copy from `env-sample`
   - Update PRESTO_* variables with your watsonx.data details
   - Location: `~/affiliate-junction-vm-labs/.env`

2. **`/certs/presto.crt`** - SSL certificate
   - Create directory: `sudo mkdir -p /certs`
   - Paste full certificate chain from watsonx.data console
   - Location: `/certs/presto.crt`

### Files You Don't Need to Touch:

- ❌ `setup.sh` - Already fixed, no changes needed
- ❌ `env-sample` - Template only, don't modify
- ❌ `hcd_schema.cql` - Schema file, no changes needed
- ❌ `presto_schema.sql` - Schema file, no changes needed
- ❌ `*.service` files - Auto-configured by setup.sh

---

## Alternative: Use configure-presto.sh (Interactive)

If you prefer an interactive approach instead of manual file editing:

```bash
# On the VM, after cloning repository
cd affiliate-junction-vm-labs
chmod +x configure-presto.sh
./configure-presto.sh
```

This script will:
1. Prompt for all connection details
2. Create/update `.env` file
3. Save certificate to `/certs/presto.crt`
4. Validate configuration

Then run `./setup.sh` as normal.

---

## Troubleshooting

### HCD Won't Start

```bash
# Check Java version (must be 11)
java -version

# Try starting manually
cd ~/affiliate-junction-vm-labs
./hcd-1.2.3/bin/hcd cassandra -R -f
```

### Presto Connection Fails

```bash
# Test DNS resolution
nslookup <YOUR_PRESTO_HOSTNAME>

# Test connectivity
curl -k https://<YOUR_PRESTO_HOSTNAME>:<YOUR_PORT>/v1/info

# Check certificate
openssl x509 -in /certs/presto.crt -text -noout | head -20
```

### Services Won't Start

```bash
# Check logs
journalctl -u hcd_to_presto -n 50
journalctl -u generate_traffic -n 50

# Restart services
sudo systemctl restart generate_traffic hcd_to_presto uvicorn
```

---

## Quick Reference: Order of Operations

```
1. Local Machine: ./setup-cloud.sh          → Provision VM
2. IBM Cloud Console: Create watsonx.data   → Get Presto details
3. VM: git clone                            → Get code
4. VM: Create .env file                     → Configure Presto
5. VM: Create /certs/presto.crt            → Save certificate
6. VM: ./setup.sh                          → Install & start everything
7. Browser: http://<IP>:10000              → Access UI
```

**Key Point:** Configure `.env` and certificate BEFORE running `setup.sh`!