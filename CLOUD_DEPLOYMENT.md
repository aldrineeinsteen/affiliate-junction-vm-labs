# IBM Cloud Deployment Guide

## Overview

This guide covers deploying the Affiliate Junction demo to IBM Cloud. The demo supports two Presto deployment models:

1. **watsonx.data Developer Edition** (Local) - Original design, Presto runs locally
2. **watsonx.data Enterprise** (Cloud) - Presto as managed IBM Cloud service

This guide focuses on **IBM Cloud deployment with watsonx.data Enterprise**.

This guide provides detailed instructions for deploying the Affiliate Junction demo application on IBM Cloud using automated provisioning scripts.

## Table of Contents

- [Overview](#overview)
- [Prerequisites](#prerequisites)
- [Quick Start](#quick-start)
- [Detailed Setup](#detailed-setup)
- [Accessing the Application](#accessing-the-application)
- [Troubleshooting](#troubleshooting)
- [Cleanup](#cleanup)
- [Advanced Configuration](#advanced-configuration)

## Overview

The IBM Cloud deployment automates the provisioning of a RHEL 9 virtual machine with all necessary networking and security configurations to run the Affiliate Junction demo application.

### ⚠️ Critical Prerequisite: watsonx.data Components Required

**This demo requires watsonx.data components (HCD and Presto) to be pre-installed.** The automated scripts provision infrastructure and install Python dependencies, but **do not install watsonx.data components**.

#### Recommended Deployment Approaches

1. **IBM TechZone watsonx.data Developer Edition** (Recommended)
   - Pre-configured with HCD (Cassandra) and Presto
   - Collection: https://techzone.ibm.com/collection/ibm-watsonxdata-developer-base-image--hcd-cassandra
   - Clone this repository on the TechZone environment and run `./setup.sh`

2. **Custom watsonx.data Installation**
   - Install watsonx.data Developer Edition on your IBM Cloud VM first
   - Then clone this repository and run `./setup.sh`

3. **Standalone Deployment** (Limited - Infrastructure Testing Only)
   - The scripts will provision VM and Python environment
   - **Services will fail without HCD/Presto**
   - Useful only for testing infrastructure provisioning

### What Gets Provisioned

- **VM Instance**: RHEL 9 with 2 vCPUs and 8GB RAM (bx2-2x8 profile)
- **Networking**: VPC, subnet, and floating IP for external access
- **Security Group**: Configured with rules for required ports
- **SSH Key**: Generated and uploaded for secure access

### Required Ports

| Port  | Service           | Description                    |
|-------|-------------------|--------------------------------|
| 22    | SSH               | Remote access to VM            |
| 9042  | HCD/Cassandra     | Database access                |
| 8080  | Presto Console    | Presto web interface           |
| 8443  | Presto HTTPS      | Presto secure API              |
## Architecture Models

### Model 1: watsonx.data Developer Edition (Local)

**Original Design** - All components run on the same VM:

```
┌─────────────────────────────────────────────────────────┐
│ IBM Cloud VM (RHEL 9)                                   │
│                                                         │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐ │
│  │ HCD          │  │ Presto       │  │ Web UI       │ │
│  │ (Cassandra)  │  │ (Local)      │  │ (Port 10000) │ │
│  │ Port 9042    │  │ Port 8443    │  │              │ │
│  └──────────────┘  └──────────────┘  └──────────────┘ │
│         ▲                 ▲                  ▲          │
│         │                 │                  │          │
│         └─────────────────┴──────────────────┘          │
│                  Python Services                        │
│         (generate_traffic, hcd_to_presto, etc.)        │
└─────────────────────────────────────────────────────────┘
```

**Setup:**
- Use TechZone watsonx.data Developer Edition
- Or install watsonx.data Developer Edition manually
- Presto hostname: `ibm-lh-presto-svc` (added to /etc/hosts)
- Certificates in `/certs/` directory
- Credentials in `/certs/passwords` file

### Model 2: watsonx.data Enterprise (Cloud)

**IBM Cloud Deployment** - Presto as managed service:

```
┌─────────────────────────────────────┐
│ IBM Cloud VM (RHEL 9)               │
│                                     │
│  ┌──────────────┐  ┌──────────────┐│
│  │ HCD          │  │ Web UI       ││
│  │ (Cassandra)  │  │ (Port 10000) ││
│  │ Port 9042    │  │              ││
│  └──────────────┘  └──────────────┘│
│         ▲                 ▲         │
│         │                 │         │
│         └─────────────────┘         │
│          Python Services            │
└─────────────────┬───────────────────┘
                  │
                  │ HTTPS (Port 8443)
                  ▼
┌─────────────────────────────────────┐
│ watsonx.data Enterprise (IBM Cloud) │
│                                     │
│  ┌──────────────────────────────┐  │
│  │ Presto Engine (Managed)      │  │
│  │ c-wxdata-xxx.lakehouse...    │  │
│  │ Port 8443                    │  │
│  └──────────────────────────────┘  │
└─────────────────────────────────────┘
```

**Setup:**
- Provision watsonx.data instance in IBM Cloud
- Use `configure-presto.sh` to connect VM to cloud Presto
- Presto hostname: Cloud endpoint (e.g., `c-wxdata-xxx.lakehouse.cloud.ibm.com`)
- Certificate downloaded from watsonx.data console
- Credentials from TechZone or console

**This guide covers Model 2 (watsonx.data Enterprise).**

| 10000 | Web UI            | Application web interface      |

## Prerequisites

### 1. IBM Cloud Account

You need an active IBM Cloud account with:
- Access to a resource group starting with `itz-`
- Permissions to create VPC resources
- Permissions to create virtual server instances

### 2. IBM Cloud CLI

Install the IBM Cloud CLI:

```bash
# macOS
curl -fsSL https://clis.cloud.ibm.com/install/osx | sh

# Linux
curl -fsSL https://clis.cloud.ibm.com/install/linux | sh

# Windows
# Download from: https://github.com/IBM-Cloud/ibm-cloud-cli-release/releases
```

Verify installation:
```bash
ibmcloud --version
```

### 3. Authentication

Log in to IBM Cloud:

```bash
# Interactive login with SSO
ibmcloud login --sso

# Or with API key
ibmcloud login --apikey @/path/to/apikey.json
```

### 4. Required Tools

The following tools should be available on your local machine:
- `jq` - JSON processor
- `curl` - HTTP client
- `ssh` - SSH client

Install on macOS:
```bash
brew install jq curl
```

Install on Linux:
```bash
# Ubuntu/Debian
sudo apt-get install jq curl openssh-client

# RHEL/CentOS
sudo yum install jq curl openssh-clients
```

## Quick Start

### 1. Clone the Repository

```bash
git clone https://github.ibm.com/Data-Labs/affiliate-junction-demo
cd affiliate-junction-demo
```

### 2. Run the Setup Script

```bash
./setup-cloud.sh
```

The script will:
1. Detect your IBM Cloud configuration
2. Create necessary SSH keys
3. Configure security groups
4. Provision the VM instance
5. Display connection information

### 3. Connect to the VM

Use the SSH command provided by the setup script:

```bash
ssh -i ~/.ssh/affiliate-junction-key root@<PUBLIC_IP>
```

### 4. Setup the Application

Once connected to the VM:

```bash
# Clone the repository
git clone https://github.ibm.com/Data-Labs/affiliate-junction-demo
cd affiliate-junction-demo

# Run the setup script (installs HCD locally)
./setup.sh
```

The setup process takes approximately 5-10 minutes and installs:
- HCD (Cassandra) locally on the VM
- Python virtual environment and dependencies
- Systemd services for data generation and ETL

### 5. Configure Presto Connection (watsonx.data Enterprise)

**For watsonx.data Enterprise (Cloud Presto)**, configure the connection:

```bash
# Run the Presto configuration helper
./configure-presto.sh
```

You'll be prompted for:
- **Presto Hostname**: From watsonx.data console (e.g., `c-wxdata-itz-wxd-xxx.lakehouse.cloud.ibm.com`)
- **Presto Port**: Usually `8443`
- **Username**: Usually `ibmlhadmin`
- **Password**: From TechZone reservation or console
- **Catalog**: Usually `iceberg_data`
- **Schema**: Usually `affiliate_junction`

The script will:
1. Update `/etc/hosts` to map `ibm-lh-presto-svc` to your Presto endpoint
2. Update `.env` file with connection details
3. Download the Presto SSL certificate to `/certs/presto.crt`

**For watsonx.data Developer Edition (Local Presto)**, skip this step - the setup.sh script already configured everything.

### 6. Access the Application

After setup completes, access the application using the URLs provided:

- **Web UI**: `http://<PUBLIC_IP>:10000`
  - Login: `watsonx` / `watsonx.data`
- **HCD (Cassandra)**: `<PUBLIC_IP>:9042`
  - Access via: `./hcd-1.2.3/bin/hcd cqlsh <PUBLIC_IP> -u cassandra -p cassandra`

**Note**: For watsonx.data Enterprise, Presto console is accessed through the IBM Cloud watsonx.data dashboard, not directly from the VM.

## Detailed Setup

### Custom VM Configuration

You can customize the VM provisioning with command-line options:

```bash
# Custom VM name
./setup-cloud.sh --vm-name my-custom-vm

# Specific resource group
./setup-cloud.sh --resource-group itz-production

# Different VM profile
./setup-cloud.sh --profile bx2-4x16

# Combine options
./setup-cloud.sh --vm-name demo-vm --profile bx2-4x16
```

### Available VM Profiles

| Profile   | vCPUs | RAM   | Use Case                    |
|-----------|-------|-------|-----------------------------|
| bx2-2x8   | 2     | 8GB   | Default, suitable for demo  |
| bx2-4x16  | 4     | 16GB  | Better performance          |
| bx2-8x32  | 8     | 32GB  | High performance            |

### Security Configuration

The setup script automatically:
- Detects your public IP address
- Restricts SSH and application access to your IP only
- Creates security group rules for required ports

To allow access from additional IPs, modify the security group after provisioning:

```bash
# Get security group ID
SG_ID=$(ibmcloud is security-groups --output json | jq -r '.[] | select(.name=="affiliate-junction-sg") | .id')

# Add rule for additional IP
ibmcloud is security-group-rule-add $SG_ID inbound tcp \
  --port-min 10000 --port-max 10000 \
  --remote <ADDITIONAL_IP>/32
```

## Accessing the Application

### Web UI

The main application interface is available at:

```
http://<PUBLIC_IP>:10000
```

**Default Credentials:**
- Username: `watsonx`
- Password: `watsonx.data`

**Available Dashboards:**
- **Index** (`/`): Overview of the affiliate marketing ecosystem
- **Publisher Dashboard** (`/publisher/{id}`): Publisher-specific metrics
- **Advertiser Dashboard** (`/advertiser/{id}`): Advertiser campaign performance
- **Fraud Detection** (`/fraud`): Anomaly detection and fraud analysis
- **Services** (`/services`): System administration and monitoring

### Presto Console

Access the Presto web interface at:

```
http://<PUBLIC_IP>:8080
```

Use this interface to:
- Execute ad-hoc SQL queries
- Monitor query performance
- View cluster statistics

### HCD/Cassandra CQL Shell

Access the Cassandra query shell via SSH:

```bash
# SSH into the VM
ssh -i ~/.ssh/affiliate-junction-key root@<PUBLIC_IP>

# Run CQL shell
./hcd-1.2.3/bin/hcd cqlsh 172.17.0.1 -u cassandra -p cassandra
```

## Troubleshooting

### Connection Issues

**Problem**: Cannot SSH into the VM

**Solutions**:
1. Verify your IP hasn't changed:
   ```bash
   curl https://api.ipify.org
   ```
2. Update security group if IP changed
3. Check SSH key permissions:
   ```bash
   chmod 600 ~/.ssh/affiliate-junction-key
   ```

**Problem**: Cannot access Web UI

**Solutions**:
1. Verify services are running:
   ```bash
   ssh -i ~/.ssh/affiliate-junction-key root@<PUBLIC_IP>
   sudo systemctl status uvicorn
   ```
2. Check if setup.sh completed successfully
3. View service logs:
   ```bash
   journalctl -u uvicorn -f
   ```

### Application Issues

**Problem**: Services not starting

**Solutions**:
1. Check service status:
   ```bash
   sudo systemctl status generate_traffic hcd_to_presto uvicorn
   ```
2. Restart services:
   ```bash
   sudo systemctl restart generate_traffic hcd_to_presto uvicorn
   ```
3. View logs for errors:
   ```bash
   journalctl -u generate_traffic -n 50
   ```

**Problem**: No data in dashboards

**Solutions**:
1. Wait 2-3 minutes for data generation to start
2. Check generate_traffic service:
   ```bash
   sudo systemctl status generate_traffic
   journalctl -u generate_traffic -f
   ```
3. Verify HCD is running:
   ```bash
   ./hcd-1.2.3/bin/hcd cqlsh 172.17.0.1 -u cassandra -p cassandra
   ```

### Performance Issues

**Problem**: Slow query performance

**Solutions**:
1. Consider upgrading to a larger VM profile:
   ```bash
   ./teardown-cloud.sh
   ./setup-cloud.sh --profile bx2-4x16
   ```
2. Check system resources:
   ```bash
   top
   df -h
   ```

## Cleanup

### Remove All Resources

To completely remove the VM and all associated resources:

```bash
./teardown-cloud.sh
```

This will delete:
- VM instance
- Floating IP
- Security group
- SSH key (from IBM Cloud)
- Local connection info file

### Selective Cleanup

Keep certain resources while removing others:

```bash
# Keep SSH key
./teardown-cloud.sh --keep-ssh-key

# Keep security group
./teardown-cloud.sh --keep-security-group

# Skip confirmation prompts
./teardown-cloud.sh --force
```

### Manual Cleanup

If the teardown script fails, manually remove resources:

```bash
# List and delete instance
ibmcloud is instances
ibmcloud is instance-delete <INSTANCE_ID>

# List and delete floating IP
ibmcloud is floating-ips
ibmcloud is floating-ip-delete <FIP_ID>

# List and delete security group
ibmcloud is security-groups
ibmcloud is security-group-delete <SG_ID>

# List and delete SSH key
ibmcloud is keys
ibmcloud is key-delete affiliate-junction-key
```

## Advanced Configuration

### Using Existing SSH Keys

To use an existing SSH key instead of generating a new one:

1. Edit `setup-cloud.sh` and modify the `SSH_KEY_NAME` and `SSH_KEY_PATH` variables
2. Ensure the key exists in IBM Cloud:
   ```bash
   ibmcloud is keys
   ```
3. Run the setup script

### Custom Security Group Rules

To add custom security group rules after provisioning:

```bash
# Get security group ID
SG_ID=$(ibmcloud is security-groups --output json | jq -r '.[] | select(.name=="affiliate-junction-sg") | .id')

# Add custom rule (example: allow port 3000)
ibmcloud is security-group-rule-add $SG_ID inbound tcp \
  --port-min 3000 --port-max 3000 \
  --remote 0.0.0.0/0
```

### Monitoring and Logging

View real-time logs for all services:

```bash
# SSH into VM
ssh -i ~/.ssh/affiliate-junction-key root@<PUBLIC_IP>

# View all service logs
journalctl -f

# View specific service
journalctl -u generate_traffic -f

# View logs since last boot
journalctl -b
```

### Backup and Restore

**Backup Data:**

```bash
# SSH into VM
ssh -i ~/.ssh/affiliate-junction-key root@<PUBLIC_IP>

# Backup HCD data
./hcd-1.2.3/bin/hcd nodetool snapshot affiliate_junction

# Backup configuration
tar -czf backup.tar.gz .env *.service
```

**Restore Data:**

```bash
# Copy backup to new VM
scp -i ~/.ssh/affiliate-junction-key backup.tar.gz root@<NEW_PUBLIC_IP>:~/

# SSH into new VM and extract
ssh -i ~/.ssh/affiliate-junction-key root@<NEW_PUBLIC_IP>
tar -xzf backup.tar.gz
```

## Connection Information

After running `setup-cloud.sh`, connection details are saved to `.cloud-connection-info`:

```bash
# View connection info
cat .cloud-connection-info

# Source it to use variables
source .cloud-connection-info
echo $PUBLIC_IP
```

## Support and Resources

- **GitHub Repository**: https://github.ibm.com/Data-Labs/affiliate-junction-demo
- **IBM Cloud Documentation**: https://cloud.ibm.com/docs
- **IBM Cloud CLI Reference**: https://cloud.ibm.com/docs/cli
- **VPC Documentation**: https://cloud.ibm.com/docs/vpc

## Security Best Practices

1. **Restrict Access**: Keep security group rules as restrictive as possible
2. **Rotate Keys**: Regularly rotate SSH keys
3. **Monitor Access**: Review access logs regularly
4. **Update System**: Keep the VM OS and packages updated
5. **Backup Data**: Regularly backup important data

## Cost Optimization

- **Stop VM when not in use**: VMs incur charges even when stopped, but at a reduced rate
- **Use appropriate profile**: Don't over-provision resources
- **Clean up unused resources**: Remove VMs and floating IPs when done
- **Monitor usage**: Use IBM Cloud cost management tools

## Next Steps

After successful deployment:

1. Explore the Web UI dashboards
2. Run sample queries in Presto Console
3. Review the [DEMO_SCRIPT.md](DEMO_SCRIPT.md) for guided walkthrough
4. Check [DEVELOPER.md](DEVELOPER.md) for development guidelines
5. Customize the application for your use case