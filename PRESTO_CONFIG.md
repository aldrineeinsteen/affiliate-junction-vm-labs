# Presto Configuration Guide

## Quick Reference

Based on your watsonx.data console connection details:

### Connection Details

```bash
Hostname: 747e742b-170a-4676-8bd7-8a0b400a9810.d4mn7ovf0r8q7913kbkg.lakehouse.ibmappdomain.cloud
Port: 31138
Username: ibmlhapikey_student_69f34f752f@techzone.ibm.com
Catalog: affiliate_info,iceberg_data (use iceberg_data for this demo)
Schema: affiliate_junction
```

### SSL Certificate

The certificate has been provided from the console (3 certificates in chain).

## Configuration Steps

### 1. Pull Latest Code

```bash
cd ~/affiliate-junction-vm-labs
git pull
```

### 2. Run Presto Configuration Script

```bash
chmod +x configure-presto.sh
./configure-presto.sh
```

When prompted, provide:
- **Hostname**: `747e742b-170a-4676-8bd7-8a0b400a9810.d4mn7ovf0r8q7913kbkg.lakehouse.ibmappdomain.cloud`
- **Port**: `31138`
- **Username**: `ibmlhapikey_student_69f34f752f@techzone.ibm.com`
- **Password**: Your API key/password
- **Catalog**: `iceberg_data`
- **Schema**: `affiliate_junction`
- **Certificate**: Paste the full certificate chain (all 3 certificates)

### 3. What the Script Does

1. Updates `/etc/hosts` to resolve the Presto hostname
2. Updates `.env` file with connection details
3. Saves SSL certificate to `/certs/presto.crt`
4. Validates the certificate

### 4. Verify Configuration

After configuration, check:

```bash
# Check .env file
cat .env | grep PRESTO

# Check certificate
ls -la /certs/presto.crt
openssl x509 -in /certs/presto.crt -text -noout | head -20

# Check /etc/hosts
grep lakehouse /etc/hosts
```

### 5. Test Connection

```bash
# Activate virtual environment
source .venv/bin/activate

# Test Presto connection
python3 -c "from web.presto_wrapper import presto_wrapper; print(presto_wrapper.execute_query_simple('SELECT 1'))"
```

## Architecture Differences

### Developer Edition (Original)
- Presto runs locally on same VM
- Hostname: `ibm-lh-presto-svc` (localhost alias)
- Port: 8443
- Certificate in `/certs/` from local installation

### Enterprise Edition (Your Setup)
- Presto is managed IBM Cloud service
- Hostname: Long IBM Cloud hostname
- Port: 31138 (custom port)
- Certificate from watsonx.data console
- API key authentication

## Troubleshooting

### Certificate Issues

If certificate validation fails:
```bash
# Check certificate format
cat /certs/presto.crt

# Should start with: -----BEGIN CERTIFICATE-----
# Should end with: -----END CERTIFICATE-----
# Should contain 3 certificates in chain
```

### Connection Issues

```bash
# Test DNS resolution
nslookup 747e742b-170a-4676-8bd7-8a0b400a9810.d4mn7ovf0r8q7913kbkg.lakehouse.ibmappdomain.cloud

# Test network connectivity
curl -k https://747e742b-170a-4676-8bd7-8a0b400a9810.d4mn7ovf0r8q7913kbkg.lakehouse.ibmappdomain.cloud:31138/v1/info

# Check if port is accessible
nc -zv 747e742b-170a-4676-8bd7-8a0b400a9810.d4mn7ovf0r8q7913kbkg.lakehouse.ibmappdomain.cloud 31138
```

### Service Logs

```bash
# Check service status
sudo systemctl status hcd_to_presto
sudo systemctl status presto_insights

# View logs
journalctl -u hcd_to_presto -f
journalctl -u presto_insights -f
```

## Next Steps

After Presto is configured:

1. **Run setup.sh** (if not already done):
   ```bash
   ./setup.sh
   ```

2. **Restart services** (if already running):
   ```bash
   sudo systemctl restart hcd_to_presto presto_to_hcd presto_insights presto_cleanup
   ```

3. **Access Web UI**:
   ```bash
   # Get your public IP
   curl ifconfig.me
   
   # Access at: http://<PUBLIC_IP>:10000
   # Login: watsonx / watsonx.data
   ```

## Important Notes

- The demo uses the **actual Presto hostname**, not `ibm-lh-presto-svc`
- Port is **31138**, not the standard 8443
- Authentication uses **API key**, not simple password
- Certificate chain has **3 certificates** (leaf + intermediates)
- Catalog is **iceberg_data**, not the default from env-sample