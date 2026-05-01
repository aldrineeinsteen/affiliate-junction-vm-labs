#!/bin/bash

set -e  # Exit on error

# Color codes for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}Presto Configuration Helper${NC}"
echo -e "${BLUE}========================================${NC}"
echo ""

# Check if .env exists, create from env-sample if not
if [ ! -f ".env" ]; then
    if [ -f "env-sample" ]; then
        echo -e "${YELLOW}Creating .env from env-sample...${NC}"
        cp env-sample .env
        echo -e "${GREEN}✓ .env file created${NC}"
    else
        echo -e "${RED}✗ env-sample file not found${NC}"
        exit 1
    fi
fi

echo -e "${YELLOW}This script will help you configure the Presto connection to watsonx.data${NC}"
echo ""
echo -e "${BLUE}Please provide the following information from your watsonx.data console:${NC}"
echo -e "${YELLOW}(From Infrastructure → Engines → Connection Details)${NC}"
echo ""

# Prompt for Presto endpoint
read -p "Presto Engine Hostname (e.g., 747e742b-xxx.d4mn7ovf0r8q7913kbkg.lakehouse.ibmappdomain.cloud): " PRESTO_HOSTNAME
read -p "Presto Engine Port (e.g., 31138): " PRESTO_PORT

read -p "Presto Username (e.g., ibmlhapikey_student_xxx@techzone.ibm.com): " PRESTO_USER

read -sp "Presto Password/API Key: " PRESTO_PASSWD
echo ""

read -p "Presto Catalog (comma-separated, e.g., affiliate_info,iceberg_data) [iceberg_data]: " PRESTO_CATALOG
PRESTO_CATALOG=${PRESTO_CATALOG:-iceberg_data}

read -p "Presto Schema [affiliate_junction]: " PRESTO_SCHEMA
PRESTO_SCHEMA=${PRESTO_SCHEMA:-affiliate_junction}

echo ""
echo -e "${BLUE}Configuration Summary:${NC}"
echo -e "  Hostname: ${PRESTO_HOSTNAME}"
echo -e "  Port: ${PRESTO_PORT}"
echo -e "  Username: ${PRESTO_USER}"
echo -e "  Password: ********"
echo -e "  Catalog: ${PRESTO_CATALOG}"
echo -e "  Schema: ${PRESTO_SCHEMA}"
echo ""

read -p "Is this correct? (y/n): " CONFIRM
if [ "$CONFIRM" != "y" ]; then
    echo -e "${YELLOW}Configuration cancelled${NC}"
    exit 0
fi

echo ""
echo -e "${BLUE}Updating configuration...${NC}"

# Update /etc/hosts to map ibm-lh-presto-svc to actual hostname
echo -e "${BLUE}Updating /etc/hosts...${NC}"
if grep -q "ibm-lh-presto-svc" /etc/hosts; then
    # Remove existing entry
    sudo sed -i '/ibm-lh-presto-svc/d' /etc/hosts
fi
# Add new entry - get IP address of Presto hostname
PRESTO_IP=$(getent hosts "$PRESTO_HOSTNAME" | awk '{ print $1 }' | head -1)
if [ -z "$PRESTO_IP" ]; then
    echo -e "${YELLOW}⚠ Could not resolve ${PRESTO_HOSTNAME}, adding hostname directly${NC}"
    echo "$PRESTO_HOSTNAME ibm-lh-presto-svc" | sudo tee -a /etc/hosts > /dev/null
else
    echo "$PRESTO_IP ibm-lh-presto-svc" | sudo tee -a /etc/hosts > /dev/null
fi
echo -e "${GREEN}✓ /etc/hosts updated${NC}"

# Update .env file
echo -e "${BLUE}Updating .env file...${NC}"
sed -i "s|^PRESTO_HOST=.*|PRESTO_HOST=${PRESTO_HOSTNAME}|" .env
sed -i "s|^PRESTO_PORT=.*|PRESTO_PORT=${PRESTO_PORT}|" .env
sed -i "s|^PRESTO_USER=.*|PRESTO_USER=${PRESTO_USER}|" .env
sed -i "s|^PRESTO_PASSWD=.*|PRESTO_PASSWD=${PRESTO_PASSWD}|" .env
sed -i "s|^PRESTO_CATALOG=.*|PRESTO_CATALOG=${PRESTO_CATALOG}|" .env
sed -i "s|^PRESTO_SCHEMA=.*|PRESTO_SCHEMA=${PRESTO_SCHEMA}|" .env
echo -e "${GREEN}✓ .env file updated${NC}"

# Fetch and save SSL certificate automatically
echo ""
echo -e "${BLUE}Fetching SSL certificate from Presto endpoint...${NC}"
sudo mkdir -p /certs

# Fetch certificate from the server
if echo | openssl s_client -connect "${PRESTO_HOSTNAME}:${PRESTO_PORT}" -servername "${PRESTO_HOSTNAME}" 2>/dev/null | openssl x509 -outform PEM | sudo tee /certs/presto.crt > /dev/null; then
    sudo chmod 644 /certs/presto.crt
    echo -e "${GREEN}✓ Certificate fetched and saved to /certs/presto.crt${NC}"
    
    # Verify certificate
    if openssl x509 -in /certs/presto.crt -text -noout > /dev/null 2>&1; then
        echo -e "${GREEN}✓ Certificate is valid${NC}"
        CERT_SUBJECT=$(openssl x509 -in /certs/presto.crt -subject -noout | sed 's/subject=//')
        CERT_ISSUER=$(openssl x509 -in /certs/presto.crt -issuer -noout | sed 's/issuer=//')
        CERT_DATES=$(openssl x509 -in /certs/presto.crt -dates -noout)
        echo -e "${BLUE}  Subject: ${CERT_SUBJECT}${NC}"
        echo -e "${BLUE}  Issuer: ${CERT_ISSUER}${NC}"
        echo -e "${BLUE}  ${CERT_DATES}${NC}"
    else
        echo -e "${RED}✗ Certificate validation failed${NC}"
        exit 1
    fi
else
    echo -e "${RED}✗ Failed to fetch certificate from ${PRESTO_HOSTNAME}:${PRESTO_PORT}${NC}"
    echo -e "${YELLOW}Please check:${NC}"
    echo -e "  • Hostname and port are correct"
    echo -e "  • Server is accessible from this machine"
    echo -e "  • Firewall allows outbound HTTPS connections"
    exit 1
fi

echo ""
echo -e "${GREEN}========================================${NC}"
echo -e "${GREEN}Presto Configuration Complete!${NC}"
echo -e "${GREEN}========================================${NC}"
echo ""
echo -e "${BLUE}Configuration saved:${NC}"
echo -e "  • /etc/hosts updated with Presto hostname"
echo -e "  • .env file updated with connection details"
echo -e "  • SSL certificate saved to /certs/presto.crt"
echo ""
echo -e "${BLUE}Next steps:${NC}"
echo -e "  1. If setup.sh hasn't been run yet, run it now: ${GREEN}./setup.sh${NC}"
echo -e "  2. If services are already running, restart them:"
echo -e "     ${GREEN}sudo systemctl restart hcd_to_presto presto_to_hcd presto_insights presto_cleanup${NC}"
echo -e "  3. Test Presto connection:"
echo -e "     ${GREEN}source .venv/bin/activate${NC}"
echo -e "     ${GREEN}python3 -c 'from web.presto_wrapper import presto_wrapper; print(presto_wrapper.execute_query_simple(\"SELECT 1\"))'${NC}"
echo -e "  4. Access Web UI: ${GREEN}http://$(curl -s ifconfig.me):10000${NC}"
echo ""
echo -e "${YELLOW}Note: The demo uses the actual Presto hostname (${PRESTO_HOSTNAME})${NC}"
echo -e "${YELLOW}      not 'ibm-lh-presto-svc' as in Developer Edition${NC}"
echo ""

# Made with Bob
