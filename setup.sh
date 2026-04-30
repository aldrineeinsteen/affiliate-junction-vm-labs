#!/bin/bash

set -e  # Exit on error

# Color codes for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}Affiliate Junction Demo Setup${NC}"
echo -e "${BLUE}========================================${NC}"
echo ""

# Bootstrap infrastructure
echo -e "${BLUE}Configuring system...${NC}"
sudo perl -i -pe 'if($.==1 && !/ibm-lh-presto-svc/){s/$/ ibm-lh-presto-svc/}' /etc/hosts

# Install Java 17 if not already installed (Java 11 is already present from cloud-init)
if ! java -version 2>&1 | grep -q "17"; then
    echo -e "${BLUE}Installing Java 17...${NC}"
    sudo dnf -y install java-17-openjdk java-17-openjdk-devel
else
    echo -e "${GREEN}✓ Java already installed${NC}"
fi

# Bootstrap python environment
echo ""
echo -e "${BLUE}Setting up Python virtual environment...${NC}"
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
echo -e "${GREEN}✓ Python environment ready${NC}"

# Create .env file
cp env-sample .env
echo -e "${GREEN}✓ Environment file created${NC}"

# Configure git
git config --global user.email "you@example.com"
git config --global user.name "Your Name"

# Enable backend services
echo ""
echo -e "${BLUE}Configuring systemd services...${NC}"
sudo cp *.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable generate_traffic hcd_to_presto presto_to_hcd presto_insights presto_cleanup uvicorn.service truncate_all_tables.service
echo -e "${GREEN}✓ Services enabled${NC}"

echo ""
echo -e "${BLUE}Starting services...${NC}"
sudo systemctl start generate_traffic hcd_to_presto uvicorn.service
echo -e "${YELLOW}⏳ Waiting 60 seconds for Presto DDL commands to complete...${NC}"
sleep 60
sudo systemctl start presto_to_hcd presto_insights presto_cleanup
echo -e "${GREEN}✓ All services started${NC}"

# Add virtual environment activation to .bashrc if not already present
if ! grep -q "source $(pwd)/.venv/bin/activate" ~/.bashrc; then
    echo "source $(pwd)/.venv/bin/activate" >> ~/.bashrc
fi

# Get public IP
PUBLIC_IP=$(curl -s ifconfig.me || hostname -I | awk '{print $1}')

# Print final access information
echo ""
echo -e "${BLUE}========================================${NC}"
echo -e "${GREEN}Setup Complete!${NC}"
echo -e "${BLUE}========================================${NC}"
echo ""
echo -e "${GREEN}Access URLs:${NC}"
echo -e "  ${BLUE}Web UI:${NC} http://${PUBLIC_IP}:10000"
echo -e "    Login: ${YELLOW}watsonx${NC} / ${YELLOW}watsonx.data${NC}"
echo ""
echo -e "  ${BLUE}Presto Console:${NC} http://${PUBLIC_IP}:8080"
echo ""
echo -e "${GREEN}HCD CQL Shell:${NC}"
echo -e "  ${BLUE}./hcd-1.2.3/bin/hcd cqlsh 172.17.0.1 -u cassandra -p cassandra${NC}"
echo ""
echo -e "${GREEN}Service Status:${NC}"
sudo systemctl status generate_traffic hcd_to_presto presto_to_hcd presto_insights presto_cleanup uvicorn --no-pager | grep -E "Active:|Loaded:" || true
echo ""
echo -e "${YELLOW}Note: Services may take a few minutes to fully initialize.${NC}"
echo -e "${YELLOW}Check service logs with: journalctl -u <service_name> -f${NC}"
echo ""

