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

# Install system prerequisites
echo -e "${BLUE}Installing system prerequisites...${NC}"
sudo dnf install -y unzip nmap-ncat > /dev/null 2>&1
echo -e "${GREEN}✓ System prerequisites installed${NC}"

# Download and install HCD if not present
echo ""
echo -e "${BLUE}Installing HCD (Hyperconverged Database)...${NC}"
if [ ! -d "./hcd-1.2.3" ] && [ ! -d "./hcd-1.2.5" ]; then
    HCD_URL="https://dsw-bld.dhe.ibm.com/sdfdl/v2/fulfill/M122SEN/Xa.2/Xb.htcOMovxHCAgZGRZV1daW19tcnqGlKK-/Xc.M122SEN/HCD_1.2.5_EN.zip/Xd./Xf.lPr.A6VR/Xg.13855290/Xi./XY.knac/XZ.f_U9slkPggG2dwXHLiP4tZ1Mpxs4xW0z/HCD_1.2.5_EN.zip"
    
    echo -e "${BLUE}Downloading HCD 1.2.5...${NC}"
    if wget -q --show-progress "$HCD_URL" -O HCD_1.2.5_EN.zip 2>&1; then
        echo -e "${GREEN}✓ HCD downloaded${NC}"
        
        echo -e "${BLUE}Extracting HCD...${NC}"
        unzip -q HCD_1.2.5_EN.zip
        tar -xzf hcd-1.2.5-bin.tar.gz
        rm -f HCD_1.2.5_EN.zip hcd-1.2.5-bin.tar.gz
        
        # Create symlink for compatibility with code expecting hcd-1.2.3
        ln -s hcd-1.2.5 hcd-1.2.3
        
        echo -e "${GREEN}✓ HCD installed${NC}"
    else
        echo -e "${RED}✗ Failed to download HCD from default URL${NC}"
        echo -e "${YELLOW}Please provide HCD tarball manually:${NC}"
        echo -e "  1. Download HCD from IBM"
        echo -e "  2. Place in current directory"
        echo -e "  3. Extract: tar -xzf hcd-*.tar.gz"
        echo -e "  4. Create symlink: ln -s hcd-1.2.* hcd-1.2.3"
        echo -e "  5. Re-run this script"
        exit 1
    fi
else
    if [ -d "./hcd-1.2.5" ]; then
        echo -e "${GREEN}✓ HCD 1.2.5 already installed${NC}"
        # Ensure symlink exists
        [ ! -L "./hcd-1.2.3" ] && ln -s hcd-1.2.5 hcd-1.2.3
    else
        echo -e "${GREEN}✓ HCD 1.2.3 already installed${NC}"
    fi
fi

# Check for and kill any existing HCD/Cassandra processes
echo -e "${BLUE}Checking for existing HCD processes...${NC}"
if pgrep -f "cassandra|hcd" > /dev/null; then
    echo -e "${YELLOW}⚠ Found existing HCD/Cassandra processes, stopping them...${NC}"
    pkill -9 -f cassandra 2>/dev/null || true
    pkill -9 -f hcd 2>/dev/null || true
    sleep 2
    echo -e "${GREEN}✓ Existing processes stopped${NC}"
fi

# Start HCD in background
echo -e "${BLUE}Starting HCD...${NC}"
# Use -R flag to allow running as root (required for cloud deployments)
./hcd-1.2.3/bin/hcd cassandra -R > /tmp/hcd_startup.log 2>&1 &
INITIAL_PID=$!
echo -e "${BLUE}HCD startup initiated...${NC}"

# Wait up to 120 seconds for HCD to start, checking every 3 seconds
# HCD can take 60+ seconds on slower systems or when replaying commit logs
MAX_WAIT=120
ELAPSED=0
while [ $ELAPSED -lt $MAX_WAIT ]; do
    sleep 3
    ELAPSED=$((ELAPSED + 3))
    
    # Check if HCD is accepting connections on port 9042
    # Use bash's /dev/tcp as fallback if nc not available
    if command -v nc >/dev/null 2>&1; then
        # Try both localhost and Docker bridge IP with nc
        if nc -z localhost 9042 2>/dev/null || nc -z 127.0.0.1 9042 2>/dev/null || nc -z 172.17.0.1 9042 2>/dev/null; then
            echo -e "${GREEN}✓ HCD started successfully (took ${ELAPSED}s)${NC}"
            break
        fi
    else
        # Fallback to /dev/tcp if nc not available
        if (echo > /dev/tcp/localhost/9042) 2>/dev/null || (echo > /dev/tcp/127.0.0.1/9042) 2>/dev/null; then
            echo -e "${GREEN}✓ HCD started successfully (took ${ELAPSED}s)${NC}"
            break
        fi
    fi
    
    # Check if any HCD/Cassandra process is running (HCD forks, so original PID exits)
    if ! pgrep -f "cassandra|hcd" > /dev/null; then
        echo -e "${RED}✗ HCD process failed to start${NC}"
        echo -e "${YELLOW}Startup log:${NC}"
        cat /tmp/hcd_startup.log
        exit 1
    fi
    
    echo -e "${BLUE}  Still starting... (${ELAPSED}s/${MAX_WAIT}s)${NC}"
done

# Final check
PORT_OPEN=false
if command -v nc >/dev/null 2>&1; then
    if nc -z localhost 9042 2>/dev/null || nc -z 127.0.0.1 9042 2>/dev/null || nc -z 172.17.0.1 9042 2>/dev/null; then
        PORT_OPEN=true
    fi
else
    if (echo > /dev/tcp/localhost/9042) 2>/dev/null || (echo > /dev/tcp/127.0.0.1/9042) 2>/dev/null; then
        PORT_OPEN=true
    fi
fi

if [ "$PORT_OPEN" = false ]; then
    echo -e "${RED}✗ HCD failed to start within ${MAX_WAIT} seconds${NC}"
    echo -e "${YELLOW}Checking logs...${NC}"
    if [ -f "/var/log/cassandra/system.log" ]; then
        echo -e "${YELLOW}Last 30 lines of system.log:${NC}"
        tail -30 /var/log/cassandra/system.log
    fi
    if [ -f "/tmp/hcd_startup.log" ]; then
        echo -e "${YELLOW}Startup log:${NC}"
        cat /tmp/hcd_startup.log
    fi
    exit 1
fi

# Initialize HCD schema
echo -e "${BLUE}Initializing HCD schema...${NC}"
# Use localhost since HCD binds to localhost/127.0.0.1, not Docker bridge IP
./hcd-1.2.3/bin/hcd cqlsh localhost -u cassandra -p cassandra -f hcd_schema.cql
echo -e "${GREEN}✓ HCD schema initialized${NC}"

# Bootstrap infrastructure
echo ""
echo -e "${BLUE}Configuring system...${NC}"
sudo perl -i -pe 'if($.==1 && !/ibm-lh-presto-svc/){s/$/ ibm-lh-presto-svc/}' /etc/hosts

# Ensure Java 11 is installed and set as default (HCD requires Java 11)
echo -e "${BLUE}Configuring Java 11 for HCD...${NC}"
if ! java -version 2>&1 | grep -q "11"; then
    echo -e "${BLUE}Java 11 already installed from cloud-init${NC}"
fi

# Set Java 11 as the default
sudo alternatives --set java /usr/lib/jvm/java-11-openjdk-11.0.25.0.9-4.el9.x86_64/bin/java 2>/dev/null || \
sudo alternatives --set java $(alternatives --display java | grep 'family java-11-openjdk' | awk '{print $1}' | head -1) 2>/dev/null || \
echo -e "${YELLOW}⚠ Could not set Java 11 as default, HCD may fail${NC}"

java -version 2>&1 | head -1
echo -e "${GREEN}✓ Java configured${NC}"

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

# Get current directory and user
CURRENT_DIR=$(pwd)
CURRENT_USER=$(whoami)

# Update service files with correct paths and user
echo -e "${BLUE}Updating service files with correct paths...${NC}"
for service_file in *.service; do
    if [ -f "$service_file" ]; then
        # Create temporary file with updated paths
        sed -e "s|/home/watsonx/affiliate-junction-demo|${CURRENT_DIR}|g" \
            -e "s|User=watsonx|User=${CURRENT_USER}|g" \
            "$service_file" > "/tmp/${service_file}"
        sudo cp "/tmp/${service_file}" /etc/systemd/system/
        rm "/tmp/${service_file}"
    fi
done

sudo systemctl daemon-reload
sudo systemctl enable generate_traffic hcd_to_presto presto_to_hcd presto_insights presto_cleanup uvicorn.service truncate_all_tables.service
echo -e "${GREEN}✓ Services configured and enabled${NC}"

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

# Save access information to file
ACCESS_INFO_FILE="access-info.txt"
cat > "$ACCESS_INFO_FILE" <<EOF
========================================
Affiliate Junction Demo - Access Information
========================================
Generated: $(date)

Web UI: http://${PUBLIC_IP}:10000
  Login: watsonx / watsonx.data

Presto Console: http://${PUBLIC_IP}:8080

HCD CQL Shell:
  ./hcd-1.2.3/bin/hcd cqlsh 172.17.0.1 -u cassandra -p cassandra

Service Management:
  sudo systemctl status generate_traffic hcd_to_presto presto_to_hcd presto_insights presto_cleanup uvicorn
  journalctl -u <service_name> -f

========================================
EOF

echo -e "${GREEN}✓ Access information saved to: ${ACCESS_INFO_FILE}${NC}"
echo ""

