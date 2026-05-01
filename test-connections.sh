#!/bin/bash

# Color codes
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}Testing Affiliate Junction Connections${NC}"
echo -e "${BLUE}========================================${NC}"
echo ""

# Load environment variables
if [ -f .env ]; then
    export $(grep -v '^#' .env | xargs)
fi

# Activate virtual environment
source .venv/bin/activate

# Test 1: HCD Connection
echo -e "${BLUE}1. Testing HCD Connection...${NC}"
HCD_HOST=${HCD_HOST:-127.0.0.1}
if ./hcd-1.2.3/bin/hcd cqlsh ${HCD_HOST} -u cassandra -p cassandra -e "SELECT cluster_name FROM system.local;" 2>/dev/null | grep -q "Test Cluster"; then
    echo -e "${GREEN}✓ HCD is connected and responding at ${HCD_HOST}${NC}"
else
    echo -e "${RED}✗ HCD connection failed at ${HCD_HOST}${NC}"
fi
echo ""

# Test 2: Presto Connection
echo -e "${BLUE}2. Testing Presto Connection...${NC}"
python3 << 'EOF'
import sys
sys.path.insert(0, '.')
try:
    from web.presto_wrapper import presto_wrapper
    result = presto_wrapper.execute_query_simple("SELECT 1 as test")
    if result and len(result) > 0:
        print("\033[0;32m✓ Presto is connected and responding\033[0m")
    else:
        print("\033[0;31m✗ Presto query returned no results\033[0m")
except Exception as e:
    print(f"\033[0;31m✗ Presto connection failed: {e}\033[0m")
EOF
echo ""

# Test 3: Check if Presto catalog exists
echo -e "${BLUE}3. Testing Presto Catalog (affiliate_info)...${NC}"
python3 << 'EOF'
import sys
sys.path.insert(0, '.')
try:
    from web.presto_wrapper import presto_wrapper
    result = presto_wrapper.execute_query_simple("SHOW SCHEMAS FROM affiliate_info")
    if result:
        print("\033[0;32m✓ Presto catalog 'affiliate_info' is accessible\033[0m")
        print(f"  Schemas found: {len(result)}")
    else:
        print("\033[0;31m✗ Presto catalog 'affiliate_info' not found\033[0m")
except Exception as e:
    print(f"\033[0;31m✗ Catalog check failed: {e}\033[0m")
EOF
echo ""

# Test 4: Check HCD tables
echo -e "${BLUE}4. Checking HCD Tables...${NC}"
HCD_TABLES=$(./hcd-1.2.3/bin/hcd cqlsh ${HCD_HOST} -u cassandra -p cassandra -e "USE affiliate_junction; DESCRIBE TABLES;" 2>/dev/null | grep -v "Warning:" | grep -v "Recommendation:" | tail -1)
if [ ! -z "$HCD_TABLES" ]; then
    echo -e "${GREEN}✓ HCD tables exist:${NC}"
    echo "  $HCD_TABLES"
else
    echo -e "${YELLOW}⚠ No HCD tables found yet (may still be initializing)${NC}"
fi
echo ""

# Test 5: Check Presto tables
echo -e "${BLUE}5. Checking Presto Tables...${NC}"
python3 << 'EOF'
import sys
sys.path.insert(0, '.')
try:
    from web.presto_wrapper import presto_wrapper
    result = presto_wrapper.execute_query_simple("SHOW TABLES FROM affiliate_info.affiliate_junction")
    if result and len(result) > 0:
        print("\033[0;32m✓ Presto tables exist:\033[0m")
        for row in result[:5]:  # Show first 5 tables
            print(f"  - {row[0]}")
        if len(result) > 5:
            print(f"  ... and {len(result) - 5} more")
    else:
        print("\033[1;33m⚠ No Presto tables found yet (may still be initializing)\033[0m")
except Exception as e:
    print(f"\033[1;33m⚠ Presto tables check failed: {e}\033[0m")
EOF
echo ""

# Test 6: Check Services Status
echo -e "${BLUE}6. Checking Service Status...${NC}"
for service in generate_traffic hcd_to_presto presto_to_hcd presto_insights presto_cleanup uvicorn; do
    if systemctl is-active --quiet $service; then
        echo -e "${GREEN}✓ $service is running${NC}"
    else
        echo -e "${RED}✗ $service is not running${NC}"
    fi
done
echo ""

# Test 7: Check for data in HCD
echo -e "${BLUE}7. Checking for Data in HCD...${NC}"
IMPRESSION_COUNT=$(./hcd-1.2.3/bin/hcd cqlsh ${HCD_HOST} -u cassandra -p cassandra -e "SELECT COUNT(*) FROM affiliate_junction.impression_tracking;" 2>/dev/null | grep -E "^\s*[0-9]+" | tr -d ' ')
if [ ! -z "$IMPRESSION_COUNT" ] && [ "$IMPRESSION_COUNT" -gt 0 ]; then
    echo -e "${GREEN}✓ HCD has data: $IMPRESSION_COUNT impression records${NC}"
else
    echo -e "${YELLOW}⚠ No data in HCD yet (services may still be starting)${NC}"
fi
echo ""

# Test 8: Check for data in Presto
echo -e "${BLUE}8. Checking for Data in Presto...${NC}"
python3 << 'EOF'
import sys
sys.path.insert(0, '.')
try:
    from web.presto_wrapper import presto_wrapper
    result = presto_wrapper.execute_query_simple("SELECT COUNT(*) FROM affiliate_info.affiliate_junction.sales")
    if result and len(result) > 0 and result[0][0] > 0:
        print(f"\033[0;32m✓ Presto has data: {result[0][0]} sales records\033[0m")
    else:
        print("\033[1;33m⚠ No data in Presto yet (ETL may still be running)\033[0m")
except Exception as e:
    print(f"\033[1;33m⚠ Presto data check failed: {e}\033[0m")
EOF
echo ""

# Test 9: Web UI accessibility
echo -e "${BLUE}9. Testing Web UI...${NC}"
PUBLIC_IP=$(curl -s ifconfig.me || hostname -I | awk '{print $1}')
if curl -s -o /dev/null -w "%{http_code}" http://localhost:10000 | grep -q "200"; then
    echo -e "${GREEN}✓ Web UI is accessible at http://${PUBLIC_IP}:10000${NC}"
else
    echo -e "${YELLOW}⚠ Web UI not responding yet (may still be starting)${NC}"
fi
echo ""

echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}Connection Test Summary${NC}"
echo -e "${BLUE}========================================${NC}"
echo ""
echo -e "${YELLOW}Note: If some tests show warnings, wait a few minutes for services to fully initialize.${NC}"
echo -e "${YELLOW}Services generate data every minute, and ETL runs every minute.${NC}"
echo ""
echo -e "${GREEN}To view service logs:${NC}"
echo -e "  journalctl -u generate_traffic -f"
echo -e "  journalctl -u hcd_to_presto -f"
echo ""
echo -e "${GREEN}To access Web UI:${NC}"
echo -e "  http://${PUBLIC_IP}:10000"
echo -e "  Login: watsonx / watsonx.data"
echo ""

# Made with Bob
