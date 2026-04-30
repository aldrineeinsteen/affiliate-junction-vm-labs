#!/bin/bash

# Color codes
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}Service Logs Viewer${NC}"
echo -e "${BLUE}========================================${NC}"
echo ""

if [ -z "$1" ]; then
    echo -e "${YELLOW}Usage: $0 <service_name> [options]${NC}"
    echo ""
    echo -e "${GREEN}Available services:${NC}"
    echo -e "  ${BLUE}generate_traffic${NC}  - Generates impressions and conversions"
    echo -e "  ${BLUE}hcd_to_presto${NC}      - ETL from HCD to Presto (runs every minute)"
    echo -e "  ${BLUE}presto_to_hcd${NC}      - Aggregations from Presto back to HCD"
    echo -e "  ${BLUE}presto_insights${NC}    - Fraud detection and insights"
    echo -e "  ${BLUE}presto_cleanup${NC}     - Data retention cleanup"
    echo -e "  ${BLUE}uvicorn${NC}            - FastAPI web UI"
    echo ""
    echo -e "${GREEN}Options:${NC}"
    echo -e "  ${BLUE}-f${NC}              Follow logs (live tail)"
    echo -e "  ${BLUE}-n <lines>${NC}      Show last N lines (default: 50)"
    echo -e "  ${BLUE}--since <time>${NC}  Show logs since time (e.g., '5 minutes ago')"
    echo ""
    echo -e "${GREEN}Examples:${NC}"
    echo -e "  $0 generate_traffic -f              # Follow generate_traffic logs"
    echo -e "  $0 uvicorn -n 100                   # Show last 100 lines of web UI logs"
    echo -e "  $0 hcd_to_presto --since '1 hour ago'  # Show ETL logs from last hour"
    echo ""
    echo -e "${GREEN}View all services status:${NC}"
    echo -e "  sudo systemctl status generate_traffic hcd_to_presto presto_to_hcd presto_insights presto_cleanup uvicorn"
    echo ""
    echo -e "${GREEN}Quick log commands:${NC}"
    echo -e "  ${BLUE}journalctl -u generate_traffic -f${NC}  # Follow traffic generation"
    echo -e "  ${BLUE}journalctl -u uvicorn -f${NC}           # Follow web UI logs"
    echo -e "  ${BLUE}journalctl -u hcd_to_presto -f${NC}     # Follow ETL logs"
    echo ""
    exit 0
fi

SERVICE=$1
shift

# Validate service name
case $SERVICE in
    generate_traffic|hcd_to_presto|presto_to_hcd|presto_insights|presto_cleanup|uvicorn)
        echo -e "${GREEN}Viewing logs for: ${BLUE}$SERVICE${NC}"
        echo -e "${YELLOW}Press Ctrl+C to exit${NC}"
        echo ""
        journalctl -u $SERVICE "$@"
        ;;
    *)
        echo -e "${RED}Error: Unknown service '$SERVICE'${NC}"
        echo -e "${YELLOW}Run '$0' without arguments to see available services${NC}"
        exit 1
        ;;
esac

# Made with Bob
