#!/bin/bash

#############################################################################
# IBM Cloud VM Teardown Script for Affiliate Junction Demo
# 
# This script removes all IBM Cloud resources created by setup-cloud.sh
# including the VM instance, floating IP, security group, and SSH key.
#
# Prerequisites:
#   - IBM Cloud CLI installed and authenticated
#   - .cloud-connection-info file from setup-cloud.sh
#
# Usage:
#   ./teardown-cloud.sh [OPTIONS]
#
# Options:
#   --force               Skip confirmation prompts
#   --keep-ssh-key        Keep SSH key (don't delete)
#   --keep-security-group Keep security group (don't delete)
#   --help                Show this help message
#############################################################################

set -e  # Exit on error

# Color codes for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Configuration
CONNECTION_INFO_FILE=".cloud-connection-info"
SSH_KEY_NAME="affiliate-junction-key"
SECURITY_GROUP_NAME="affiliate-junction-sg"

# Options
FORCE=false
KEEP_SSH_KEY=false
KEEP_SECURITY_GROUP=false

#############################################################################
# Helper Functions
#############################################################################

print_header() {
    echo -e "${BLUE}========================================${NC}"
    echo -e "${BLUE}$1${NC}"
    echo -e "${BLUE}========================================${NC}"
}

print_success() {
    echo -e "${GREEN}✓ $1${NC}"
}

print_error() {
    echo -e "${RED}✗ $1${NC}"
}

print_warning() {
    echo -e "${YELLOW}⚠ $1${NC}"
}

print_info() {
    echo -e "${BLUE}ℹ $1${NC}"
}

confirm() {
    if [ "$FORCE" = true ]; then
        return 0
    fi
    
    local prompt="$1"
    read -p "$prompt [y/N]: " -n 1 -r
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        return 0
    else
        return 1
    fi
}

#############################################################################
# Parse Command Line Arguments
#############################################################################

parse_args() {
    while [[ $# -gt 0 ]]; do
        case $1 in
            --force)
                FORCE=true
                shift
                ;;
            --keep-ssh-key)
                KEEP_SSH_KEY=true
                shift
                ;;
            --keep-security-group)
                KEEP_SECURITY_GROUP=true
                shift
                ;;
            --help)
                grep "^#" "$0" | grep -v "#!/bin/bash" | sed 's/^# //'
                exit 0
                ;;
            *)
                print_error "Unknown option: $1"
                echo "Use --help for usage information"
                exit 1
                ;;
        esac
    done
}

#############################################################################
# Load Connection Information
#############################################################################

load_connection_info() {
    print_header "Loading Connection Information"
    
    if [ ! -f "$CONNECTION_INFO_FILE" ]; then
        print_warning "Connection info file not found: $CONNECTION_INFO_FILE"
        print_info "Will search for all affiliate-junction resources"
        
        # Set defaults for resource group detection
        RESOURCE_GROUP=$(ibmcloud resource groups --output json | jq -r '.[].name' | grep '^itz-' | head -n 1)
        if [ -z "$RESOURCE_GROUP" ]; then
            RESOURCE_GROUP="Default"
        fi
        REGION="eu-de"
        VM_NAME="affiliate-junction-vm"
        INSTANCE_ID=""
        PUBLIC_IP=""
        echo ""
        return
    fi
    
    # Source the connection info file
    source "$CONNECTION_INFO_FILE"
    
    if [ -z "$INSTANCE_ID" ]; then
        print_error "INSTANCE_ID not found in connection info file"
        exit 1
    fi
    
    print_success "Loaded connection information"
    echo "  VM Name: $VM_NAME"
    echo "  Instance ID: $INSTANCE_ID"
    echo "  Public IP: $PUBLIC_IP"
    echo "  Resource Group: $RESOURCE_GROUP"
    echo ""
}

#############################################################################
# Check Prerequisites
#############################################################################

check_prerequisites() {
    print_header "Checking Prerequisites"
    
    # Check IBM Cloud CLI
    if ! command -v ibmcloud &> /dev/null; then
        print_error "IBM Cloud CLI is not installed"
        exit 1
    fi
    print_success "IBM Cloud CLI is installed"
    
    # Check if logged in
    if ! ibmcloud target &> /dev/null; then
        print_error "Not logged in to IBM Cloud"
        echo "Run: ibmcloud login --sso"
        exit 1
    fi
    print_success "Logged in to IBM Cloud"
    
    # Target the resource group
    ibmcloud target -g "$RESOURCE_GROUP" -r "$REGION" > /dev/null 2>&1
    print_success "Targeted resource group: $RESOURCE_GROUP"
    
    echo ""
}

#############################################################################
# Delete VM Instance
#############################################################################

delete_instance() {
    print_header "Deleting VM Instances"
    
    # Find all instances matching affiliate-junction-vm*
    local instances=$(ibmcloud is instances --output json | jq -r '.[] | select(.name | startswith("affiliate-junction-vm")) | .id')
    
    if [ -z "$instances" ]; then
        print_warning "No affiliate-junction-vm instances found"
        return
    fi
    
    # Count instances
    local instance_count=$(echo "$instances" | wc -l | tr -d ' ')
    print_info "Found $instance_count affiliate-junction-vm instance(s)"
    
    # List instances
    echo ""
    ibmcloud is instances --output json | jq -r '.[] | select(.name | startswith("affiliate-junction-vm")) | "  - \(.name) (\(.id)) - Status: \(.status)"'
    echo ""
    
    if ! confirm "Delete all these instances?"; then
        print_warning "Skipping instance deletion"
        return
    fi
    
    # Delete each instance
    while IFS= read -r instance_id; do
        local instance_name=$(ibmcloud is instance "$instance_id" --output json 2>/dev/null | jq -r '.name' || echo "unknown")
        print_info "Deleting instance: $instance_name ($instance_id)..."
        ibmcloud is instance-delete "$instance_id" -f
    done <<< "$instances"
    
    # Wait for all deletions
    print_info "Waiting for instances to be deleted..."
    local attempts=0
    local max_attempts=30
    
    while [ $attempts -lt $max_attempts ]; do
        local remaining=$(ibmcloud is instances --output json 2>/dev/null | jq -r '.[] | select(.name | startswith("affiliate-junction-vm")) | .id' | wc -l | tr -d ' ')
        if [ "$remaining" -eq 0 ]; then
            break
        fi
        sleep 2
        attempts=$((attempts + 1))
        echo -n "."
    done
    echo ""
    
    print_success "All instances deleted"
    echo ""
}

#############################################################################
# Delete Floating IP
#############################################################################

delete_floating_ip() {
    print_header "Deleting Floating IPs"
    
    # Find all floating IPs matching affiliate-junction-vm*-fip
    local fips=$(ibmcloud is floating-ips --output json | jq -r '.[] | select(.name | startswith("affiliate-junction-vm")) | .id')
    
    if [ -z "$fips" ]; then
        print_warning "No affiliate-junction-vm floating IPs found"
        return
    fi
    
    # Count floating IPs
    local fip_count=$(echo "$fips" | wc -l | tr -d ' ')
    print_info "Found $fip_count floating IP(s)"
    
    # List floating IPs
    echo ""
    ibmcloud is floating-ips --output json | jq -r '.[] | select(.name | startswith("affiliate-junction-vm")) | "  - \(.name) (\(.address))"'
    echo ""
    
    if ! confirm "Delete all these floating IPs?"; then
        print_warning "Skipping floating IP deletion"
        return
    fi
    
    # Delete each floating IP
    while IFS= read -r fip_id; do
        local fip_name=$(ibmcloud is floating-ip "$fip_id" --output json 2>/dev/null | jq -r '.name' || echo "unknown")
        print_info "Deleting floating IP: $fip_name..."
        ibmcloud is floating-ip-release "$fip_id" -f
    done <<< "$fips"
    
    print_success "All floating IPs deleted"
    echo ""
}

#############################################################################
# Delete Security Group
#############################################################################

delete_security_group() {
    if [ "$KEEP_SECURITY_GROUP" = true ]; then
        print_warning "Keeping security group as requested"
        return
    fi
    
    print_header "Deleting Security Group"
    
    # Find security group
    local sg_id=$(ibmcloud is security-groups --output json | jq -r ".[] | select(.name==\"${SECURITY_GROUP_NAME}\") | .id" || true)
    
    if [ -z "$sg_id" ] || [ "$sg_id" = "null" ]; then
        print_warning "Security group not found (may have been already deleted)"
        return
    fi
    
    print_info "Security Group: $SECURITY_GROUP_NAME"
    
    if ! confirm "Delete this security group?"; then
        print_warning "Skipping security group deletion"
        return
    fi
    
    print_info "Deleting security group..."
    ibmcloud is security-group-delete "$sg_id" -f
    print_success "Security group deleted"
    echo ""
}

#############################################################################
# Delete SSH Key
#############################################################################

delete_ssh_key() {
    if [ "$KEEP_SSH_KEY" = true ]; then
        print_warning "Keeping SSH key as requested"
        return
    fi
    
    print_header "Deleting SSH Key"
    
    # Check if key exists in IBM Cloud
    if ! ibmcloud is keys --output json | jq -e ".[] | select(.name==\"${SSH_KEY_NAME}\")" > /dev/null 2>&1; then
        print_warning "SSH key not found in IBM Cloud (may have been already deleted)"
    else
        print_info "SSH Key: $SSH_KEY_NAME"
        
        if confirm "Delete SSH key from IBM Cloud?"; then
            print_info "Deleting SSH key from IBM Cloud..."
            ibmcloud is key-delete "$SSH_KEY_NAME" -f
            print_success "SSH key deleted from IBM Cloud"
        else
            print_warning "Skipping SSH key deletion from IBM Cloud"
        fi
    fi
    
    # Ask about local SSH key
    if [ -f "$SSH_KEY_PATH" ]; then
        print_info "Local SSH Key: $SSH_KEY_PATH"
        
        if confirm "Delete local SSH key files?"; then
            rm -f "$SSH_KEY_PATH" "${SSH_KEY_PATH}.pub"
            print_success "Local SSH key files deleted"
        else
            print_warning "Keeping local SSH key files"
        fi
    fi
    
    echo ""
}

#############################################################################
# Cleanup Connection Info File
#############################################################################

cleanup_connection_info() {
    print_header "Cleaning Up Connection Info"
    
    if [ -f "$CONNECTION_INFO_FILE" ]; then
        if confirm "Delete connection info file ($CONNECTION_INFO_FILE)?"; then
            rm -f "$CONNECTION_INFO_FILE"
            print_success "Connection info file deleted"
        else
            print_warning "Keeping connection info file"
        fi
    fi
    
    echo ""
}

#############################################################################
# Display Summary
#############################################################################

display_summary() {
    print_header "Teardown Complete"
    
    echo -e "${GREEN}Resources Removed:${NC}"
    echo "  ✓ VM Instance: $VM_NAME"
    echo "  ✓ Floating IP: $PUBLIC_IP"
    
    if [ "$KEEP_SECURITY_GROUP" = false ]; then
        echo "  ✓ Security Group: $SECURITY_GROUP_NAME"
    else
        echo "  ⊘ Security Group: $SECURITY_GROUP_NAME (kept)"
    fi
    
    if [ "$KEEP_SSH_KEY" = false ]; then
        echo "  ✓ SSH Key: $SSH_KEY_NAME"
    else
        echo "  ⊘ SSH Key: $SSH_KEY_NAME (kept)"
    fi
    
    echo ""
    print_success "All requested resources have been removed"
    echo ""
}

#############################################################################
# Main Execution
#############################################################################

main() {
    print_header "IBM Cloud VM Teardown for Affiliate Junction"
    echo ""
    
    # Parse command line arguments
    parse_args "$@"
    
    # Load connection information
    load_connection_info
    
    # Check prerequisites
    check_prerequisites
    
    # Confirm teardown
    if ! confirm "This will delete the VM and associated resources. Continue?"; then
        print_warning "Teardown cancelled"
        exit 0
    fi
    
    echo ""
    
    # Delete resources in order
    delete_instance
    delete_floating_ip
    delete_security_group
    delete_ssh_key
    cleanup_connection_info
    
    # Display summary
    display_summary
}

# Run main function
main "$@"

# Made with Bob
