#!/bin/bash

#############################################################################
# IBM Cloud VM Provisioning Script for Affiliate Junction Demo
# 
# This script provisions a RHEL 9 VM in IBM Cloud (eu-de region) with all
# necessary networking and security configurations for running the 
# affiliate junction demo application.
#
# Prerequisites:
#   - IBM Cloud CLI installed (https://cloud.ibm.com/docs/cli)
#   - Authenticated to IBM Cloud (ibmcloud login)
#
# Usage:
#   ./setup-cloud.sh [OPTIONS]
#
# Options:
#   --vm-name NAME        Custom VM name (default: affiliate-junction-vm-TIMESTAMP)
#   --resource-group RG   Specific resource group (default: auto-detect itz-*)
#   --profile PROFILE     VM profile (default: bx2-2x8)
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
REGION="eu-de"
ZONE="eu-de-1"
GITHUB_REPO="https://github.ibm.com/Data-Labs/affiliate-junction-demo"
SSH_KEY_NAME="affiliate-junction-key"
SSH_KEY_PATH="$HOME/.ssh/${SSH_KEY_NAME}"
SECURITY_GROUP_NAME="affiliate-junction-sg"
CONNECTION_INFO_FILE=".cloud-connection-info"

# Default values
VM_PROFILE="bx2-2x8"  # 2 vCPUs, 8GB RAM
VM_NAME="affiliate-junction-vm-$(date +%Y%m%d-%H%M%S)"
RESOURCE_GROUP=""

# Required ports
PORTS=(22 9042 8080 8443 10000)
PORT_DESCRIPTIONS=(
    "SSH"
    "HCD/Cassandra"
    "Presto Console"
    "Presto HTTPS"
    "Web UI"
)

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

check_command() {
    if ! command -v "$1" &> /dev/null; then
        print_error "$1 is not installed"
        return 1
    fi
    return 0
}

#############################################################################
# Parse Command Line Arguments
#############################################################################

parse_args() {
    while [[ $# -gt 0 ]]; do
        case $1 in
            --vm-name)
                VM_NAME="$2"
                shift 2
                ;;
            --resource-group)
                RESOURCE_GROUP="$2"
                shift 2
                ;;
            --profile)
                VM_PROFILE="$2"
                shift 2
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
# Pre-flight Checks
#############################################################################

check_prerequisites() {
    print_header "Checking Prerequisites"
    
    # Check IBM Cloud CLI
    if ! check_command ibmcloud; then
        print_error "IBM Cloud CLI is not installed"
        echo "Install from: https://cloud.ibm.com/docs/cli"
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
    
    # Check required plugins
    if ! ibmcloud plugin list | grep -q "vpc-infrastructure"; then
        print_warning "VPC Infrastructure plugin not found, installing..."
        ibmcloud plugin install vpc-infrastructure -f
    fi
    print_success "VPC Infrastructure plugin is available"
    
    echo ""
}

#############################################################################
# Resource Group Detection
#############################################################################

detect_resource_group() {
    print_header "Detecting Resource Group"
    
    if [ -n "$RESOURCE_GROUP" ]; then
        print_info "Using specified resource group: $RESOURCE_GROUP"
    else
        print_info "Auto-detecting resource group starting with 'itz-'..."
        
        # Get all resource groups and filter for itz-*
        local rgs=$(ibmcloud resource groups --output json | jq -r '.[].name' | grep "^itz-" || true)
        
        if [ -z "$rgs" ]; then
            print_error "No resource groups found starting with 'itz-'"
            echo "Available resource groups:"
            ibmcloud resource groups
            exit 1
        fi
        
        # If multiple found, use the first one
        local rg_count=$(echo "$rgs" | wc -l)
        if [ "$rg_count" -gt 1 ]; then
            print_warning "Multiple itz-* resource groups found:"
            echo "$rgs"
            RESOURCE_GROUP=$(echo "$rgs" | head -n 1)
            print_info "Using: $RESOURCE_GROUP"
        else
            RESOURCE_GROUP="$rgs"
            print_success "Found resource group: $RESOURCE_GROUP"
        fi
    fi
    
    # Target the resource group
    ibmcloud target -g "$RESOURCE_GROUP" -r "$REGION"
    print_success "Targeted resource group: $RESOURCE_GROUP in region: $REGION"
    
    echo ""
}

#############################################################################
# SSH Key Management
#############################################################################

setup_ssh_key() {
    print_header "Setting Up SSH Key"
    
    # Check if SSH key already exists locally
    if [ -f "${SSH_KEY_PATH}" ]; then
        print_info "SSH key already exists locally: ${SSH_KEY_PATH}"
    else
        print_info "Generating new SSH key pair..."
        ssh-keygen -t rsa -b 4096 -f "${SSH_KEY_PATH}" -N "" -C "affiliate-junction-vm"
        chmod 600 "${SSH_KEY_PATH}"
        chmod 644 "${SSH_KEY_PATH}.pub"
        print_success "SSH key pair generated"
    fi
    
    # Check if key exists in IBM Cloud
    if ibmcloud is keys --output json | jq -e ".[] | select(.name==\"${SSH_KEY_NAME}\")" > /dev/null 2>&1; then
        print_info "SSH key already exists in IBM Cloud: ${SSH_KEY_NAME}"
    else
        print_info "Uploading SSH key to IBM Cloud..."
        ibmcloud is key-create "${SSH_KEY_NAME}" @"${SSH_KEY_PATH}.pub" --resource-group-name "$RESOURCE_GROUP"
        print_success "SSH key uploaded to IBM Cloud"
    fi
    
    echo ""
}

#############################################################################
# Get Client IP
#############################################################################

get_client_ip() {
    # Try multiple services to get public IP
    local ip=""
    
    # Try ipify
    ip=$(curl -s https://api.ipify.org 2>/dev/null || true)
    
    # Try ifconfig.me as fallback
    if [ -z "$ip" ]; then
        ip=$(curl -s https://ifconfig.me 2>/dev/null || true)
    fi
    
    # Try icanhazip as second fallback
    if [ -z "$ip" ]; then
        ip=$(curl -s https://icanhazip.com 2>/dev/null || true)
    fi
    
    if [ -z "$ip" ]; then
        echo "0.0.0.0/0"
    else
        echo "${ip}/32"
    fi
}

#############################################################################
# Security Group Setup
#############################################################################

setup_security_group() {
    print_header "Setting Up Security Group" >&2
    
    print_info "Detecting client IP address..." >&2
    local client_ip=$(get_client_ip)
    
    if [ "$client_ip" = "0.0.0.0/0" ]; then
        print_warning "Could not detect public IP address" >&2
        print_warning "Security group will allow access from all IPs (0.0.0.0/0)" >&2
    else
        local ip_only=$(echo "$client_ip" | cut -d'/' -f1)
        print_success "Detected client IP: $ip_only" >&2
    fi
    
    # Get or create VPC
    local vpc_id=$(ibmcloud is vpcs --output json | jq -r '.[0].id')
    if [ -z "$vpc_id" ] || [ "$vpc_id" = "null" ]; then
        print_info "No VPC found. Creating VPC..." >&2
        local vpc_name="affiliate-junction-vpc"
        vpc_id=$(ibmcloud is vpc-create "$vpc_name" --resource-group-name "$RESOURCE_GROUP" --output json | jq -r '.id')
        print_success "VPC created: $vpc_name ($vpc_id)" >&2
        
        # Create subnet in the VPC
        print_info "Creating subnet..." >&2
        local subnet_name="affiliate-junction-subnet"
        local subnet_id=$(ibmcloud is subnet-create "$subnet_name" "$vpc_id" \
            --zone "$ZONE" \
            --ipv4-address-count 256 \
            --resource-group-name "$RESOURCE_GROUP" \
            --output json | jq -r '.id')
        print_success "Subnet created: $subnet_name ($subnet_id)" >&2
    else
        print_info "Using existing VPC: $vpc_id" >&2
    fi
    
    # Check if security group exists
    local sg_id=$(ibmcloud is security-groups --output json | jq -r ".[] | select(.name==\"${SECURITY_GROUP_NAME}\") | .id" || true)
    
    if [ -n "$sg_id" ] && [ "$sg_id" != "null" ]; then
        print_info "Security group already exists: ${SECURITY_GROUP_NAME}" >&2
    else
        print_info "Creating security group..." >&2
        sg_id=$(ibmcloud is security-group-create "${SECURITY_GROUP_NAME}" "$vpc_id" --resource-group-name "$RESOURCE_GROUP" --output json | jq -r '.id')
        print_success "Security group created: ${SECURITY_GROUP_NAME}" >&2
        
        # Add rules for each required port
        print_info "Adding security group rules..." >&2
        for i in "${!PORTS[@]}"; do
            local port="${PORTS[$i]}"
            local desc="${PORT_DESCRIPTIONS[$i]}"
            
            print_info "  Adding rule for ${desc} (port ${port})..." >&2
            ibmcloud is security-group-rule-add "$sg_id" inbound tcp \
                --port-min "$port" --port-max "$port" \
                --remote "$client_ip" > /dev/null
        done
        
        # Add outbound rule (allow all)
        print_info "  Adding outbound rule (allow all)..." >&2
        ibmcloud is security-group-rule-add "$sg_id" outbound tcp > /dev/null
        ibmcloud is security-group-rule-add "$sg_id" outbound udp > /dev/null
        ibmcloud is security-group-rule-add "$sg_id" outbound icmp > /dev/null
        
        print_success "Security group rules configured" >&2
    fi
    
    echo "$sg_id"
}

#############################################################################
# Check for Existing VM
#############################################################################

check_existing_vm() {
    print_header "Checking for Existing VM" >&2
    
    # Check if connection info file exists
    if [ -f "$CONNECTION_INFO_FILE" ]; then
        print_info "Found existing connection info file" >&2
        
        # Source the file to get instance ID
        source "$CONNECTION_INFO_FILE"
        
        # Check if instance still exists and is running
        if [ -n "$INSTANCE_ID" ]; then
            local instance_status=$(ibmcloud is instance "$INSTANCE_ID" --output json 2>/dev/null | jq -r '.status' || echo "not_found")
            
            if [ "$instance_status" = "running" ] || [ "$instance_status" = "starting" ]; then
                print_success "Found existing running VM: $VM_NAME" >&2
                print_info "Instance ID: $INSTANCE_ID" >&2
                print_info "Status: $instance_status" >&2
                
                # Get current IPs
                local current_public_ip=$(ibmcloud is instance "$INSTANCE_ID" --output json | jq -r '.network_interfaces[0].floating_ips[0].address // empty')
                local current_private_ip=$(ibmcloud is instance "$INSTANCE_ID" --output json | jq -r '.primary_network_interface.primary_ipv4_address')
                
                if [ -z "$current_public_ip" ]; then
                    current_public_ip="$PUBLIC_IP"
                fi
                
                echo "$INSTANCE_ID|$current_public_ip|$current_private_ip"
                return 0
            else
                print_warning "Previous instance no longer exists or is stopped" >&2
                print_info "Will create a new instance" >&2
            fi
        fi
    fi
    
    # Also check for any running instances with our naming pattern
    local existing_instance=$(ibmcloud is instances --output json | \
        jq -r ".[] | select(.name | startswith(\"affiliate-junction-vm\")) | select(.status==\"running\" or .status==\"starting\") | .id" | head -n 1)
    
    if [ -n "$existing_instance" ]; then
        print_warning "Found existing affiliate-junction VM instance" >&2
        local existing_name=$(ibmcloud is instance "$existing_instance" --output json | jq -r '.name')
        print_info "Instance: $existing_name ($existing_instance)" >&2
        
        read -p "Do you want to use this existing instance? (y/n): " -n 1 -r >&2
        echo "" >&2
        
        if [[ $REPLY =~ ^[Yy]$ ]]; then
            local existing_public_ip=$(ibmcloud is instance "$existing_instance" --output json | jq -r '.network_interfaces[0].floating_ips[0].address // empty')
            local existing_private_ip=$(ibmcloud is instance "$existing_instance" --output json | jq -r '.primary_network_interface.primary_ipv4_address')
            
            # Update VM_NAME to match existing
            VM_NAME="$existing_name"
            
            echo "$existing_instance|$existing_public_ip|$existing_private_ip"
            return 0
        fi
    fi
    
    return 1
}

#############################################################################
# VM Provisioning
#############################################################################

provision_vm() {
    local sg_id="$1"
    
    print_header "Provisioning VM Instance" >&2
    
    print_info "VM Configuration:" >&2
    echo "  Name: $VM_NAME" >&2
    echo "  Profile: $VM_PROFILE" >&2
    echo "  Region: $REGION" >&2
    echo "  Zone: $ZONE" >&2
    echo "  Resource Group: $RESOURCE_GROUP" >&2
    echo "" >&2
    
    # Get default VPC and subnet
    local vpc_id=$(ibmcloud is vpcs --output json | jq -r '.[0].id')
    local subnet_id=$(ibmcloud is subnets --output json | jq -r ".[] | select(.vpc.id==\"${vpc_id}\") | select(.zone.name==\"${ZONE}\") | .id" | head -n 1)
    
    if [ -z "$subnet_id" ] || [ "$subnet_id" = "null" ]; then
        print_error "No subnet found in zone ${ZONE}" >&2
        exit 1
    fi
    
    # Get RHEL 9 image
    print_info "Finding RHEL 9 image..." >&2
    local image_id=$(ibmcloud is images --visibility public --output json | \
        jq -r '.[] | select(.operating_system.name | contains("red-9")) | select(.status=="available") | .id' | head -n 1)
    
    if [ -z "$image_id" ] || [ "$image_id" = "null" ]; then
        print_error "Could not find RHEL 9 image" >&2
        exit 1
    fi
    print_success "Found RHEL 9 image: $image_id" >&2
    
    # Get SSH key ID and public key content
    local ssh_key_id=$(ibmcloud is keys --output json | jq -r ".[] | select(.name==\"${SSH_KEY_NAME}\") | .id")
    if [ -z "$ssh_key_id" ] || [ "$ssh_key_id" = "null" ]; then
        print_error "SSH key not found: ${SSH_KEY_NAME}" >&2
        exit 1
    fi
    
    # Get the public key content for cloud-init (workaround for IBM Cloud CLI bug)
    local ssh_public_key=$(cat "${SSH_KEY_PATH}.pub")
    
    # Create cloud-init user-data to inject SSH key and install prerequisites
    local user_data=$(cat <<EOF
#cloud-config
users:
  - name: root
    ssh_authorized_keys:
      - ${ssh_public_key}

packages:
  - git
  - python3
  - python3-pip
  - java-11-openjdk-devel
  - wget
  - curl
  - jq

runcmd:
  - echo "Prerequisites installed successfully" > /root/cloud-init-complete.txt
EOF
)
    
    # Create the VM instance with user-data workaround
    print_info "Creating VM instance (this may take a few minutes)..." >&2
    print_info "Note: Using cloud-init to inject SSH key due to IBM Cloud CLI limitation" >&2
    local instance_json=$(ibmcloud is instance-create "$VM_NAME" "$vpc_id" "$ZONE" "$VM_PROFILE" "$subnet_id" \
        --image "$image_id" \
        --keys "$ssh_key_id" \
        --security-group-ids "$sg_id" \
        --resource-group-name "$RESOURCE_GROUP" \
        --user-data "$user_data" \
        --output json)
    
    local instance_id=$(echo "$instance_json" | jq -r '.id')
    
    if [ -z "$instance_id" ] || [ "$instance_id" = "null" ]; then
        print_error "Failed to create VM instance" >&2
        exit 1
    fi
    
    print_success "VM instance created: $instance_id" >&2
    
    # Wait for instance to be running
    print_info "Waiting for instance to be running..." >&2
    local status=""
    local attempts=0
    local max_attempts=60
    
    while [ "$status" != "running" ] && [ $attempts -lt $max_attempts ]; do
        sleep 5
        status=$(ibmcloud is instance "$instance_id" --output json | jq -r '.status')
        attempts=$((attempts + 1))
        echo -n "." >&2
    done
    echo "" >&2
    
    if [ "$status" != "running" ]; then
        print_error "Instance failed to start (status: $status)" >&2
        exit 1
    fi
    
    print_success "Instance is running" >&2
    
    # Create and attach floating IP
    print_info "Creating floating IP..." >&2
    local fip_json=$(ibmcloud is floating-ip-reserve "${VM_NAME}-fip" \
        --zone "$ZONE" \
        --resource-group-name "$RESOURCE_GROUP" \
        --output json)
    
    local fip_id=$(echo "$fip_json" | jq -r '.id')
    local fip_address=$(echo "$fip_json" | jq -r '.address')
    
    # Get primary network interface
    local nic_id=$(ibmcloud is instance "$instance_id" --output json | jq -r '.primary_network_interface.id')
    
    # Attach floating IP to instance
    print_info "Attaching floating IP to instance..." >&2
    ibmcloud is floating-ip-update "$fip_id" --nic "$nic_id" > /dev/null
    
    print_success "Floating IP attached: $fip_address" >&2
    
    # Get private IP
    local private_ip=$(ibmcloud is instance "$instance_id" --output json | jq -r '.primary_network_interface.primary_ipv4_address')
    
    print_warning "Note: Wait 2-3 minutes for cloud-init to complete before attempting SSH" >&2
    
    echo "" >&2
    echo "$instance_id|$fip_address|$private_ip"
}

#############################################################################
# Save Connection Information
#############################################################################

save_connection_info() {
    local instance_id="$1"
    local public_ip="$2"
    local private_ip="$3"
    
    print_header "Saving Connection Information"
    
    cat > "$CONNECTION_INFO_FILE" <<EOF
# Affiliate Junction VM Connection Information
# Generated: $(date)

VM_NAME=$VM_NAME
INSTANCE_ID=$instance_id
PUBLIC_IP=$public_ip
PRIVATE_IP=$private_ip
REGION=$REGION
ZONE=$ZONE
RESOURCE_GROUP=$RESOURCE_GROUP
SSH_KEY_PATH=$SSH_KEY_PATH
GITHUB_REPO=$GITHUB_REPO

# SSH Connection
# ssh -i $SSH_KEY_PATH root@$public_ip

# Access URLs (after running setup.sh in VM)
# Web UI: http://$public_ip:10000
# Presto Console: http://$public_ip:8080
# HCD CQL: ssh -i $SSH_KEY_PATH root@$public_ip then run: ./hcd-1.2.3/bin/hcd cqlsh 172.17.0.1 -u cassandra -p cassandra
EOF
    
    chmod 600 "$CONNECTION_INFO_FILE"
    print_success "Connection information saved to: $CONNECTION_INFO_FILE"
    echo ""
}

#############################################################################
# Display Final Instructions
#############################################################################

display_instructions() {
    local public_ip="$1"
    
    print_header "IBM Cloud VM Provisioning Complete!"
    
    echo ""
    echo -e "${GREEN}VM Details:${NC}"
    echo "  Name: $VM_NAME"
    echo "  Public IP: $public_ip"
    echo "  Region: $REGION"
    echo "  Resource Group: $RESOURCE_GROUP"
    echo ""
    
    echo -e "${GREEN}SSH Connection:${NC}"
    echo -e "  ${BLUE}ssh -i $SSH_KEY_PATH root@$public_ip${NC}"
    echo ""
    
    echo -e "${GREEN}Next Steps:${NC}"
    echo "  1. SSH into the VM using the command above"
    echo "  2. Clone the repository:"
    echo -e "     ${BLUE}git clone $GITHUB_REPO${NC}"
    echo "  3. Navigate to the project directory:"
    echo -e "     ${BLUE}cd affiliate-junction-demo${NC}"
    echo "  4. Run the setup script:"
    echo -e "     ${BLUE}./setup.sh${NC}"
    echo ""
    
    echo -e "${GREEN}Access URLs (after setup.sh completes):${NC}"
    echo -e "  Web UI: ${BLUE}http://$public_ip:10000${NC}"
    echo "    Login: watsonx / watsonx.data"
    echo ""
    echo -e "  Presto Console: ${BLUE}http://$public_ip:8080${NC}"
    echo ""
    echo -e "  HCD CQL Shell:${NC}"
    echo "    SSH into VM, then run:"
    echo -e "    ${BLUE}./hcd-1.2.3/bin/hcd cqlsh 172.17.0.1 -u cassandra -p cassandra${NC}"
    echo ""
    
    echo -e "${YELLOW}Note: The setup.sh script will take approximately 5-10 minutes to complete.${NC}"
    echo -e "${YELLOW}Wait for all services to start before accessing the URLs.${NC}"
    echo ""
    
    print_success "Connection details saved to: $CONNECTION_INFO_FILE"
    echo ""
}

#############################################################################
# Main Execution
#############################################################################

main() {
    print_header "IBM Cloud VM Provisioning for Affiliate Junction"
    echo ""
    
    # Parse command line arguments
    parse_args "$@"
    
    # Run pre-flight checks
    check_prerequisites
    
    # Detect or validate resource group
    detect_resource_group
    
    # Setup SSH key
    setup_ssh_key
    
    # Setup security group and get its ID
    sg_id=$(setup_security_group)
    echo ""
    
    # Check for existing VM first
    if vm_info=$(check_existing_vm); then
        print_success "Using existing VM instance" >&2
        instance_id=$(echo "$vm_info" | cut -d'|' -f1)
        public_ip=$(echo "$vm_info" | cut -d'|' -f2)
        private_ip=$(echo "$vm_info" | cut -d'|' -f3)
    else
        # Provision a new VM
        vm_info=$(provision_vm "$sg_id")
        instance_id=$(echo "$vm_info" | cut -d'|' -f1)
        public_ip=$(echo "$vm_info" | cut -d'|' -f2)
        private_ip=$(echo "$vm_info" | cut -d'|' -f3)
    fi
    
    # Save connection information
    save_connection_info "$instance_id" "$public_ip" "$private_ip"
    
    # Display final instructions
    display_instructions "$public_ip"
    
    print_success "Provisioning complete!"
}

# Run main function
main "$@"

# Made with Bob
