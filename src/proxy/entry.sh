#!/bin/bash

ls -la ~/.oci

CONFIG_FILE="${OCI_CONFIG:-${HOME}/.oci/config}"
if [ ! -f "$CONFIG_FILE" ]; then
  echo "OCI config not found at $CONFIG_FILE"
  exit 0
fi

export TF_VAR_private_key_path=${HOME}/.oci/key.pem
if [ ! -f "$TF_VAR_private_key_path" ]; then
  echo "OCI private_key_path not found at $TF_VAR_private_key_path"
  exit 0
fi

oci_cfg() {
  local key="$1"
  awk -F'=' -v k="$key" '
    /^\s*\[/ { next }
    {
      gsub(/^[[:space:]]+|[[:space:]]+$/, "", $1)
      gsub(/^[[:space:]]+|[[:space:]]+$/, "", $2)
      if (tolower($1) == tolower(k)) { print $2; exit }
    }
  ' $CONFIG_FILE
}

export TF_VAR_region="$(oci_cfg region)"
export TF_VAR_tenancy_ocid="$(oci_cfg tenancy)"
export TF_VAR_user_ocid="$(oci_cfg user)"
export TF_VAR_fingerprint="$(oci_cfg fingerprint)"

export TF_VAR_subnet_id="${SUBNET_ID:?SUBNET_ID not set}"
export TF_VAR_compartment_id="${COMPARTMENT_ID:?COMPARTMENT_ID not set}"

BASTION_HOST="${BASTION_IP:?BASTION_IP not set}"
BASTION_USER_VALUE="${BASTION_USER:-ubuntu}"
BASTION_KEY_PATH="${BASTION_SSH_KEY:-/root/.oci/bastion_private_key}"

export TF_VAR_bastion_host="$BASTION_HOST"
export TF_VAR_bastion_user="$BASTION_USER_VALUE"
export TF_VAR_bastion_private_key_path="$BASTION_KEY_PATH"

if [ ! -r "$BASTION_KEY_PATH" ]; then
  echo "Bastion SSH key not readable at $BASTION_KEY_PATH"
  exit
fi

BASTION_SSH_CMD="ssh -i $BASTION_KEY_PATH -o StrictHostKeyChecking=no -o ConnectTimeout=10 -l $BASTION_USER_VALUE $BASTION_HOST echo bastion-ok"

eval "$BASTION_SSH_CMD"

if [ $? -ne 0 ]; then
  echo "Failed to connect to bastion host at $BASTION_HOST"
  exit
fi

# Generate VM SSH key pair if missing
VM_SSH_DIR="${HOME}/.ssh"
VM_SSH_KEY_PATH="${VM_SSH_DIR}/nf_executor"
mkdir -p "$VM_SSH_DIR"
chmod 700 "$VM_SSH_DIR"
if [ ! -f "$VM_SSH_KEY_PATH" ] || [ ! -f "${VM_SSH_KEY_PATH}.pub" ]; then
  ssh-keygen -t rsa -b 4096 -f "$VM_SSH_KEY_PATH" -N "" -q
fi
chmod 600 "$VM_SSH_KEY_PATH"

if [ ! -f "${VM_SSH_KEY_PATH}.pub" ]; then
  echo "Unable to find VM public key at ${VM_SSH_KEY_PATH}.pub"
  exit 1
fi

export VM_SSH_PRIVATE_KEY_PATH="$VM_SSH_KEY_PATH"
export TF_VAR_ssh_authorized_key="$(cat "${VM_SSH_KEY_PATH}.pub")"

env

exec "$@"
