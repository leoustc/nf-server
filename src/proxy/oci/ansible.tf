# Stage 2, setup the systemd running job

locals {
  nodes_ip = join(",", oci_core_instance.instance[*].private_ip)
}

resource "null_resource" "bootstrap_submit" {
  depends_on = [oci_core_instance.instance]

  provisioner "local-exec" {
    command = <<-EOT
      set -e

      TARGET_LIST="${local.nodes_ip}"
      BASTION="${var.bastion_user}@${var.bastion_host}"
      BASTION_KEY="${var.bastion_private_key_path}"
      VM_KEY="${var.vm_private_key_path}"
      VM_USER="${var.ssh_user}"

      if [[ -z "$TARGET_LIST" ]]; then
        echo "No target instances found" >&2
        exit 1
      fi

      export ANSIBLE_HOST_KEY_CHECKING=False
      export ANSIBLE_SSH_COMMON_ARGS='-o ProxyCommand="ssh -i '"$BASTION_KEY"' -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null '"$BASTION"' -W %h:%p" -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null'

      ansible-playbook -i "$TARGET_LIST," -u "$VM_USER" --private-key "$VM_KEY" ${path.module}/bootstrap_submit.yml
    EOT
  }
}

# Stage 3, check running job status once

resource "null_resource" "bootstrap_check" {
  depends_on = [null_resource.bootstrap_submit]

  triggers = {
    always_run = timestamp()
  }

  provisioner "local-exec" {
    command = <<-EOT
      set -e

      TARGET_LIST="${local.nodes_ip}"
      BASTION="${var.bastion_user}@${var.bastion_host}"
      BASTION_KEY="${var.bastion_private_key_path}"
      VM_KEY="${var.vm_private_key_path}"
      VM_USER="${var.ssh_user}"

      if [[ -z "$TARGET_LIST" ]]; then
        echo "No target instances found" >&2
        exit 1
      fi

      export ANSIBLE_HOST_KEY_CHECKING=False
      export ANSIBLE_SSH_COMMON_ARGS='-o ProxyCommand="ssh -i '"$BASTION_KEY"' -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null '"$BASTION"' -W %h:%p" -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null'

      ansible-playbook -i "$TARGET_LIST," -u "$VM_USER" --private-key "$VM_KEY" ${path.module}/bootstrap_check.yml
    EOT
  }
}

# Stage 4, wait for running job to finish

resource "null_resource" "bootstrap_wait" {
  depends_on = [null_resource.bootstrap_submit]

  triggers = {
    always_run = timestamp()
  }

  provisioner "local-exec" {
    command = <<-EOT
      set -e

      TARGET_LIST="${local.nodes_ip}"
      BASTION="${var.bastion_user}@${var.bastion_host}"
      BASTION_KEY="${var.bastion_private_key_path}"
      VM_KEY="${var.vm_private_key_path}"
      VM_USER="${var.ssh_user}"

      if [[ -z "$TARGET_LIST" ]]; then
        echo "No target instances found" >&2
        exit 1
      fi

      export ANSIBLE_HOST_KEY_CHECKING=False
      export ANSIBLE_SSH_COMMON_ARGS='-o ProxyCommand="ssh -i '"$BASTION_KEY"' -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null '"$BASTION"' -W %h:%p" -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null'

      ansible-playbook -i "$TARGET_LIST," -u "$VM_USER" --private-key "$VM_KEY" ${path.module}/bootstrap_wait.yml
    EOT
  }
}
