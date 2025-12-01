variable "tenancy_ocid" {}
variable "user_ocid" {}
variable "fingerprint" {}
variable "private_key_path" {}
variable "region" {}

variable "num_node" {
  description = "Number of instances to launch; if greater than 1, RDMA cluster resources are created."
  type        = number
  default     = 1
}

variable "name" {}

variable "ssh_user" {
  description = "SSH user for the instance"
  type        = string
  default     = "ubuntu"
}
variable "shape" { type = string }
variable "ocpus" {
  description = "OCPUs (required for Flex shapes)"
  type        = number
  default     = 1
}
variable "memory_gbs" {
  description = "Memory in GB (required for Flex shapes)"
  type        = number
  default     = 4
}
variable "image_id" {
  description = "Optional explicit image OCID override. If empty, we search by image_name_regex."
  type        = string
  default     = ""
}
variable "image_name_regex" {
  description = "Regex to match image display_name; used if image_id is not set."
  type        = string
  default     = ".*Ubuntu.*22.04.*"
}

variable "image_boot_size" {
  type    = number
  default = 1024
}

variable "subnet_id" { type = string }
variable "compartment_id" { type = string }

variable "cmd_script" { default = "hostname"}
variable "env_vars" { default = "export BOTNAME=leosbot" }
variable "ssh_authorized_key" {
  description = "SSH public key injected into the VM"
  type        = string
}
variable "bastion_host" {
  description = "Optional bastion IP/hostname for remote-exec jump host"
  default     = ""
}
variable "bastion_user" {
  description = "SSH user for bastion (when used)"
  default     = "opc"
}
variable "bastion_private_key_path" {
  description = "Path to private key for bastion (when used)"
  default     = ""
}

variable "vm_private_key_path" {
  description = "Path to private key for direct VM SSH (used by Ansible)"
  type        = string
  default     = "/root/.ssh/nf_executor"
}
