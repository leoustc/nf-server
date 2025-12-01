terraform {
  required_version = ">= 1.0.0"
  required_providers {
    oci = {
      source  = "oracle/oci"
    }
  }
}
provider "oci" {
  tenancy_ocid     = var.tenancy_ocid
  user_ocid        = var.user_ocid
  fingerprint      = var.fingerprint
  private_key_path = var.private_key_path
  region           = var.region
}

data "oci_identity_availability_domains" "ads" {
  compartment_id = var.compartment_id
}

data "oci_core_images" "ubuntu" {
  compartment_id           = var.compartment_id
  operating_system         = "Canonical Ubuntu"
  shape                    = var.shape
  sort_by                  = "TIMECREATED"
  sort_order               = "DESC"

  filter {
    name   = "display_name"
    values = [var.image_name_regex]
    regex  = true
  }
}

data "oci_core_images" "ubuntu_fallback" {
  compartment_id   = var.compartment_id
  operating_system = "Canonical Ubuntu"
  shape            = var.shape
  sort_by          = "TIMECREATED"
  sort_order       = "DESC"
}

locals {
  # Check if the shape name includes "flex" (case-insensitive)
  is_flex = can(regex("flex", lower(var.shape)))
  is_bm = can(regex("BM", upper(var.shape)))
  resolved_image_id = var.image_id != "" ? var.image_id : (
    length(data.oci_core_images.ubuntu.images) > 0 ? data.oci_core_images.ubuntu.images[0].id :
    length(data.oci_core_images.ubuntu_fallback.images) > 0 ? data.oci_core_images.ubuntu_fallback.images[0].id : ""
  )
}

# Stage 1, create the instance

resource "oci_core_compute_cluster" "compute_cluster" {
  count               = (var.num_node > 1 && local.is_bm) ? 1 : 0
  availability_domain = data.oci_identity_availability_domains.ads.availability_domains[0].name
  compartment_id      = var.compartment_id
  display_name        = "rdma-cluster-${count.index}"
}

resource "oci_core_instance" "instance" {
  count = var.num_node
  compartment_id      = var.compartment_id
  availability_domain = data.oci_identity_availability_domains.ads.availability_domains[0].name
  shape               = var.shape
  display_name        = "nf-runner-${var.name}-${count.index}"

  compute_cluster_id = (var.num_node > 1 && local.is_bm) ? try(oci_core_compute_cluster.compute_cluster[0].id, null) : null

  dynamic "shape_config" {
    for_each = local.is_flex ? [1] : []
    content {
      ocpus         = var.ocpus
      memory_in_gbs = var.memory_gbs
    }
  }

  source_details {
    source_type               = "image"
    source_id                 = local.resolved_image_id
    boot_volume_size_in_gbs   = var.image_boot_size
  }

  create_vnic_details {
    subnet_id              = var.subnet_id
    assign_public_ip       = false
    skip_source_dest_check = true
  }

  metadata = {
    ssh_authorized_keys = trimspace(var.ssh_authorized_key)
  }

  agent_config {
    are_all_plugins_disabled = false
    is_management_disabled   = true
    is_monitoring_disabled   = false

    plugins_config {
      name          = "Compute HPC RDMA Auto-Configuration"
      desired_state = "ENABLED"
    }

    plugins_config {
      name          = "Compute HPC RDMA Authentication"
      desired_state = "ENABLED"
    }

    plugins_config {
      name          = "Compute Instance Run Command"
      desired_state = "ENABLED"
    }

    plugins_config {
      name          = "Management Agent"
      desired_state = "ENABLED"
    }

    plugins_config {
      name          = "OS Management Service Agent"
      desired_state = "DISABLED"
    }

    plugins_config {
      name          = "Compute Instance Monitoring"
      desired_state = "ENABLED"
    }
  }

  lifecycle {
    precondition {
      condition     = local.resolved_image_id != ""
      error_message = "No matching image found. Adjust image_name_regex or set image_id explicitly."
    }
  }
}

output "instance_id" {
  value = oci_core_instance.instance[*].id
}

output "private_ip" {
  value = oci_core_instance.instance[*].private_ip
}
