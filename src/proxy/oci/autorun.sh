#!/bin/bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
RUN_DIR_INPUT="${1:-}"
ACTION="${2:-apply}"
VAR_FILE="${3:-$RUN_DIR_INPUT/tfvars.json}"
RUN_LOCAL="${RUN_LOCAL:-0}"

usage() {
  echo "Usage: $0 <run_dir> [apply|check|wait|kill|clean|destroy] [var_file]" >&2
}

require_run_dir_input() {
  if [[ -z "$RUN_DIR_INPUT" ]]; then
    echo "RUN_DIR_INPUT (job run directory) is required" >&2
    usage
    exit 1
  fi
  if [[ ! -d "$RUN_DIR_INPUT" ]]; then
    echo "RUN_DIR_INPUT '$RUN_DIR_INPUT' does not exist" >&2
    exit 1
  fi
}

require_var_file() {
  if [[ -z "$VAR_FILE" || ! -f "$VAR_FILE" ]]; then  
    echo "Terraform var-file '$VAR_FILE' not found" >&2
    exit 1
  fi
}

run_local_cmd_script() {
  # Only meaningful for apply, skip otherwise.
  if [[ "$ACTION" == "check" ]]; then
    return 0
  fi

  if [[ "$ACTION" == "apply" ]]; then
    local script_path="${RUN_DIR_INPUT}/cmd_script.sh"
    echo "=== local cmd_script test ==="
    if [[ ! -f "$script_path" ]]; then
      echo "cmd_script.sh not found in $RUN_DIR_INPUT" >&2
      exit 1
    fi
    echo "Running $script_path"
    bash "$script_path"
  fi
}

terraform_init_with_retries() {
  local init_max_retries=3
  local init_attempt=1
  while true; do
    if terraform init; then
      break
    fi
    if (( init_attempt >= init_max_retries )); then
      echo "terraform init failed after ${init_attempt} attempts; giving up"
      exit 1
    fi
    echo "terraform init failed on attempt ${init_attempt}, retrying in 30 seconds..."
    init_attempt=$((init_attempt+1))
    sleep 30
  done
}

terraform_apply_target_with_retries() {
  local target="$1"
  local max_retries="${2:-3}"
  local destroy_on_fail="${3:-0}"
  local attempt=1
  while true; do
    if terraform apply -target="$target" -auto-approve -var-file="$VAR_FILE"; then
      return 0
    fi
    if (( attempt >= max_retries )); then
      echo "terraform apply -target=${target} failed after ${attempt} attempts; giving up"
      return 1
    fi
    echo "terraform apply -target=${target} failed on attempt ${attempt}, retrying in 30 seconds..."
    attempt=$((attempt+1))
    if [[ "$destroy_on_fail" == "1" ]]; then
      terraform destroy -target="$target" -auto-approve -var-file="$VAR_FILE" || true
    fi
    sleep 30
  done
}

terraform_bootstrap_submit_with_retries() {
  terraform_apply_target_with_retries "null_resource.bootstrap_submit" 3 1
}

terraform_bootstrap_check_with_retries() {
  terraform_apply_target_with_retries "null_resource.bootstrap_check" 3 0
}

terraform_bootstrap_wait_with_retries() {
  terraform_apply_target_with_retries "null_resource.bootstrap_wait" 10 0
}

run_apply() {
  require_var_file
  terraform_init_with_retries
  if ! terraform_bootstrap_submit_with_retries; then
    echo "[bootstrap_submit] failed, aborting"
    exit 1
  fi
}

run_check() {
  require_var_file
  terraform_init_with_retries
  terraform_bootstrap_check_with_retries
}

run_wait() {
  require_var_file
  terraform_init_with_retries
  terraform_bootstrap_wait_with_retries
}

run_clean() {
  local target="$RUN_DIR_INPUT"
  if [[ "$target" == "/" ]]; then
    echo "[clean] Refusing to remove '/'" >&2
    exit 1
  fi
  echo "[clean] Removing $target"
  cd && rm -rf "$target"
}

run_destroy() {
  require_var_file
  terraform destroy -auto-approve -var-file="$VAR_FILE" || true
  run_clean
}

handle_run_local() {
  run_local_cmd_script
}

main() {
  if [[ "$RUN_LOCAL" == "1" ]]; then
    cd "$RUN_DIR_INPUT"
    handle_run_local
    exit 0
  fi

  case "$ACTION" in
    apply)
      cd "$RUN_DIR_INPUT" && run_apply
      ;;
    check)
      cd "$RUN_DIR_INPUT" && run_check
      ;;
    wait)
      cd "$RUN_DIR_INPUT" && run_wait
      ;;
    destroy)
      cd "$RUN_DIR_INPUT" && run_destroy
      ;;
    kill)
      cd "$RUN_DIR_INPUT" && run_destroy
      ;;
    clean)
      cd "$RUN_DIR_INPUT" && run_clean
      ;;
    *)
      echo "Unsupported action '$ACTION'" >&2
      usage
      exit 1
      ;;
  esac
}

require_run_dir_input
main "$@" > "$RUN_DIR_INPUT/output-${ACTION}.log" 2> "$RUN_DIR_INPUT/error-${ACTION}.log"
