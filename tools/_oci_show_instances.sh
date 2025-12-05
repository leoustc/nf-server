#!/usr/bin/env bash

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SCRIPT_PATH="${SCRIPT_DIR}/$(basename "${BASH_SOURCE[0]}")"
ENV_FILE="${SCRIPT_DIR}/../.env"

if [[ -f "$ENV_FILE" ]]; then
  # Load environment variables from the .env file.
  # shellcheck source=/dev/null
  source "$ENV_FILE"
else
  echo "Missing env file: $ENV_FILE" >&2
  exit 1
fi

COMPARTMENT_OCID="${COMPARTMENT_ID:-}"

if [[ -z "$COMPARTMENT_OCID" ]]; then
  echo "COMPARTMENT_ID is not set in $ENV_FILE" >&2
  exit 1
fi

list_instances() {
  local raw_instances
  raw_instances="$(oci compute instance list --compartment-id "$COMPARTMENT_OCID" --all --output json)"

  # Collect boot-volume attachments across all availability domains used by nf-runner instances.
  local attachments_file
  attachments_file="$(mktemp)"
  printf '%s' '[]' > "$attachments_file"
  trap 'rm -f "$attachments_file"' RETURN

  mapfile -t ads < <(echo "$raw_instances" | jq -r '.data[]? | select((."display-name" // "" | tostring | startswith("nf-runner"))) | ."availability-domain"' | sort -u)
  for ad in "${ads[@]}"; do
    if [[ -n "$ad" ]]; then
      resp="$(oci compute boot-volume-attachment list --compartment-id "$COMPARTMENT_OCID" --availability-domain "$ad" --all --output json 2>/dev/null || echo "{}")"
      resp_file="$(mktemp)"
      printf '%s' "$resp" > "$resp_file"
      tmp_file="$(mktemp)"
      jq -s '.[0] + (.[1].data // [])' "$attachments_file" "$resp_file" > "$tmp_file"
      mv "$tmp_file" "$attachments_file"
      rm -f "$resp_file"
    fi
  done

  # Read attachments JSON to avoid jq --argfile (not available in some jq builds).
  local attachments_json
  attachments_json="$(cat "$attachments_file")"

  jq --argjson attachments "$attachments_json" '
    def boot_size_for(id):
      ($attachments // [])
      | map(select(.["instance-id"] == id or .instanceId == id))
      | map(.["boot-volume-size-in-gbs"] // .bootVolumeSizeInGBs)
      | first // null;

    (.data // [])
    | map(
        select(
          (.["display-name"] // "" | tostring | startswith("nf-runner")) and
          ((.["lifecycle-state"] as $state | ["PROVISIONING","RUNNING","TERMINATING","STOPPING","STOPPED"] | index($state)) != null)
        )
      )
    | sort_by(."display-name")
    | map({
        id: .id,
        name: ."display-name",
        state: ."lifecycle-state",
        shape: .shape,
        cpu: ."shape-config".ocpus,
        mem: ."shape-config"."memory-in-gbs",
        gpu: (
          ."shape-config".gpus
          // ."shape-config"."gpu"
          // .["shape-config"]["gpu-count"]
          // .["shape-config"]["gpuCount"]
          // null
        ),
        disk: (
          boot_size_for(.id)
          // .["sourceDetails"]["bootVolumeSizeInGBs"]
          // .["source-details"]["boot-volume-size-in-gbs"]
          ."shape-config"."local-disk-total-size-in-gbs"
          // .["shape-config"]["localDiskTotalSizeInGBs"]
          // .["shape-config"]["local-disk-size-in-gbs"]
          // .["shape-config"]["boot-volume-size-in-gbs"]
          // null
        ),
        net_gbps: (
          ."shape-config"."networking-bandwidth-in-gbps"
          // .["shape-config"]["networkingBandwidthInGbps"]
          // .["shape-config"]["max-networking-bandwidth-in-gbps"]
          // null
        )
      })
  ' <<<"$raw_instances"
}

print_table() {
  local json="$1"
  echo "$json" | jq -r '(["index","name","shape","state","cpu","mem","gpu"] | @tsv),
                        (to_entries[] | [.key + 1, .value.name, .value.shape, .value.state, .value.cpu, .value.mem, (.value.gpu // "")] | @tsv)' | \
    column -t
}

terminate_instances() {
  local json="$1"
  local ids=()
  mapfile -t ids < <(echo "$json" | jq -r '.[].id')

  if [[ ${#ids[@]} -eq 0 ]]; then
    echo "No nf-runner instances found to terminate."
    return 0
  fi

  print_table "$json"
  echo "Terminating ${#ids[@]} instance(s) in 5 seconds... (Ctrl+C to cancel)"
  sleep 5

  for id in "${ids[@]}"; do
    echo "Terminating $id"
    oci compute instance terminate --instance-id "$id" --force >/dev/null
  done
}

command="${1:-}"

instances_json="$(list_instances)"

case "$command" in
  display)
    print_table "$instances_json"
    ;;
  kill)
    terminate_instances "$instances_json"
    ;;
  *)
    print_table "$instances_json"
    ;;
esac
