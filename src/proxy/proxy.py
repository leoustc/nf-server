#!/usr/bin/env python3
import json
import logging
import math
import os
import signal
import subprocess
import shutil
import threading
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pymysql
from pydantic import BaseModel
from pymysql.cursors import DictCursor

logger = logging.getLogger("rest-agent")
logging.basicConfig(level=logging.INFO, format="[%(asctime)s] %(message)s", datefmt="%s")


def log(worker_id: Optional[Any], job_id: Optional[str], action: str, message: str = "") -> None:
    """
    Normalized log format: [worker_id][job_id][action] message
    """
    worker_part = f"[{worker_id}]" if worker_id is not None else ""
    job_part = f"[{job_id[:8]}]" if job_id else ""
    suffix = f" {message}" if message else ""
    logger.info(f"{worker_part}{job_part}[{action}]{suffix}")

class ResourceSpec(BaseModel):
    cpu: Optional[int] = None
    ram: Optional[int] = None
    accelerator: Optional[int] = 0
    shape: Optional[str] = None
    disk: Optional[int] = None

    class Config:
        extra = "allow"


class TFVarConfig(BaseModel):
    tenancy_ocid: str
    user_ocid: str
    fingerprint: str
    private_key_path: str
    region: str
    compartment_id: str
    subnet_id: str
    shape: str
    ocpus: int
    memory_gbs: int
    image_boot_size: int
    num_node: int
    cmd_script: str
    env_vars: str
    ssh_authorized_key: str
    bastion_host: str
    bastion_user: str
    bastion_private_key_path: str
    vm_private_key_path: str
    name: str


DB_HOST = os.getenv("DB_HOST", "mysql")
DB_PORT = int(os.getenv("DB_PORT") or 3306)
DB_USER = os.getenv("DB_USER", "rest")
DB_PASSWORD = os.getenv("DB_PASSWORD", "restpass")
DB_NAME = os.getenv("DB_NAME", "rest_executor")
AUTORUN_SCRIPT = Path(__file__).resolve().parent / "oci" / "autorun.sh"
ACTION_LOG_BASENAME = "output"
RUNNING_RECHECK_DELAY = int(os.getenv("RUNNING_RECHECK_DELAY", "30"))
LOCK_COLUMNS_READY = False
LOCK_COLUMNS_LOCK = threading.Lock()


def get_db_connection():
    return pymysql.connect(
        host=DB_HOST,
        port=DB_PORT,
        user=DB_USER,
        password=DB_PASSWORD,
        database=DB_NAME,
        autocommit=True,
        cursorclass=DictCursor,
    )


def init_db() -> None:
    """
    Initialize the jobs table, retrying until the DB is reachable.
    """
    global LOCK_COLUMNS_READY
    max_attempts = int(os.getenv("DB_INIT_RETRIES", "30"))
    delay = float(os.getenv("DB_INIT_DELAY", "1.0"))
    for attempt in range(1, max_attempts + 1):
        try:
            conn = get_db_connection()
            with conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        CREATE TABLE IF NOT EXISTS jobs (
                            job_id VARCHAR(64) PRIMARY KEY,
                            status VARCHAR(32) NOT NULL,
                            command TEXT NOT NULL,
                            workdir TEXT NOT NULL,
                            env TEXT,
                            metadata TEXT,
                            resources TEXT,
                            returncode INT NULL,
                            stdout LONGTEXT,
                            stderr LONGTEXT,
                            pid INT NULL,
                            job_lock TINYINT DEFAULT 0,
                            lock_time BIGINT DEFAULT 0,
                            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
                        )
                        """
                    )
                try:
                    cur.execute("ALTER TABLE jobs ADD COLUMN job_lock TINYINT DEFAULT 0")
                except Exception:
                    pass
                try:
                    cur.execute("ALTER TABLE jobs ADD COLUMN lock_time BIGINT DEFAULT 0")
                except Exception:
                    pass
            LOCK_COLUMNS_READY = True
            log(None, None, "db", "jobs table is ready")
            return
        except Exception as e:
            log(None, None, "db", f"init attempt {attempt} failed: {e}")
            if attempt == max_attempts:
                log(None, None, "db", "giving up initializing database")
                raise
            time.sleep(delay)


def _ensure_lock_columns() -> None:
    """
    Ensure lock columns exist; set global flag on success.
    """
    global LOCK_COLUMNS_READY
    if LOCK_COLUMNS_READY:
        return
    with LOCK_COLUMNS_LOCK:
        if LOCK_COLUMNS_READY:
            return
        try:
            conn = get_db_connection()
            with conn:
                with conn.cursor() as cur:
                    cur.execute(
                        "ALTER TABLE jobs ADD COLUMN IF NOT EXISTS job_lock TINYINT DEFAULT 0"
                    )
                    cur.execute(
                        "ALTER TABLE jobs ADD COLUMN IF NOT EXISTS lock_time BIGINT DEFAULT 0"
                    )
            LOCK_COLUMNS_READY = True
        except Exception as exc:
            log(None, None, "lock", f"ensure columns failed: {exc}")


def reset_running_jobs_to_failed() -> int:
    """
    On startup, mark any jobs left in 'running' as 'failed'.
    Returns the number of rows updated.
    """
    conn = get_db_connection()
    affected = 0
    with conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE jobs SET status = %s WHERE status = %s",
                ("failed", "running"),
            )
            affected = cur.rowcount or 0
    if affected:
        log(None, None, "db", f"reset {affected} running jobs to failed on startup")
    return affected


def _fetch_job(job_id: str) -> Optional[Dict[str, Any]]:
    conn = get_db_connection()
    with conn:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM jobs WHERE job_id = %s", (job_id,))
            return cur.fetchone()

def _update_job_after_run(
    job_id: str, returncode: int, stdout: str, stderr: str, status: str
) -> None:
    conn = get_db_connection()
    with conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE jobs
                SET returncode = %s, stdout = %s, stderr = %s, status = %s
                WHERE job_id = %s
                """,
                (returncode, stdout, stderr, status, job_id),
            )
    log(None, job_id, "status", f"changed {status}")


def _read_action_logs(run_dir: Path, action: str) -> Tuple[str, str]:
    """
    Read autorun stdout/stderr log files for the given action.
    """
    out_file = run_dir / f"{ACTION_LOG_BASENAME}-{action}.log"
    err_file = run_dir / f"error-{action}.log"
    stdout = ""
    stderr = ""
    try:
        if out_file.exists():
            stdout = out_file.read_text(errors="ignore")
    except Exception:
        stdout = ""
    try:
        if err_file.exists():
            stderr = err_file.read_text(errors="ignore")
    except Exception:
        stderr = ""
    return stdout, stderr


def _update_job_logs_and_status(
    job_id: str,
    stdout_append: str = "",
    stderr_append: str = "",
    status: Optional[str] = None,
    returncode: Optional[int] = None,
) -> Tuple[str, str]:
    """
    Append stdout/stderr for a job and optionally update status/returncode.
    """
    conn = get_db_connection()
    with conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT stdout, stderr, returncode FROM jobs WHERE job_id = %s",
                (job_id,),
            )
            row = cur.fetchone() or {}
            current_stdout = row.get("stdout") or ""
            current_stderr = row.get("stderr") or ""
            new_stdout = current_stdout + (stdout_append or "")
            new_stderr = current_stderr + (stderr_append or "")

            fields = ["stdout = %s", "stderr = %s"]
            params: List[Any] = [new_stdout, new_stderr]
            if status is not None:
                fields.append("status = %s")
                params.append(status)
            if returncode is not None:
                fields.append("returncode = %s")
                params.append(returncode)
            params.append(job_id)
            sql = f"UPDATE jobs SET {', '.join(fields)} WHERE job_id = %s"
            cur.execute(sql, tuple(params))
    return new_stdout, new_stderr


def normalize_resources(resources: Optional[ResourceSpec]) -> Optional[ResourceSpec]:
    """
    Allow alternate field names (accelerator, acceleratornum, acceleratorshape, acceleratorShape) and coerce into ResourceSpec.
    """
    if resources is None:
        return None

    data = resources.dict() if isinstance(resources, ResourceSpec) else dict(resources)

    def get_first(keys):
        for key in keys:
            if key in data and data[key] is not None:
                return data[key]
        return None

    accelerator_raw = data.get("accelerator")
    if accelerator_raw is None:
        accelerator_raw = data.get("gpu")
    if accelerator_raw is None:
        accelerator_raw = data.get("acceleratornum")
    try:
        accelerator = int(accelerator_raw) if accelerator_raw is not None else 0
    except Exception:
        accelerator = 0
    shape = get_first(
        ("acceleratorShape", "acceleratorshape", "accelerator_shape", "shape", "gpushape")
    )
    disk = get_first(
        (
            "disk",
            "disk_size",
            "disk_gb",
            "image_boot_size",
            "boot_disk",
            "bootdisk",
            "bootdisk_size",
        )
    )

    return ResourceSpec(
        cpu=data.get("cpu"),
        ram=data.get("ram"),
        accelerator=accelerator,
        shape=shape,
        disk=disk,
    )


def normalize_ram_gb(value: Optional[Any]) -> Optional[int]:
    """
    Convert memory values expressed in bytes (as emitted by Nextflow) to whole GB.
    """
    if value is None:
        return None
    try:
        ram = float(value)
    except (TypeError, ValueError):
        return None
    if ram <= 0:
        return None
    if ram > 1_000_000:
        ram = math.ceil(ram / (1024 ** 3))
    return int(ram)


def build_tfvars(worker_id: int, job_id: str, row: Dict[str, Any], name: str) -> Dict[str, Any]:
    """
    Assemble tfvars payload for autorun/terraform apply using TFVarConfig.
    """

    log(worker_id, job_id, "row", f"{row}")

    try:
        raw_meta = row.get("metadata") or "{}"
        metadata: Dict[str, Any] = json.loads(raw_meta)
    except Exception:
        metadata = {}

    storage: Dict[str, Any] = metadata.get("storage") or {}
    executor_storage: Dict[str, Any] = metadata.get("workstor") or {}
    
    params_input = metadata.get("paramsInput") or []
    if isinstance(params_input, str):
        params_input = [params_input]
    elif not isinstance(params_input, list):
        try:
            params_input = list(params_input)
        except Exception:
            params_input = []
    params_input_type = str(metadata.get("paramsInputType") or "").lower()

    resources_obj: Optional[ResourceSpec] = None
    raw_resources = row.get("resources")
    if raw_resources:
        try:
            resources_dict = json.loads(raw_resources)
            resources_obj = normalize_resources(resources_dict)
        except Exception as exc:
            log(None, job_id, "resources", f"unable to parse resources: {exc}")

    def as_int(val: Optional[Any], fallback: int) -> int:
        try:
            return int(val)
        except Exception:
            return fallback

    tenancy = os.getenv("TF_VAR_tenancy_ocid", "")
    user = os.getenv("TF_VAR_user_ocid", "")
    fingerprint = os.getenv("TF_VAR_fingerprint", "")
    region = os.getenv("TF_VAR_region", "")
    key_file = os.getenv("TF_VAR_private_key_path", "")

    shape_default = os.getenv("DEFAULT_SHAPE", "VM.Standard.E3.Flex")
    cpu_default = int(os.getenv("DEFAULT_OCPU", "1"))
    ram_default = int(os.getenv("DEFAULT_MEMORY_IN_GB", "2"))
    boot_default = int(os.getenv("DEFAULT_BOOTDISK_IN_GB", "1024"))

    accelerator = 0
    if resources_obj is not None:
        accelerator = as_int(resources_obj.accelerator, 0)
        shape = resources_obj.shape or shape_default
        ram_norm = normalize_ram_gb(resources_obj.ram)
        ram = as_int(ram_norm, ram_default)
        cpu = as_int(resources_obj.cpu, cpu_default)
        boot = as_int(resources_obj.disk, boot_default)
    else:
        shape = shape_default
        cpu = cpu_default
        ram = ram_default
        boot = boot_default

    num_nodes = max(1, accelerator)

    s3_bucket = str(storage.get("bucket") or "")
    s3_workdir = str(storage.get("s3_workdir") or row.get("workdir") or "")
    s3_endpoint = str(storage.get("endpoint") or "")
    s3_region = str(storage.get("region") or "us-east-1")
    s3_access = str(storage.get("accessKey") or "")
    s3_secret = str(storage.get("secretKey") or "")
    nxf_command = str(row.get("command") or "bash .command.run")
    nxf_workdir = str(row.get("workdir") or "/opt/nxf-work")
    nxf_s3_uri = f"s3:/{s3_workdir}"

    log(worker_id, job_id, "resources", f"accelerator={accelerator} shape={shape} cpu={cpu} ram={ram} boot={boot}")
    log(worker_id, job_id, "storage", f"endpoint={s3_endpoint} region={s3_region} bucket={s3_bucket} s3_workdir={s3_workdir}")
    if executor_storage:
        exec_endpoint = str(executor_storage.get("endpoint") or "")
        exec_bucket = str(executor_storage.get("bucket") or executor_storage.get("user_param") or "")
        log(worker_id, job_id, "workstor", f"endpoint={exec_endpoint} bucket={exec_bucket}")

    env_vars = "\n".join(
        [
            f'export S3_ENDPOINT="{s3_endpoint}"',
            f'export S3_REGION="{s3_region}"',
            f'export S3_BUCKET="{s3_bucket}"',
            f'export S3_ACCESS_KEY="{s3_access}"',
            f'export S3_SECRET_KEY="{s3_secret}"',
            f'export S3_WORKDIR="{s3_workdir}"',
            f'export NXF_COMMAND="{nxf_command}"',
            f'export NXF_WORKDIR="{nxf_workdir}"',
            f'export NXF_S3_URI="{nxf_s3_uri}"',
        ]
    )

    stage_inputs_lines = ""
    if params_input_type == "file":
        for src in params_input:
            if not src:
                continue
            input_file = str(src)
            dir_name = str(Path(input_file).parent)  # -> "/nf-data/input"
            stage_inputs_lines += f"mkdir -p {dir_name}\naws --endpoint-url {s3_endpoint} s3 cp s3:/{input_file} {input_file}\n"

    if s3_endpoint != "" and s3_bucket != "":
        s3_url = f"s3:/{s3_workdir}"
        localdir = f"{s3_workdir}"
        
        cmd_script = f"""
#!/bin/bash
set -euo pipefail
{env_vars}
export AWS_ACCESS_KEY_ID={s3_access}
export AWS_SECRET_ACCESS_KEY={s3_secret}
export AWS_DEFAULT_REGION={s3_region}
export AWS_S3_FORCE_PATH_STYLE=1

{stage_inputs_lines}

mkdir -p {localdir}
aws --endpoint-url {s3_endpoint} s3 sync {s3_url}/ {localdir}/

cd {localdir}
eval {nxf_command}

aws --endpoint-url {s3_endpoint} s3 sync {localdir}/ {s3_url}/
""".strip()
    else:
        cmd_script = f"""
#!/bin/bash
set -euo pipefail
{env_vars}

cd {nxf_workdir}
eval {nxf_command}
""".strip()


    log(worker_id, job_id, "cmd_script", f"{cmd_script}")

    bastion_host = os.getenv("TF_VAR_bastion_host") or os.getenv("BASTION_IP", "")
    bastion_user = os.getenv("TF_VAR_bastion_user") or os.getenv("BASTION_USER", "")
    bastion_key = os.getenv("TF_VAR_bastion_private_key_path") or os.getenv(
        "BASTION_SSH_KEY", ""
    )

    vm_key = os.getenv("TF_VAR_vm_private_key_path") or os.getenv(
        "VM_SSH_PRIVATE_KEY_PATH", ""
    )

    ssh_key = os.getenv("TF_VAR_ssh_authorized_key", "")

    selected_shape = shape_default if accelerator == 0 else (resources_obj.shape if resources_obj and resources_obj.shape else shape_default)

    cfg = TFVarConfig(
        tenancy_ocid=tenancy,
        user_ocid=user,
        fingerprint=fingerprint,
        private_key_path=str(Path(key_file).expanduser()),
        region=region,
        compartment_id=os.getenv("COMPARTMENT_ID", os.getenv("TF_VAR_compartment_id", "")),
        subnet_id=os.getenv("SUBNET_ID", os.getenv("TF_VAR_subnet_id", "")),
        shape=selected_shape,
        ocpus=cpu,
        memory_gbs=ram,
        image_boot_size=boot,
        num_node=num_nodes,
        cmd_script=cmd_script,
        env_vars=env_vars,
        ssh_authorized_key=ssh_key,
        bastion_host=bastion_host,
        bastion_user=bastion_user or "opc",
        bastion_private_key_path=bastion_key,
        vm_private_key_path=str(Path(vm_key).expanduser()),
        name=name,
    )

    return cfg.dict(exclude_none=True)


def _run_job_sync(work_id: int, job_id: str) -> Tuple[int, str, str]:
    """
    Synchronous job runner executed in a worker thread, backed by MySQL.
    """
    row = _fetch_job(job_id)
    if not row:
        err = f"job {job_id} not found in DB"
        log(work_id, job_id, "error", err)
        return 1, "", err

    run_dir, _ = _get_run_dir(job_id)
    run_dir.mkdir(parents=True, exist_ok=True)
    _set_metadata_value(job_id, "runDir", str(run_dir))

    tfvars = build_tfvars(work_id, job_id, row, name=f"{work_id}-{job_id[:8]}")
    tfvars_path = run_dir / "tfvars.json"
    tfvars_path.write_text(json.dumps(tfvars, indent=2))

    script_dir = AUTORUN_SCRIPT.parent
    for tf_file in script_dir.glob("*.tf"):
        shutil.copy2(tf_file, run_dir / tf_file.name)
    for yml_file in script_dir.glob("*.yml"):
        shutil.copy2(yml_file, run_dir / yml_file.name)

    cmd_script_content = tfvars.get("cmd_script") or ""
    cmd_script_path = run_dir / "cmd_script.sh"
    if cmd_script_content:
        if not cmd_script_content.endswith("\n"):
            cmd_script_content = f"{cmd_script_content}\n"
        cmd_script_path.write_text(cmd_script_content)
        log(work_id, job_id, "cmd_script", f"wrote cmd_script.sh to {cmd_script_path}")
    else:
        log(work_id, job_id, "cmd_script", "empty; no cmd_script.sh written")

    # log(work_id, job_id, "cmd_script", f"\n{cmd_script_content}")

    stdout_parts: List[str] = []
    stderr_parts: List[str] = []

    stdout_parts.append(f"[executor] (job={job_id}, worker={work_id})\ncmd_script:\n{cmd_script_content}")
    stderr_parts.append(f"[executor] (job={job_id}, worker={work_id})\ncmd_script:\n{cmd_script_content}")

    apply_cmd = ["bash", str(AUTORUN_SCRIPT), str(run_dir), "apply"] # the tfvars path is implied by default
    log(work_id, job_id, "apply", f"applying infra via {apply_cmd}")
    apply_proc = subprocess.run(
        apply_cmd,
        cwd=str(AUTORUN_SCRIPT.parent),
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    apply_stdout, apply_stderr = _read_action_logs(run_dir, "apply")
    stdout_parts.append(apply_proc.stdout or apply_stdout)

    success = apply_proc.returncode == 0
    status = "running" if success else "failed"
    if not success:
        stderr_parts.append(
            f"[executor] (job={job_id}, worker={work_id}) autorun apply failed (code {apply_proc.returncode})\n"
        )
    stderr_parts.append(apply_proc.stderr or apply_stderr)

    log(work_id, job_id, "apply", status)

    return (0 if success else 1), "".join(stdout_parts), "".join(stderr_parts)


def _run_autorun_action(
    run_dir: Path, action: str, tfvars_path: Optional[Path] = None
) -> Tuple[int, str, str]:
    """
    Invoke autorun.sh for a given action and return (returncode, stdout, stderr) by reading logs.
    """
    if tfvars_path is None:
        tfvars_path = run_dir / "tfvars.json"
    cmd = ["bash", str(AUTORUN_SCRIPT), str(run_dir), action, str(tfvars_path)]
    proc = subprocess.run(
        cmd,
        cwd=str(AUTORUN_SCRIPT.parent),
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    stdout, stderr = _read_action_logs(run_dir, action)
    return proc.returncode, stdout, stderr

def _claim_next_job_id() -> Optional[str]:
    """
    Atomically claim the next queued job by switching it to running.
    """
    conn = get_db_connection()
    with conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT job_id FROM jobs WHERE status = %s ORDER BY created_at LIMIT 1",
                ("queued",),
            )
            row = cur.fetchone()
            if not row:
                return None
            job_id = row["job_id"]
            cur.execute(
                "UPDATE jobs SET status = %s WHERE job_id = %s AND status = %s",
                ("running", job_id, "queued"),
            )
            if cur.rowcount == 0:
                return None
            return job_id

def _update_job_status(job_id: str, from_status: str, to_status: str) -> bool:
    """
    Atomically move a job from one status to another.
    """
    conn = get_db_connection()
    with conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE jobs SET status = %s WHERE job_id = %s AND status = %s",
                (to_status, job_id, from_status),
            )
            return cur.rowcount > 0


def _set_lock(job_id: str, locked: int) -> None:
    """
    Set the lock flag and reset lock_time when unlocking.
    """
    _ensure_lock_columns()
    conn = get_db_connection()
    with conn:
        with conn.cursor() as cur:
            try:
                if locked:
                    cur.execute(
                        "UPDATE jobs SET job_lock = 1, lock_time = UNIX_TIMESTAMP() WHERE job_id = %s",
                        (job_id,),
                    )
                else:
                    cur.execute(
                        "UPDATE jobs SET job_lock = 0, lock_time = 0 WHERE job_id = %s",
                        (job_id,),
                    )
            except Exception as exc:
                log(None, job_id, "lock", f"set_lock failed: {exc}")


def _claim_job_with_lock(status: str, new_status: Optional[str] = None) -> Optional[str]:
    """
    Claim the next job with given status when lock=0, set lock=1 and optionally change status.
    """
    _ensure_lock_columns()
    conn = get_db_connection()
    with conn:
        with conn.cursor() as cur:
            try:
                cur.execute(
                    """
                    SELECT job_id FROM jobs
                    WHERE status = %s AND job_lock = 0
                    ORDER BY created_at
                    LIMIT 1
                    """,
                    (status,),
                )
                row = cur.fetchone()
                if not row:
                    return None
                job_id = row["job_id"]
                target_status = new_status or status
                cur.execute(
                    """
                    UPDATE jobs
                    SET job_lock = 1, lock_time = UNIX_TIMESTAMP(), status = %s
                    WHERE job_id = %s AND status = %s AND job_lock = 0
                    """,
                    (target_status, job_id, status),
                )
                if cur.rowcount == 0:
                    return None
                return job_id
            except Exception as exc:
                log(None, None, "lock", f"claim failed for status={status}: {exc}")
                return None


def _set_metadata_value(job_id: str, key: str, value: Any) -> None:
    """
    Update a single key in the metadata JSON for a job.
    """
    conn = get_db_connection()
    with conn:
        with conn.cursor() as cur:
            cur.execute("SELECT metadata FROM jobs WHERE job_id = %s", (job_id,))
            row = cur.fetchone()
            raw_metadata = row.get("metadata") if row else "{}"
            try:
                metadata_obj = json.loads(raw_metadata)
                if not isinstance(metadata_obj, dict):
                    metadata_obj = {}
            except Exception:
                metadata_obj = {}
            metadata_obj[key] = value
            metadata_str = json.dumps(metadata_obj)
            cur.execute(
                "UPDATE jobs SET metadata = %s WHERE job_id = %s",
                (metadata_str, job_id),
            )


def _get_run_dir(job_id: str) -> Tuple[Path, int]:
    """
    Return the bot run directory path (prefer metadata.runDir) and a flag: 0 if it exists, 1 if not.
    """
    run_base = Path(os.getenv("BOT_RUN_BASE_DIR", "/opt/bot"))
    run_dir: Optional[Path] = None

    # Try to read runDir from metadata for stability across workers.
    try:
        conn = get_db_connection()
        with conn:
            with conn.cursor() as cur:
                cur.execute("SELECT metadata FROM jobs WHERE job_id = %s", (job_id,))
                row = cur.fetchone()
                if row:
                    raw_meta = row.get("metadata") or "{}"
                    meta_obj = json.loads(raw_meta)
                    if isinstance(meta_obj, dict):
                        run_dir_meta = meta_obj.get("runDir")
                        if run_dir_meta:
                            run_dir = Path(run_dir_meta)
    except Exception:
        run_dir = None

    if run_dir is None:
        run_dir = run_base / f".bot-{job_id}"

    return run_dir, (0 if run_dir.exists() else 1)

def _run_failed_job(work_id: int, job_id: str) -> bool:
    """
    Handle a job found in failed queue; run cleanup and mark cleaned.
    """
    run_dir, _ = _get_run_dir(job_id)

    rc, out, err = _run_autorun_action(run_dir, "destroy")

    _set_lock(job_id, 0)
    return True

def _run_running_job(work_id: int, job_id: str) -> bool:
    """
    Handle a job in running state by checking completion; keep it running if not done.
    """
    run_dir, _ = _get_run_dir(job_id)

    rc, out, err = _run_autorun_action(run_dir, "check")

    if rc == 0:
        log(work_id, job_id, "check", f"finished rc={rc}")
        _update_job_logs_and_status(
            job_id,
            out,
            err,
            status="finished",
            returncode=rc,
        )
        _run_autorun_action(run_dir, "destroy")
        _set_lock(job_id, 0)
        return True

    if rc != 0:
        log(work_id, job_id, "check", f"still running rc={rc}")

    _set_lock(job_id, 0)
    return True

def _run_queued_job(work_id: int, job_id: str) -> bool:
    """
    Run a job originally queued (now pending) using the synchronous runner.
    """
    run_dir, _ = _get_run_dir(job_id)
    returncode, stdout, stderr = _run_job_sync(work_id, job_id)
    status = "running" if returncode == 0 else "failed"
    if returncode != 0:
        _set_metadata_value(job_id, "error", stderr or "job failed")
        _run_autorun_action(run_dir, "destroy")
        _update_job_after_run(job_id, returncode, stdout, stderr, status)
    _set_lock(job_id, 0)
    log(work_id, job_id, "finish", f"status={status}")
    return True

def _claim_and_run_next_job(work_id: int) -> bool:
    """
    Claim the next queued job and run it synchronously in the same thread.
    Returns True if a job was claimed and run, False if no job was available.
    """
    job_id = _claim_job_with_lock("running")
    if job_id:
        log(work_id, job_id, "claim", "running job")
        return _run_running_job(work_id, job_id)

    job_id = _claim_job_with_lock("killing", new_status="failed")
    if job_id:
        log(work_id, job_id, "claim", "failed job")
        return _run_failed_job(work_id, job_id)

    job_id = _claim_job_with_lock("queued", new_status="running")
    if job_id:
        log(work_id, job_id, "claim", "queued job")
        return _run_queued_job(work_id, job_id)

    #job_id = _claim_next_job_id_new("pending")
    #if job_id:
    #    log(f"[executor] worker {work_id} claimed for pending job {job_id}")
    #    return _run_pending_job(work_id, job_id)
    return False


def worker_loop(worker_id: int, stop_event: threading.Event) -> None:
    """
    Background worker that pulls jobs from the DB and runs them.
    """
    log(worker_id, None, "worker", "started")
    while not stop_event.is_set():
        try:
            processed = _claim_and_run_next_job(worker_id)
        except Exception as e:
            log(worker_id, None, "error", str(e))
            processed = False
        if not processed:
            stop_event.wait(5.0)


def main() -> None:
    if not AUTORUN_SCRIPT.exists():
        log(None, None, "agent", f"autorun script not found at {AUTORUN_SCRIPT}; exiting")
        raise SystemExit(1)

    stop_event = threading.Event()

    def handle_signal(signum, frame):
        log(None, None, "agent", f"received signal {signum}, shutting down")
        stop_event.set()

    signal.signal(signal.SIGTERM, handle_signal)
    signal.signal(signal.SIGINT, handle_signal)

    init_db()
    reset_running_jobs_to_failed()

    worker_env = os.getenv("EXECUTOR_WORKERS")
    worker_count = int(worker_env) if worker_env else 1

    threads: List[threading.Thread] = []
    for i in range(worker_count):
        t = threading.Thread(target=worker_loop, args=(i, stop_event), daemon=False)
        t.start()
        threads.append(t)

    log(None, None, "agent", f"started {worker_count} workers")

    try:
        for t in threads:
            t.join()
    except KeyboardInterrupt:
        stop_event.set()
        for t in threads:
            t.join()


if __name__ == "__main__":
    main()
