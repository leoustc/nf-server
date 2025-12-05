#!/usr/bin/env python3
import asyncio
import json
import logging
import fcntl
import os
import time
import uuid
from typing import Any, Dict, Optional

from fastapi import Depends, FastAPI, HTTPException, Header, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field
import uvicorn
import pymysql
from pymysql.cursors import DictCursor

logger = logging.getLogger("rest-executor")
logging.basicConfig(level=logging.INFO, format="[%(asctime)s] %(message)s", datefmt="%s")

app = FastAPI(title="Nextflow REST Executor Service")

def log(endpoint: str, job_id: Optional[str] = None, status: str = None, message: str = None) -> None:
    """
    Normalized log format: [gateway][endpoint][job id][status] message
    """
    job_part = f"[{job_id[:8]}]" if job_id else ""
    suffix = f" {message}" if message else ""
    logger.info(f"[gateway][{endpoint}]{job_part}[{status}]{suffix}")

class ResourceSpec(BaseModel):
    cpu: Optional[int] = None
    ram: Optional[int] = None
    accelerator: Optional[int] = 0
    shape: Optional[str] = None
    disk: Optional[int] = None

    class Config:
        extra = "allow"


class StorageConfig(BaseModel):
    endpoint: Optional[str] = None
    region: Optional[str] = None
    bucket: Optional[str] = None
    accessKey: Optional[str] = None
    secretKey: Optional[str] = None
    s3_workdir: Optional[str] = None
    s3PathStyleAccess: Optional[bool] = None

class JobSubmit(BaseModel):
    # For Nextflow integration, you'd likely pass these:
    command: str                # shell command, e.g. "bash .command.run"
    workdir: str                # working directory path (local or remote)
    env: Dict[str, str] = {}    # environment vars
    metadata: Dict[str, Any] = {}   # workflowId, taskId, process name, etc.
    resources: Optional[ResourceSpec] = None
    storage: Optional[StorageConfig] = None   # S3/minio endpoint, region, and user-defined param per job
    executor_storage: Optional[StorageConfig] = Field(default=None, alias="executorStorage")

class JobStatus(BaseModel):
    job_id: str
    status: str              # queued | running | finished | failed | killed | killing | cleaned
    returncode: Optional[int]
    stdout: Optional[str]
    stderr: Optional[str]
    metadata: Dict[str, Any]
    resources: Optional[ResourceSpec] = None


DB_HOST = os.getenv("DB_HOST", "mysql")
DB_PORT = int(os.getenv("DB_PORT") or 3306)
DB_USER = os.getenv("DB_USER", "rest")
DB_PASSWORD = os.getenv("DB_PASSWORD", "restpass")
DB_NAME = os.getenv("DB_NAME", "rest_executor")
_raw_keys = [os.getenv("API_KEYS", ""), os.getenv("NXF_RES_AUTH_API_KEY", "")]
API_KEYS = {
    t.strip()
    for raw in _raw_keys
    for t in raw.split(",")
    if t and t.strip()
}
_raw_heartbeat = (os.getenv("HEARTBEAT_SECONDS") or "").strip()
HEARTBEAT_SECONDS = float(_raw_heartbeat or "3")
HEARTBEAT_RANK = int(
    os.getenv("GATEWAY_RANK")
    or os.getenv("WORKER_RANK")
    or os.getenv("UVICORN_WORKER")  # set by uvicorn --workers, starts at 0
    or "0"
)
_heartbeat_task: Optional[asyncio.Task] = None
_heartbeat_lock_fd: Optional[int] = None


def _acquire_heartbeat_lock() -> bool:
    """
    Attempt to acquire a simple file lock so only one process runs the heartbeat.
    """
    global _heartbeat_lock_fd
    lock_path = os.getenv("HEARTBEAT_LOCK_FILE", "/tmp/gateway-heartbeat.lock")
    fd = os.open(lock_path, os.O_RDWR | os.O_CREAT, 0o644)
    try:
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        _heartbeat_lock_fd = fd
        return True
    except BlockingIOError:
        os.close(fd)
        return False
    except Exception as exc:
        os.close(fd)
        log("heartbeat", None, "lock-error", str(exc))
        return False


def _release_heartbeat_lock() -> None:
    global _heartbeat_lock_fd
    if _heartbeat_lock_fd is None:
        return
    try:
        fcntl.flock(_heartbeat_lock_fd, fcntl.LOCK_UN)
    except Exception:
        pass
    try:
        os.close(_heartbeat_lock_fd)
    except Exception:
        pass
    _heartbeat_lock_fd = None

def require_api_key(
    api_key: Optional[str] = Header(
        default=None, alias="api_key", convert_underscores=False
    ),
    x_api_key: Optional[str] = Header(default=None, alias="X-API-Key"),
) -> Optional[str]:
    """
    Require clients to pass API keys via the api_key header when configured.
    """
    if not API_KEYS:
        return None
    provided = api_key or x_api_key
    if not provided or provided not in API_KEYS:
        raise HTTPException(status_code=401, detail="invalid api key")
    return provided


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

@app.exception_handler(pymysql.MySQLError)
async def mysql_exception_handler(request: Request, exc: pymysql.MySQLError):
    log("db", None, "error", str(exc))
    return JSONResponse(
        status_code=503,
        content={"detail": "database unavailable"},
    )


@app.exception_handler(Exception)
async def unhandled_exception_handler(request: Request, exc: Exception):
    log("gateway", None, "error", str(exc))
    return JSONResponse(
        status_code=500,
        content={"detail": "internal server error"},
    )


@app.on_event("startup")
async def _startup_heartbeat() -> None:
    global _heartbeat_task
    if HEARTBEAT_SECONDS <= 0:
        log("heartbeat", None, "disabled", "HEARTBEAT_SECONDS <= 0")
        return
    if HEARTBEAT_RANK != 0:
        log("heartbeat", None, "skipped", f"rank={HEARTBEAT_RANK}")
        return
    if not await asyncio.to_thread(_acquire_heartbeat_lock):
        log("heartbeat", None, "skipped", "another process owns the heartbeat lock")
        return
    _heartbeat_task = asyncio.create_task(_heartbeat_loop())
    log("heartbeat", None, "started", f"rank={HEARTBEAT_RANK}, interval {HEARTBEAT_SECONDS}s")


@app.on_event("shutdown")
async def _shutdown_heartbeat() -> None:
    global _heartbeat_task
    if _heartbeat_task:
        _heartbeat_task.cancel()
        try:
            await _heartbeat_task
        except asyncio.CancelledError:
            pass
    await asyncio.to_thread(_release_heartbeat_lock)


def init_db() -> None:
    """
    Initialize the jobs table, retrying until the DB is reachable.
    """
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
            log("db", None, "ready", "jobs table is ready")
            return
        except Exception as e:
            log("db", None, "retry", f"attempt {attempt} failed: {e}")
            if attempt == max_attempts:
                log("db", None, "error", "giving up initializing database")
                raise
            time.sleep(delay)


def reset_running_jobs_to_failed() -> int:
    """
    On startup, mark any jobs left in 'running' as 'killing' and any still 'queued' as 'failed', and clear locks (including any stuck in 'killing').
    Returns the number of rows updated.
    """
    conn = get_db_connection()
    affected = 0
    with conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE jobs
                SET status = CASE
                    WHEN status = %s THEN %s
                    WHEN status = %s THEN %s
                    ELSE status
                END,
                    job_lock = 0,
                    lock_time = 0
                WHERE status IN (%s, %s, %s)
                """,
                ("running", "killing", "queued", "failed", "running", "queued", "killing"),
            )
            affected = cur.rowcount or 0
    if affected:
        log("db", None, "reset", f"reset {affected} jobs (running->killing, queued->failed, killing cleared locks) on startup")
    return affected


def get_job_status_counts() -> Dict[str, int]:
    """
    Return the number of jobs in each major status plus worker count.
    Keys: running, queued, pending, finished, failed, killing, killed, cleaned.
    """
    counts: Dict[str, int] = {
        "running": 0,
        "queued": 0,
        "finished": 0,
        "failed": 0,
        "killing": 0,
    }

    conn = get_db_connection()
    with conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT status, COUNT(*) AS cnt FROM jobs GROUP BY status"
            )
            for row in cur.fetchall():
                status = row.get("status")
                if status in counts:
                    counts[status] = int(row.get("cnt") or 0)

    return counts


async def _heartbeat_loop() -> None:
    while True:
        try:
            counts = await asyncio.to_thread(get_job_status_counts)
            log("heartbeat", None, "ok", f"queue {counts}")
        except asyncio.CancelledError:
            raise
        except Exception as e:
            log("heartbeat", None, "error", str(e))
        await asyncio.sleep(HEARTBEAT_SECONDS)


def _insert_job(job_id: str, submit: JobSubmit) -> None:
    conn = get_db_connection()
    with conn:
        with conn.cursor() as cur:
            # merge storage config into metadata so workers/VMs can see per-job S3 info
            metadata: Dict[str, Any] = dict(submit.metadata or {})
            if submit.storage:
                metadata.setdefault("storage", submit.storage.dict(exclude_none=True))
            if submit.executor_storage:
                metadata.setdefault("workstor", submit.executor_storage.dict(exclude_none=True))
            cur.execute(
                """
                INSERT INTO jobs (job_id, status, command, workdir, env, metadata, resources)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    job_id,
                    "queued",
                    submit.command,
                    submit.workdir,
                    json.dumps(submit.env or {}),
                    json.dumps(metadata),
                    json.dumps(submit.resources.dict() if submit.resources else {}),
                ),
            )


def _fetch_job(job_id: str) -> Optional[Dict[str, Any]]:
    conn = get_db_connection()
    with conn:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM jobs WHERE job_id = %s", (job_id,))
            row = cur.fetchone()
            return row


@app.post("/jobs", response_model=JobStatus)
async def submit_job(
    submit: JobSubmit, api_key: Optional[str] = Depends(require_api_key)
):
    """
    Submit a job: run a shell command in a given workdir with env.
    Returns a job_id you can poll later.
    """
    log("POST /jobs", None, "received")
    normalized_resources = normalize_resources(submit.resources)
    submit = submit.copy(update={"resources": normalized_resources})
    if submit.resources:
        log(
            "POST /jobs",
            None,
            "resources",
            (
                f"cpu={submit.resources.cpu} ram={submit.resources.ram} "
                f"accelerator={submit.resources.accelerator} shape={submit.resources.shape}"
            ),
        )
    job_id = str(uuid.uuid4())
    log("POST /jobs", job_id, "queued", f"{submit.command} @ {submit.workdir}")

    # Persist job record in MySQL
    await asyncio.to_thread(_insert_job, job_id, submit)

    return JobStatus(
        job_id=job_id,
        status="queued",
        returncode=None,
        stdout=None,   # not ready yet
        stderr=None,
        metadata=submit.metadata,
        resources=submit.resources,
    )


async def _get_job_status(job_id: str) -> JobStatus:
    row = await asyncio.to_thread(_fetch_job, job_id)
    if not row:
        raise HTTPException(status_code=404, detail="Job not found")

    status = row.get("status") or "queued"
    if status == "queued":
        # For clients that expect immediate progression, report queued as running.
        status = "running"
    stdout = row.get("stdout") if status in ("finished", "failed", "killed", "cleaned") else None
    stderr = row.get("stderr") if status in ("finished", "failed", "killed", "cleaned") else None

    rc_raw = row.get("returncode")
    returncode = int(rc_raw) if rc_raw is not None else None

    try:
        metadata: Dict[str, Any] = json.loads(row.get("metadata") or "{}")
    except Exception:
        metadata = {}

    resources_dict: Optional[Dict[str, Any]] = None
    raw_resources = row.get("resources")
    if raw_resources:
        try:
            resources_dict = json.loads(raw_resources)
        except Exception:
            resources_dict = None

    resources = normalize_resources(resources_dict) if resources_dict else None

    log("GET /jobs", job_id, status, f"queue {get_job_status_counts()}")

    return JobStatus(
        job_id=job_id,
        status=status,
        returncode=returncode,
        stdout=stdout,
        stderr=stderr,
        metadata=metadata,
        resources=resources,
    )


@app.get("/jobs/{job_id}", response_model=JobStatus)
async def get_job(job_id: str, api_key: Optional[str] = Depends(require_api_key)):
    """
    Check job status, get stdout/stderr when finished.
    """
    return await _get_job_status(job_id)


@app.post("/jobs/{job_id}/kill", response_model=JobStatus)
async def kill_job(
    job_id: str, api_key: Optional[str] = Depends(require_api_key)
):
    """
    Request a job kill: mark status as killing so workers can react.
    """
    log("POST /jobs/{job_id}/kill", job_id, "requested")

    row = await asyncio.to_thread(_fetch_job, job_id)
    if not row:
        raise HTTPException(status_code=404, detail="Job not found")

    conn = get_db_connection()
    with conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE jobs SET status = %s WHERE job_id = %s",
                ("killing", job_id),
            )
    log("POST /jobs/{job_id}/kill", job_id, "killing", f"queue {get_job_status_counts()}")

    return await _get_job_status(job_id)


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

@app.on_event("startup")
async def start_workers() -> None:
    """
    Initialize DB and reset any stale running jobs to failed.
    """
    await asyncio.to_thread(init_db)
    await asyncio.to_thread(reset_running_jobs_to_failed)
    log("startup", None, "ready", f"queue {get_job_status_counts()}")

if __name__ == "__main__":
    # Run: python rest_executor_service.py
    uvicorn.run(app, host="0.0.0.0", port=8080)
