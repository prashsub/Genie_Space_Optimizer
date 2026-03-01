"""Persistent Databricks Job launcher for optimization pipelines.

The app uploads a runner notebook + wheel, ensures a reusable named Databricks Job
exists, then triggers each optimization run via ``jobs.run_now()``. This keeps
all optimization runs visible under a single Workflow job in the UI.
"""

from __future__ import annotations

import base64
import hashlib
import logging
import threading
from pathlib import Path

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.compute import Environment
from databricks.sdk.service.iam import AccessControlRequest, PermissionLevel
from databricks.sdk.service.jobs import (
    JobEnvironment,
    JobParameterDefinition,
    JobSettings,
    NotebookTask,
    QueueSettings,
    TaskDependency,
    Source,
    Task,
)
from databricks.sdk.service.workspace import ImportFormat, Language

logger = logging.getLogger(__name__)

_WS_BASE = "/Workspace/Shared/genie-space-optimizer"
_WS_WHEEL_DIR = f"{_WS_BASE}/dist"
_PERSISTENT_JOB_NAME = "genie-space-optimizer-runner"
_JOB_ENVIRONMENT_CLIENT_VERSION = "4"
_DAG_TASK_KEYS = {
    "preflight": "preflight",
    "baseline": "baseline_eval",
    "lever_loop": "lever_loop",
    "finalize": "finalize",
    "deploy": "deploy",
}
_JOB_PARAM_DEFAULTS = {
    "run_id": "",
    "space_id": "",
    "domain": "default",
    "catalog": "",
    "schema": "",
    "apply_mode": "genie_config",
    "levers": "[1,2,3,4,5]",
    "max_iterations": "5",
    "triggered_by": "",
    "experiment_name": "",
    "deploy_target": "",
}

_NOTEBOOK_SOURCES = {
    "run_preflight": Path(__file__).resolve().parent.parent / "jobs" / "run_preflight.py",
    "run_baseline": Path(__file__).resolve().parent.parent / "jobs" / "run_baseline.py",
    "run_lever_loop": Path(__file__).resolve().parent.parent / "jobs" / "run_lever_loop.py",
    "run_finalize": Path(__file__).resolve().parent.parent / "jobs" / "run_finalize.py",
    "run_deploy": Path(__file__).resolve().parent.parent / "jobs" / "run_deploy.py",
}
_WS_NOTEBOOKS = {name: f"{_WS_BASE}/{name}" for name in _NOTEBOOK_SOURCES}

_cached_wheel_path: str | None = None
_cached_wheel_hash: str | None = None
_cached_job_id: int | None = None
_cached_job_wheel_path: str | None = None
# Only guards single-process races; with multiple uvicorn workers cross-process
# safety relies on the Databricks idempotency_token passed to run_now().
_job_submit_lock = threading.Lock()


def _build_idempotency_token(*, run_id: str, space_id: str, triggered_by: str) -> str:
    """Build a stable <=64-char idempotency token for run_now."""
    raw = f"{space_id}|{triggered_by}|{run_id}"
    digest = hashlib.sha256(raw.encode("utf-8")).hexdigest()
    # Databricks enforces a 64-char max.
    return f"gso-{digest[:60]}"


def _resolve_project_root() -> Path:
    """Find the source tree root that contains ``pyproject.toml``."""
    for parent in Path(__file__).resolve().parents:
        if (parent / "pyproject.toml").is_file():
            return parent
    app_source = Path("/app/python/source_code")
    if (app_source / "pyproject.toml").is_file():
        return app_source
    return Path.cwd()


def _find_wheel() -> Path:
    """Locate the newest project wheel across ALL search directories.

    Scans every candidate directory and picks the globally-newest wheel by
    filename (which embeds a build timestamp).  Falls back to building from
    source in dev environments.
    """
    app_source = Path("/app/python/source_code")
    project_root = _resolve_project_root()

    search_dirs = [
        app_source,
        app_source / ".build",
        app_source / "dist",
        project_root / ".build",
        project_root / "dist",
        project_root,
    ]

    all_wheels: list[Path] = []
    for search_dir in search_dirs:
        if search_dir.is_dir():
            all_wheels.extend(search_dir.glob("genie_space_optimizer-*.whl"))

    if all_wheels:
        best = sorted(all_wheels, key=lambda p: p.name)[-1]
        logger.info(
            "Discovered wheel: %s (from %d candidate(s) across %d dirs)",
            best,
            len(all_wheels),
            len([d for d in search_dirs if d.is_dir()]),
        )
        return best

    import subprocess

    dist_dir = project_root / "dist"
    dist_dir.mkdir(exist_ok=True)
    last_error: Exception | None = None
    for cmd in [
        ["uv", "build", "--wheel", "-o", str(dist_dir)],
        ["python", "-m", "build", "--wheel", "-o", str(dist_dir)],
    ]:
        try:
            subprocess.run(cmd, cwd=str(project_root), check=True, capture_output=True)
            break
        except (FileNotFoundError, subprocess.CalledProcessError) as exc:
            last_error = exc
            continue

    wheels = sorted(dist_dir.glob("genie_space_optimizer-*.whl"))
    if not wheels:
        searched = ", ".join(str(d) for d in search_dirs if d.is_dir())
        raise RuntimeError(
            "Could not find or build the genie_space_optimizer wheel. "
            f"Searched: {searched}. Build root: {project_root}. "
            f"Last build error: {last_error!r}"
        )
    return wheels[-1]


def _ensure_artifacts(ws: WorkspaceClient) -> tuple[str, str]:
    """Upload wheel + runner notebooks to the workspace.

    Always re-discovers the local wheel and computes a content hash so that
    new wheels are detected even if the app process was not restarted after a
    deploy.

    Returns ``(ws_wheel_path, wheel_hash)``.
    """
    global _cached_wheel_path, _cached_wheel_hash

    wheel_local = _find_wheel()
    wheel_bytes = wheel_local.read_bytes()
    wheel_hash = hashlib.md5(wheel_bytes).hexdigest()
    ws_wheel_path = f"{_WS_WHEEL_DIR}/{wheel_local.name}"

    if _cached_wheel_hash == wheel_hash and _cached_wheel_path == ws_wheel_path:
        logger.debug(
            "Wheel unchanged (hash=%s), skipping upload", wheel_hash[:8]
        )
        return ws_wheel_path, wheel_hash

    ws.workspace.mkdirs(_WS_BASE)
    ws.workspace.mkdirs(_WS_WHEEL_DIR)

    ws.workspace.import_(
        ws_wheel_path,
        content=base64.b64encode(wheel_bytes).decode("ascii"),
        format=ImportFormat.AUTO,
        overwrite=True,
    )
    logger.info(
        "Uploaded wheel → %s (%d bytes, hash=%s)",
        ws_wheel_path,
        len(wheel_bytes),
        wheel_hash[:8],
    )

    for name, src_path in _NOTEBOOK_SOURCES.items():
        notebook_src = src_path.read_text(encoding="utf-8")
        ws_path = _WS_NOTEBOOKS[name]
        ws.workspace.import_(
            ws_path,
            content=base64.b64encode(notebook_src.encode("utf-8")).decode("ascii"),
            format=ImportFormat.SOURCE,
            language=Language.PYTHON,
            overwrite=True,
        )
        logger.info("Uploaded notebook → %s", ws_path)

    _cached_wheel_path = ws_wheel_path
    _cached_wheel_hash = wheel_hash
    return ws_wheel_path, wheel_hash


def _job_settings(ws_wheel_path: str, wheel_hash: str = "") -> JobSettings:
    def _job_param_ref(name: str) -> str:
        return f"{{{{job.parameters.{name}}}}}"

    return JobSettings(
        name=_PERSISTENT_JOB_NAME,
        description=(
            "Persistent DAG optimization runner managed by Genie Space Optimizer app "
            "(preflight → baseline_eval → lever_loop → finalize → deploy). "
            "SP executes with granted privileges on user schemas."
        ),
        max_concurrent_runs=20,
        queue=QueueSettings(enabled=True),
        parameters=[
            JobParameterDefinition(name=name, default=default)
            for name, default in _JOB_PARAM_DEFAULTS.items()
        ],
        tasks=[
            Task(
                task_key=_DAG_TASK_KEYS["preflight"],
                notebook_task=NotebookTask(
                    notebook_path=_WS_NOTEBOOKS["run_preflight"],
                    source=Source.WORKSPACE,
                    base_parameters={
                        "run_id": _job_param_ref("run_id"),
                        "space_id": _job_param_ref("space_id"),
                        "domain": _job_param_ref("domain"),
                        "catalog": _job_param_ref("catalog"),
                        "schema": _job_param_ref("schema"),
                        "apply_mode": _job_param_ref("apply_mode"),
                        "levers": _job_param_ref("levers"),
                        "max_iterations": _job_param_ref("max_iterations"),
                        "experiment_name": _job_param_ref("experiment_name"),
                        "deploy_target": _job_param_ref("deploy_target"),
                    },
                ),
                environment_key="default",
                timeout_seconds=7200,
                max_retries=0,
            ),
            Task(
                task_key=_DAG_TASK_KEYS["baseline"],
                depends_on=[TaskDependency(task_key=_DAG_TASK_KEYS["preflight"])],
                notebook_task=NotebookTask(
                    notebook_path=_WS_NOTEBOOKS["run_baseline"],
                    source=Source.WORKSPACE,
                ),
                environment_key="default",
                timeout_seconds=7200,
                max_retries=0,
            ),
            Task(
                task_key=_DAG_TASK_KEYS["lever_loop"],
                depends_on=[TaskDependency(task_key=_DAG_TASK_KEYS["baseline"])],
                notebook_task=NotebookTask(
                    notebook_path=_WS_NOTEBOOKS["run_lever_loop"],
                    source=Source.WORKSPACE,
                ),
                environment_key="default",
                timeout_seconds=7200,
                max_retries=0,
            ),
            Task(
                task_key=_DAG_TASK_KEYS["finalize"],
                depends_on=[TaskDependency(task_key=_DAG_TASK_KEYS["lever_loop"])],
                notebook_task=NotebookTask(
                    notebook_path=_WS_NOTEBOOKS["run_finalize"],
                    source=Source.WORKSPACE,
                ),
                environment_key="default",
                timeout_seconds=7200,
                max_retries=0,
            ),
            Task(
                task_key=_DAG_TASK_KEYS["deploy"],
                depends_on=[TaskDependency(task_key=_DAG_TASK_KEYS["finalize"])],
                notebook_task=NotebookTask(
                    notebook_path=_WS_NOTEBOOKS["run_deploy"],
                    source=Source.WORKSPACE,
                ),
                environment_key="default",
                timeout_seconds=7200,
                max_retries=0,
            ),
        ],
        environments=[
            JobEnvironment(
                environment_key="default",
                spec=Environment(
                    client=_JOB_ENVIRONMENT_CLIENT_VERSION,
                    dependencies=[
                        ws_wheel_path,
                        "mlflow[databricks]>=3.4.0",
                        "databricks-sdk>=0.40.0",
                    ],
                ),
            ),
        ],
        tags={
            "app": "genie-space-optimizer",
            "managed-by": "backend-job-launcher",
            "pattern": "persistent-dag",
            "wheel-hash": wheel_hash[:12] if wheel_hash else "unknown",
        },
    )


def _ensure_job_visibility(ws: WorkspaceClient, job_id: int) -> None:
    """Best-effort permission update so users can view job runs in the UI."""
    try:
        ws.permissions.update(
            "jobs",
            str(job_id),
            access_control_list=[
                AccessControlRequest(
                    group_name="users",
                    permission_level=PermissionLevel.CAN_VIEW,
                )
            ],
        )
    except Exception as exc:
        logger.warning(
            "Could not set CAN_VIEW permissions on job %s: %s",
            job_id,
            exc,
        )


def _ensure_persistent_job(
    ws: WorkspaceClient, ws_wheel_path: str, wheel_hash: str = ""
) -> int:
    """Find or create the persistent optimization job and return job_id.

    Does NOT set ``run_as`` here — that is applied per-submission in
    ``submit_optimization`` so each run uses the correct user identity.
    """
    global _cached_job_id, _cached_job_wheel_path

    if _cached_job_id is not None and _cached_job_wheel_path == ws_wheel_path:
        return _cached_job_id

    settings = _job_settings(ws_wheel_path, wheel_hash=wheel_hash)

    if _cached_job_id is not None:
        try:
            ws.jobs.reset(_cached_job_id, new_settings=settings)
            _cached_job_wheel_path = ws_wheel_path
            return _cached_job_id
        except Exception:
            logger.warning(
                "Failed to reset cached job %s, rediscovering by name",
                _cached_job_id,
            )
            _cached_job_id = None

    existing_job_id: int | None = None
    for job in ws.jobs.list(name=_PERSISTENT_JOB_NAME, limit=1):
        if job.job_id is not None:
            existing_job_id = int(job.job_id)
            break

    if existing_job_id is not None:
        try:
            ws.jobs.reset(existing_job_id, new_settings=settings)
            job_id = existing_job_id
            logger.info("Updated persistent optimization job %s", job_id)
        except Exception as exc:
            logger.warning(
                "Could not update existing job %s (%s); creating app-owned job instead",
                existing_job_id,
                exc,
            )
            existing_job_id = None
            created = ws.jobs.create(
                name=settings.name,
                description=settings.description,
                max_concurrent_runs=settings.max_concurrent_runs,
                queue=settings.queue,
                parameters=settings.parameters,
                tasks=settings.tasks,
                environments=settings.environments,
                tags=settings.tags,
            )
            if created.job_id is None:
                raise RuntimeError("Job creation returned no job_id")
            job_id = int(created.job_id)
            logger.info("Created persistent optimization job %s", job_id)
    else:
        created = ws.jobs.create(
            name=settings.name,
            description=settings.description,
            max_concurrent_runs=settings.max_concurrent_runs,
            queue=settings.queue,
            parameters=settings.parameters,
            tasks=settings.tasks,
            environments=settings.environments,
            tags=settings.tags,
        )
        if created.job_id is None:
            raise RuntimeError("Job creation returned no job_id")
        job_id = int(created.job_id)
        logger.info("Created persistent optimization job %s", job_id)

    _ensure_job_visibility(ws, job_id)
    _cached_job_id = job_id
    _cached_job_wheel_path = ws_wheel_path
    return job_id


def submit_optimization(
    ws: WorkspaceClient,
    *,
    run_id: str,
    space_id: str,
    domain: str,
    catalog: str,
    schema: str,
    apply_mode: str = "genie_config",
    levers: str = "[1,2,3,4,5]",
    max_iterations: str = "5",
    triggered_by: str = "",
    experiment_name: str = "",
) -> tuple[str, int]:
    """Trigger a run on a persistent serverless optimization job.

    The job runs as the SP which has been granted SELECT/EXECUTE on user
    schemas via the Data Access Settings page.
    """
    ws_wheel_path, wheel_hash = _ensure_artifacts(ws)
    job_id = _ensure_persistent_job(ws, ws_wheel_path, wheel_hash=wheel_hash)

    with _job_submit_lock:
        waiter = ws.jobs.run_now(
            job_id=job_id,
            idempotency_token=_build_idempotency_token(
                run_id=run_id,
                space_id=space_id,
                triggered_by=triggered_by,
            ),
            job_parameters={
                "run_id": run_id,
                "space_id": space_id,
                "domain": domain,
                "catalog": catalog,
                "schema": schema,
                "apply_mode": apply_mode,
                "levers": levers,
                "max_iterations": max_iterations,
                "triggered_by": triggered_by,
                "experiment_name": experiment_name,
                "deploy_target": "",
            },
        )

    job_run_id = str(waiter.run_id)
    logger.info(
        "Triggered optimization run %s on persistent job %s (job run %s)",
        run_id,
        job_id,
        job_run_id,
    )
    return job_run_id, job_id
