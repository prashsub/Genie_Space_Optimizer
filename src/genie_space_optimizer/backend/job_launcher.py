"""Persistent Databricks Job launcher for optimization pipelines.

The app uploads a runner notebook + wheel, ensures a reusable named Databricks Job
exists, then triggers each optimization run via ``jobs.run_now()``. This keeps
all optimization runs visible under a single Workflow job in the UI.
"""

from __future__ import annotations

import base64
import hashlib
import io
import logging
import os
import threading
from pathlib import Path

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.catalog import VolumeType
from databricks.sdk.service.compute import Environment
from databricks.sdk.service.iam import AccessControlRequest, PermissionLevel
from databricks.sdk.service.jobs import (
    JobEnvironment,
    JobParameterDefinition,
    JobRunAs,
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
_VOLUME_NAME = "app_artifacts"
_PERSISTENT_JOB_NAME = "genie-space-optimizer-runner"
_JOB_ENVIRONMENT_CLIENT_VERSION = "4"
_DAG_TASK_KEYS = {
    "preflight": "preflight",
    "baseline": "baseline_eval",
    "enrichment": "enrichment",
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
    "run_enrichment": Path(__file__).resolve().parent.parent / "jobs" / "run_enrichment.py",
    "run_lever_loop": Path(__file__).resolve().parent.parent / "jobs" / "run_lever_loop.py",
    "run_finalize": Path(__file__).resolve().parent.parent / "jobs" / "run_finalize.py",
    "run_deploy": Path(__file__).resolve().parent.parent / "jobs" / "run_deploy.py",
    "run_cross_env_deploy": Path(__file__).resolve().parent.parent / "jobs" / "run_cross_env_deploy.py",
}
_WS_NOTEBOOKS = {name: f"{_WS_BASE}/{name}" for name in _NOTEBOOK_SOURCES}

_DEPLOY_JOB_ENVIRONMENT_CLIENT_VERSION = "4"

_cached_wheel_path: str | None = None
_cached_wheel_hash: str | None = None
_cached_job_id: int | None = None
_cached_job_wheel_path: str | None = None
_volume_ensured: bool = False
_legacy_cleaned: bool = False
# Only guards single-process races; with multiple uvicorn workers cross-process
# safety relies on the Databricks idempotency_token passed to run_now().
_job_submit_lock = threading.Lock()


def _volume_wheel_dir(catalog: str, schema: str) -> str:
    """Return the Volume-based directory path for wheel storage."""
    return f"/Volumes/{catalog}/{schema}/{_VOLUME_NAME}/dist"


def _ensure_volume(ws: WorkspaceClient, catalog: str, schema: str) -> None:
    """Create the managed Volume for app artifacts if it doesn't already exist.

    Idempotent and cached per process — only attempts creation once.
    """
    global _volume_ensured
    if _volume_ensured:
        return
    try:
        ws.volumes.create(
            catalog_name=catalog,
            schema_name=schema,
            name=_VOLUME_NAME,
            volume_type=VolumeType.MANAGED,
        )
        logger.info("Created managed volume %s.%s.%s", catalog, schema, _VOLUME_NAME)
    except Exception as exc:
        if "already_exists" in str(exc).lower() or "already exists" in str(exc).lower():
            logger.debug("Volume %s.%s.%s already exists", catalog, schema, _VOLUME_NAME)
        else:
            raise RuntimeError(
                f"Could not create volume {catalog}.{schema}.{_VOLUME_NAME}: {exc}. "
                f"Create it manually with: CREATE VOLUME IF NOT EXISTS "
                f"{catalog}.{schema}.{_VOLUME_NAME}"
            ) from exc
    _volume_ensured = True


def _cleanup_legacy_workspace_wheels(ws: WorkspaceClient) -> None:
    """One-time removal of the old workspace-based wheel directory.

    After migrating to UC Volumes, the legacy ``/Workspace/Shared/.../dist/``
    tree is no longer used.  Best-effort, non-fatal.
    """
    global _legacy_cleaned
    if _legacy_cleaned:
        return
    _legacy_cleaned = True
    legacy_dir = f"{_WS_BASE}/dist"
    try:
        ws.workspace.delete(legacy_dir, recursive=True)
        logger.info("Cleaned up legacy workspace wheel directory: %s", legacy_dir)
    except Exception:
        logger.debug("No legacy workspace wheel directory to clean up", exc_info=True)


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


def _cleanup_stale_wheels(ws: WorkspaceClient, dist_dir: str, keep_path: str) -> None:
    """Remove old hash-bucketed wheel directories from the Volume dist directory.

    Wheels are stored as ``{dist_dir}/{hash[:8]}/{filename}.whl``.
    This deletes any sibling hash directories that don't contain the current wheel.
    """
    keep_dir = keep_path.rsplit("/", 1)[0] if "/" in keep_path else keep_path
    try:
        for entry in ws.files.list_directory_contents(dist_dir):
            path = entry.path or ""
            if entry.is_directory and path and path != keep_dir:
                try:
                    ws.files.delete_directory(path)
                    logger.info("Deleted stale wheel artifact: %s", path)
                except Exception:
                    logger.debug("Could not delete stale artifact: %s", path, exc_info=True)
    except Exception:
        logger.debug("Could not list %s for cleanup", dist_dir, exc_info=True)


def _ensure_artifacts(
    ws: WorkspaceClient, catalog: str, schema: str,
) -> tuple[str, str]:
    """Upload wheel to a UC Volume and runner notebooks to the workspace.

    The wheel is stored in a managed Volume at
    ``/Volumes/{catalog}/{schema}/app_artifacts/dist/{hash[:8]}/{filename}``.
    Notebooks remain in the workspace (required by ``NotebookTask``).

    Always re-discovers the local wheel and computes a content hash so that
    new wheels are detected even if the app process was not restarted after a
    deploy.

    Returns ``(vol_wheel_path, wheel_hash)``.
    """
    global _cached_wheel_path, _cached_wheel_hash

    _ensure_volume(ws, catalog, schema)

    wheel_local = _find_wheel()
    wheel_bytes = wheel_local.read_bytes()
    wheel_hash = hashlib.md5(wheel_bytes).hexdigest()
    wheel_filename = wheel_local.name
    dist_dir = _volume_wheel_dir(catalog, schema)
    vol_wheel_path = f"{dist_dir}/{wheel_hash[:8]}/{wheel_filename}"

    logger.info(
        "Wheel resolved: %s (local=%s, size=%d, hash=%s)",
        wheel_filename,
        wheel_local,
        len(wheel_bytes),
        wheel_hash[:8],
    )

    if _cached_wheel_hash == wheel_hash and _cached_wheel_path == vol_wheel_path:
        logger.debug(
            "Wheel unchanged (hash=%s), skipping upload", wheel_hash[:8]
        )
        return vol_wheel_path, wheel_hash

    ws.files.upload(vol_wheel_path, io.BytesIO(wheel_bytes), overwrite=True)
    logger.info(
        "Uploaded wheel → %s (%d bytes, hash=%s)",
        vol_wheel_path,
        len(wheel_bytes),
        wheel_hash[:8],
    )

    _cleanup_stale_wheels(ws, dist_dir=dist_dir, keep_path=vol_wheel_path)
    _cleanup_legacy_workspace_wheels(ws)

    ws.workspace.mkdirs(_WS_BASE)
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

    _cached_wheel_path = vol_wheel_path
    _cached_wheel_hash = wheel_hash
    return vol_wheel_path, wheel_hash


def _job_settings(
    ws_wheel_path: str, wheel_hash: str = "", sp_client_id: str = "",
) -> JobSettings:
    def _job_param_ref(name: str) -> str:
        return f"{{{{job.parameters.{name}}}}}"

    run_as = JobRunAs(service_principal_name=sp_client_id) if sp_client_id else None

    return JobSettings(
        name=_PERSISTENT_JOB_NAME,
        run_as=run_as,
        description=(
            "Persistent DAG optimization runner managed by Genie Space Optimizer app "
            "(preflight → baseline_eval → enrichment → lever_loop → finalize → deploy). "
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
                task_key=_DAG_TASK_KEYS["enrichment"],
                depends_on=[TaskDependency(task_key=_DAG_TASK_KEYS["baseline"])],
                notebook_task=NotebookTask(
                    notebook_path=_WS_NOTEBOOKS["run_enrichment"],
                    source=Source.WORKSPACE,
                ),
                environment_key="default",
                timeout_seconds=7200,
                max_retries=0,
            ),
            Task(
                task_key=_DAG_TASK_KEYS["lever_loop"],
                depends_on=[TaskDependency(task_key=_DAG_TASK_KEYS["enrichment"])],
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


def _resolve_sp_client_id(ws: WorkspaceClient) -> str:
    return ws.config.client_id or os.getenv("DATABRICKS_CLIENT_ID", "")


def _is_owner_current(owner_ref: str, sp_client_id: str) -> bool:
    """True if *owner_ref* matches the current SP."""
    return bool(sp_client_id) and sp_client_id in owner_ref


def _ensure_persistent_job(
    ws: WorkspaceClient,
    ws_wheel_path: str,
    wheel_hash: str = "",
    sp_client_id: str = "",
) -> int:
    """Find or create the persistent optimization job and return job_id.

    If an existing job is found but its owner is orphaned or belongs to a
    stale service principal, the job is deleted and recreated so the current
    SP owns it.  ``run_as`` is always set explicitly.
    """
    global _cached_job_id, _cached_job_wheel_path

    if _cached_job_id is not None and _cached_job_wheel_path == ws_wheel_path:
        return _cached_job_id

    settings = _job_settings(
        ws_wheel_path, wheel_hash=wheel_hash, sp_client_id=sp_client_id,
    )

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

    if existing_job_id is not None and sp_client_id:
        try:
            job_detail = ws.jobs.get(existing_job_id)
            creator = job_detail.creator_user_name or ""
            run_as_name = job_detail.run_as_user_name or ""
            owner_ref = run_as_name or creator
            owner_is_orphaned = not owner_ref

            if owner_is_orphaned or not _is_owner_current(owner_ref, sp_client_id):
                logger.warning(
                    "Job %s owned by '%s' (current SP: %s) — "
                    "deleting orphaned/stale job and recreating.",
                    existing_job_id, owner_ref, sp_client_id,
                )
                try:
                    ws.jobs.delete(existing_job_id)
                except Exception as del_exc:
                    logger.warning(
                        "Could not delete orphaned job %s: %s",
                        existing_job_id, del_exc,
                    )
                existing_job_id = None
        except Exception:
            logger.warning(
                "Could not inspect job %s ownership — will try reset",
                existing_job_id,
            )

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
                run_as=settings.run_as,
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
            run_as=settings.run_as,
        )
        if created.job_id is None:
            raise RuntimeError("Job creation returned no job_id")
        job_id = int(created.job_id)
        logger.info("Created persistent optimization job %s", job_id)

    _ensure_job_visibility(ws, job_id)
    _cached_job_id = job_id
    _cached_job_wheel_path = ws_wheel_path
    return job_id


def ensure_deployment_job(
    ws: WorkspaceClient,
    space_id: str,
    catalog: str,
    schema: str,
) -> int:
    """Find or create a per-space deployment job and return its job_id.

    The job runs ``run_cross_env_deploy`` notebook which reads model_name,
    model_version, target_workspace_url, and target_space_id as parameters.
    """
    from genie_space_optimizer.common.config import (
        DEPLOYMENT_JOB_NAME_TEMPLATE,
        format_mlflow_template,
    )

    job_name = format_mlflow_template(
        DEPLOYMENT_JOB_NAME_TEMPLATE, space_id=space_id,
    )

    for job in ws.jobs.list(name=job_name, limit=1):
        if job.job_id is not None:
            logger.info("Found existing deployment job %s (id=%s)", job_name, job.job_id)
            return int(job.job_id)

    ws_wheel_path, _ = _ensure_artifacts(ws, catalog=catalog, schema=schema)
    sp_client_id = _resolve_sp_client_id(ws)
    run_as = JobRunAs(service_principal_name=sp_client_id) if sp_client_id else None

    settings = JobSettings(
        name=job_name,
        run_as=run_as,
        description=(
            f"Cross-environment deployment job for Genie Space {space_id}. "
            "Triggered after a new UC model version is registered."
        ),
        max_concurrent_runs=1,
        queue=QueueSettings(enabled=True),
        parameters=[
            JobParameterDefinition(name="model_name", default=""),
            JobParameterDefinition(name="model_version", default=""),
            JobParameterDefinition(name="target_workspace_url", default=""),
            JobParameterDefinition(name="target_space_id", default=space_id),
        ],
        tasks=[
            Task(
                task_key="cross_env_deploy",
                notebook_task=NotebookTask(
                    notebook_path=_WS_NOTEBOOKS["run_cross_env_deploy"],
                    source=Source.WORKSPACE,
                    base_parameters={
                        "model_name": "{{job.parameters.model_name}}",
                        "model_version": "{{job.parameters.model_version}}",
                        "target_workspace_url": "{{job.parameters.target_workspace_url}}",
                        "target_space_id": "{{job.parameters.target_space_id}}",
                    },
                ),
                environment_key="default",
                timeout_seconds=3600,
                max_retries=0,
            ),
        ],
        environments=[
            JobEnvironment(
                environment_key="default",
                spec=Environment(
                    client=_DEPLOY_JOB_ENVIRONMENT_CLIENT_VERSION,
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
            "pattern": "deployment-job",
            "space_id": space_id,
        },
    )

    created = ws.jobs.create(
        name=settings.name,
        description=settings.description,
        max_concurrent_runs=settings.max_concurrent_runs,
        queue=settings.queue,
        parameters=settings.parameters,
        tasks=settings.tasks,
        environments=settings.environments,
        tags=settings.tags,
        run_as=settings.run_as,
    )
    if created.job_id is None:
        raise RuntimeError(f"Deployment job creation for space {space_id} returned no job_id")

    job_id = int(created.job_id)
    _ensure_job_visibility(ws, job_id)
    logger.info("Created deployment job %s (id=%s) for space %s", job_name, job_id, space_id)
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
    deploy_target: str = "",
) -> tuple[str, int]:
    """Trigger a run on a persistent serverless optimization job.

    The job runs as the SP which has been granted SELECT/EXECUTE on user
    schemas via the Data Access Settings page.
    """
    sp_client_id = _resolve_sp_client_id(ws)
    ws_wheel_path, wheel_hash = _ensure_artifacts(ws, catalog=catalog, schema=schema)
    job_id = _ensure_persistent_job(
        ws, ws_wheel_path, wheel_hash=wheel_hash, sp_client_id=sp_client_id,
    )

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
                "deploy_target": deploy_target,
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


def get_job_url(ws: WorkspaceClient) -> str | None:
    """Resolve the URL to the persistent optimization job, if it exists."""
    job_id = _cached_job_id
    if job_id is None:
        try:
            for job in ws.jobs.list(name=_PERSISTENT_JOB_NAME, limit=1):
                if job.job_id is not None:
                    job_id = int(job.job_id)
                    break
        except Exception:
            return None
    if job_id is None:
        return None
    host = (ws.config.host or "").rstrip("/")
    if not host:
        return None
    return f"{host}/jobs/{job_id}"


def check_job_health(ws: WorkspaceClient, sp_client_id: str) -> tuple[bool, str]:
    """Check if the persistent runner job exists and is owned by the current SP.

    Returns ``(is_healthy, message)``.  Never raises.
    """
    try:
        for job in ws.jobs.list(name=_PERSISTENT_JOB_NAME, limit=1):
            if job.job_id is None:
                continue
            detail = ws.jobs.get(int(job.job_id))
            owner = detail.run_as_user_name or detail.creator_user_name or ""
            if not owner:
                return False, (
                    f"Runner job {job.job_id} has no owner (orphaned). "
                    f"Start an optimization run to auto-repair it, "
                    f"or restart the app."
                )
            if sp_client_id and not _is_owner_current(owner, sp_client_id):
                return False, (
                    f"Runner job {job.job_id} is owned by '{owner}' but the "
                    f"current app SP is '{sp_client_id}'. Start an optimization "
                    f"run to auto-repair it, or restart the app."
                )
            return True, ""
        return True, ""
    except Exception:
        return True, ""
