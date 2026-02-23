# BUILD-06: Jobs Layer + FastAPI Backend (APX)

> **APX project.** The backend uses APX conventions throughout: `create_router()` for routes, `Dependencies.Client` / `Dependencies.UserClient` / `Dependencies.Config` for dependency injection, and `operation_id` on every endpoint to drive auto-generated TypeScript hooks. See `APX-BUILD-GUIDE.md` for full patterns.

## Context

You are building the **Genie Space Optimizer**, a Databricks App (APX). This step creates:
1. **Jobs Layer** — Databricks notebook entry points that call the optimization harness on dedicated compute
2. **FastAPI Backend** — REST API routes using APX patterns that the React frontend calls

**Read these reference docs before starting:**
- `docs/genie_space_optimizer/05-databricks-app-frontend.md` — Sections 9-10: full API endpoint specs, Pydantic models, stage-to-step mapping
- `docs/genie_space_optimizer/06-jobs-and-deployment.md` — job definitions, notebook entry points
- `docs/genie_space_optimizer/00-architecture-overview.md` — three-tier architecture
- `docs/genie_space_optimizer/APX-BUILD-GUIDE.md` — APX-specific patterns (Steps 8-9)

**Depends on (already built):**
- `src/genie_space_optimizer/optimization/harness.py` — `optimize_genie_space()` (BUILD-05)
- `src/genie_space_optimizer/optimization/state.py` — Delta read/write (BUILD-02)
- `src/genie_space_optimizer/common/genie_client.py` — Genie API (BUILD-01)
- `src/genie_space_optimizer/common/config.py` — constants (BUILD-01)

---

## Part A: Jobs Layer (Multi-Task)

The optimization runs as a **single Databricks Job with 5 tasks**. Each task is a notebook that calls one stage function from the harness library. See `06-jobs-and-deployment.md` Section 1 for the full job YAML and notebook code.

### 1. Task Notebooks (5 files in `jobs/`)

Each notebook follows this pattern:
1. Read params from `dbutils.widgets` (job-level base_parameters)
2. Read upstream outputs from `dbutils.jobs.taskValues.get(taskKey="...", key="...")`
3. Call the corresponding stage function from `optimization/harness.py`
4. Write outputs via `dbutils.jobs.taskValues.set(key="...", value="...")`

| Notebook | Stage Function | Key Inputs | Key Outputs (task values) |
|----------|---------------|------------|--------------------------|
| `run_preflight.py` | `_run_preflight()` | widgets: run_id, space_id, domain | model_id, experiment_name |
| `run_baseline.py` | `_run_baseline()` | task values from preflight | baseline_scores, thresholds_met |
| `run_lever_loop.py` | `_run_lever_loop()` | task values from baseline_eval | final_scores, best_model_id |
| `run_finalize.py` | `_run_finalize()` | task values from lever_loop OR baseline | final_status, report_path |
| `run_deploy.py` | `_run_deploy()` | task values from finalize | held_out_accuracy |

See `06-jobs-and-deployment.md` for complete notebook source code.

**Important:** The `finalize` task reads from `lever_loop` if it ran, otherwise falls back to `baseline_eval` task values (when lever_loop was skipped because thresholds were already met).

### 2. `jobs/run_evaluation_only.py` — Standalone evaluation notebook

Same pattern but calls `run_evaluation()` from `optimization/evaluation.py`. Params: `space_id`, `experiment_name`, `iteration`, `domain`, `model_id`, `eval_scope`, `catalog`, `schema`. Benchmarks loaded from MLflow eval dataset. Runs on Serverless compute (`environment_key: default`).

---

## Part B: FastAPI Backend (APX Patterns)

### APX Pattern Reference

| Pattern | APX Way | NOT This |
|---------|---------|----------|
| Create router | `from ..core import create_router; router = create_router()` | `from fastapi import APIRouter; router = APIRouter()` |
| App-level auth | `ws: Dependencies.Client` in route args | `WorkspaceClient()` global |
| User-level auth | `ws: Dependencies.UserClient` | Manual `X-Forwarded-Access-Token` |
| Config access | `config: Dependencies.Config` | `os.environ.get()` |
| API prefix | Set via `api-prefix = "/api/genie"` in `pyproject.toml` | Hardcoded in `include_router(prefix=...)` |
| Hook generation | `operation_id="listSpaces"` on every endpoint | — (breaks auto-gen) |
| Spark access | Get from `Dependencies.Config` or create `DatabricksSession` | Global spark |

**Every endpoint MUST have `operation_id`** — this drives the auto-generated TypeScript hooks. `operation_id="listSpaces"` → `useListSpaces()` hook in the frontend.

### 3. `backend/models.py` — All Pydantic response models

Copy all models from `05-databricks-app-frontend.md` Section 9 "Response Models" + Section 15.9 extended types:

```python
from pydantic import BaseModel

class SpaceSummary(BaseModel):
    id: str
    name: str
    description: str
    tableCount: int
    lastModified: str
    qualityScore: float | None = None

class TableInfo(BaseModel):
    name: str
    catalog: str
    schema_: str
    columnCount: int
    hasDescriptions: bool

class SpaceDetail(BaseModel):
    id: str
    name: str
    description: str
    instructions: str
    sampleQuestions: list[str]
    tables: list[TableInfo]
    runs: list["RunSummary"]

class RunSummary(BaseModel):
    runId: str
    status: str
    startedAt: str
    baselineScore: float | None = None
    optimizedScore: float | None = None
    initiatedBy: str

class OptimizeResponse(BaseModel):
    runId: str
    jobRunId: str

class PipelineStep(BaseModel):
    stepNumber: int
    name: str
    status: str           # pending | running | completed | failed
    durationSeconds: float | None = None
    summary: str | None = None
    inputs: dict | None = None
    outputs: dict | None = None

class LeverStatus(BaseModel):
    lever: int
    name: str
    status: str           # pending | running | evaluating | accepted | rolled_back | skipped
    patchCount: int = 0
    scoreBefore: float | None = None
    scoreAfter: float | None = None
    scoreDelta: float | None = None
    rollbackReason: str | None = None
    patches: list[dict] = []

class PatchSummary(BaseModel):
    patchType: str
    target: str
    riskLevel: str
    status: str

class PipelineRun(BaseModel):
    runId: str
    spaceId: str
    spaceName: str
    status: str           # QUEUED | RUNNING | COMPLETED | FAILED | CANCELLED
    startedAt: str
    completedAt: str | None = None
    initiatedBy: str
    baselineScore: float | None = None
    optimizedScore: float | None = None
    steps: list[PipelineStep]
    levers: list[LeverStatus] = []
    convergenceReason: str | None = None

class DimensionScore(BaseModel):
    name: str
    baseline: float
    optimized: float
    delta: float

class SpaceConfiguration(BaseModel):
    instructions: str
    sampleQuestions: list[str]
    tableDescriptions: list["TableDescription"]

class TableDescription(BaseModel):
    tableName: str
    description: str
    columns: list[dict]

class ComparisonData(BaseModel):
    runId: str
    spaceId: str
    spaceName: str
    baselineScore: float
    optimizedScore: float
    improvementPct: float
    perDimensionScores: list[DimensionScore]
    original: SpaceConfiguration
    optimized: SpaceConfiguration

class ActionResponse(BaseModel):
    success: bool
    message: str
    runId: str

class ActivityItem(BaseModel):
    runId: str
    spaceId: str
    spaceName: str
    status: str
    startedAt: str
    baselineScore: float | None = None
    optimizedScore: float | None = None
    initiatedBy: str
```

### 4. `backend/routes/spaces.py` — Space endpoints

```python
from ..core import Dependencies, create_router
from ..models import SpaceSummary, SpaceDetail, OptimizeResponse

router = create_router()

@router.get("/spaces", response_model=list[SpaceSummary], operation_id="listSpaces")
def list_spaces(ws: Dependencies.Client, config: Dependencies.Config):
    """List Genie Spaces enriched with optimization metadata from Delta."""
    from genie_space_optimizer.common.genie_client import list_spaces as _list
    from genie_space_optimizer.optimization.state import load_recent_activity
    # 1. Fetch spaces via Genie API using ws (WorkspaceClient)
    # 2. For each space, look up last run from Delta
    # 3. Return SpaceSummary list
    ...

@router.get("/spaces/{space_id}", response_model=SpaceDetail, operation_id="getSpaceDetail")
def get_space_detail(space_id: str, ws: Dependencies.Client, config: Dependencies.Config):
    """Full space config + UC metadata + optimization history."""
    from genie_space_optimizer.common.genie_client import fetch_space_config
    from genie_space_optimizer.optimization.state import load_runs_for_space
    # 1. Fetch space config
    # 2. Load run history from Delta
    # 3. Return SpaceDetail
    ...

@router.post("/spaces/{space_id}/optimize", response_model=OptimizeResponse, operation_id="startOptimization")
def start_optimization(space_id: str, ws: Dependencies.Client, config: Dependencies.Config,
                       apply_mode: str = "genie_config"):
    """Create QUEUED run in Delta, submit multi-task Job via run_now, return {runId, jobRunId}.

    apply_mode: 'genie_config' (default) | 'uc_artifact' | 'both'.
    Controls where levers 1-3 write patches. Levers 4-6 always use genie_config.
    """
    import uuid
    from genie_space_optimizer.optimization.state import create_run, update_run_status
    # 1. Generate run_id = str(uuid.uuid4())
    # 2. Infer domain from space (or accept as param)
    # 3. create_run(spark, run_id, space_id, domain, config.catalog, config.schema_name,
    #              apply_mode=apply_mode)
    # 4. Look up pre-deployed multi-task job by name:
    #        jobs = ws.jobs.list(name=f"{config.bundle_target}-genie-optimization")
    #        job_id = next(iter(jobs)).job_id
    # 5. Submit via run_now with notebook_params override:
    #        ws.jobs.run_now(job_id=job_id, notebook_params={
    #            "run_id": run_id, "space_id": space_id, "domain": domain,
    #            "catalog": config.catalog, "schema": config.schema_name,
    #            "apply_mode": apply_mode,
    #            "levers": "1,2,3,4,5,6", ...
    #        })
    # 6. update_run_status(spark, run_id, ..., job_run_id=str(job.run_id))
    # 7. Return OptimizeResponse(runId=run_id, jobRunId=str(job.run_id))
    ...
```

### 5. `backend/routes/runs.py` — Run endpoints

```python
from ..core import Dependencies, create_router
from ..models import PipelineRun, ComparisonData, ActionResponse

router = create_router()

@router.get("/runs/{run_id}", response_model=PipelineRun, operation_id="getRun")
def get_run(run_id: str, config: Dependencies.Config):
    """Run status with 5 user-facing pipeline steps + lever detail.

    CRITICAL: implements stage-to-step mapping from 05-databricks-app-frontend.md Section 10.
    """
    from genie_space_optimizer.optimization.state import load_run, load_stages, load_iterations
    # 1. Load run, stages, iterations from Delta
    # 2. Call map_stages_to_steps(stages) → 5 PipelineStep objects
    # 3. Build levers array from LEVER_* stages for Step 4 sub-progress
    # 4. Return PipelineRun
    ...

@router.get("/runs/{run_id}/comparison", response_model=ComparisonData, operation_id="getComparison")
def get_comparison(run_id: str, ws: Dependencies.Client, config: Dependencies.Config):
    """Side-by-side original vs optimized config with per-dimension scores."""
    # 1. Load original config from PREFLIGHT detail_json
    # 2. Load current config via Genie API (after patches)
    # 3. Load baseline (iter 0) and final scores
    # 4. Compute per-dimension deltas
    # 5. Return ComparisonData
    ...

@router.post("/runs/{run_id}/apply", response_model=ActionResponse, operation_id="applyOptimization")
def apply_optimization(run_id: str, ws: Dependencies.Client, config: Dependencies.Config):
    """Apply optimized config. Update run status to APPLIED."""
    # Patches are already applied during optimization.
    # "Apply" = confirm: mark run as APPLIED in Delta.
    ...

@router.post("/runs/{run_id}/discard", response_model=ActionResponse, operation_id="discardOptimization")
def discard_optimization(run_id: str, ws: Dependencies.Client, config: Dependencies.Config):
    """Discard results, rollback patches. Update status to DISCARDED."""
    from genie_space_optimizer.optimization.applier import rollback
    # 1. Load apply_log from Delta
    # 2. rollback(apply_log, ws, space_id, original_config)
    # 3. Mark run as DISCARDED
    ...
```

**Critical: Stage-to-Step Mapping**

Implement `map_stages_to_steps()` exactly per `05-databricks-app-frontend.md` Section 10:

| User Step | Internal Stage Prefixes |
|-----------|------------------------|
| 1. Configuration Analysis | `PREFLIGHT` (first half) |
| 2. Metadata Collection | `PREFLIGHT` (second half, split using detail_json) |
| 3. Baseline Evaluation | `BASELINE_EVAL` |
| 4. Configuration Generation | `LEVER_` |
| 5. Optimized Evaluation | `FINALIZING`, `REPEATABILITY`, `DEPLOYING`, `COMPLETE` |

Also build the `levers` array from `LEVER_*` stages for Step 4 sub-progress (see Section 15.9 for `LeverStatus` shape).

### 6. `backend/routes/activity.py`

```python
from ..core import Dependencies, create_router
from ..models import ActivityItem

router = create_router()

@router.get("/activity", response_model=list[ActivityItem], operation_id="getActivity")
def get_activity(space_id: str | None = None, limit: int = 20, config: Dependencies.Config = None):
    """Recent optimization runs for Dashboard activity table."""
    from genie_space_optimizer.optimization.state import load_recent_activity
    ...
```

### 7. Register routes in `backend/app.py`

APX generates the app scaffolding. Register your route modules:

```python
from .routes import spaces, runs, activity

# APX's app.py includes these via include_router.
# The api-prefix ("/api/genie") is set in pyproject.toml and applied automatically.
```

### 8. Extend `backend/core.py` — AppConfig

```python
from pydantic import Field
# APX provides the base AppConfig; extend it:

class AppConfig(BaseAppConfig):
    catalog: str = Field(default="", description="Unity Catalog name")
    schema_name: str = Field(default="", alias="schema", description="Gold schema")
    warehouse_id: str = Field(default="", description="SQL Warehouse ID")

    class Config:
        env_prefix = "GENIE_SPACE_OPTIMIZER_"
```

Access in routes as `config: Dependencies.Config` → `config.catalog`, `config.schema_name`.

---

## File Structure

```
src/genie_space_optimizer/
├── jobs/
│   ├── run_preflight.py             # Task 1 notebook
│   ├── run_baseline.py              # Task 2 notebook
│   ├── run_lever_loop.py            # Task 3 notebook
│   ├── run_finalize.py              # Task 4 notebook
│   ├── run_deploy.py                # Task 5 notebook
│   └── run_evaluation_only.py       # Standalone eval notebook
│
├── backend/
│   ├── __init__.py
│   ├── app.py                       # APX-generated, register route modules
│   ├── core.py                      # AppConfig, Dependencies, create_router()
│   ├── models.py                    # All Pydantic response models
│   └── routes/
│       ├── __init__.py
│       ├── spaces.py                # /spaces, /spaces/:id, /spaces/:id/optimize
│       ├── runs.py                  # /runs/:id, comparison, apply, discard
│       └── activity.py              # /activity
```

---

## Key Design Decisions

1. **No optimization logic in the backend.** Only reads Delta + submits/cancels jobs. Never imports from `optimization/` (except `state.py` for reads and `applier.rollback` for discard).
2. **APX dependency injection.** `Dependencies.Client` for app-level `WorkspaceClient`, `Dependencies.Config` for env config. Makes testing trivial (override deps).
3. **Stage-to-step mapping in backend**, not frontend. API returns user-facing steps → React stays simple.
4. **Apply/Discard is a user action.** Patches applied during optimization; "Apply" = confirm (keep), "Discard" = rollback.
5. **`operation_id` on every endpoint.** Drives auto-generated TypeScript hooks: `operation_id="listSpaces"` → `useListSpaces()`.
6. **No manual static file serving.** APX handles embedding the built frontend into the wheel and serving it automatically.

---

## Acceptance Criteria

1. `POST /api/genie/spaces/:id/optimize` creates Delta row + submits job → returns within 5s
2. `GET /api/genie/runs/:runId` returns exactly 5 steps with correct status mapping
3. `GET /api/genie/runs/:runId` returns `levers` array when Step 4 is active
4. `GET /api/genie/runs/:runId/comparison` returns both configs + score deltas
5. `POST /api/genie/runs/:runId/apply` → status APPLIED
6. `POST /api/genie/runs/:runId/discard` → rollback + status DISCARDED
7. All 5 task notebooks read task values, call stage functions, write task values correctly
8. All errors return proper HTTP status codes (404 unknown run, 400 invalid state, 409 conflict)
9. Every endpoint has `operation_id` and `response_model` → OpenAPI schema complete for TS codegen

---

## Predecessor Prompts
- BUILD-01 through BUILD-05

## Successor Prompts
- BUILD-07 (React Frontend) — consumes these endpoints via auto-generated hooks
- BUILD-08 (Deployment) — bundles jobs + app
