# BUILD-08: Deployment, Integration & Testing (APX)

> **APX project.** Build with `apx build`, deploy with `databricks bundle deploy`. APX produces a Python wheel with the frontend embedded — no manual Vite build or static file serving. See `APX-BUILD-GUIDE.md` Steps 11-12.

## Context

You are building the **Genie Space Optimizer**, a Databricks App (APX). This is the final step: wiring everything for deployment via Databricks Asset Bundles (DABs), defining job resources, and creating the test suite.

**Read these reference docs before starting:**
- `docs/genie_space_optimizer/06-jobs-and-deployment.md` — job YAML, app manifest, bundle config, deployment flow
- `docs/genie_space_optimizer/08-module-map.md` — Section 5: test strategy (unit, integration, E2E, app tests)
- `docs/genie_space_optimizer/00-architecture-overview.md` — project structure
- `docs/genie_space_optimizer/APX-BUILD-GUIDE.md` Steps 11-12 — dev server, build, deploy

**Depends on (already built):**
All BUILD-01 through BUILD-07. The entire app is built. This prompt integrates and deploys it.

---

## Part A: Job Resources

### 1. Optimization job (multi-task) — `resources/genie_optimization_job.yml`

The optimization runs as **5 tasks in one job**, connected by `depends_on` and `run_if`. See `06-jobs-and-deployment.md` Section 1 for the complete YAML. Key structure:

```yaml
resources:
  jobs:
    genie_optimization_job:
      name: "${bundle.target}-genie-optimization"
      description: "Multi-task Genie Space optimization pipeline"
      tasks:
        - task_key: preflight
          notebook_task:
            notebook_path: ../src/genie_space_optimizer/jobs/run_preflight.py
            base_parameters: { run_id: "", space_id: "", catalog: "${var.catalog}", schema: "${var.gold_schema}", domain: "", triggered_by: "", apply_mode: "genie_config" }
          environment_key: default
          timeout_seconds: 600
          max_retries: 1

        - task_key: baseline_eval
          depends_on: [{ task_key: preflight }]
          notebook_task:
            notebook_path: ../src/genie_space_optimizer/jobs/run_baseline.py
            base_parameters: { run_id: "", catalog: "${var.catalog}", schema: "${var.gold_schema}" }
          environment_key: default
          timeout_seconds: 3600
          max_retries: 1

        - task_key: lever_loop
          depends_on: [{ task_key: baseline_eval }]
          condition_task:
            op: "NOT_EQUAL"
            left: "{{tasks.baseline_eval.values.thresholds_met}}"
            right: "true"
          notebook_task:
            notebook_path: ../src/genie_space_optimizer/jobs/run_lever_loop.py
            base_parameters: { run_id: "", catalog: "${var.catalog}", schema: "${var.gold_schema}", max_iterations: "5", levers: "1,2,3,4,5,6", apply_mode: "genie_config" }
          environment_key: default
          timeout_seconds: 5400
          max_retries: 1

        - task_key: finalize
          depends_on: [{ task_key: lever_loop, outcome: "succeeded_or_skipped" }]
          notebook_task:
            notebook_path: ../src/genie_space_optimizer/jobs/run_finalize.py
            base_parameters: { run_id: "", catalog: "${var.catalog}", schema: "${var.gold_schema}", run_repeatability: "true" }
          environment_key: default
          timeout_seconds: 1800
          max_retries: 1

        - task_key: deploy
          depends_on: [{ task_key: finalize }]
          condition_task:
            op: "NOT_EQUAL"
            left: "{{tasks.finalize.values.deploy_target}}"
            right: ""
          notebook_task:
            notebook_path: ../src/genie_space_optimizer/jobs/run_deploy.py
            base_parameters: { run_id: "", catalog: "${var.catalog}", schema: "${var.gold_schema}" }
          environment_key: default
          timeout_seconds: 1800
          max_retries: 1

      environments:
        - environment_key: default
          spec:
            client: "1"
            dependencies:
              - mlflow[databricks]>=3.4.0
              - databricks-sdk>=0.40.0
              - databricks-connect>=15.4.0
      max_concurrent_runs: 3

    genie_evaluation_job:
      name: "${bundle.target}-genie-evaluation"
      description: "Standalone evaluation (triggered by lever_loop and baseline_eval tasks)"
      tasks:
        - task_key: evaluate
          notebook_task:
            notebook_path: ../src/genie_space_optimizer/jobs/run_evaluation_only.py
            base_parameters: { space_id: "", experiment_name: "", iteration: "0", domain: "", model_id: "", eval_scope: "full", catalog: "${var.catalog}", schema: "${var.gold_schema}" }
          environment_key: default
          timeout_seconds: 3600
          max_retries: 1
      environments:
        - environment_key: default
          spec:
            client: "1"
            dependencies:
              - mlflow[databricks]>=3.4.0
              - databricks-sdk>=0.40.0
              - databricks-connect>=15.4.0
```

### 2. Bundle config updates — `databricks.yml`

APX generates the base `databricks.yml`. Add:

```yaml
variables:
  catalog:
    default: "your_catalog"
  gold_schema:
    default: "your_gold_schema"
  warehouse_id:
    default: "your_warehouse_id"

include:
  - resources/genie_optimization_job.yml

# APX manages the app resource automatically.
# Add env vars to the app's config:
resources:
  apps:
    genie_space_optimizer:
      config:
        env:
          - name: GENIE_SPACE_OPTIMIZER_CATALOG
            value: ${var.catalog}
          - name: GENIE_SPACE_OPTIMIZER_SCHEMA
            value: ${var.gold_schema}
          - name: GENIE_SPACE_OPTIMIZER_WAREHOUSE_ID
            value: ${var.warehouse_id}
      resources:
        - name: sql-warehouse
          sql_warehouse:
            id: ${var.warehouse_id}
            permission: CAN_USE
      permissions:
        - user_name: "users"
          level: "CAN_USE"
```

---

## Part B: Build & Deploy

### APX Build

```bash
apx build
```

This does:
1. `bun build` — compiles React/TS → static assets into `__dist__/`
2. `uv build` — packages backend + embedded frontend into a Python wheel in `.build/`

### Deploy

```bash
databricks bundle deploy -p dev
```

This:
1. Uploads the wheel to workspace
2. Creates/updates the Databricks App (genie-space-optimizer)
3. Creates/updates the optimization + evaluation Jobs
4. Syncs notebook job entry points

### Access

```
https://<workspace>.cloud.databricks.com/apps/genie-space-optimizer
```

SSO handles auth. User identity forwarded via `X-Forwarded-Access-Token`.

---

## Part C: Dev Server

```bash
# Start (frontend HMR + backend auto-reload + OpenAPI codegen watcher)
apx dev start

# Monitor
apx dev logs

# Stop
apx dev stop
```

- Frontend: `http://localhost:5173` (Vite HMR)
- Backend: `http://localhost:8000` (uvicorn auto-reload)
- Frontend proxies `/api/genie/*` to backend
- TS hooks auto-regenerated when Python routes change

---

## Part D: Test Suite

### 3. Unit Tests — `tests/unit/`

| Test File | What It Tests | Mocking |
|-----------|---------------|---------|
| `test_config.py` | All constants have expected types/values | None |
| `test_scorers.py` | Each of 8 scorers returns correct Feedback | Mock spark, mock LLM |
| `test_optimizer.py` | `cluster_failures`, `detect_regressions`, `_map_to_lever`, `detect_conflicts_and_batch` | None (pure fns) |
| `test_applier.py` | `render_patch`, `_apply_action_to_config`, `classify_risk`, `validate_patch_set` | None (pure fns) |
| `test_benchmarks.py` | `assign_splits`, `load_benchmarks_from_dataset`, `validate_ground_truth_sql`, `generate_benchmarks` | Mock spark, Mock LLM |
| `test_evaluation.py` | `normalize_scores`, `all_thresholds_met`, `result_signature`, `normalize_result_df` | None (pure fns) |
| `test_state.py` | `write_stage` SQL generation, `load_run` returns dict | Mock spark |

```bash
# Run unit tests
uv run pytest tests/unit/ -v
```

### 4. Integration Tests — `tests/integration/`

| Test File | What It Tests | Mocking |
|-----------|---------------|---------|
| `test_preflight_integration.py` | Config fetch + UC metadata + benchmark validation | Mock Genie API, mock UC |
| `test_baseline_integration.py` | Full predict + score pipeline with known inputs | Mock Genie API, real spark |
| `test_lever_loop_integration.py` | Cluster → propose → apply → eval → accept/rollback (1 lever) | Mock Genie API, mock LLM |
| `test_resume_integration.py` | Load from Delta, continue from last stage | Pre-populated Delta |
| `test_rollback_integration.py` | Detect regression → rollback → skip lever | Orchestrated failure |

```bash
uv run pytest tests/integration/ -v
```

### 5. API Tests — `tests/api/`

Use FastAPI's `TestClient` with **APX dependency overrides** for `WorkspaceClient` and `spark`:

```python
from fastapi.testclient import TestClient
from genie_space_optimizer.backend.app import app
from genie_space_optimizer.backend.core import Dependencies

# Override dependencies for testing
def mock_client():
    return MockWorkspaceClient()

app.dependency_overrides[Dependencies.Client] = mock_client
client = TestClient(app)
```

| Test File | What It Tests |
|-----------|---------------|
| `test_spaces_api.py` | `GET /api/genie/spaces`, `GET .../spaces/:id`, `POST .../optimize` |
| `test_runs_api.py` | `GET /api/genie/runs/:id`, stage-to-step mapping, `GET .../comparison` |
| `test_apply_discard_api.py` | `POST .../apply`, `POST .../discard` |
| `test_activity_api.py` | `GET /api/genie/activity` ± space_id filter |

```bash
uv run pytest tests/api/ -v
```

### 6. Frontend Tests — `src/genie_space_optimizer/ui/__tests__/`

Use **Vitest + React Testing Library** (APX default test stack):

| Test File | What It Tests |
|-----------|---------------|
| `Dashboard.test.tsx` | Grid renders, search filters, activity table |
| `SpaceDetail.test.tsx` | Config panels render, Start Optimization button |
| `PipelineView.test.tsx` | Polling, step status, lever sub-progress |
| `ComparisonView.test.tsx` | Score card, config diff, apply/discard |
| `components/*.test.tsx` | Individual component rendering + interaction |

Mock API responses using MSW (Mock Service Worker) to intercept the auto-generated hooks.

```bash
bun test
```

### 7. `tests/conftest.py` — Shared fixtures

```python
import pytest
from unittest.mock import MagicMock

@pytest.fixture
def mock_spark():
    spark = MagicMock()
    spark.sql.return_value.toPandas.return_value = pd.DataFrame()
    return spark

@pytest.fixture
def mock_ws():
    ws = MagicMock()
    ws.genie.list_spaces.return_value = [...]
    return ws

@pytest.fixture
def sample_run():
    return {"run_id": "test-run-001", "space_id": "space-abc", ...}
```

---

## File Structure (additions to existing project)

```
genie-space-optimizer/
├── resources/
│   └── genie_optimization_job.yml     # Multi-task job YAML (5 tasks) + evaluation job
│
├── tests/
│   ├── conftest.py                     # Shared fixtures
│   ├── unit/
│   │   ├── test_config.py
│   │   ├── test_scorers.py
│   │   ├── test_optimizer.py
│   │   ├── test_applier.py
│   │   ├── test_benchmarks.py
│   │   ├── test_evaluation.py
│   │   └── test_state.py
│   ├── integration/
│   │   ├── test_preflight_integration.py
│   │   ├── test_baseline_integration.py
│   │   ├── test_lever_loop_integration.py
│   │   ├── test_resume_integration.py
│   │   └── test_rollback_integration.py
│   └── api/
│       ├── test_spaces_api.py
│       ├── test_runs_api.py
│       ├── test_apply_discard_api.py
│       └── test_activity_api.py
│
├── src/genie_space_optimizer/ui/__tests__/
│   ├── Dashboard.test.tsx
│   ├── SpaceDetail.test.tsx
│   ├── PipelineView.test.tsx
│   └── ComparisonView.test.tsx
```

---

## Deployment Flow (Complete)

```
1. Dev workflow:
   apx dev start                    # Frontend HMR + backend reload + OpenAPI codegen
   uv run pytest tests/ -v          # Python tests
   bun test                          # Frontend tests

2. Build:
   apx build                        # React → __dist__, Python → wheel in .build/

3. Deploy:
   databricks bundle deploy -p dev   # Upload wheel, create/update app + jobs

4. Verify:
   databricks bundle validate        # Validate bundle config
   # Open https://<workspace>.cloud.databricks.com/apps/genie-space-optimizer

5. End-to-end:
   User opens app → Dashboard
     → Click Space → Space Detail
       → Click "Start Optimization" → Pipeline View (polling)
         → Multi-task job runs: preflight → baseline → lever_loop → finalize
         → Workflows UI shows task progress (task 3/5)
         → App polls Delta for lever-level detail
         → Pipeline completes → "View Results"
           → Comparison View → Apply or Discard
             → Back to Dashboard
```

---

## Acceptance Criteria

1. `databricks bundle validate` passes
2. `apx build` produces wheel in `.build/`
3. `databricks bundle deploy -p dev` deploys app + jobs + notebooks
4. App URL is accessible, shows Dashboard
5. Starting optimization creates a multi-task job visible in Databricks Workflows (5 tasks in DAG view)
6. Unit tests pass: `uv run pytest tests/unit/ -v`
7. API tests pass: `uv run pytest tests/api/ -v`
8. Frontend tests pass: `bun test`
9. End-to-end: user can select space → optimize → watch pipeline → view results → apply/discard

---

## Predecessor Prompts
- BUILD-01 through BUILD-07 — the entire application

## Successor Prompts
None — this is the final build step. The app is deployable.
