# BUILD-05: Optimization Harness

> **APX project.** All Python code lives under `src/genie_space_optimizer/`. See `APX-BUILD-GUIDE.md` for full project conventions.

## Context

You are building the **Genie Space Optimizer**, a Databricks App (APX). This step creates the harness library — the stage functions that power the 5-task Databricks Job — plus the 5 job notebooks that call them.

**Architecture: Multi-task Databricks Job.** The optimization runs as a single Databricks Job with 5 tasks: `preflight` → `baseline_eval` → `lever_loop` → `finalize` → `deploy`. Each task is a separate notebook on Serverless compute. Tasks pass parameters via `dbutils.jobs.taskValues` and write detailed state to Delta. The lever loop task has internal Python iteration for levers 1-6 with convergence checking. The global `apply_mode` parameter controls where patches are written (Genie config vs UC artifacts).

**Read these reference docs before starting:**
- `docs/genie_space_optimizer/02-optimization-harness.md` — full spec: multi-task architecture, stage functions, task values, convergence, resume, error handling
- `docs/genie_space_optimizer/06-jobs-and-deployment.md` — multi-task job YAML, task notebooks, inter-task data flow
- `docs/genie_space_optimizer/00-architecture-overview.md` — three-tier architecture, resilience model

**Depends on (already built):**
- `src/genie_space_optimizer/common/config.py` — all constants
- `src/genie_space_optimizer/common/genie_client.py` — Genie API
- `src/genie_space_optimizer/common/uc_metadata.py` — UC introspection
- `src/genie_space_optimizer/optimization/state.py` — Delta state machine (BUILD-02)
- `src/genie_space_optimizer/optimization/evaluation.py` — evaluation engine (BUILD-03)
- `src/genie_space_optimizer/optimization/optimizer.py` — failure analysis (BUILD-04)
- `src/genie_space_optimizer/optimization/applier.py` — patch application (BUILD-04)
- `src/genie_space_optimizer/optimization/benchmarks.py` — benchmark management (BUILD-04)
- `src/genie_space_optimizer/optimization/repeatability.py` — repeatability testing (BUILD-03)

---

## What to Build

### 1. `optimization/harness.py` — Stage Functions Library

The harness is a library of 5 stage functions, each callable independently from a job notebook:

```python
def _run_preflight(w, spark, run_id, space_id, catalog, schema, domain, experiment_name=None):
    """Stage 1: Fetch config, UC metadata, generate/load benchmarks, create experiment."""

def _run_baseline(run_id, space_id, benchmarks, exp_name, model_id, catalog, schema):
    """Stage 2: Run full 8-judge evaluation, check thresholds."""

def _run_lever_loop(run_id, space_id, domain, benchmarks, exp_name,
                    prev_scores, prev_accuracy, prev_model_id, config,
                    catalog, schema, levers, max_iterations, thresholds=None,
                    apply_mode="genie_config"):
    """Stage 3: Iterate levers 1-6 with convergence checking (internal Python loop).
    apply_mode: Where patches are written — 'genie_config' | 'uc_artifact' | 'both'."""

def _run_finalize(run_id, space_id, domain, exp_name, prev_scores, prev_model_id,
                  iteration_counter, catalog, schema, run_repeatability=True):
    """Stage 4: Repeatability test, promote model, generate report."""

def _run_deploy(run_id, deploy_target, space_id, exp_name, domain,
                prev_model_id, iteration_counter, catalog, schema):
    """Stage 5: Deploy via DABs, held-out evaluation."""
```

**Stage 1: PREFLIGHT** — `_run_preflight()`
1. Fetch Genie Space config via API
2. Fetch UC metadata (columns, tags, routines)
3. Create MLflow experiment
4. **Load or generate benchmarks** (see BUILD-03 Section 4)
5. Validate benchmark SQL via EXPLAIN
6. Sync to MLflow evaluation dataset
7. Register judge prompts (idempotent)
8. Create LoggedModel iteration 0
9. Write PREFLIGHT_STARTED → PREFLIGHT_COMPLETE to Delta

**Stage 2: BASELINE EVAL** — `_run_baseline()`
1. Submit evaluation job (or run inline)
2. Normalize scores 0-1 → 0-100
3. Write iteration 0 to Delta
4. Check if all thresholds met → return `thresholds_met` flag
5. Write BASELINE_EVAL_STARTED → BASELINE_EVAL_COMPLETE

**Stage 3: LEVER LOOP** — `_run_lever_loop()` (internal Python loop)
For each lever in [1, 2, 3, 4, 5, 6]:
1. Check convergence → `break` if met
2. Check max iterations → `break`
3. Cluster failures, generate proposals, skip if none
4. Apply patches, verify, wait 30s propagation
5. Slice gate → rollback if score drops > 5 pts
6. P0 gate → rollback if any P0 fails
7. Full evaluation → rollback if regression > 2 pts
8. Accept: update Delta, update best scores

Write stage transitions for every lever sub-step.

**Resume within lever loop:** If this task is retried after a crash, `_resume_lever_loop()` reads Delta to find the last completed lever and continues from there.

**Stage 4: FINALIZE** — `_run_finalize()`
1. Repeatability test (if enabled)
2. Promote best model
3. Generate comprehensive report
4. Determine final status

**Stage 5: DEPLOY** — `_run_deploy()` (optional, skipped by `run_if`)

### 2. `optimization/harness.py` — Supporting Pieces

**`OptimizationResult` dataclass (used by convenience function):**
```python
@dataclass
class OptimizationResult:
    run_id: str
    space_id: str
    domain: str
    status: str           # CONVERGED | STALLED | MAX_ITERATIONS | FAILED
    best_iteration: int
    best_accuracy: float
    best_repeatability: float
    best_model_id: str | None
    convergence_reason: str | None
    total_iterations: int
    levers_attempted: list[int]
    levers_accepted: list[int]
    levers_rolled_back: list[int]
    final_scores: dict[str, float]
    experiment_name: str
    experiment_id: str
    report_path: str | None
    error: str | None
```

**Error handling wrapper:**
```python
def _safe_stage(run_id, stage_name, fn, *args, **kwargs):
    """Wraps every stage in every task. On exception: write FAILED to Delta, re-raise."""
```

**Lever loop resume:**
```python
def _resume_lever_loop(spark, run_id, catalog, schema) -> dict:
    """Read Delta to find last completed lever for resume after task retry.
    Returns: resume_from_lever, iteration_counter, prev_scores, prev_model_id."""
```

**Evaluation dispatch:**
```python
def run_evaluation_via_job(w, space_id, experiment_name, iteration,
                           domain, model_id, eval_scope, **kwargs) -> dict:
    """Submit evaluation job via w.jobs.submit_run(), poll, return results.
    Uses SDK. Benchmarks from MLflow eval dataset. Serverless compute.
    """
```

**Convenience function (for notebooks/tests):**
```python
def optimize_genie_space(space_id, catalog, schema, domain, *, apply_mode="genie_config", **kwargs) -> OptimizationResult:
    """Run all 5 stages in a single process. NOT used by the multi-task job.
    For ad-hoc notebook use and testing only.
    apply_mode: 'genie_config' (default) | 'uc_artifact' | 'both'"""
```

### 3. `optimization/preflight.py` — Preflight logic

Extracted to keep `harness.py` focused on orchestration.

- `run_preflight(w, spark, run_id, space_id, catalog, schema, domain, experiment_name)` → `(config, benchmarks, model_id, experiment_name)`
  - Tries to load existing benchmarks from MLflow eval dataset
  - If no dataset or too few benchmarks: calls `generate_benchmarks()` (from evaluation.py), writes BENCHMARK_GENERATION stage
  - Validates benchmark SQL via EXPLAIN, registers eval dataset, creates LoggedModel

### 4. `optimization/models.py` — LoggedModel management

- `create_genie_model_version(w, space_id, config, iteration, domain, *, uc_schema, uc_columns=None, uc_tags=None, uc_routines=None, patch_set=None, parent_model_id=None)` — MLflow LoggedModel snapshot
- `promote_best_model(spark, run_id, catalog, schema)` — promote best iteration's model
- `rollback_to_model(w, model_id)` — restore config from snapshot
- `link_eval_scores_to_model(model_id, scores)` — link metrics

### 5. `optimization/report.py` — Comprehensive Report Generation

- `generate_report(spark, run_id, domain, catalog, schema, output_dir)` → markdown path
- Reads from Delta: all 5 tables (runs, stages, iterations, patches, ASI)

**Report structure (all sections required):**

1. **Header:** Run ID, space name, domain, triggered by, start/end time, final status, convergence reason
2. **Executive Summary:** Baseline score → final score, improvement %, iterations run, levers accepted / rolled back
3. **Per-Iteration Detail Table:**
   - Iteration number, lever applied, MLflow eval run ID, model ID
   - Per-judge scores (all 8) with delta from previous iteration
   - Number of questions correct / total
   - Verdict: accepted / rolled_back / skipped + reason
4. **Per-Lever Detail:**
   - Failure clusters found (count, root causes, blamed objects)
   - Proposals generated (count, descriptions, confidence scores)
   - Patches applied (type, target, risk level, command summary)
   - Score before → after → delta per judge
   - If rolled back: reason, regression details
5. **Patch Inventory:** Full table of every patch (including rolled-back), with: iteration, lever, patch_type, target_object, risk_level, status (applied / rolled_back), rollback_reason
6. **ASI Summary:** Top failure types by frequency, most-blamed objects, counterfactual fixes acted on vs ignored
7. **Repeatability Report:** Per-question classification (IDENTICAL / MINOR / SIGNIFICANT / CRITICAL)
8. **MLflow Links:** Experiment URL, all eval run IDs (clickable), model version links

Helper functions:
- `_build_header(run_row)` → markdown
- `_build_executive_summary(run_row, iterations_df)` → markdown
- `_build_iteration_table(iterations_df)` → markdown table
- `_build_lever_detail(iterations_df, patches_df, asi_df)` → markdown
- `_build_patch_inventory(patches_df)` → markdown table
- `_build_asi_summary(asi_df)` → markdown
- `_build_repeatability_report(iterations_df)` → markdown
- `_build_mlflow_links(run_row, iterations_df)` → markdown

---

## File Structure

```
src/genie_space_optimizer/
├── optimization/
│   ├── __init__.py
│   ├── harness.py          # Stage functions + _safe_stage + _resume_lever_loop + convenience fn
│   ├── preflight.py        # Stage 1 extracted logic
│   ├── state.py            # (BUILD-02)
│   ├── evaluation.py       # (BUILD-03)
│   ├── optimizer.py        # (BUILD-04)
│   ├── applier.py          # (BUILD-04)
│   ├── benchmarks.py       # (BUILD-04)
│   ├── repeatability.py    # (BUILD-03)
│   ├── models.py           # LoggedModel management
│   ├── report.py           # Report generation
│   └── scorers/            # (BUILD-03)
│
├── jobs/                    # One notebook per task
│   ├── run_preflight.py     # Task 1
│   ├── run_baseline.py      # Task 2
│   ├── run_lever_loop.py    # Task 3
│   ├── run_finalize.py      # Task 4
│   ├── run_deploy.py        # Task 5
│   └── run_evaluation_only.py  # Standalone
```

---

## Key Design Decisions

1. **Multi-task job, not monolithic.** 5 tasks in one Databricks Job. Workflows UI shows stage progress. Per-task retry and timeout.
2. **`dbutils.jobs.taskValues` for inter-task data.** Small parameters (run_id, model_id, scores JSON). Detailed state goes to Delta.
3. **Lever loop is one task with internal iteration.** `for_each_task` doesn't support convergence/early exit. The Python `for` loop handles levers dynamically.
4. **`_resume_lever_loop()` reads Delta on task retry.** Finds last completed lever, skips already-done levers.
5. **`_safe_stage` wrapper in every task.** Each sub-step within each task is wrapped — UI always sees where failure occurred.
6. **`run_if` conditions.** Lever loop skipped if baseline meets thresholds. Deploy skipped if no deploy_target.
7. **Convenience function for tests.** `optimize_genie_space()` runs all 5 stages in-process for notebook/test use.

---

## Acceptance Criteria

1. Each stage function is independently callable with explicit parameters (no global state)
2. Each job notebook reads task values from upstream, calls the stage function, writes task values for downstream
3. `lever_loop` task handles internal iteration and writes lever-level stages to Delta
4. `_resume_lever_loop()`: kill lever_loop mid-lever → retry → picks up from last completed lever
5. `run_if`: baseline meets thresholds → lever_loop task skipped → finalize reads from baseline_eval
6. Error: Genie API fails → FAILED in Delta, task fails, downstream tasks don't run
7. `optimize_genie_space()` still works as a single-process convenience for testing

---

## Predecessor Prompts
- BUILD-01 through BUILD-04

## Successor Prompts
- BUILD-06 (Jobs + Backend) — wraps harness in job, exposes state via API
