# BUILD-02: Delta State Machine

> **APX project.** All Python code lives under `src/genie_space_optimizer/`. See `APX-BUILD-GUIDE.md` for full project conventions.

## Context

You are building the **Genie Space Optimizer**, a Databricks App (APX). This step creates the Delta-backed state machine that persists every stage transition, iteration score, and patch record. Both the optimization harness (writer) and the FastAPI backend (reader) depend on it.

**Read these reference docs before starting:**
- `docs/genie_space_optimizer/01-delta-state-schema.md` — full DDL, column definitions, state machine diagram, query patterns
- `docs/genie_space_optimizer/07-configuration-and-constants.md` — constants referenced by state helpers

**Depends on (already built in BUILD-01):**
- `src/genie_space_optimizer/common/config.py` — constants
- `src/genie_space_optimizer/common/delta_helpers.py` — generic Delta read/write helpers

---

## What to Build

### 1. `optimization/state.py`

Optimization-specific state module. Knows about the 5 Delta tables and provides typed read/write functions.

#### Table Bootstrapping

```python
def ensure_optimization_tables(spark, catalog: str, schema: str) -> None:
    """Create 5 optimization Delta tables if they don't exist (idempotent).
    Uses CREATE TABLE IF NOT EXISTS with exact DDL from 01-delta-state-schema.md.
    """
```

Tables (see `01-delta-state-schema.md` for DDL):
1. `genie_opt_runs` — one row per optimization run
2. `genie_opt_stages` — one row per stage transition
3. `genie_opt_iterations` — one row per evaluation iteration with scores
4. `genie_opt_patches` — one row per applied patch
5. `genie_eval_asi_results` — ASI feedback from judges

#### Write Functions (called by harness/task notebooks)

```python
def create_run(spark, run_id, space_id, domain, catalog, schema, **kwargs) -> None:
    """INSERT new row into genie_opt_runs with status QUEUED."""

def update_run_status(spark, run_id, catalog, schema, *, status=None,
                      best_iteration=None, best_accuracy=None,
                      best_repeatability=None, best_model_id=None,
                      convergence_reason=None, job_run_id=None) -> None:
    """UPDATE genie_opt_runs — only sets non-None fields."""

def write_stage(spark, run_id, stage, status, *, task_key=None,
                lever=None, iteration=None,
                detail=None, error_message=None, catalog=None, schema=None) -> None:
    """INSERT into genie_opt_stages.
    task_key: which Databricks Job task wrote this (preflight, baseline_eval, lever_loop, finalize, deploy).
    detail dict → JSON-serialized into detail_json.
    Computes duration_seconds for COMPLETE/FAILED by diffing STARTED row.
    """

def write_iteration(spark, run_id, iteration, eval_result, *, catalog, schema,
                    lever=None, eval_scope="full", model_id=None) -> None:
    """INSERT into genie_opt_iterations with scores_json, failures_json, etc."""

def write_patch(spark, run_id, iteration, lever, patch_index, patch_record,
                catalog, schema) -> None:
    """INSERT into genie_opt_patches."""

def mark_patches_rolled_back(spark, run_id, iteration, reason, catalog, schema) -> None:
    """UPDATE genie_opt_patches SET rolled_back=true WHERE run_id+iteration."""
```

#### Read Functions (called by harness for resume + backend for display)

```python
def load_run(spark, run_id, catalog, schema) -> dict | None:
    """Returns plain dict (not Spark Row) or None."""

def load_stages(spark, run_id, catalog, schema) -> pd.DataFrame:
    """ORDER BY started_at ASC."""

def load_iterations(spark, run_id, catalog, schema) -> pd.DataFrame:
    """ORDER BY iteration ASC."""

def load_patches(spark, run_id, catalog, schema) -> pd.DataFrame:
    """ORDER BY applied_at ASC."""

def read_latest_stage(spark, run_id, catalog, schema) -> dict | None:
    """Most recent stage row."""

def load_latest_full_iteration(spark, run_id, catalog, schema) -> dict | None:
    """Latest iteration with eval_scope='full'. Used for resume + convergence."""

def load_runs_for_space(spark, space_id, catalog, schema) -> pd.DataFrame:
    """All runs for a space, ordered by started_at DESC. Used by Space Detail."""

def load_recent_activity(spark, catalog, schema, *, space_id=None, limit=20) -> pd.DataFrame:
    """Recent runs across workspace or for a space. Used by Dashboard."""
```

---

## File Structure

```
src/genie_space_optimizer/
├── optimization/
│   ├── __init__.py
│   └── state.py
```

---

## Key Design Decisions

1. **Two layers of orchestration.** The Databricks Jobs DAG (5 tasks with `depends_on`/`run_if`) handles coarse stage sequencing and per-task retry. The Delta state machine provides fine-grained tracking within each task (especially lever_loop's internal iterations):
   - Every stage transition is written to Delta **before** the task proceeds
   - If `lever_loop` crashes, Databricks retries that task. `_resume_lever_loop()` reads the last completed lever from Delta
   - The `task_key` column in `genie_opt_stages` identifies which job task wrote each row
   - Multiple concurrent runs are isolated by `run_id` partitioning — no global locks needed
2. **Duration computation:** `write_stage()` with COMPLETE/FAILED diffs the matching STARTED row automatically.
3. **JSON columns:** `detail_json`, `scores_json`, `failures_json` are STRING containing JSON. `json.dumps()` for writes, `json.loads()` for reads.
4. **SQL safety:** Use parameterized values with proper escaping. For production hardening, consider `spark.createDataFrame + write.mode("append")` instead of raw INSERT.
5. **Timestamps:** `CURRENT_TIMESTAMP()` in SQL or `datetime.now(timezone.utc).isoformat()`.
6. **Partitioning:** `genie_opt_stages` and `genie_opt_iterations` partitioned by `run_id`.

---

## Acceptance Criteria

1. `ensure_optimization_tables()` creates all 5 tables, is idempotent
2. `write_stage()` auto-computes duration for COMPLETE/FAILED
3. `load_run()` returns a plain Python dict for JSON serialization
4. All read functions return pandas DataFrames (`.toPandas()`)
5. All functions accept `spark`, `catalog`, `schema` as explicit args — no globals
6. Roundtrip: write run + stages + iterations → read back → values match

---

## Predecessor Prompts
- BUILD-01 — provides `common/delta_helpers.py` and `common/config.py`

## Successor Prompts
- BUILD-05 (Harness) — calls all write functions
- BUILD-06 (Backend) — calls all read functions
