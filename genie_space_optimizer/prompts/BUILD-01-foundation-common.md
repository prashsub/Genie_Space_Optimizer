# BUILD-01: Foundation — Common Package

> **APX project.** This app was scaffolded with `apx init genie-space-optimizer --addons=ui,sidebar,sql,cursor`. All code lives under `src/genie_space_optimizer/`. The Python package is managed by `uv`, the frontend by `bun`. See `APX-BUILD-GUIDE.md` for the full APX setup.

## Context

You are building the **Genie Space Optimizer**, a Databricks App using [apx](https://github.com/databricks-solutions/apx). This is the first build step: creating the shared `common/` package that every other layer depends on.

**Read these reference docs before starting:**
- `docs/genie_space_optimizer/00-architecture-overview.md` — overall architecture and project structure
- `docs/genie_space_optimizer/07-configuration-and-constants.md` — all constants, thresholds, feature flags
- `docs/genie_space_optimizer/08-module-map.md` — function-by-function mapping from source to target modules

**Source code to extract from (existing codebase):**
- `data_product_accelerator/skills/semantic-layer/genie-optimization-workers/` — worker scripts with functions to extract
- `data_product_accelerator/skills/semantic-layer/05-genie-optimization-orchestrator/` — orchestrator with shared functions

---

## What to Build

Create `src/genie_space_optimizer/common/` with 4 modules:

### 1. `common/__init__.py`
Empty init file.

### 2. `common/config.py`
Consolidate ALL constants into one module. Reference `07-configuration-and-constants.md` for the complete list:

- `DEFAULT_THRESHOLDS` — per-judge quality targets (0-100 scale)
- `MLFLOW_THRESHOLDS` — same in 0-1 scale for `mlflow.genai.evaluate()`
- `REPEATABILITY_TARGET` — default 90.0
- Rate limits: `RATE_LIMIT_SECONDS=12`, `PROPAGATION_WAIT_SECONDS=30`
- Polling: `GENIE_POLL_INITIAL=3`, `GENIE_POLL_MAX=10`, `GENIE_MAX_WAIT=120`, `JOB_POLL_INTERVAL=30`, `JOB_MAX_WAIT=3600`, `UI_POLL_INTERVAL=5`
- Iteration: `MAX_ITERATIONS=5`, `SLICE_GATE_TOLERANCE=5.0`, `REGRESSION_THRESHOLD=2.0`, `ARBITER_CORRECTION_TRIGGER=3`
- LLM: `LLM_ENDPOINT="databricks-claude-opus-4-6"`, `LLM_TEMPERATURE=0`, `LLM_MAX_RETRIES=3`
- Benchmark generation: `TARGET_BENCHMARK_COUNT=20`, `BENCHMARK_CATEGORIES`, `BENCHMARK_GENERATION_PROMPT` (full template), `TEMPLATE_VARIABLES`
- Proposal generation prompts (all 4):
  - `PROPOSAL_GENERATION_PROMPT` — levers 1-3 (UC metadata proposals)
  - `LEVER_4_JOIN_SPEC_PROMPT` — lever 4 (join specification proposals)
  - `LEVER_5_DISCOVERY_PROMPT` — lever 5 (column discovery settings proposals)
  - `LEVER_6_INSTRUCTION_PROMPT` — lever 6 (Genie Space instruction proposals)
- `PATCH_TYPES` — all 35 patch type definitions (list of dicts: `type`, `scope`, `risk`, `affected_objects`). Scope is either `"genie_config"`, `"uc_artifact"`, or `"apply_mode"` (resolved at runtime)
- `CONFLICT_RULES` — all 18 conflict pairs
- `FAILURE_TAXONOMY` — all 22 failure types (set of strings), including: `missing_join_spec`, `wrong_join_spec`, `missing_format_assistance`, `missing_entity_matching`
- `JUDGE_PROMPTS` — 5 LLM judge prompt templates
- `ASI_SCHEMA` — 12-field Actionable Side Information schema
- `NON_EXPORTABLE_FIELDS` — Genie Space fields to strip before config export
- `_LEVER_TO_PATCH_TYPE` — (failure_type, lever) → patch type (updated for 6 levers)
- `LOW_RISK_PATCHES`, `MEDIUM_RISK_PATCHES`, `HIGH_RISK_PATCHES` — risk classification sets (updated with Lever 4/5 patch types)
- `REPEATABILITY_CLASSIFICATIONS` — thresholds for IDENTICAL/MINOR/SIGNIFICANT/CRITICAL
- `LEVER_NAMES` — lever number → display name (6 entries: 1=Tables & Columns, 2=Metric Views, 3=Table-Valued Functions, 4=Join Specifications, 5=Column Discovery Settings, 6=Genie Space Instructions)
- `DEFAULT_LEVER_ORDER` — `[1, 2, 3, 4, 5, 6]`
- `MAX_VALUE_DICTIONARY_COLUMNS` — 120 (limit for `build_value_dictionary` per space)
- `APPLY_MODE` — default `"genie_config"`. One of: `"genie_config"` | `"uc_artifact"` | `"both"`. Controls where levers 1-3 write changes (levers 4-6 always genie_config)
- Feature flags: `USE_PATCH_DSL=True`, `USE_JOB_MODE=True`, `USE_LEVER_AWARE=True`, `ENABLE_CONTINUOUS_MONITORING=False`, `APPLY_MODE="genie_config"`

### 3. `common/genie_client.py`
All Genie Space API interactions. Every function takes `WorkspaceClient` as first arg (dependency injection — this is the APX pattern used with `Dependencies.Client`).

Must include:
- `fetch_space_config(w, space_id)` — GET /api/2.0/genie/spaces/{id}?include_serialized_space=true
- `list_spaces(w)` — list all Genie Spaces accessible to the user
- `run_genie_query(w, space_id, question, max_wait=120)` — start_conversation + poll
- `detect_asset_type(sql)` → `"MV" | "TVF" | "TABLE" | "NONE"`
- `resolve_sql(sql, catalog, gold_schema)` — substitute ${catalog} and ${gold_schema}
- `sanitize_sql(sql)` — extract first statement, strip comments/semicolons
- `patch_space_config(w, space_id, config)` — PATCH /api/2.0/genie/spaces/{id}

### 4. `common/uc_metadata.py`
UC introspection queries. Every function takes `spark` as first arg.

Must include:
- `get_columns(spark, catalog, schema)` — SELECT from information_schema.columns
- `get_tags(spark, catalog, schema)` — SELECT from information_schema.table_tags + column_tags
- `get_routines(spark, catalog, schema)` — SELECT from information_schema.routines
- `get_table_descriptions(spark, catalog, schema)` — table-level descriptions
- `get_metric_views(spark, catalog, schema)` — discover metric views

### 5. `common/delta_helpers.py`
Generic Delta read/write helpers (no optimization-specific schemas).

Must include:
- `read_table(spark, catalog, schema, table, filters=None)` → pd.DataFrame
- `insert_row(spark, catalog, schema, table, row_dict)` — INSERT INTO
- `update_row(spark, catalog, schema, table, key_cols, update_cols)` — UPDATE SET WHERE
- `run_query(spark, sql)` → pd.DataFrame

---

## File Structure

```
src/genie_space_optimizer/
├── common/
│   ├── __init__.py
│   ├── config.py
│   ├── genie_client.py
│   ├── uc_metadata.py
│   └── delta_helpers.py
```

---

## Acceptance Criteria

1. Every constant from `07-configuration-and-constants.md` is in `config.py` — nothing hardcoded elsewhere
2. `genie_client.py` uses `WorkspaceClient` from `databricks-sdk` — no `requests`, no subprocess
3. `uc_metadata.py` uses `spark.sql()` — no REST API calls to UC
4. `delta_helpers.py` is generic — no optimization table names inside it
5. No circular imports — `common/` imports only external packages + stdlib
6. All functions have type hints and docstrings
7. No global state — all dependencies passed as arguments

---

## Predecessor Prompts
None — this is the first build step.

## Successor Prompts
- BUILD-02 depends on `common/delta_helpers.py` and `common/config.py`
- BUILD-03 depends on `common/genie_client.py` and `common/config.py`
- BUILD-04 depends on `common/config.py` and `common/genie_client.py`
