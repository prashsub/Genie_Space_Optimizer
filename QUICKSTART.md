# Quickstart Guide

Get the Genie Space Optimizer running in your Databricks workspace in under 15 minutes.

---

## 1. Prerequisites

Before you start, ensure you have the following installed and configured:

| Tool | Version | Install |
|------|---------|---------|
| Python | 3.11+ | [python.org](https://www.python.org/downloads/) |
| uv | latest | `curl -LsSf https://astral.sh/uv/install.sh \| sh` |
| bun | latest | `curl -fsSL https://bun.sh/install \| bash` |
| apx | latest | `uv tool install apx` |
| Databricks CLI | latest | `curl -fsSL https://raw.githubusercontent.com/databricks/setup-cli/main/install.sh \| sh` |

### Databricks Workspace Requirements

- A Databricks workspace with **Unity Catalog** enabled
- A **SQL Warehouse** (serverless recommended) with `CAN_USE` permission
- At least one **Genie Space** to optimize
- Workspace permissions to create **Databricks Apps** and **Jobs**

### Authenticate the Databricks CLI

```bash
databricks auth login --host https://<your-workspace>.cloud.databricks.com
```

Verify with:

```bash
databricks auth env
```

---

## 2. Clone and Install Dependencies

```bash
git clone https://github.com/prashsub/Genie_Space_Optimizer.git
cd Genie_Space_Optimizer
```

Install Python dependencies:

```bash
uv sync
```

Install frontend dependencies:

```bash
apx bun install
```

---

## 3. Configure Your Workspace

Edit `databricks.yml` to point to your workspace resources:

```yaml
variables:
  catalog:
    default: "your_catalog_name"        # Unity Catalog with your Genie Space tables
  gold_schema:
    default: "genie_optimization"        # Schema for optimizer state tables (will be created)
  warehouse_id:
    default: "your_warehouse_id"         # SQL Warehouse ID
```

**Finding your Warehouse ID:** Navigate to SQL Warehouses in your workspace. The warehouse ID is in the URL: `https://<workspace>/sql/warehouses/<warehouse_id>`.

---

## 4. Deploy to Databricks

Build and deploy the app, database, and jobs:

```bash
databricks bundle deploy -p <your-profile>
```

This provisions:
- A **Databricks App** at `https://<workspace>/apps/genie-space-optimizer`
- A **PostgreSQL database** instance (Lakebase, CU_1 capacity)
- Two **Databricks Jobs**: the main optimization pipeline and a standalone evaluation job
- **Environment variables** automatically wired from your `databricks.yml` variables

Wait for the deployment to complete (~2-3 minutes).

### Grant Data Access

After deployment, the app's service principal needs access to your Unity Catalog schemas. You can do this from the **Settings** page in the app, or run the grant script:

```bash
python resources/grant_app_uc_permissions.py --catalog your_catalog --schema your_schema
```

---

## 5. Open the App

After deployment, find your app URL:

```bash
databricks apps list
```

Or navigate to **Apps** in the Databricks workspace sidebar and click **genie-space-optimizer**.

---

## 6. Optimize Your First Genie Space

### Step 0: Configure Data Access (First Time Only)

Navigate to the **Settings** page (gear icon in the navbar). The app auto-detects available schemas and shows the service principal's current permissions. Click **Grant Access** for each schema your Genie Spaces depend on.

### Step 1: Browse Your Spaces

The dashboard shows all Genie Spaces you have edit access to. Each card displays the space name, table count, and quality score (if previously evaluated).

### Step 2: Select a Space

Click a space card to view its details:
- **Tables** -- Registered tables with column counts and descriptions
- **Instructions** -- Current Genie instructions
- **Sample Questions** -- Configured example queries
- **History** -- Previous optimization runs (if any)

### Step 3: Start Optimization

Click **Optimize** to kick off the pipeline. This submits a multi-task Databricks Job with 5 stages:

| Stage | Duration | What Happens |
|-------|----------|-------------|
| Preflight | ~1 min | Validates config, collects UC metadata |
| Baseline Eval | ~5-15 min | Generates 20 benchmark questions, runs through Genie, scores with 8 LLM judges |
| Lever Loop | ~10-30 min | Iterates through 6 optimization levers (up to 5 iterations each) |
| Finalize | ~5-10 min | Repeatability testing, final scoring, report generation |
| Deploy | ~1 min | (Optional) Deploys optimized config to version control |

**Total time: 20-60 minutes** depending on space complexity and number of tables.

### Step 4: Monitor Progress

Click through to the run detail page to watch the pipeline in real-time:

- **Pipeline Steps** -- 5-step progress tracker with status, duration, and summaries
- **Lever Detail** -- Per-lever status: accepted, rolled back (with reason), or skipped
- **Score Progression** -- Baseline vs. optimized scores across 7 quality dimensions

The page auto-polls every 5 seconds.

### Step 5: Review and Apply

When the pipeline completes (status: `CONVERGED`, `STALLED`, or `MAX_ITERATIONS`):

1. Navigate to the **Comparison** view
2. Review the side-by-side diff:
   - Original vs. optimized **instructions**
   - Original vs. optimized **sample questions**
   - Original vs. optimized **table descriptions**
   - Per-dimension score improvements
3. Choose an action:
   - **Apply** -- Keep the optimized configuration (marks run as `APPLIED`)
   - **Discard** -- Roll back all patches to the original configuration (marks run as `DISCARDED`)

---

## 7. Local Development

For making changes to the app itself:

### Start the Dev Server

```bash
apx dev start
```

This starts three processes in the background:
- **Backend** (FastAPI) on port 8000
- **Frontend** (Vite) on port 5173
- **OpenAPI watcher** that regenerates `ui/lib/api.ts` on backend changes

### Check Your Code

```bash
apx dev check
```

Runs TypeScript and Python type checks in parallel.

### View Logs

```bash
apx dev logs -f     # Stream logs live
```

### Stop the Dev Server

```bash
apx dev stop
```

---

## 8. Key Configuration Knobs

All tunable parameters are in `src/genie_space_optimizer/common/config.py`:

### Quality Thresholds

If the baseline evaluation already meets all thresholds, the lever loop is skipped entirely.

```python
DEFAULT_THRESHOLDS = {
    "syntax_validity": 98.0,
    "schema_accuracy": 95.0,
    "logical_accuracy": 90.0,
    "semantic_equivalence": 90.0,
    "completeness": 90.0,
    "result_correctness": 85.0,
    "asset_routing": 95.0,
}
```

### Iteration Limits

```python
MAX_ITERATIONS = 5            # Max lever loop iterations
REGRESSION_THRESHOLD = 10.0   # % drop that triggers rollback
SLICE_GATE_TOLERANCE = 15.0   # % tolerance for per-dimension slice gate
PLATEAU_ITERATIONS = 2        # Consecutive no-improvement before stopping
```

### Benchmark Generation

```python
TARGET_BENCHMARK_COUNT = 20  # Number of evaluation questions to generate
```

### Rate Limiting

```python
RATE_LIMIT_SECONDS = 12                         # Delay between Genie API calls
PROPAGATION_WAIT_SECONDS = 30                   # Wait after config patch before evaluation
PROPAGATION_WAIT_ENTITY_MATCHING_SECONDS = 90   # Extra wait for entity matching propagation
```

These can also be overridden via environment variables (`GENIE_SPACE_OPTIMIZER_PROPAGATION_WAIT`, `GENIE_SPACE_OPTIMIZER_PROPAGATION_WAIT_ENTITY_MATCHING`).

### LLM

```python
LLM_ENDPOINT = "databricks-claude-opus-4-6"  # Foundation Model API endpoint
LLM_TEMPERATURE = 0                           # Deterministic output
```

---

## Troubleshooting

### "Optimization job not found"

The Databricks Job hasn't been deployed yet. Run:

```bash
databricks bundle deploy -p <your-profile>
```

### "Space not found" on the dashboard

You need **CAN_EDIT** permission on the Genie Space. The app filters to spaces you can modify.

### Delta tables not available

On the first run, Delta state tables are created automatically in the configured catalog/schema. If you see "Delta tables not yet available" warnings, this is normal for first use -- they will appear after the first optimization completes.

### Preflight fails with "information_schema permission" errors

The preflight stage now uses the Unity Catalog **REST API** by default to fetch column and routine metadata, bypassing `system.information_schema` permission requirements. If the REST API fails, it falls back to Spark SQL automatically. If both fail, ensure the app's service principal has `USE CATALOG` and `USE SCHEMA` grants on the target catalog/schema (see the **Settings** page or `resources/grant_app_uc_permissions.py`).

### Evaluation takes a long time

Each benchmark question requires a round-trip to the Genie API (with rate limiting at 12s intervals) plus LLM judge evaluation. For 20 questions with 8 judges, expect ~15 minutes for a full evaluation pass. You can reduce `TARGET_BENCHMARK_COUNT` in `config.py` for faster iteration during testing.

### Score didn't improve

Not all Genie Spaces benefit from metadata optimization. If the baseline score is already high (>90%), there may be limited room for improvement. The optimizer will report `STALLED` or `CONVERGED` and you can review the comparison to see what (if any) changes were attempted.

---

## Architecture Overview

```
┌─────────────────────┐       ┌─────────────────────┐
│   Browser (React)   │◄─────▶│   FastAPI Backend    │
│                     │  API   │   /api/genie/*       │
│  Dashboard          │       │                     │
│  Space Detail       │       │  ┌─────────────────┐│
│  Run Monitor        │       │  │ Dependencies     ││
│  Comparison Diff    │       │  │ .UserClient (OBO)││
│                     │       │  │ .Config (env)    ││
└─────────────────────┘       │  │ .Spark (serverless)│
                              │  └─────────────────┘│
                              └──────────┬──────────┘
                                         │ triggers
                              ┌──────────▼──────────┐
                              │  Databricks Job      │
                              │  (Multi-task)        │
                              │                     │
                              │  preflight           │
                              │    ↓                 │
                              │  baseline_eval       │
                              │    ↓                 │
                              │  lever_loop          │
                              │    ↓                 │
                              │  finalize            │
                              │    ↓                 │
                              │  deploy (optional)   │
                              └──────────┬──────────┘
                                         │ reads/writes
                    ┌────────────────────┼────────────────────┐
                    │                    │                    │
           ┌────────▼───────┐   ┌────────▼───────┐   ┌──────▼───────┐
           │  Delta Tables   │   │  Genie API     │   │  MLflow      │
           │  (5 state       │   │  (Space CRUD,  │   │  (Experiment │
           │   tables)       │   │   query exec)  │   │   tracking)  │
           └────────────────┘   └────────────────┘   └──────────────┘
```

**Data flow:**
1. User clicks **Optimize** in the React frontend
2. Backend creates a `QUEUED` run in Delta and submits the Databricks Job
3. Job tasks execute sequentially: preflight → baseline → lever loop → finalize
4. Each task reads/writes Delta state tables and calls the Genie API
5. LLM judges (via Foundation Model API) score each evaluation
6. Frontend polls the backend every 5 seconds for updated pipeline status
7. On completion, user reviews the comparison and applies or discards
