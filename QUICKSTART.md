# Quickstart Guide

Get the Genie Space Optimizer running in your Databricks workspace in under 15 minutes.

---

## 1. Prerequisites

Before you start, ensure you have the following installed and configured:


| Tool           | Version | Install                                                                                  |
| -------------- | ------- | ---------------------------------------------------------------------------------------- |
| Python         | 3.11+   | [python.org](https://www.python.org/downloads/)                                          |
| uv             | latest  | `curl -LsSf https://astral.sh/uv/install.sh | sh`                                        |
| bun            | latest  | `curl -fsSL https://bun.sh/install | bash`                                               |
| apx            | latest  | `uv tool install apx`                                                                    |
| Databricks CLI | latest  | `curl -fsSL https://raw.githubusercontent.com/databricks/setup-cli/main/install.sh | sh` |


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

The recommended deployment method uses `make deploy`, which orchestrates the full pipeline:

```bash
make deploy PROFILE=<your-profile>
```

This runs four steps automatically:

1. **clean-wheels** -- Removes stale `.whl` files from the workspace to prevent cache issues
2. **bundle deploy** -- Builds the wheel via `apx build`, removes `.build/.gitignore`, syncs files to workspace
3. **apps deploy** -- Creates a new deployment snapshot and restarts the app with the synced code
4. **verify** -- Confirms the new wheel is present on the workspace

Both `databricks bundle deploy` (file sync) and `databricks apps deploy` (snapshot + restart) are required. The bundle deploy syncs files but does not restart the app; the apps deploy creates an immutable snapshot from the synced files and restarts.

This provisions:

- A **Databricks App** at `https://<workspace>/apps/genie-space-optimizer`
- A **PostgreSQL database** instance (Lakebase, CU_1 capacity)
- Two **Databricks Jobs**: the main optimization pipeline and a standalone evaluation job
- **Environment variables** automatically wired from your `databricks.yml` variables

Wait for the deployment to complete (~2-3 minutes).

### Grant Data Access

After deployment, the app's service principal needs access to your Unity Catalog schemas. The app operates as an **advisor** — it shows you exactly what permissions are missing and provides **copyable SQL commands** on the Settings page. Run those commands in a SQL editor or use the grant script:

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

### Step 0: Configure Permissions (First Time Only)

Navigate to the **Settings** page (gear icon in the navbar). The app is an **advisor** — it reads current permissions and tells you exactly what's missing, with **copyable SQL commands** and **sharing instructions**. You run those yourself:

- **Schema Permissions** — shows per-schema read/write status for the app service principal. Copy the provided `GRANT USE CATALOG/USE SCHEMA/SELECT/MODIFY` SQL into a query editor to grant missing access.
- **Space Permissions** — shows whether the SP has CAN_MANAGE on each Genie Space. Use the provided sharing instructions to grant access from the Genie Space UI.

If you prefer using `apply_mode = both` (writes UC artifacts), ensure MODIFY is granted on the relevant schemas — the preflight stage will fail fast with a clear error if it's missing.

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


| Stage         | Duration   | What Happens                                                                                         |
| ------------- | ---------- | ---------------------------------------------------------------------------------------------------- |
| Preflight     | ~1 min     | Validates config, collects UC metadata                                                               |
| Baseline Eval | ~5-15 min  | Generates 20 benchmark questions, runs through Genie, scores with 9 LLM judges                       |
| Lever Loop    | ~10-30 min | Stage 2.5 prompt matching, arbiter benchmark corrections, then 5 levers with tiered failure analysis |
| Finalize      | ~5-10 min  | Repeatability testing, final scoring, report generation                                              |
| Deploy        | ~1 min     | (Optional) Deploys optimized config to version control                                               |


**Total time: 20-60 minutes** depending on space complexity and number of tables.

The lever loop now uses **tiered failure analysis**: hard failures (arbiter says `ground_truth_correct` or `neither_correct`) drive targeted fixes, while soft signals (individual judge failures on otherwise-correct questions) inform best-practice guidance in Lever 5 instructions. Lever 4 (Join Specifications) always runs its discovery path to document implicit joins. Lever 5 generates a holistic instruction rewrite considering all evaluation learnings.

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

## 7. Deploy & Run (Quick Reference)

One-liner to build, deploy, and trigger an optimization run:

```bash
# 1. Deploy (build wheel + sync + restart app)
make deploy PROFILE=genie-test

# 2. Trigger optimization for a Genie Space
APP_URL=$(databricks apps get genie-space-optimizer -p genie-test -o json | python3 -c "import sys,json; print(json.load(sys.stdin)['url'])")
TOKEN=$(databricks auth token -p genie-test | python3 -c "import sys,json; print(json.load(sys.stdin)['access_token'])")

curl -s -X POST "${APP_URL}/api/genie/trigger" \
  -H "Authorization: Bearer ${TOKEN}" \
  -H "Content-Type: application/json" \
  -d '{"space_id": "<GENIE_SPACE_ID>"}' | python3 -m json.tool
```

**Known Space IDs (genie-test profile):**


| Space                           | ID                                 |
| ------------------------------- | ---------------------------------- |
| Revenue & Property Intelligence | `01f10e84df3b14d993c30773abde7f44` |


**Poll run status:**

```bash
curl -s "${APP_URL}/api/genie/trigger/status/<RUN_ID>" \
  -H "Authorization: Bearer ${TOKEN}" | python3 -m json.tool
```

---

## 8. Programmatic API (Headless Trigger)

You can trigger optimization runs programmatically without the UI, using the `/trigger` endpoint:

```bash
# Trigger optimization
curl -X POST https://<app-url>/api/genie/trigger \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"space_id": "<genie-space-id>"}'

# Poll status
curl https://<app-url>/api/genie/trigger/status/<run_id> \
  -H "Authorization: Bearer $TOKEN"
```

This is useful for CI/CD pipelines or scheduled optimization workflows.

---

## 9. Local Development

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

## 10. Key Configuration Knobs

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
MAX_NOISE_FLOOR = 3.0         # Min score improvement to accept (below = cosmetic noise)
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

### Benchmark dates look wrong (stale "this year" / "last quarter")

The optimizer auto-detects relative time references in benchmark questions ("this year", "YTD", "last quarter", "last N months/days") and rewrites the ground-truth SQL date literals to match the current date. This is automatic and requires no manual intervention. If you still see date mismatches, the question may contain an explicit year (e.g., "for 2024") which is intentionally not rewritten.

### Evaluation takes a long time

Each benchmark question requires a round-trip to the Genie API (with rate limiting and exponential backoff on 429 rate limits) plus LLM judge evaluation. For 20 questions with 9 judges, expect ~15 minutes for a full evaluation pass. You can reduce `TARGET_BENCHMARK_COUNT` in `config.py` for faster iteration during testing.

### Spark session errors ("InvalidAccessKeyId", "ExpiredToken")

The backend automatically detects stale AWS STS credentials and recreates the Spark session. If you see transient credential errors in logs, they should self-heal within one retry. Persistent errors indicate the workspace's credential vending is misconfigured.

### Delta "SCHEMA_CHANGE_SINCE_ANALYSIS" errors

The Delta state tables may be dropped and recreated by upstream pipeline tasks. The `read_table` helper automatically retries with `REFRESH TABLE` on schema change errors. If persistent, check that concurrent optimization runs aren't conflicting.

### Stale wheel after deployment (optimization runs old code)

The `apx build` step generates a `.build/.gitignore` with `*` that blocks wheel sync. The `make deploy` pipeline handles this by removing the `.gitignore`, running `databricks bundle deploy` (file sync), and then `databricks apps deploy` (snapshot + restart). Always use `make deploy` to avoid stale wheels:

```bash
# Full redeploy (clean + bundle deploy + app deploy + verify)
make deploy PROFILE=<your-profile>

# Or just verify the current state
make verify PROFILE=<your-profile>
```

The app logs the resolved wheel at startup (`App wheel: genie_space_optimizer-x.y.z.whl (size=... bytes)`). Check app logs to confirm the correct wheel is loaded:

```bash
databricks apps logs genie-space-optimizer -p <your-profile>
```

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

