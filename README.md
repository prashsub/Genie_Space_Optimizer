# Genie Space Optimizer

A full-stack Databricks App that automatically optimizes [Genie Spaces](https://docs.databricks.com/en/genie/index.html) through iterative, LLM-driven metadata improvement. It evaluates your Genie Space configuration against benchmark questions, identifies quality gaps across 7 dimensions, and applies targeted patches through 6 optimization levers -- all with full rollback support.

Built with [apx](https://github.com/databricks-solutions/apx) (React + FastAPI).

---

## How It Works

```
┌─────────────────────────────────────────────────────────────────────┐
│                     Genie Space Optimizer                           │
│                                                                     │
│  ┌──────────┐   ┌──────────┐   ┌──────────┐   ┌──────────┐       │
│  │ Preflight│──▶│ Baseline │──▶│  Lever   │──▶│ Finalize │       │
│  │ Analysis │   │ Eval     │   │  Loop    │   │ & Report │       │
│  └──────────┘   └──────────┘   └──────────┘   └──────────┘       │
│       │              │              │               │              │
│  Config scan    8 LLM judges   6 levers ×5     Repeatability      │
│  UC metadata    20 benchmarks  iterations       Final scores       │
│  Validation     7 dimensions   Auto-rollback    Deploy (opt.)      │
└─────────────────────────────────────────────────────────────────────┘
```

### Optimization Pipeline (5 Steps)

1. **Configuration Analysis** -- Scans the Genie Space config, counts tables/instructions/sample questions, and validates structure.
2. **Metadata Collection** -- Queries Unity Catalog `information_schema` for columns, data types, tags, routines, and table descriptions.
3. **Baseline Evaluation** -- Generates ~20 benchmark questions via LLM, runs them through Genie, and scores responses across 7 quality dimensions using 8 LLM judges.
4. **Configuration Generation (Lever Loop)** -- Iterates through 6 optimization levers (up to 5 iterations), applying targeted patches. Each patch is evaluated; regressions trigger automatic rollback.
5. **Optimized Evaluation & Repeatability** -- Final evaluation of the optimized config, including repeatability testing to ensure consistent results.

### 6 Optimization Levers

| Lever | Name | What It Optimizes |
|-------|------|-------------------|
| 1 | Tables & Columns | Descriptions, visibility, column aliases |
| 2 | Metric Views | Measures, dimensions, MV YAML definitions |
| 3 | Table-Valued Functions | Parameters, TVF SQL, function signatures |
| 4 | Join Specifications | Table relationships, join columns, cardinality |
| 5 | Column Discovery | Example values, value dictionaries, synonyms |
| 6 | Genie Instructions | Routing rules, disambiguation, default behaviors |

### 7 Quality Dimensions

| Dimension | Target | What It Measures |
|-----------|--------|------------------|
| Syntax Validity | 98% | Generated SQL parses correctly |
| Schema Accuracy | 95% | Correct tables, columns, and joins referenced |
| Logical Accuracy | 90% | Correct aggregations, filters, GROUP BY, ORDER BY |
| Semantic Equivalence | 90% | Same business metric as expected answer |
| Completeness | 90% | All requested dimensions/measures included |
| Result Correctness | 85% | Correct final result values |
| Asset Routing | 95% | Correct asset type (table, metric view, TVF) selected |

---

## Tech Stack

| Layer | Technology |
|-------|------------|
| **Backend** | Python 3.11+, FastAPI, Pydantic, Uvicorn |
| **Frontend** | React 19, TypeScript, TanStack Router & Query, Vite |
| **UI Components** | shadcn/ui, Radix UI, Tailwind CSS 4 |
| **Data** | Delta Lake (state), PostgreSQL (Lakebase), Unity Catalog |
| **ML/AI** | MLflow 3.4+, Claude Opus 4.6 (via Databricks Foundation Model API) |
| **Infrastructure** | Databricks Apps, Databricks Jobs, SQL Warehouse |
| **SDK** | Databricks SDK 0.40+, Databricks Connect 15+ |
| **Build** | apx, uv (Python), bun (JavaScript) |

---

## Project Structure

```
Genie_Space_Optimizer/
├── pyproject.toml                    # Python project config & apx metadata
├── databricks.yml                    # Databricks Asset Bundle definition
├── app.yml                           # Databricks App entry point (uvicorn)
├── resources/
│   └── genie_optimization_job.yml    # Multi-task job definition (5 tasks)
│
├── src/genie_space_optimizer/
│   ├── backend/                      # FastAPI backend
│   │   ├── app.py                    # App factory (registers routes, serves frontend)
│   │   ├── models.py                 # Pydantic response models
│   │   ├── router.py                 # System routes (/version, /current-user)
│   │   ├── _spark.py                 # Serverless Spark session factory
│   │   ├── core/                     # Dependency injection & infrastructure
│   │   │   ├── dependencies.py       # Dependencies.Client, .UserClient, .Config, etc.
│   │   │   ├── _config.py            # AppConfig (env vars → typed config)
│   │   │   ├── _factory.py           # create_app(), create_router()
│   │   │   ├── lakebase.py           # PostgreSQL/SQLModel setup
│   │   │   └── sql.py                # SQL Warehouse statement execution
│   │   └── routes/
│   │       ├── spaces.py             # GET /spaces, GET /spaces/{id}, POST /spaces/{id}/optimize
│   │       ├── runs.py               # GET /runs/{id}, GET /runs/{id}/comparison, POST apply/discard
│   │       └── activity.py           # GET /activity (recent runs feed)
│   │
│   ├── ui/                           # React + Vite frontend
│   │   ├── main.tsx                  # React entry point
│   │   ├── routes/                   # File-based TanStack Router pages
│   │   │   ├── __root.tsx            # Root layout (navbar, theme)
│   │   │   ├── index.tsx             # Dashboard (spaces grid, activity, stats)
│   │   │   ├── spaces/$spaceId.tsx   # Space detail & optimization trigger
│   │   │   ├── runs/$runId.tsx       # Run monitoring (pipeline steps, levers)
│   │   │   └── runs/$runId/comparison.tsx  # Side-by-side config diff
│   │   ├── components/               # React components
│   │   │   ├── SpaceCard.tsx         # Space summary card
│   │   │   ├── ScoreCard.tsx         # Metric display widget
│   │   │   ├── PipelineStepCard.tsx  # Pipeline step visualization
│   │   │   ├── LeverProgress.tsx     # Lever progress bar
│   │   │   ├── ConfigDiff.tsx        # Configuration comparison viewer
│   │   │   └── ui/                   # shadcn/ui components
│   │   └── lib/
│   │       ├── api.ts                # Auto-generated OpenAPI client (DO NOT edit)
│   │       └── selector.ts           # Query selector helper
│   │
│   ├── common/                       # Shared utilities
│   │   ├── config.py                 # All constants (thresholds, prompts, taxonomy)
│   │   ├── genie_client.py           # Genie Space API wrapper (list, fetch, patch, query)
│   │   ├── uc_metadata.py            # Unity Catalog introspection (columns, tags, routines)
│   │   └── delta_helpers.py          # Delta table read/write operations
│   │
│   ├── optimization/                 # Core optimization engine
│   │   ├── optimizer.py              # Failure analysis → proposal generation → patch application
│   │   ├── evaluation.py             # Benchmark generation, 8-judge scoring, MLflow tracking
│   │   ├── applier.py                # Patch application & rollback
│   │   ├── harness.py                # Full pipeline orchestration
│   │   ├── preflight.py              # Pre-flight validation
│   │   ├── state.py                  # Delta-backed state machine (5 tables)
│   │   ├── benchmarks.py             # Benchmark question definitions
│   │   ├── repeatability.py          # Repeatability testing & variance classification
│   │   ├── report.py                 # Run report generation
│   │   ├── models.py                 # Internal data models
│   │   └── scorers/                  # 8 quality scorers
│   │       ├── syntax_validity.py
│   │       ├── schema_accuracy.py
│   │       ├── logical_accuracy.py
│   │       ├── semantic_equivalence.py
│   │       ├── completeness.py
│   │       ├── result_correctness.py
│   │       ├── asset_routing.py
│   │       └── arbiter.py            # Tiebreaker judge
│   │
│   └── jobs/                         # Databricks Job entry points
│       ├── run_preflight.py          # Task 1: config analysis & metadata collection
│       ├── run_baseline.py           # Task 2: baseline evaluation & benchmark generation
│       ├── run_lever_loop.py         # Task 3: iterative optimization (6 levers × 5 iterations)
│       ├── run_evaluation_only.py    # Standalone evaluation (called by other tasks)
│       ├── run_finalize.py           # Task 4: repeatability tests & final report
│       └── run_deploy.py             # Task 5: deploy to version control (conditional)
```

---

## API Reference

All endpoints are prefixed with `/api/genie`.

| Method | Endpoint | Operation ID | Description |
|--------|----------|-------------|-------------|
| `GET` | `/spaces` | `listSpaces` | List Genie Spaces with quality scores |
| `GET` | `/spaces/{space_id}` | `getSpaceDetail` | Full space config, tables, instructions, history |
| `POST` | `/spaces/{space_id}/optimize` | `startOptimization` | Trigger multi-task optimization job |
| `GET` | `/runs/{run_id}` | `getRun` | Run status with 5 pipeline steps & lever detail |
| `GET` | `/runs/{run_id}/comparison` | `getComparison` | Side-by-side original vs optimized config |
| `POST` | `/runs/{run_id}/apply` | `applyOptimization` | Confirm and keep optimized config |
| `POST` | `/runs/{run_id}/discard` | `discardOptimization` | Rollback to original config |
| `GET` | `/activity` | `listActivity` | Recent optimization runs feed |
| `GET` | `/version` | `getVersion` | App version |
| `GET` | `/current-user` | `getCurrentUser` | Authenticated user info |

---

## State Management

The optimizer maintains state across 5 Delta tables (partitioned by `run_id` or `space_id`):

| Table | Purpose |
|-------|---------|
| `genie_opt_runs` | Run lifecycle: status, scores, config snapshots, convergence reason |
| `genie_opt_stages` | Per-stage tracking: preflight, lever iterations, finalize |
| `genie_opt_iterations` | Per-iteration scores across all 7 quality dimensions |
| `genie_opt_patches` | Individual patches: type, lever, old/new values, applied/rolled-back |
| `genie_eval_asi_results` | Failure assessments: type, severity, blame set, counterfactual fixes |

Run statuses: `QUEUED` → `IN_PROGRESS` → `CONVERGED` | `STALLED` | `MAX_ITERATIONS` | `FAILED` → `APPLIED` | `DISCARDED`

---

## Databricks Resources

The `databricks.yml` bundle provisions:

- **Databricks App** -- Serves the full-stack web application with OBO (on-behalf-of) user authentication
- **PostgreSQL Database** (Lakebase) -- `CU_1` capacity instance
- **SQL Warehouse** -- For statement execution and UC metadata queries
- **Multi-task Job** (`genie_optimization_job`) -- 5-task pipeline with conditional execution and auto-retry
- **Standalone Evaluation Job** (`genie_evaluation_job`) -- Triggered by optimization tasks for isolated evaluation

### Required API Scopes

- `dashboards.genie` -- Read/write Genie Space configurations
- `sql` -- Execute SQL statements
- `iam.access-control:read` -- Check space permissions
- `iam.current-user:read` -- Identify the authenticated user

---

## Configuration

Environment variables (set via `databricks.yml`):

| Variable | Description | Default |
|----------|-------------|---------|
| `GENIE_SPACE_OPTIMIZER_CATALOG` | Unity Catalog name for Delta state tables | `vibe_coding_workshop_catalog` |
| `GENIE_SPACE_OPTIMIZER_SCHEMA` | Schema name for Delta state tables | `genie_optimization` |
| `GENIE_SPACE_OPTIMIZER_WAREHOUSE_ID` | SQL Warehouse ID for query execution | (workspace-specific) |

All optimization parameters (thresholds, rate limits, iterations, LLM config) are centralized in `src/genie_space_optimizer/common/config.py` and can be tuned without code changes.

---

## Development

### Prerequisites

- Python 3.11+
- [uv](https://docs.astral.sh/uv/) (Python package manager)
- [bun](https://bun.sh/) (JavaScript runtime)
- [apx](https://github.com/databricks-solutions/apx) CLI
- [Databricks CLI](https://docs.databricks.com/en/dev-tools/cli/index.html) (authenticated)

### Start Development Server

```bash
apx dev start
```

This launches backend (FastAPI), frontend (Vite), and OpenAPI watcher in detached mode. The API client auto-regenerates on backend changes.

### Common Commands

```bash
apx dev status       # Check running server status and ports
apx dev logs         # View recent logs (default: last 10m)
apx dev logs -f      # Stream logs in real-time
apx dev stop         # Stop all servers
apx dev check        # Run TypeScript + Python type checks
apx build            # Create production build
```

### Deploy to Databricks

```bash
databricks bundle deploy -p <your-profile>
```

---

## License

Internal Databricks project.
