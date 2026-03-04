# Genie Space Optimizer

A full-stack Databricks App that automatically optimizes [Genie Spaces](https://docs.databricks.com/en/genie/index.html) through iterative, LLM-driven metadata improvement. It evaluates your Genie Space configuration against benchmark questions, identifies quality gaps across 7 dimensions, and applies targeted patches through 6 optimization levers -- all with full rollback support.

Built with [apx](https://github.com/databricks-solutions/apx) (React + FastAPI).

> **Quick links:** [Quickstart Guide](QUICKSTART.md) | [E2E Testing Guide](E2E_TESTING_GUIDE.md) | [Code Review](CODE_REVIEW.md) | [Changelog](CHANGELOG.md)

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
│  Config scan    9 LLM judges   6 levers ×5     Repeatability      │
│  UC metadata    20 benchmarks  iterations       Final scores       │
│  Validation     7 dimensions   Auto-rollback    Deploy (opt.)      │
└─────────────────────────────────────────────────────────────────────┘
```

### Optimization Pipeline (5 Steps)

1. **Configuration Analysis** -- Scans the Genie Space config, counts tables/instructions/sample questions, and validates structure.
2. **Metadata Collection** -- Queries Unity Catalog via REST API (with Spark SQL fallback) for columns, data types, tags, routines, and table descriptions.
3. **Baseline Evaluation** -- Generates ~20 benchmark questions via LLM (with coverage gap fill for uncovered assets), auto-resolves temporal date references, runs them through Genie, and scores responses across 7 quality dimensions using 9 LLM judges. All evaluation traces are tagged with `genie.optimization_run_id`, `genie.iteration`, and `genie.lever` for end-to-end traceability. The arbiter judge identifies `genie_correct` verdicts for benchmark correction and tiered soft signals for best-practice guidance.
4. **Configuration Generation (Lever Loop)** -- Stage 2.5 applies deterministic prompt matching (format assistance + entity matching). Stage 2.75 proactively enriches insufficient column *and table* descriptions using UC metadata, and mines proven example SQLs from high-scoring benchmark questions. Arbiter benchmark corrections rewrite stale gold SQL. A holistic strategist then triages all failures and generates a unified optimization strategy. Iterates through 5 optimization levers (up to 5 iterations), applying targeted patches with tiered failure analysis (hard failures + soft signals). Each patch is evaluated via a 3-gate system (slice, P0, full); regressions trigger automatic rollback. Score improvements below the noise floor (3 pts) are treated as cosmetic and rejected.
5. **Optimized Evaluation & Repeatability** -- Final evaluation of the optimized config, including repeatability testing to ensure consistent results.

### Optimization Levers

Before the main lever loop, four preparatory stages run:
- **Stage 2.5 (Prompt Matching)** -- Deterministic format assistance (`get_example_values`) and entity matching (`build_value_dictionary`) on prioritized columns. No LLM involved.
- **Stage 2.75 (Description Enrichment)** -- LLM-generated structured descriptions for columns *and tables* with insufficient descriptions (< 10 chars). Also generates space descriptions and sample questions for spaces that lack them, and mines proven example SQLs from high-scoring benchmark questions.
- **Stage 2.85 (Proactive Join Discovery)** -- Parses JOIN clauses from successful baseline queries, corroborates with UC foreign key constraints, and codifies execution-proven joins as Genie Space join specifications.
- **Strategist** -- Holistic triage of all failures into a unified optimization strategy with per-lever action plans.

The **5-lever loop** then iterates up to 5 times:

| Lever | Name | What It Optimizes |
|-------|------|-------------------|
| 1 | Tables & Columns | Descriptions, visibility, column aliases |
| 2 | Metric Views | Measures, dimensions, MV YAML definitions |
| 3 | Table-Valued Functions | Parameters, TVF SQL, function signatures |
| 4 | Join Specifications | Table relationships, join columns, cardinality (always runs discovery) |
| 5 | Genie Instructions | Holistic instruction rewrite (routing, disambiguation, best practices) |

Lever 4 always runs its join discovery path, even without explicit join failures, to document implicit joins from successful Genie queries. Lever 5 generates a single cohesive instruction document considering the space's purpose, all evaluation learnings, and prior lever tweaks.

### 7 Quality Dimensions (9 Scorers)

| Dimension | Target | What It Measures |
|-----------|--------|------------------|
| Syntax Validity | 98% | Generated SQL parses correctly |
| Schema Accuracy | 95% | Correct tables, columns, and joins referenced |
| Logical Accuracy | 90% | Correct aggregations, filters, GROUP BY, ORDER BY |
| Semantic Equivalence | 90% | Same business metric as expected answer |
| Completeness | 90% | All requested dimensions/measures included |
| Result Correctness | 85% | Correct final result values |
| Asset Routing | 95% | Correct asset type (table, metric view, TVF) selected |

Plus **Response Quality** (LLM analysis accuracy), **Repeatability** (variance detection across repeated runs), and an **Arbiter** (tiebreaker judge for conflicting scorer verdicts). All judges return structured JSON with `failure_type`, `blame_set`, `counterfactual_fix`, and `rationale` for direct ASI metadata flow.

---

## Tech Stack

| Layer | Technology |
|-------|------------|
| **Backend** | Python 3.11+, FastAPI, Pydantic, Uvicorn |
| **Frontend** | React 19, TypeScript, TanStack Router & Query, Vite |
| **UI Components** | shadcn/ui, Radix UI, Tailwind CSS 4 |
| **Data** | Delta Lake (state), PostgreSQL (Lakebase), Unity Catalog |
| **ML/AI** | MLflow 3.4+, Claude Opus 4.6 (via Databricks Foundation Model API), MLflow GenAI scorers, MLflow Labeling Sessions |
| **Infrastructure** | Databricks Apps, Databricks Jobs, SQL Warehouse |
| **SDK** | Databricks SDK 0.40+, Databricks Connect 15+ |
| **Build** | apx, uv (Python), bun (JavaScript) |

---

## Project Structure

```
Genie_Space_Optimizer/
├── pyproject.toml                    # Python project config & apx metadata
├── databricks.yml                    # Databricks Asset Bundle definition
├── Makefile                          # Deployment helpers (build → clean → deploy → verify)
├── app.yml                           # Databricks App entry point (uvicorn)
├── resources/
│   └── grant_app_uc_permissions.py   # Script to grant app SP access to UC schemas
├── docs/                             # Reference documentation & config samples
│
├── src/genie_space_optimizer/
│   ├── backend/                      # FastAPI backend
│   │   ├── app.py                    # App factory (registers routes, serves frontend)
│   │   ├── models.py                 # Pydantic response models
│   │   ├── router.py                 # System routes (/version, /current-user)
│   │   ├── job_launcher.py           # Databricks Jobs submission helper
│   │   ├── constants.py              # Backend-specific constants
│   │   ├── utils.py                  # Backend utility functions
│   │   ├── _spark.py                 # Serverless Spark session factory (auto-recreate on credential errors)
│   │   ├── core/                     # Dependency injection & infrastructure
│   │   │   ├── dependencies.py       # Dependencies.Client, .UserClient, .Config, etc.
│   │   │   ├── _config.py            # AppConfig (env vars → typed config)
│   │   │   ├── _factory.py           # create_app(), create_router()
│   │   │   ├── lakebase.py           # PostgreSQL/SQLModel setup
│   │   │   └── sql.py                # SQL Warehouse statement execution
│   │   └── routes/
│   │       ├── spaces.py             # GET /spaces, GET /spaces/{id}, POST /spaces/{id}/optimize
│   │       ├── runs.py               # GET /runs/{id}, comparison, iterations, ASI, provenance, apply/discard
│   │       ├── activity.py           # GET /activity (permission-filtered recent runs)
│   │       ├── settings.py           # Advisor-only permission dashboard (read-only, copyable grants)
│   │       └── trigger.py            # POST /trigger, GET /trigger/status (programmatic API)
│   │
│   ├── ui/                           # React + Vite frontend
│   │   ├── main.tsx                  # React entry point
│   │   ├── routes/                   # File-based TanStack Router pages
│   │   │   ├── __root.tsx            # Root layout (navbar, theme)
│   │   │   ├── index.tsx             # Dashboard (spaces grid, activity, stats)
│   │   │   ├── settings.tsx          # Advisor-only settings (read permissions, copyable grants)
│   │   │   ├── spaces/$spaceId.tsx   # Space detail & optimization trigger
│   │   │   ├── runs/$runId.tsx       # Run monitoring (pipeline steps, levers)
│   │   │   └── runs/$runId/comparison.tsx  # Side-by-side config diff
│   │   ├── components/               # React components
│   │   │   ├── SpaceCard.tsx         # Space summary card
│   │   │   ├── ScoreCard.tsx         # Metric display widget
│   │   │   ├── PipelineStepCard.tsx  # Pipeline step visualization
│   │   │   ├── LeverProgress.tsx     # Lever progress bar
│   │   │   ├── ConfigDiff.tsx        # Configuration comparison viewer
│   │   │   ├── ResourceLinks.tsx     # Workspace resource links
│   │   │   ├── IterationChart.tsx    # Score-over-iterations line chart
│   │   │   ├── AsiResultsPanel.tsx   # ASI failure analysis breakdown
│   │   │   ├── ProvenancePanel.tsx   # Judge → cluster → patch provenance viewer
│   │   │   ├── StageTimeline.tsx     # Pipeline stage timeline visualization
│   │   │   ├── CrossRunChart.tsx     # Cross-run score comparison chart
│   │   │   ├── ProcessFlow.tsx       # Pipeline process flow visualization
│   │   │   └── ui/                   # shadcn/ui components (incl. chart, accordion, alert-dialog)
│   │   └── lib/
│   │       ├── api.ts                # Auto-generated OpenAPI client (DO NOT edit)
│   │       ├── transparency-api.ts   # Transparency API hooks (iterations, ASI, provenance)
│   │       └── selector.ts           # Query selector helper
│   │
│   ├── common/                       # Shared utilities
│   │   ├── config.py                 # All constants (thresholds, prompts, taxonomy, noise floor)
│   │   ├── genie_client.py           # Genie Space API wrapper (list, fetch, patch, query, result DFs)
│   │   ├── genie_schema.py           # Genie Space config schema validation (lenient + strict modes)
│   │   ├── uc_metadata.py            # Unity Catalog introspection (REST API + Spark SQL fallback + FK extraction)
│   │   └── delta_helpers.py          # Delta table read/write operations
│   │
│   ├── optimization/                 # Core optimization engine
│   │   ├── optimizer.py              # Strategist, failure analysis, proposal generation, table/column enrichment, example SQL mining
│   │   ├── evaluation.py             # Benchmark generation, temporal date resolution, 9-judge scoring, MLflow tracking
│   │   ├── applier.py                # Patch application & rollback
│   │   ├── harness.py                # Full pipeline orchestration
│   │   ├── preflight.py              # Pre-flight validation
│   │   ├── labeling.py               # MLflow labeling sessions (human-in-the-loop review)
│   │   ├── state.py                  # Delta-backed state machine (6 tables + provenance)
│   │   ├── benchmarks.py             # Benchmark question definitions
│   │   ├── repeatability.py          # Repeatability testing & variance classification
│   │   ├── report.py                 # Run report generation
│   │   ├── models.py                 # MLflow LoggedModel snapshots & metric linking
│   │   └── scorers/                  # 10 quality scorers
│   │       ├── syntax_validity.py
│   │       ├── schema_accuracy.py
│   │       ├── logical_accuracy.py
│   │       ├── semantic_equivalence.py
│   │       ├── completeness.py
│   │       ├── response_quality.py   # NL analysis accuracy judge
│   │       ├── result_correctness.py
│   │       ├── asset_routing.py
│   │       ├── repeatability.py      # Variance detection across repeated runs
│   │       └── arbiter.py            # Tiebreaker judge
│   │
│   └── jobs/                         # Databricks Job entry points
│       ├── run_preflight.py          # Task 1: config analysis & metadata collection
│       ├── run_baseline.py           # Task 2: baseline evaluation & benchmark generation
│       ├── run_lever_loop.py         # Task 3: iterative optimization (6 levers × 5 iterations)
│       ├── run_evaluation_only.py    # Standalone evaluation (called by other tasks)
│       ├── run_optimization.py       # Single-entry-point optimization runner
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
| `GET` | `/activity` | `getActivity` | Recent optimization runs (permission-filtered to user's spaces) |
| `GET` | `/settings/permissions` | `getPermissionDashboard` | Advisor-only dashboard: schema read/write, space ACLs, copyable grant commands |
| `POST` | `/trigger` | `triggerOptimization` | Trigger optimization programmatically (headless API) |
| `GET` | `/trigger/status/{run_id}` | `getTriggerStatus` | Poll status of a triggered optimization run |
| `GET` | `/runs/{run_id}/iterations` | `getIterations` | Per-iteration scores for iteration chart |
| `GET` | `/runs/{run_id}/asi` | `getAsiResults` | ASI failure analysis breakdown |
| `GET` | `/runs/{run_id}/provenance` | `getProvenance` | End-to-end provenance (judge → cluster → patch → gate) |
| `GET` | `/version` | `getVersion` | App version |
| `GET` | `/current-user` | `getCurrentUser` | Authenticated user info |

---

## State Management

The optimizer maintains state across 6 Delta tables (partitioned by `run_id` or `space_id`):

| Table | Purpose |
|-------|---------|
| `genie_opt_runs` | Run lifecycle: status, scores, config snapshots, convergence reason |
| `genie_opt_stages` | Per-stage tracking: preflight, lever iterations, finalize |
| `genie_opt_iterations` | Per-iteration scores across all 7 quality dimensions |
| `genie_opt_patches` | Individual patches: type, lever, old/new values, applied/rolled-back, provenance chain |
| `genie_eval_asi_results` | Failure assessments: type, severity, blame set, counterfactual fixes, MLflow run ID for trace linking |
| `genie_opt_provenance` | End-to-end provenance: links every patch to originating judge verdicts, clusters, and gate outcomes |

Run statuses: `QUEUED` → `IN_PROGRESS` → `CONVERGED` | `STALLED` | `MAX_ITERATIONS` | `FAILED` → `APPLIED` | `DISCARDED`

---

## Databricks Resources

The `databricks.yml` bundle provisions:

- **Databricks App** -- Serves the full-stack web application with OBO (on-behalf-of) user authentication. The app operates as an **advisor**: it reads permissions and shows copyable grant commands but never executes GRANT/REVOKE on the user's behalf
- **PostgreSQL Database** (Lakebase) -- `CU_1` capacity instance
- **SQL Warehouse** -- For statement execution and UC metadata queries
- **Optimization Job** -- Triggered on-demand via UI or `/trigger` API; runs preflight → baseline → lever loop → finalize pipeline

### Required OBO API Scopes

- `dashboards.genie` -- Read/write Genie Space configurations
- `files.files` -- File operations (workspace artifacts)
- `catalog.catalogs:read` -- List Unity Catalog catalogs
- `catalog.schemas:read` -- List UC schemas
- `catalog.tables:read` -- Read UC table/column metadata via REST
- `sql` -- Execute SQL statements
- `iam.access-control:read` -- Check space permissions
- `iam.current-user:read` -- Identify the authenticated user

---

## Configuration

Environment variables (set via `databricks.yml`):

| Variable | Description | Default |
|----------|-------------|---------|
| `GENIE_SPACE_OPTIMIZER_CATALOG` | Unity Catalog name for Delta state tables | `main` |
| `GENIE_SPACE_OPTIMIZER_SCHEMA` | Schema name for Delta state tables | `genie_optimization` |
| `GENIE_SPACE_OPTIMIZER_WAREHOUSE_ID` | SQL Warehouse ID for query execution | (workspace-specific) |
| `GENIE_SPACE_OPTIMIZER_PROPAGATION_WAIT` | Seconds to wait after config patch before re-evaluation | `30` |
| `GENIE_SPACE_OPTIMIZER_PROPAGATION_WAIT_ENTITY_MATCHING` | Extra wait for entity matching propagation | `90` |
| `GENIE_SPACE_OPTIMIZER_EVAL_DEBUG` | Enable verbose evaluation logging | `true` |
| `GENIE_SPACE_OPTIMIZER_EVAL_MAX_ATTEMPTS` | Max retry attempts per evaluation query | `4` |

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

The recommended deployment method uses `make deploy`, which orchestrates the full pipeline:

```bash
make deploy PROFILE=<your-profile>
```

This runs four steps:
1. **clean-wheels** -- Removes stale `.whl` files from the workspace
2. **bundle deploy** -- Builds the wheel, removes `.build/.gitignore`, syncs files to workspace
3. **apps deploy** -- Creates a new deployment snapshot and restarts the app
4. **verify** -- Confirms the wheel is present on the workspace

You can also run individual targets:

```bash
make clean-wheels PROFILE=<your-profile>   # Remove stale wheels
make verify PROFILE=<your-profile>         # Confirm wheel on workspace
```

---

## License

Internal Databricks project.
