# Changelog

All notable changes to the Genie Space Optimizer are documented here.

---

## [Unreleased]

### Added — UC REST API, Scorer Hardening, Harness Improvements
- **UC metadata REST API** (`uc_metadata.py`): preflight now fetches columns and routines
  via `WorkspaceClient` REST API by default, falling back to Spark SQL only when needed —
  eliminates `system.information_schema` permission issues
- **Arbiter baseline extraction**: harness extracts `genie_correct` arbiter actions from
  baseline iteration to seed lever loop with known-good signals
- **Entity matching propagation wait**: new `PROPAGATION_WAIT_ENTITY_MATCHING_SECONDS`
  config (default 90s) for longer waits after `build_value_dictionary` patches
- **Evaluation debug mode**: `GENIE_SPACE_OPTIMIZER_EVAL_DEBUG` env var enables verbose
  evaluation logging; assessment extraction now handles both trace-based and top-level
  `assessments` column formats (MLflow genai >=2.x compatibility)
- `fetch_genie_result_df` added to `genie_client.py` for direct result DataFrame retrieval
- Prompt matching diagnostic logging in harness with change summaries

### Changed
- **Tuned convergence thresholds**: `REGRESSION_THRESHOLD` raised from 2.0 → 10.0,
  `SLICE_GATE_TOLERANCE` raised from 5.0 → 15.0 (less aggressive rollback)
- **Scorers hardened**: `completeness`, `logical_accuracy`, `schema_accuracy`,
  `semantic_equivalence`, and `arbiter` scorers now use explicit `LLM_ENDPOINT` config
  and structured logging for better debugging
- Preflight uses REST → Spark SQL fallback chain (was Spark-only)
- Lever loop job (`run_lever_loop.py`) expanded with arbiter action handling
- `applier.py` improved patch application logic
- `genie_client.py` enhanced with schema-aware operations
- Config values (`PROPAGATION_WAIT_SECONDS`, `PROPAGATION_WAIT_ENTITY_MATCHING_SECONDS`)
  now environment-variable-overridable

---

## 2026-02-23

### Added — Documentation Update (`d770458`)
- Updated `README.md` with settings API endpoints, new modules, and quick links
- Updated `QUICKSTART.md` with actual repo clone URL and data access setup
- Created `CHANGELOG.md` with comprehensive history

### Added — Optimizer & Applier Refinements (`fd42c53`)
- Expanded optimizer with improved lever handling and proposal generation
- Enhanced applier with better rollback and state transitions
- Updated preflight, harness, and evaluation state management
- Expanded unit tests for optimizer, applier, config, and rollback scenarios
- Expanded integration tests for lever loop, resume, and rollback flows
- Added `docs/` directory with Genie Space config reference samples

### Added — Evaluation Engine & Repeatability Scorer (`947afd4`)
- Major evaluation engine expansion with improved judge logic and multi-dimension scoring
- Added `repeatability` scorer to detect variance across repeated evaluation runs
- Updated scorer registry to include repeatability in the scoring pipeline
- Refined settings, runs, spaces, and activity route handlers
- Enhanced optimization harness and preflight with better error handling
- Added `backend/constants.py` and `backend/utils.py` helper modules
- Added `scripts/` directory with browser testing tooling

### Added — Settings Routes & Code Review (`95de94e`)
- New **Settings** backend route (`/settings/data-access`) with GET, POST, DELETE endpoints for managing UC data-access grants
- New **Settings** frontend page (`/settings`) for data access management UI
- Enhanced evaluation engine with improved scoring and error handling
- Updated job launcher, harness, and preflight modules
- Added `CODE_REVIEW.md` documentation

### Added — Optimization Harness Updates (`45cdbe2`)
- Refined harness orchestration and error handling
- Updated optimization models with improved typing
- Enhanced optimizer with additional lever support
- Updated `pyproject.toml` dependencies

### Added — End-to-End Pipeline (`61456fc`)
- Full runs and spaces API routes with job launcher integration
- Expanded evaluation engine with comprehensive scorer improvements
- Preflight checks, harness orchestration, and lever loop job logic
- Enhanced frontend with richer run detail, space detail, and pipeline views
- `E2E_TESTING_GUIDE.md` and `QUICKSTART.md` documentation
- UC permissions grant script (`resources/grant_app_uc_permissions.py`)
- `ResourceLinks` UI component for workspace resource links

### Initial Release (`2c83b87`)
- Full-stack Databricks App scaffolding (React + FastAPI via apx)
- 6 optimization levers: tables & columns, metric views, TVFs, joins, column discovery, instructions
- 7 quality dimensions with 8 LLM judge scorers + arbiter
- Delta-backed state machine with 5 state tables
- Multi-task Databricks Job pipeline (preflight → baseline → lever loop → finalize → deploy)
- React dashboard with space listing, run monitoring, pipeline visualization, and config diff comparison
- Genie Space API client, Unity Catalog metadata introspection, and Delta helpers
- MLflow experiment tracking integration
- Databricks Asset Bundle (`databricks.yml`) for one-command deployment
