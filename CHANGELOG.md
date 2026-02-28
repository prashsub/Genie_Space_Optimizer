# Changelog

All notable changes to the Genie Space Optimizer are documented here.

---

## [Unreleased]

### Added — Lever 5 Quality, Result Matching, Anti-Hallucination
- **Lever 5 prompt improvements** (`config.py`): routing failures now force
  `example_sql` instruction type instead of falling back to text instructions;
  added structured markdown formatting rules, anti-hallucination guard that
  returns empty proposals when no actionable fix is identified, and explicit
  requirement that all instructions reference specific Genie Space assets
- **Description cleanup** (`optimizer.py`): `_detect_instruction_content_in_description()`
  detects when the Genie Space description field contains LLM-facing routing rules
  or SQL patterns and proposes stripping them to user-facing summary only
- **Cluster deduplication** (`optimizer.py`): improved failure clustering with
  downgrade tracking when LLM returns text_instruction for routing failures
- **Fuzzy result matching** (`evaluation.py`): `normalize_result_df` and
  `result_signature` now round to 4 decimals (was 6) to handle Spark vs REST API
  float precision differences; new subset/superset matching and column alias
  detection for result correctness scoring
- **Scorer enhancements**: `completeness`, `logical_accuracy`, `result_correctness`,
  `schema_accuracy`, and `semantic_equivalence` scorers enhanced with additional
  context and edge-case handling
- Expanded unit tests for optimizer (cluster dedup, description cleanup) and applier

---

## [Previous Unreleased]

### Added — Trigger API, Response Quality Scorer, Schema Validation v2
- **Programmatic trigger API** (`trigger.py`): new `POST /trigger` and
  `GET /trigger/status/{run_id}` endpoints for headless optimization — enables
  CI/CD and scheduled workflows without the UI
- **Response quality scorer** (`response_quality.py`): 9th judge that evaluates
  whether Genie's natural language analysis accurately describes the SQL query
  and answers the user's question; returns `unknown` when NL response is absent
- **Genie schema validation v2** (`genie_schema.py`): dual-mode validation —
  lenient (structural/type checks) and strict (32-char hex IDs, sort order,
  uniqueness, size limits per Databricks API spec); `generate_genie_id()` helper
  for time-ordered UUID generation
- **Robust LLM JSON parsing** (`_extract_json` in `evaluation.py`): handles
  markdown fences, prose-wrapped JSON, trailing data, and nested objects
- **Genie API rate limiting**: `genie_client.py` now retries on `ResourceExhausted`
  (HTTP 429) and `TimeoutError` with exponential backoff; configurable via
  `GENIE_RATE_LIMIT_BASE_DELAY` and `GENIE_RATE_LIMIT_RETRIES`
- UC metadata: additional REST API helpers for tags retrieval
- Expanded unit tests: schema validation (+686 lines), evaluation, scorers, optimizer

### Changed
- **Job definition moved inline**: `resources/genie_optimization_job.yml` removed;
  job is now defined directly in `databricks.yml`
- Scorer assembly bumped from 8 → 9 judges (response quality added between
  completeness and asset routing)
- `applier.py` refactored with improved patch sequencing and validation
- `benchmarks.py` enhanced benchmark generation with better question diversity
- `optimizer.py` expanded failure analysis and proposal generation
- Frontend: space detail page updated for trigger API integration

### Removed
- `resources/genie_optimization_job.yml` (migrated into `databricks.yml`)

---

## 2026-02-23

### Added — UC REST API, Scorer Hardening, Harness Improvements (`74f1eb4`)
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
- Tuned convergence: `REGRESSION_THRESHOLD` 2.0 → 10.0, `SLICE_GATE_TOLERANCE` 5.0 → 15.0
- Scorers hardened with explicit `LLM_ENDPOINT` config and structured logging

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
