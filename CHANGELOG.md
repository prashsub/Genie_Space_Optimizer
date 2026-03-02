# Changelog

All notable changes to the Genie Space Optimizer are documented here.

---

## [Unreleased]

### Added — Dynamic Counterfactual Fixes in Judge Prompts
- **Structured judge responses** (`config.py`): all LLM judge prompts
  (`schema_accuracy`, `logical_accuracy`, `completeness`, `semantic_equivalence`,
  `arbiter`, `response_quality`) now require structured JSON responses with
  `failure_type`, `blame_set`, `counterfactual_fix`, `wrong_clause`, and `rationale`
  fields instead of simple yes/no answers
- **Dynamic counterfactual extraction** (scorers): `completeness`, `logical_accuracy`,
  `result_correctness`, `schema_accuracy`, and `response_quality` scorers now extract
  LLM-provided `counterfactual_fix` from the structured response; falls back to a
  generated fix using `failure_type` and `blame_set` when the LLM omits it
- **Richer ASI metadata**: judge-provided blame sets and counterfactual fixes flow
  directly into ASI metadata, failure clustering, and provenance — eliminating the need
  for separate rationale-pattern extraction for these fields

---

### Added — MLflow Provenance, Labeling Sessions, Trace Tagging, SCD Filters
- **End-to-end provenance table** (`state.py`): new `genie_opt_provenance` Delta table
  linking every patch back to originating judge verdicts, failure clusters, and gate
  outcomes — columns include `signal_type`, `resolution_method`, `gate_result`,
  `gate_regression` for full traceability
- **MLflow Labeling Sessions** (`labeling.py`): new module with custom labeling schemas
  (`judge_verdict_accuracy`, `corrected_expected_sql`, `patch_approval`,
  `improvement_suggestions`) for human-in-the-loop review; auto-creates review
  sessions after lever loop runs; preflight ingests human feedback from prior sessions
  to correct benchmarks and improve subsequent runs
- **MLflow trace tagging** (`evaluation.py`, `harness.py`): all evaluation traces now
  tagged with `genie.optimization_run_id`, `genie.iteration`, `genie.lever`, and
  `genie.eval_scope` for cross-run traceability; experiment-level tags set during
  preflight (`genie.space_id`, `genie.domain`, `genie.pipeline_version`)
- **MLflow Feedback on traces** (`evaluation.py`): `log_asi_feedback_on_traces()` attaches
  ASI root-cause analysis as Feedback; `log_gate_feedback_on_traces()` attaches gate
  pass/fail verdicts with regression details to evaluation traces
- **Model-level metric logging** (`models.py`): `link_eval_scores_to_model()` now logs
  metrics both on the active MLflow Run and directly to the LoggedModel for UI display
- **SCD filter detection** (`optimizer.py`): new `missing_scd_filter` and
  `wrong_filter_condition` failure types; `_classify_sql_diff` detects missing
  `is_current`/`is_active` filters and WHERE condition differences
- **Join type analysis** (`optimizer.py`): new `wrong_join_type` failure type for
  LEFT vs INNER join mismatches; lever mapping sends to Lever 5 for instruction guidance
- **ASI & provenance Delta writes in harness**: lever loop now writes per-iteration
  ASI results and provenance rows to Delta, linking clusters → proposals → gate outcomes
- **Evaluation dataset upsert** (`evaluation.py`): `create_evaluation_dataset()` now uses
  `merge_records` (upsert by question_id) to preserve version history instead of drop/recreate
- **Preflight experiment tags**: `run_preflight()` sets experiment-level tags for
  pipeline version, catalog, schema, space_id, and domain

### Fixed
- **Scope filter fallback**: `run_evaluation()` falls back to full benchmark set when
  scope filtering returns empty results (prevents empty evaluations)
- **Provenance JSON**: patches now carry `provenance_json` column linking to full
  provenance chain from judge verdicts

---

### Added — Holistic Lever 5, Tiered Arbiter Soft Signals, Join Discovery, Deployment Pipeline

- **Holistic Lever 5 instructions** (`config.py`, `optimizer.py`, `applier.py`, `harness.py`):
  Lever 5 now evaluates the entire Genie Space instruction set holistically rather than
  patching individual instructions. A new `LEVER_5_HOLISTIC_PROMPT` (inspired by the
  [AgentSkills.io](https://agentskills.io/specification) specification) generates a
  single cohesive instruction document considering the space's purpose, all benchmark
  evaluation learnings, and prior lever tweaks. New `rewrite_instruction` patch type
  replaces the full instruction body in one operation.
- **Tiered arbiter soft signals** (`harness.py`, `optimizer.py`, `config.py`):
  Questions with `genie_correct` or `both_correct` arbiter verdicts that still have
  individual judge failures are now extracted as "soft signal" rows. These are clustered
  separately (tagged `signal_type: soft`) and passed to Levers 4 and 5 alongside hard
  failure clusters, providing best-practice guidance without triggering aggressive fixes.
- **Lever 4 always-run join discovery** (`harness.py`, `optimizer.py`):
  Lever 4 (Join Specifications) now always runs its discovery path regardless of
  failure cluster count, detecting implicit joins from successful Genie queries and
  proposing explicit join documentation. `_classify_sql_diff` reordered to check
  missing dimension JOINs and wrong join columns before aggregation checks.
- **Robust QID extraction** (`optimizer.py`, `evaluation.py`, `harness.py`):
  New `_row_qid()` helper robustly extracts `question_id` from eval rows, accounting
  for MLflow's `request` column and nested structures. Fixes ASI data merging that was
  silently failing due to mismatched QID keys.
- **Deployment pipeline hardening** (`databricks.yml`, `Makefile`, `job_launcher.py`, `app.py`):
  - `databricks.yml` build step now removes `.build/.gitignore` (which `apx build`
    generates with a blanket `*` pattern that blocks file sync)
  - New `Makefile` with `make deploy` orchestrating four steps: `clean-wheels` →
    `databricks bundle deploy` (builds wheel + syncs to workspace) →
    `databricks apps deploy` (creates snapshot + restarts app) → `verify`
  - `_cleanup_stale_wheels()` in `job_launcher.py` removes old wheels from the
    workspace dist directory after uploading the new one
  - `_WheelHealthCheck` lifespan dependency in `app.py` logs the resolved wheel
    name and size at app startup for immediate deploy verification
  - Enhanced wheel logging throughout `_ensure_artifacts` (hash, size, path)
- **Enhanced soft signal logging** (`harness.py`): soft signal clusters now print
  full per-cluster detail (judge, root cause, ASI type, blame, questions) matching
  the hard cluster format. Per-question failed judge names shown in the Failure
  Analysis section for debugging. New `_get_failed_judges()` helper.
- **Wheel cache busting** (`job_launcher.py`): workspace wheel path now embeds a
  content hash (`{stem}_{hash[:8]}.whl`) to prevent stale wheel caching

### Fixed — Lever Loop Effectiveness

- **Arbiter filter**: now excludes both `genie_correct` AND `both_correct` verdicts
  from failure clustering (previously only excluded `genie_correct`)
- **`_classify_sql_diff` over-classification**: introduced `format_difference` and
  `extra_columns_only` types mapped to lever 0 (no-op) to prevent benign SQL
  differences from triggering unnecessary patches
- **Deduplication**: `_deduplicate_proposals` now checks `update_description` patch type
- **No-op filtering**: `_filter_no_op_proposals` uses 0.97 n-gram similarity threshold
  to drop cosmetic-only proposals while preserving surgical improvements
- **No-op filtering crash** (`optimizer.py`): `_filter_no_op_proposals` crashed with
  `'list' object has no attribute 'strip'` when Genie metadata `description` field
  was a list instead of a string; now coerces to string safely
- **Instruction ID validation** (`applier.py`): generates a valid 32-char hex ID when
  the default `genie_opt` ID fails validation
- **Tied subset matching** (`evaluation.py`): `LIMIT` queries with tied values that
  produce different but semantically equivalent row orderings now score as `tied_subset`
  instead of `mismatch`
- **`mapped_genie_df` UnboundLocalError** (`evaluation.py`): moved assignment before
  exception-prone code path to ensure it's always defined
- **MLflow experiment permissions** (`spaces.py`): Service Principal is now explicitly
  granted `CAN_MANAGE` on the MLflow experiment via the Permissions API

---

### Added — Temporal Date Resolution, Coverage Gap Benchmarks, Spark Resilience
- **Temporal date resolution** (`evaluation.py`): auto-detects relative time
  references ("this year", "last quarter", "YTD", "last N months/days") in
  benchmark questions and rewrites GT SQL date literals to current-date-relative
  values before scoring — eliminates false failures from stale benchmark dates
- **Benchmark coverage gap generation** (`evaluation.py`, `config.py`): new
  `BENCHMARK_COVERAGE_GAP_PROMPT` and `COVERAGE_GAP_SOFT_CAP_FACTOR` to generate
  targeted benchmarks for uncovered assets (tables, MVs, TVFs with zero questions)
- **Spark session resilience** (`_spark.py`): session factory now detects
  `InvalidAccessKeyId`, `ExpiredToken`, `AccessDenied` credential errors and
  auto-recreates the session; `run_with_retry()` wrapper retries Spark operations
  with session recreation on credential failures
- **Delta table resilience** (`delta_helpers.py`): `read_table` now issues
  `REFRESH TABLE` before reads and retries on `DELTA_SCHEMA_CHANGE_SINCE_ANALYSIS`
  to handle tables dropped/recreated by upstream tasks
- **Job launcher improvements** (`job_launcher.py`): picks globally-newest wheel
  across all search directories; caches wheel content hash for upload dedup
- **Arbiter scorer overhaul** (`arbiter.py`): result-match now returns
  `both_correct` (was `skipped`); temporal context injected into LLM prompts;
  slim comparison payloads to stay within MLflow trace size limits
- **Scorer temporal awareness**: all LLM scorers (`completeness`, `logical_accuracy`,
  `result_correctness`, `schema_accuracy`, `semantic_equivalence`) now receive
  temporal rewrite context so judges account for date drift
- Expanded test coverage: +481 lines for evaluation (temporal rewriting, coverage
  gap, benchmark generation), +61 lines for optimizer

---

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
