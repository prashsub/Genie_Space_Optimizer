# BUILD-03: Evaluation Engine

> **APX project.** All Python code lives under `src/genie_space_optimizer/`. See `APX-BUILD-GUIDE.md` for full project conventions.

## Context

You are building the **Genie Space Optimizer**, a Databricks App (APX). This step creates the evaluation engine — the core quality measurement system that runs 8 judges against benchmark questions using `mlflow.genai.evaluate()`. Every optimization iteration depends on this engine for scores and Actionable Side Information (ASI).

**Read these reference docs before starting:**
- `docs/genie_space_optimizer/03-evaluation-engine.md` — full spec: predict function, all 8 scorers, ASI schema, MLflow integration, repeatability
- `docs/genie_space_optimizer/07-configuration-and-constants.md` — judge prompts, thresholds, rate limits, LLM config

**Source code to extract from:**
- `src/wanderbricks_semantic/run_genie_evaluation.py` — existing evaluation notebook with scorer implementations, predict fn, MLflow integration

**Depends on (already built in BUILD-01):**
- `src/genie_space_optimizer/common/config.py` — JUDGE_PROMPTS, ASI_SCHEMA, thresholds, rate limits
- `src/genie_space_optimizer/common/genie_client.py` — run_genie_query, resolve_sql, sanitize_sql, detect_asset_type

---

## What to Build

### 1. `optimization/scorers/` — One file per judge

8 scorer files + `__init__.py` assembling `all_scorers`.

Each scorer is an `@mlflow.genai.scorer` decorated function returning `Feedback` (or dict with `value` + `rationale`).

| File | Scorer | Type | Mechanism |
|------|--------|------|-----------|
| `syntax_validity.py` | `syntax_validity_scorer` | CODE | `spark.sql(f"EXPLAIN {genie_sql}")` — pass if no error |
| `schema_accuracy.py` | `schema_accuracy_judge` | LLM | Does SQL reference correct tables/columns? |
| `logical_accuracy.py` | `logical_accuracy_judge` | LLM | Is the SQL logic correct for the question? |
| `semantic_equivalence.py` | `semantic_equivalence_judge` | LLM | Are the two SQLs semantically equivalent? |
| `completeness.py` | `completeness_judge` | LLM | Does the SQL fully answer the question? |
| `asset_routing.py` | `asset_routing_scorer` | CODE | Did Genie use the correct asset type (MV/TVF/TABLE)? |
| `result_correctness.py` | `result_correctness_scorer` | CODE | Compare GT vs Genie result DataFrames (hash + signature) |
| `arbiter.py` | `arbiter_scorer` | LLM | Fires only when result_correctness=false. Arbitrates. |

**`__init__.py`:**
```python
from .syntax_validity import syntax_validity_scorer
from .schema_accuracy import schema_accuracy_judge
# ... all 8
all_scorers = [syntax_validity_scorer, schema_accuracy_judge, ...]
```

### 2. `optimization/evaluation.py` — Predict function + helpers

**Predict Function (factory closure):**
```python
def make_predict_fn(w, space_id, spark, catalog, schema):
    """Return a predict function with bound workspace/spark context."""
    @mlflow.trace
    def genie_predict_fn(question: str, expected_sql: str = "", **kwargs) -> dict:
        """Query Genie, execute both SQLs, return response + comparison.
        Steps: rate-limit → Genie call → sanitize → resolve GT SQL →
               dual-execute → normalize → compare hashes.
        Returns: response, genie_sql, gt_sql, genie_result_df,
                 gt_result_df, comparison dict.
        """
    return genie_predict_fn
```

**Shared Helpers (used by scorers):**
- `_extract_response_text(response)` — extract text from MLflow prediction output
- `_call_llm_for_scoring(prompt, context)` — call LLM endpoint with retry
- `normalize_result_df(df)` — deterministic normalization (lowercase cols, sort, reset index)
- `result_signature(df)` — compute hash of DataFrame for comparison
- `build_asi_metadata(judge, verdict, rationale, ...)` — construct ASI dict per ASI_SCHEMA
- `normalize_scores(scores)` — convert 0-1 → 0-100 scale
- `all_thresholds_met(scores, targets=None)` — check all judges vs thresholds

**MLflow Integration:**
- `run_evaluation(space_id, experiment_name, iteration, benchmarks, domain, model_id, eval_scope, ...)` — wraps `mlflow.genai.evaluate()`
- `register_judge_prompts(uc_schema, domain, experiment_name)` — register in MLflow Prompt Registry (idempotent)
- `create_evaluation_dataset(spark, benchmarks, uc_schema, domain)` — create/update MLflow eval dataset
- `filter_benchmarks_by_scope(benchmarks, scope, patched_objects=None)` — "full"/"slice"/"p0"/"held_out"

### 4. `optimization/evaluation.py` — Benchmark Generation

The evaluation module also owns benchmark generation (since it manages the eval dataset):

- `generate_benchmarks(config, uc_columns, uc_tags, uc_routines, domain, catalog, schema, spark, target_count=20)` → `list[dict]`
  1. Build schema context string from Genie Space config + UC metadata
  2. Call **Databricks Claude Opus 4.6** (`LLM_ENDPOINT`) with `BENCHMARK_GENERATION_PROMPT` from config
  3. Parse JSON response into benchmark dicts
  4. Validate each `expected_sql` via `spark.sql(f"EXPLAIN {resolve_sql(sql)}")` — discard failures
  5. Assign `question_id` (domain prefix + sequential), `priority` (top 3 = P0), `split` (80% train, 20% held_out)
  6. Return list of benchmark dicts

- `load_benchmarks_from_dataset(spark_or_dataset, uc_schema, domain)` → `list[dict]`
  - Load benchmarks from existing MLflow evaluation dataset (`{uc_schema}.genie_benchmarks_{domain}`)
  - Replaces old `load_benchmarks(path, domain)` which read from YAML

See `03-evaluation-engine.md` Section 12 for the full benchmark generation spec.

### 3. `optimization/repeatability.py`

- `run_repeatability_test(w, space_id, benchmarks, spark)` — re-query each benchmark, compare SQL hashes
- `_classify_repeatability(original_sql, requery_sql)` → IDENTICAL/MINOR/SIGNIFICANT/CRITICAL
- `compute_cross_iteration_repeatability(iterations)` — compare SQL across iterations
- `synthesize_repeatability_failures(details)` — summarize for reporting

---

## File Structure

```
src/genie_space_optimizer/
├── optimization/
│   ├── __init__.py
│   ├── evaluation.py
│   ├── repeatability.py
│   └── scorers/
│       ├── __init__.py
│       ├── syntax_validity.py
│       ├── schema_accuracy.py
│       ├── logical_accuracy.py
│       ├── semantic_equivalence.py
│       ├── completeness.py
│       ├── asset_routing.py
│       ├── result_correctness.py
│       └── arbiter.py
```

---

## Key Design Decisions

1. **Factory closure for predict fn.** `make_predict_fn(w, space_id, spark, catalog, schema)` binds context without globals.
2. **Scorer isolation.** Each file is independent. LLM scorers call `_call_llm_for_scoring()` from `evaluation.py`. Code scorers use comparison data from predict output.
3. **ASI generation.** Every LLM scorer that returns `"no"` also populates ASI metadata with failure_type, blame_set, counterfactual_fix → flows to optimizer.
4. **Reuse verbatim.** Copy all 8 scorer implementations and helpers from existing `run_genie_evaluation.py`; only adjust import paths.

---

## Acceptance Criteria

1. `all_scorers` list contains exactly 8 scorers, all importable from `optimization.scorers`
2. `genie_predict_fn` handles: successful query, Genie timeout, SQL error, empty results
3. `normalize_result_df` is deterministic — same input always → same output
4. `result_signature` produces identical hashes for semantically identical DataFrames (column-order independent)
5. LLM judges use prompts from `common/config.py` — no hardcoded prompt strings
6. `normalize_scores` converts 0-1 → 0-100; leaves 0-100 unchanged
7. `all_thresholds_met` returns True only when every judge meets its threshold

---

## Predecessor Prompts
- BUILD-01 — provides genie_client, config

## Successor Prompts
- BUILD-05 (Harness) — calls `run_evaluation()`, `all_thresholds_met()`, `normalize_scores()`
- BUILD-06 (Backend) — reads evaluation results from Delta
