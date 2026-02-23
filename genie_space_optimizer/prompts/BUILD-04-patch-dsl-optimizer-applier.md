# BUILD-04: Patch DSL, Optimizer & Applier

> **APX project.** All Python code lives under `src/genie_space_optimizer/`. See `APX-BUILD-GUIDE.md` for full project conventions.

## Context

You are building the **Genie Space Optimizer**, a Databricks App (APX). This step creates the intelligence layer — the optimizer that analyzes evaluation failures and proposes targeted metadata changes, and the applier that renders proposals into API calls with rollback support.

**Read these reference docs before starting:**
- `docs/genie_space_optimizer/04-patch-dsl-and-optimizer.md` — failure taxonomy, 6 control levers, 35 patch types, 18 conflict rules, apply_mode, LLM-powered proposal generation (4 prompts), patch rendering, apply/rollback
- `docs/genie_space_optimizer/07-configuration-and-constants.md` — PATCH_TYPES, CONFLICT_RULES, FAILURE_TAXONOMY, proposal prompts (PROPOSAL_GENERATION_PROMPT, LEVER_4_JOIN_SPEC_PROMPT, LEVER_5_DISCOVERY_PROMPT, LEVER_6_INSTRUCTION_PROMPT), LLM_ENDPOINT, APPLY_MODE, MAX_VALUE_DICTIONARY_COLUMNS

**Source code to extract from:**
- `data_product_accelerator/skills/semantic-layer/genie-optimization-workers/03-genie-metadata-optimizer/` — metadata_optimizer.py
- `data_product_accelerator/skills/semantic-layer/genie-optimization-workers/04-genie-optimization-applier/` — optimization_applier.py

**Depends on (already built in BUILD-01):**
- `src/genie_space_optimizer/common/config.py` — PATCH_TYPES, CONFLICT_RULES, FAILURE_TAXONOMY, _LEVER_TO_PATCH_TYPE, risk sets, APPLY_MODE, MAX_VALUE_DICTIONARY_COLUMNS, proposal prompts
- `src/genie_space_optimizer/common/genie_client.py` — fetch_space_config, patch_space_config

---

## What to Build

### 1. `optimization/optimizer.py` — Failure analysis + proposal generation

Takes evaluation failures (ASI) + current metadata snapshot → targeted change proposals grouped by lever.

**Failure Analysis:**
- `cluster_failures(eval_results, metadata_snapshot)` → list of failure clusters: `failure_type`, `affected_questions`, `blame_set`, `counterfactual_fixes`, `assigned_lever`
- `_map_to_lever(root_cause, asi_failure_type, blame_set)` → lever 1-6

**Scope Resolution:**
- `_resolve_scope(lever, apply_mode)` → `"genie_config"` | `"uc_artifact"` | `"both"`. Levers 4-6 always return `"genie_config"`. Levers 1-3 return whatever `apply_mode` says.

**Proposal Generation (LLM-powered):**
- `generate_metadata_proposals(clusters, metadata_snapshot, target_lever, apply_mode="genie_config", w=None)` — for a lever, generate concrete proposals
  - For each cluster: calls `_call_llm_for_proposal()` to generate `proposed_value` using **Databricks Claude Opus 4.6**
  - Prompt selection by lever:
    - Levers 1-3: `PROPOSAL_GENERATION_PROMPT`
    - Lever 4: `LEVER_4_JOIN_SPEC_PROMPT` (join specification context)
    - Lever 5: `LEVER_5_DISCOVERY_PROMPT` (column discovery context, respects `MAX_VALUE_DICTIONARY_COLUMNS=120`)
    - Lever 6: `LEVER_6_INSTRUCTION_PROMPT` (routing/disambiguation context)
  - All 4 prompts defined in `config.py`
  - Resolves scope via `_resolve_scope(lever, apply_mode)` to determine target
- `_call_llm_for_proposal(cluster, metadata_snapshot, patch_type, lever)` — the actual LLM call. Returns `{"proposed_value": str, "rationale": str}`
- `propose_patch_set_from_asi(asi_rows, metadata_snapshot, lever)` — proposals from ASI records
- `score_patch_set(proposals, metadata_snapshot)` — expected impact, risk, conflict potential (pure Python, no LLM)
- `detect_conflicts_and_batch(proposals)` — identify CONFLICT_RULES pairs, batch non-conflicting (pure Python, no LLM)

**Where LLM calls occur in the optimizer:**

| Function | LLM? | Purpose |
|----------|-------|---------|
| `cluster_failures()` | No | Group ASI rows by (failure_type, blame_set) |
| `_map_to_lever()` | No | Static lookup table |
| `_resolve_scope()` | No | Determine target (genie_config / uc_artifact / both) |
| `generate_metadata_proposals()` | **Yes** | Generate proposed_value text via Claude Opus 4.6 |
| `_call_llm_for_proposal()` | **Yes** | The actual LLM call |
| `score_patch_set()` | No | Algorithmic scoring |
| `detect_conflicts_and_batch()` | No | CONFLICT_RULES check |
| `render_patch()` | No | Convert to action dict |
| `apply_patch_set()` | No | API calls to Genie or UC |

**ASI Extraction:**
- `read_asi_from_uc(spark, mlflow_run_id, catalog, schema)` — query `genie_eval_asi_results`
- `_extract_asi_from_assessments(eval_result)` — from MLflow assessments
- `_extract_judge_feedbacks_from_eval(eval_result)` — per-judge feedback
- `_infer_blame_from_rationale(rationale, metadata_snapshot)` — blame set from LLM rationale

**Regression Detection:**
- `detect_regressions(current_scores, previous_scores, threshold=2.0)` — detect > threshold drop

**Lever 4 — Join Specification Optimization:**
Lever 4 optimizes `join_specs` in the Genie Space config. Uses `LEVER_4_JOIN_SPEC_PROMPT` with join context. Patch types: `add_join_spec`, `update_join_spec`, `remove_join_spec`. Always `genie_config` scope.

**Lever 5 — Column Discovery Settings:**
Lever 5 optimizes per-column discovery flags: `get_example_values`, `build_value_dictionary`, and `synonyms`. Uses `LEVER_5_DISCOVERY_PROMPT`. Respects the 120-column limit for `build_value_dictionary`. Patch types: `enable_example_values`, `disable_example_values`, `enable_value_dictionary`, `disable_value_dictionary`, `add_column_synonym`, `remove_column_synonym`. Always `genie_config` scope.

**Lever 6 — Instruction Optimization:**
Lever 6 uses the same `cluster_failures()` → `generate_metadata_proposals(target_lever=6)` → `apply_patch_set()` pattern as levers 1-3. Uses `LEVER_6_INSTRUCTION_PROMPT`. No GEPA dependency, no external packages.

### 2. `optimization/applier.py` — Patch rendering, application, rollback

**Patch Rendering:**
- `proposals_to_patches(proposals)` — convert to Patch DSL format
- `render_patch(patch)` → `{command: {...}, rollback: {...}}`
- `validate_patch_set(patches, metadata_snapshot)` — validate against current state

**Patch Application:**
- `apply_patch_set(w, space_id, patches, metadata_snapshot, *, apply_mode="genie_config", deploy_target=None)` → `apply_log` with `applied`, `skipped`, `patched_objects`
  - Resolve scope per patch via `_resolve_scope(lever, apply_mode)`
  - Apply in risk order: LOW → MEDIUM → HIGH
  - For `genie_config` scope: `_apply_action_to_config(config, action)` → mutate Genie Space config
  - For `uc_artifact` scope: `_apply_action_to_uc(w, action)` → execute DDL against UC
  - For `both`: apply to both targets
  - Each: render → apply → verify → record
- `_apply_action_to_config(config, action)` — mutate config for one action (extended for `join_specs` and `column_configs` sections)
- `_apply_action_to_uc(w, action)` — apply via `ALTER TABLE`, `CREATE OR REPLACE FUNCTION`, etc. (levers 1-3 only, when `apply_mode` includes `uc_artifact`)
- `classify_risk(patch_type)` → "low" | "medium" | "high"

**Rollback:**
- `rollback(apply_log, w, space_id, metadata_snapshot)` — undo in reverse order (HIGH → MEDIUM → LOW)

**Config Helpers:**
- `strip_non_exportable_fields(config)` — remove non-settable fields
- `sort_genie_config(config)` — deterministic key ordering
- `_get_general_instructions(config)` / `_set_general_instructions(config, text)`

**Dual Persistence:**
- `verify_dual_persistence(applied_patches)` — verify Genie config + UC objects updated
- `verify_repo_update(patch, w)` — verify specific patch persisted

### 3. `optimization/benchmarks.py` — Benchmark management

Benchmarks are stored as MLflow evaluation datasets in UC — no YAML files.

- `load_benchmarks_from_dataset(spark_or_dataset, uc_schema, domain)` → `list[dict]` — load from MLflow eval dataset (`{uc_schema}.genie_benchmarks_{domain}`)
- `validate_benchmarks(benchmarks, spark)` — validate each expected_sql via `spark.sql(f"EXPLAIN {resolve_sql(sql)}")`
- `assign_splits(benchmarks, train_ratio=0.8)` — train/held_out split
- `validate_ground_truth_sql(sql, spark)` — validate single SQL
- `build_eval_records(benchmarks)` — convert to MLflow record format
- `apply_benchmark_corrections(corrections, spark, uc_schema, domain)` — apply arbiter corrections to the eval dataset (not YAML)

---

## File Structure

```
src/genie_space_optimizer/
├── optimization/
│   ├── __init__.py
│   ├── optimizer.py
│   ├── applier.py
│   └── benchmarks.py
```

---

## Key Design Decisions

1. **Pure functions where possible:** `cluster_failures`, `detect_regressions`, `render_patch`, `validate_patch_set`, `classify_risk` — no side effects.
2. **Side effects isolated:** Only `apply_patch_set` and `rollback` make API calls. Accept `WorkspaceClient` as arg.
3. **Risk ordering:** Apply LOW → MEDIUM → HIGH. Rollback HIGH → MEDIUM → LOW.
4. **Rollback completeness:** Every `render_patch()` includes both `command` and `rollback`. The `apply_log` stores these.
5. **Reuse verbatim.** Most functions copied from worker scripts — see `08-module-map.md` Section 3.

---

## Acceptance Criteria

1. `cluster_failures` groups same failure_type + overlapping blame_set → same cluster
2. `generate_metadata_proposals` respects `target_lever` — only that lever's proposals
3. `_resolve_scope` returns correct scope for all 6 levers under each `apply_mode`
4. `detect_conflicts_and_batch` catches all 18 CONFLICT_RULES pairs
5. `apply_patch_set` applies LOW → MEDIUM → HIGH, dispatches to `_apply_action_to_config` or `_apply_action_to_uc` based on resolved scope
6. `rollback` undoes in reverse order
7. `validate_patch_set` rejects non-existent columns/tables
8. `render_patch` produces `command` + `rollback` for all 35 patch types (including Lever 4 join_spec and Lever 5 column discovery)
9. `detect_regressions` catches drops > 2.0 percentage points
10. Lever 5 optimizer respects `MAX_VALUE_DICTIONARY_COLUMNS=120` limit

---

## Predecessor Prompts
- BUILD-01 — provides config.py, genie_client.py

## Successor Prompts
- BUILD-05 (Harness) — orchestrates optimizer → applier → evaluation loop
