"""
All configurable constants for the Genie Space Optimizer.

Module-level constants with sensible defaults. Can be overridden via
environment variables, job parameters, or the `thresholds` argument
to `optimize_genie_space()`.
"""

from __future__ import annotations

# ── 1. Quality Thresholds ───────────────────────────────────────────────

DEFAULT_THRESHOLDS = {
    "syntax_validity": 98.0,
    "schema_accuracy": 95.0,
    "logical_accuracy": 90.0,
    "semantic_equivalence": 90.0,
    "completeness": 90.0,
    "result_correctness": 85.0,
    "asset_routing": 95.0,
}

REPEATABILITY_TARGET = 90.0

MLFLOW_THRESHOLDS = {
    "syntax_validity/mean": 0.98,
    "schema_accuracy/mean": 0.95,
    "logical_accuracy/mean": 0.90,
    "semantic_equivalence/mean": 0.90,
    "completeness/mean": 0.90,
    "result_correctness/mean": 0.85,
    "asset_routing/mean": 0.95,
}

# ── 2. Rate Limits and Timing ──────────────────────────────────────────

RATE_LIMIT_SECONDS = 12
PROPAGATION_WAIT_SECONDS = 30
GENIE_POLL_INITIAL = 3
GENIE_POLL_MAX = 10
GENIE_MAX_WAIT = 120
JOB_POLL_INTERVAL = 30
JOB_MAX_WAIT = 3600
UI_POLL_INTERVAL = 5
SQL_STATEMENT_POLL_LIMIT = 30
SQL_STATEMENT_POLL_INTERVAL = 2
INLINE_EVAL_DELAY = 12

# ── 3. Iteration and Convergence ───────────────────────────────────────

MAX_ITERATIONS = 5
SLICE_GATE_TOLERANCE = 5.0
REGRESSION_THRESHOLD = 2.0
PLATEAU_ITERATIONS = 2
ARBITER_CORRECTION_TRIGGER = 3
REPEATABILITY_EXTRA_QUERIES = 2

# ── 4. LLM Configuration ──────────────────────────────────────────────

LLM_ENDPOINT = "databricks-claude-opus-4-6"
LLM_TEMPERATURE = 0
LLM_MAX_RETRIES = 3

# ── 5. Benchmark Generation ────────────────────────────────────────────

TARGET_BENCHMARK_COUNT = 20

BENCHMARK_CATEGORIES = [
    "aggregation",
    "ranking",
    "time-series",
    "comparison",
    "detail",
    "list",
    "threshold",
]

TEMPLATE_VARIABLES = {
    "${catalog}": "catalog",
    "${gold_schema}": "gold_schema",
}

BENCHMARK_GENERATION_PROMPT = (
    'You are a Databricks Genie Space evaluation expert.\n'
    '\n'
    'Given the following schema context for domain "{domain}":\n'
    '\n'
    '## VALID Data Assets (ONLY use these in SQL)\n'
    '{valid_assets_context}\n'
    '\n'
    '## Tables and Columns\n'
    '{tables_context}\n'
    '\n'
    '## Metric Views\n'
    '{metric_views_context}\n'
    '\n'
    '## Table-Valued Functions\n'
    '{tvfs_context}\n'
    '\n'
    '## Genie Space Instructions\n'
    '{instructions_context}\n'
    '\n'
    '## Sample Questions (from Genie Space config)\n'
    '{sample_questions_context}\n'
    '\n'
    'CRITICAL CONSTRAINT: Your expected_sql MUST ONLY reference the tables, metric views, '
    'and functions listed in "VALID Data Assets" above. Do NOT invent, hallucinate, or guess '
    'table/view names. Every FROM clause, JOIN, and function call must reference a real asset '
    'from the list above. If you are unsure whether an asset exists, do NOT include a benchmark '
    'for it.\n'
    'STRICT COLUMN CONSTRAINT: required_columns and SQL-selected/filter columns must come only '
    'from the provided metadata context. Do NOT invent field names or aliases that are not '
    'resolvable to listed metadata columns.\n'
    '\n'
    'Generate exactly {target_count} diverse benchmark questions that a business user would ask.\n'
    '\n'
    'For each question, provide:\n'
    '1. "question": The natural language question\n'
    '2. "expected_sql": The correct SQL referencing ONLY the valid assets listed above.\n'
    '   Use fully-qualified names (catalog.schema.table) from the VALID Data Assets list.\n'
    '   - For metric views: use MEASURE() syntax\n'
    '   - For TVFs: use function call syntax\n'
    '   - For tables: use standard SQL\n'
    '3. "expected_asset": "MV" | "TVF" | "TABLE"\n'
    '4. "category": one of {categories}\n'
    '5. "required_tables": list of table names referenced\n'
    '6. "required_columns": list of column names referenced\n'
    '7. "expected_facts": list of 1-2 facts the answer should contain\n'
    '\n'
    'Ensure diversity: at least 2 questions per category. Include edge cases\n'
    '(filters, multi-table joins, temporal ranges, NULL handling).\n'
    '\n'
    'Return a JSON array of question objects. No markdown, just JSON.'
)

BENCHMARK_CORRECTION_PROMPT = (
    'You are a Databricks SQL expert fixing invalid benchmark questions.\n'
    '\n'
    'The following benchmark questions have SQL errors. Fix each one so the '
    'expected_sql is valid, using ONLY the valid assets listed below.\n'
    '\n'
    '## VALID Data Assets (ONLY these exist)\n'
    '{valid_assets_context}\n'
    '\n'
    '## Tables and Columns\n'
    '{tables_context}\n'
    '\n'
    '## Benchmarks to Fix\n'
    '{benchmarks_to_fix}\n'
    '\n'
    'For each benchmark:\n'
    '- If the error is a wrong table/view name, find the closest matching valid asset '
    'and rewrite the SQL.\n'
    '- If the error is a field drift issue (e.g., property_name vs property), map it to the '
    'closest valid metadata column from the provided schema context.\n'
    '- If no valid asset can answer the question, set "expected_sql" to null and '
    '"unfixable_reason" to explain why.\n'
    '- Preserve the original question text.\n'
    '\n'
    'Return a JSON array of objects with: "question", "expected_sql" (corrected or null), '
    '"expected_asset", "category", "required_tables", "required_columns", "expected_facts", '
    '"unfixable_reason" (null if fixed).\n'
    '\n'
    'No markdown, just JSON.'
)

# ── 5b. Proposal Generation Prompts ───────────────────────────────────

PROPOSAL_GENERATION_PROMPT = (
    'You are a Databricks metadata optimization expert.\n'
    '\n'
    'Given these evaluation failures and the current metadata state, generate a concrete\n'
    'metadata change to fix the failures.\n'
    '\n'
    '## Failure Cluster\n'
    '- Failure type: {failure_type}\n'
    '- Blamed objects: {blame_set}\n'
    '- Affected questions: {affected_questions}\n'
    '- Counterfactual fixes suggested by judges: {counterfactual_fixes}\n'
    '- Severity: {severity}\n'
    '\n'
    '## Current Metadata for Blamed Objects\n'
    '{current_metadata}\n'
    '\n'
    '## Target Change Type\n'
    '{patch_type_description}\n'
    '\n'
    'Generate the exact new value for the metadata field. Be specific and actionable.\n'
    'For column descriptions: include business definition, data type context, and usage hints.\n'
    '\n'
    'Return JSON: {{"proposed_value": "...", "rationale": "..."}}'
)

LEVER_4_JOIN_SPEC_PROMPT = (
    'You are a Databricks Genie Space join optimization expert.\n'
    '\n'
    'Given these join-related failures and the current join_specs configuration,\n'
    'generate a join specification that would help Genie correctly join the relevant tables.\n'
    '\n'
    '## Failures\n'
    '{failures_context}\n'
    '\n'
    '## Current Join Specs\n'
    '{current_join_specs}\n'
    '\n'
    '## Table Relationships\n'
    '{table_relationships}\n'
    '\n'
    '## Available Tables\n'
    '{table_names}\n'
    '\n'
    'Generate a valid join_spec JSON object. Include:\n'
    '- left_table_name and right_table_name (fully qualified)\n'
    '- join_columns array with left_column and right_column pairs\n'
    '- relationship_type annotation (e.g., "--rt=FROM_RELATIONSHIP_TYPE_MANY_TO_ONE--")\n'
    '\n'
    'Return JSON: {{"join_spec": {{...}}, "rationale": "..."}}'
)

LEVER_5_DISCOVERY_PROMPT = (
    'You are a Databricks Genie Space column discovery expert.\n'
    '\n'
    'Given these column discovery failures and the current column_configs,\n'
    'recommend which columns should have format assistance (get_example_values),\n'
    'entity matching (build_value_dictionary), or synonyms.\n'
    '\n'
    '## Failures\n'
    '{failures_context}\n'
    '\n'
    '## Current Column Configs\n'
    '{current_column_configs}\n'
    '\n'
    '## Constraints\n'
    '- Maximum {max_value_dictionary_cols} string columns can have build_value_dictionary enabled\n'
    '- Currently {string_column_count} string columns exist, {current_dictionary_count} have build_value_dictionary enabled\n'
    '\n'
    'Generate specific column-level recommendations.\n'
    '\n'
    'Return JSON: {{"recommendations": [{{"table": "...", "column": "...", '
    '"action": "enable_example_values|enable_value_dictionary|add_synonym", '
    '"value": "..."|null}}], "rationale": "..."}}'
)

LEVER_6_INSTRUCTION_PROMPT = (
    'You are a Databricks Genie Space instruction expert.\n'
    '\n'
    'Given these routing/disambiguation failures and the current Genie Space instructions,\n'
    'generate a new instruction that would help Genie route to the correct asset or\n'
    'disambiguate the user\'s question.\n'
    '\n'
    '## Failures\n'
    '{failures_context}\n'
    '\n'
    '## Current Instructions\n'
    '{current_instructions}\n'
    '\n'
    '## Available Assets\n'
    'Tables: {table_names}\n'
    'Metric Views: {mv_names}\n'
    'TVFs: {tvf_names}\n'
    '\n'
    'Generate a clear, concise instruction. Focus on:\n'
    '- When to use which asset type\n'
    '- How to interpret ambiguous terms\n'
    '- Default behaviors for common question patterns\n'
    '\n'
    'Return JSON: {{"instruction_text": "...", "rationale": "..."}}'
)

# ── 6. Non-Exportable Genie Config Fields ──────────────────────────────

NON_EXPORTABLE_FIELDS = {
    "id",
    "title",
    "description",
    "creator",
    "creator_id",
    "updated_by",
    "updated_at",
    "created_at",
    "warehouse_id",
    "execute_as_user_id",
    "space_status",
}

# ── 7. Feature Flags ──────────────────────────────────────────────────

USE_PATCH_DSL = True
USE_JOB_MODE = True
USE_LEVER_AWARE = True
ENABLE_CONTINUOUS_MONITORING = False

APPLY_MODE = "genie_config"
"""Where patches are applied. One of:
  - "genie_config": All changes go to Genie Space config overlays.
  - "uc_artifact": Column descriptions go to UC via ALTER TABLE, etc.
  - "both": Apply to both targets for maximum coverage.
Levers 4-6 are always genie_config regardless of this setting."""

# ── 8. Risk Classification Sets ────────────────────────────────────────

LOW_RISK_PATCHES = {
    "add_description",
    "update_description",
    "add_column_description",
    "update_column_description",
    "hide_column",
    "unhide_column",
    "add_instruction",
    "enable_example_values",
    "disable_example_values",
    "enable_value_dictionary",
    "disable_value_dictionary",
    "add_column_synonym",
    "remove_column_synonym",
}

MEDIUM_RISK_PATCHES = {
    "update_instruction",
    "remove_instruction",
    "rename_column_alias",
    "add_default_filter",
    "remove_default_filter",
    "update_filter_condition",
    "add_tvf_parameter",
    "remove_tvf_parameter",
    "add_mv_measure",
    "update_mv_measure",
    "remove_mv_measure",
    "add_mv_dimension",
    "remove_mv_dimension",
    "add_join_spec",
    "update_join_spec",
    "remove_join_spec",
}

HIGH_RISK_PATCHES = {
    "add_table",
    "remove_table",
    "update_tvf_sql",
    "add_tvf",
    "remove_tvf",
    "update_mv_yaml",
}

# ── 9. Repeatability Classification ────────────────────────────────────

REPEATABILITY_CLASSIFICATIONS = {
    100: "IDENTICAL",
    70: "MINOR_VARIANCE",
    50: "SIGNIFICANT_VARIANCE",
    0: "CRITICAL_VARIANCE",
}

# ── 10. Repeatability Fix Routing by Asset Type ───────────────────────

REPEATABILITY_FIX_BY_ASSET = {
    "TABLE": (
        "Add structured metadata (business_definition, synonyms[], grain, join_keys[]) "
        "to column descriptions. Add UC tags: preferred_for_genie=true, domain=<value>."
    ),
    "MV": (
        "Add structured column metadata to metric view columns. "
        "Use synonyms[] and preferred_questions[] to constrain dimension selection."
    ),
    "TVF": (
        "Add instruction clarifying deterministic parameter selection. "
        "TVF signature already constrains output; focus on parameter disambiguation."
    ),
    "NONE": "Add routing instruction to direct questions to the appropriate asset type.",
}

# ── 11. Lever Descriptions ─────────────────────────────────────────────

LEVER_NAMES = {
    1: "Tables & Columns",
    2: "Metric Views",
    3: "Table-Valued Functions",
    4: "Join Specifications",
    5: "Column Discovery Settings",
    6: "Genie Space Instructions",
}

DEFAULT_LEVER_ORDER = [1, 2, 3, 4, 5, 6]

MAX_VALUE_DICTIONARY_COLUMNS = 120
"""Maximum number of string columns per Genie Space that can have
build_value_dictionary=true. Enforced by the Lever 5 optimizer."""

# ── 12. Delta Table Names ─────────────────────────────────────────────

TABLE_RUNS = "genie_opt_runs"
TABLE_STAGES = "genie_opt_stages"
TABLE_ITERATIONS = "genie_opt_iterations"
TABLE_PATCHES = "genie_opt_patches"
TABLE_ASI = "genie_eval_asi_results"

# ── 13. MLflow Conventions ─────────────────────────────────────────────

EXPERIMENT_PATH_TEMPLATE = "/Users/{user_email}/genie-optimization/{domain}"
RUN_NAME_TEMPLATE = "genie_eval_iter{iteration}_{timestamp}"
MODEL_NAME_TEMPLATE = "genie-space-{space_id}"
PROMPT_NAME_TEMPLATE = "{uc_schema}.genie_opt_{judge_name}"
PROMPT_ALIAS = "production"

INSTRUCTION_PROMPT_NAME_TEMPLATE = "{uc_schema}.genie_instructions_{space_id}"
INSTRUCTION_PROMPT_ALIAS = "latest"

# ── 14. Patch DSL Constants ────────────────────────────────────────────

MAX_PATCH_OBJECTS = 5

RISK_LEVEL_SCORE = {
    "low": 1,
    "medium": 2,
    "high": 3,
}

GENERIC_FIX_PREFIXES = (
    "review ",
    "check ",
    "verify ",
    "ensure ",
    "investigate ",
)

# ── 15. Assessment Sources ─────────────────────────────────────────────

CODE_SOURCE_ID = "genie-optimizer-v2"
LLM_SOURCE_ID_TEMPLATE = "databricks:/{endpoint}"

# ── 16. Temporal Validation Patterns ───────────────────────────────────

TEMPORAL_PHRASES = (
    r"\b(this year|last year|last quarter|this quarter|last \d+ months?"
    r"|last \d+ days?|this month|last month|year to date|ytd)\b"
)
HARDCODED_DATE = r"'\d{4}-\d{2}-\d{2}'"

# ── 17. Patch Types (35 entries) ───────────────────────────────────────

PATCH_TYPES = {
    # Lever 1: Tables & Columns — descriptions, visibility, aliases
    "add_description": {
        "type": "add_description",
        "scope": "genie_config",
        "risk_level": "low",
        "affects": ["descriptions", "column_metadata"],
    },
    "update_description": {
        "type": "update_description",
        "scope": "genie_config",
        "risk_level": "low",
        "affects": ["descriptions", "column_metadata"],
    },
    "add_column_description": {
        "type": "add_column_description",
        "scope": "genie_config",
        "risk_level": "low",
        "affects": ["column_metadata", "descriptions"],
    },
    "update_column_description": {
        "type": "update_column_description",
        "scope": "genie_config",
        "risk_level": "low",
        "affects": ["column_metadata", "descriptions"],
    },
    "hide_column": {
        "type": "hide_column",
        "scope": "genie_config",
        "risk_level": "low",
        "affects": ["column_visibility", "column_metadata"],
    },
    "unhide_column": {
        "type": "unhide_column",
        "scope": "genie_config",
        "risk_level": "low",
        "affects": ["column_visibility", "column_metadata"],
    },
    "rename_column_alias": {
        "type": "rename_column_alias",
        "scope": "genie_config",
        "risk_level": "medium",
        "affects": ["column_metadata", "aliases"],
    },
    "add_table": {
        "type": "add_table",
        "scope": "uc_artifact",
        "risk_level": "high",
        "affects": ["tables", "schema"],
    },
    "remove_table": {
        "type": "remove_table",
        "scope": "uc_artifact",
        "risk_level": "high",
        "affects": ["tables", "schema"],
    },
    # Lever 2: Metric Views
    "add_mv_measure": {
        "type": "add_mv_measure",
        "scope": "uc_artifact",
        "risk_level": "medium",
        "affects": ["metric_view", "measures"],
    },
    "update_mv_measure": {
        "type": "update_mv_measure",
        "scope": "uc_artifact",
        "risk_level": "medium",
        "affects": ["metric_view", "measures"],
    },
    "remove_mv_measure": {
        "type": "remove_mv_measure",
        "scope": "uc_artifact",
        "risk_level": "medium",
        "affects": ["metric_view", "measures"],
    },
    "add_mv_dimension": {
        "type": "add_mv_dimension",
        "scope": "uc_artifact",
        "risk_level": "medium",
        "affects": ["metric_view", "dimensions"],
    },
    "remove_mv_dimension": {
        "type": "remove_mv_dimension",
        "scope": "uc_artifact",
        "risk_level": "medium",
        "affects": ["metric_view", "dimensions"],
    },
    "update_mv_yaml": {
        "type": "update_mv_yaml",
        "scope": "uc_artifact",
        "risk_level": "high",
        "affects": ["metric_view", "mv_yaml"],
    },
    # Lever 3: Table-Valued Functions
    "add_tvf_parameter": {
        "type": "add_tvf_parameter",
        "scope": "uc_artifact",
        "risk_level": "medium",
        "affects": ["tvf_parameters", "tvf_definition"],
    },
    "remove_tvf_parameter": {
        "type": "remove_tvf_parameter",
        "scope": "uc_artifact",
        "risk_level": "medium",
        "affects": ["tvf_parameters", "tvf_definition"],
    },
    "update_tvf_sql": {
        "type": "update_tvf_sql",
        "scope": "uc_artifact",
        "risk_level": "high",
        "affects": ["tvf_definition", "tvf_sql"],
    },
    "add_tvf": {
        "type": "add_tvf",
        "scope": "uc_artifact",
        "risk_level": "high",
        "affects": ["tvfs", "tvf_definition"],
    },
    "remove_tvf": {
        "type": "remove_tvf",
        "scope": "uc_artifact",
        "risk_level": "high",
        "affects": ["tvfs", "tvf_definition"],
    },
    # Lever 4: Join Specifications
    "add_join_spec": {
        "type": "add_join_spec",
        "scope": "genie_config",
        "risk_level": "medium",
        "affects": ["join_specs", "relationships"],
    },
    "update_join_spec": {
        "type": "update_join_spec",
        "scope": "genie_config",
        "risk_level": "medium",
        "affects": ["join_specs", "relationships"],
    },
    "remove_join_spec": {
        "type": "remove_join_spec",
        "scope": "genie_config",
        "risk_level": "medium",
        "affects": ["join_specs", "relationships"],
    },
    # Lever 5: Column Discovery Settings
    "enable_example_values": {
        "type": "enable_example_values",
        "scope": "genie_config",
        "risk_level": "low",
        "affects": ["column_config", "discovery"],
    },
    "disable_example_values": {
        "type": "disable_example_values",
        "scope": "genie_config",
        "risk_level": "low",
        "affects": ["column_config", "discovery"],
    },
    "enable_value_dictionary": {
        "type": "enable_value_dictionary",
        "scope": "genie_config",
        "risk_level": "low",
        "affects": ["column_config", "discovery"],
    },
    "disable_value_dictionary": {
        "type": "disable_value_dictionary",
        "scope": "genie_config",
        "risk_level": "low",
        "affects": ["column_config", "discovery"],
    },
    "add_column_synonym": {
        "type": "add_column_synonym",
        "scope": "genie_config",
        "risk_level": "low",
        "affects": ["column_config", "synonyms"],
    },
    "remove_column_synonym": {
        "type": "remove_column_synonym",
        "scope": "genie_config",
        "risk_level": "low",
        "affects": ["column_config", "synonyms"],
    },
    # Lever 6: Genie Space Instructions
    "add_instruction": {
        "type": "add_instruction",
        "scope": "genie_config",
        "risk_level": "low",
        "affects": ["instructions"],
    },
    "update_instruction": {
        "type": "update_instruction",
        "scope": "genie_config",
        "risk_level": "medium",
        "affects": ["instructions"],
    },
    "remove_instruction": {
        "type": "remove_instruction",
        "scope": "genie_config",
        "risk_level": "medium",
        "affects": ["instructions"],
    },
    # Shared: Filters
    "add_default_filter": {
        "type": "add_default_filter",
        "scope": "genie_config",
        "risk_level": "medium",
        "affects": ["filters", "default_filters"],
    },
    "remove_default_filter": {
        "type": "remove_default_filter",
        "scope": "genie_config",
        "risk_level": "medium",
        "affects": ["filters", "default_filters"],
    },
    "update_filter_condition": {
        "type": "update_filter_condition",
        "scope": "genie_config",
        "risk_level": "medium",
        "affects": ["filters", "default_filters"],
    },
}

# ── 18. Conflict Rules (18 pairs) ─────────────────────────────────────

CONFLICT_RULES = [
    ("add_table", "remove_table"),
    ("add_column_synonym", "remove_column_synonym"),
    ("add_instruction", "remove_instruction"),
    ("add_instruction", "update_instruction"),
    ("update_instruction", "remove_instruction"),
    ("add_join_spec", "remove_join_spec"),
    ("add_default_filter", "remove_default_filter"),
    ("add_tvf_parameter", "remove_tvf_parameter"),
    ("add_tvf", "remove_tvf"),
    ("add_mv_measure", "remove_mv_measure"),
    ("add_mv_dimension", "remove_mv_dimension"),
    ("hide_column", "unhide_column"),
    ("add_column_description", "update_column_description"),
    ("add_description", "update_description"),
    ("update_mv_measure", "remove_mv_measure"),
    ("enable_example_values", "disable_example_values"),
    ("enable_value_dictionary", "disable_value_dictionary"),
    ("update_join_spec", "remove_join_spec"),
]

# ── 19. Failure Taxonomy (22 types) ───────────────────────────────────

FAILURE_TAXONOMY = {
    "wrong_table",
    "wrong_column",
    "wrong_join",
    "missing_filter",
    "missing_temporal_filter",
    "wrong_aggregation",
    "wrong_measure",
    "missing_instruction",
    "ambiguous_question",
    "asset_routing_error",
    "tvf_parameter_error",
    "compliance_violation",
    "performance_issue",
    "repeatability_issue",
    "missing_synonym",
    "description_mismatch",
    "stale_data",
    "data_freshness",
    "missing_join_spec",
    "wrong_join_spec",
    "missing_format_assistance",
    "missing_entity_matching",
}

# ── 20. Judge Prompts (5 templates) ───────────────────────────────────

JUDGE_PROMPTS = {
    "schema_accuracy": (
        "You are a SQL schema expert evaluating SQL for a Databricks Genie Space.\n"
        "Determine if the GENERATED SQL references the correct tables, columns, and joins.\n\n"
        "User question: {{ inputs }}\n"
        "Generated SQL: {{ outputs }}\n"
        "Expected SQL: {{ expectations }}\n\n"
        "Respond with yes if the generated SQL references the correct tables, columns, "
        "and joins for the question, or no if it does not."
    ),
    "logical_accuracy": (
        "You are a SQL logic expert evaluating SQL for a Databricks Genie Space.\n"
        "Determine if the GENERATED SQL applies correct aggregations, filters, GROUP BY, "
        "ORDER BY, and WHERE clauses for the business question.\n\n"
        "User question: {{ inputs }}\n"
        "Generated SQL: {{ outputs }}\n"
        "Expected SQL: {{ expectations }}\n\n"
        "Respond with yes if the generated SQL applies the correct logic "
        "for the question, or no if it does not."
    ),
    "semantic_equivalence": (
        "You are a SQL semantics expert evaluating SQL for a Databricks Genie Space.\n"
        "Determine if the two SQL queries measure the SAME business metric and would "
        "answer the same question, even if written differently.\n\n"
        "User question: {{ inputs }}\n"
        "Generated SQL: {{ outputs }}\n"
        "Expected SQL: {{ expectations }}\n\n"
        "Respond with yes if the two queries are semantically equivalent "
        "for the question, or no if they are not."
    ),
    "completeness": (
        "You are a SQL completeness expert evaluating SQL for a Databricks Genie Space.\n"
        "Determine if the GENERATED SQL fully answers the user's question without "
        "missing dimensions, measures, or filters.\n\n"
        "User question: {{ inputs }}\n"
        "Generated SQL: {{ outputs }}\n"
        "Expected SQL: {{ expectations }}\n\n"
        "Respond with yes if the generated SQL fully answers the question, "
        "or no if it is missing dimensions, measures, or filters."
    ),
    "arbiter": (
        "You are a senior SQL arbiter for a Databricks Genie Space evaluation.\n"
        "Two SQL queries attempted to answer the same business question but produced different results.\n"
        "Analyze both queries and determine which is correct.\n\n"
        "User question and expected SQL: {{ inputs }}\n"
        "Genie response and comparison: {{ outputs }}\n"
        "Expected result: {{ expectations }}\n\n"
        "Return one of: genie_correct, ground_truth_correct, both_correct, neither_correct\n"
        'Respond with JSON: {"verdict": "...", "rationale": "explanation"}'
    ),
}

# ── 21. ASI Schema (12 fields) ─────────────────────────────────────────

ASI_SCHEMA = {
    "failure_type": "str (from FAILURE_TAXONOMY)",
    "severity": "str (critical|major|minor)",
    "confidence": "float (0.0-1.0)",
    "wrong_clause": "str|None (SELECT, FROM, WHERE, JOIN, GROUP BY, ORDER BY, MEASURE)",
    "blame_set": "list[str] (metadata fields blamed: table names, column names, instructions)",
    "quoted_metadata_text": "str|None (exact text from Genie config that caused the issue)",
    "missing_metadata": "str|None (what should exist but doesn't)",
    "ambiguity_detected": "bool",
    "expected_value": "str|None",
    "actual_value": "str|None",
    "counterfactual_fix": "str|None (suggested metadata change to fix)",
    "affected_question_pattern": "str|None (regex or description of affected questions)",
}

# ── 22. Lever-to-Patch-Type Mapping ────────────────────────────────────

_LEVER_TO_PATCH_TYPE: dict[tuple[str, int], str] = {
    # Lever 1: Tables & Columns
    ("wrong_column", 1): "update_column_description",
    ("wrong_table", 1): "update_description",
    ("description_mismatch", 1): "update_column_description",
    ("missing_synonym", 1): "add_column_synonym",
    # Lever 2: Metric Views
    ("wrong_aggregation", 2): "update_mv_measure",
    ("wrong_measure", 2): "update_mv_measure",
    ("missing_filter", 2): "update_mv_yaml",
    ("missing_temporal_filter", 2): "update_mv_yaml",
    # Lever 3: Table-Valued Functions
    ("tvf_parameter_error", 3): "add_tvf_parameter",
    ("repeatability_issue", 3): "add_tvf_parameter",
    # Lever 4: Join Specifications
    ("wrong_join", 4): "update_join_spec",
    ("missing_join_spec", 4): "add_join_spec",
    ("wrong_join_spec", 4): "update_join_spec",
    # Lever 5: Column Discovery Settings
    ("missing_format_assistance", 5): "enable_example_values",
    ("missing_entity_matching", 5): "enable_value_dictionary",
    ("missing_synonym", 5): "add_column_synonym",
    # Lever 6: Genie Space Instructions
    ("asset_routing_error", 6): "add_instruction",
    ("missing_instruction", 6): "add_instruction",
    ("ambiguous_question", 6): "add_instruction",
    ("missing_filter", 6): "add_instruction",
}
