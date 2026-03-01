"""
All configurable constants for the Genie Space Optimizer.

Module-level constants with sensible defaults. Can be overridden via
environment variables, job parameters, or the `thresholds` argument
to `optimize_genie_space()`.
"""

from __future__ import annotations

import os

# ── 1. Quality Thresholds ───────────────────────────────────────────────

DEFAULT_THRESHOLDS = {
    "syntax_validity": 98.0,
    "schema_accuracy": 95.0,
    "logical_accuracy": 90.0,
    "semantic_equivalence": 90.0,
    "completeness": 90.0,
    "response_quality": 0.0,
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
    "response_quality/mean": 0.0,
    "result_correctness/mean": 0.85,
    "asset_routing/mean": 0.95,
}

# ── 2. Rate Limits and Timing ──────────────────────────────────────────

RATE_LIMIT_SECONDS = 12
GENIE_RATE_LIMIT_RETRIES = 3
GENIE_RATE_LIMIT_BASE_DELAY = 30
PROPAGATION_WAIT_SECONDS = int(os.getenv("GENIE_SPACE_OPTIMIZER_PROPAGATION_WAIT", "30"))
PROPAGATION_WAIT_ENTITY_MATCHING_SECONDS = int(
    os.getenv("GENIE_SPACE_OPTIMIZER_PROPAGATION_WAIT_ENTITY_MATCHING", "90")
)
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
SLICE_GATE_TOLERANCE = 15.0
REGRESSION_THRESHOLD = 5.0
PLATEAU_ITERATIONS = 2
ARBITER_CORRECTION_TRIGGER = 3
REPEATABILITY_EXTRA_QUERIES = 2

# ── 4. LLM Configuration ──────────────────────────────────────────────

LLM_ENDPOINT = "databricks-claude-opus-4-6"
LLM_TEMPERATURE = 0
LLM_MAX_RETRIES = 3

# ── 5. Benchmark Generation ────────────────────────────────────────────

TARGET_BENCHMARK_COUNT = 20
COVERAGE_GAP_SOFT_CAP_FACTOR = 1.5

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
    'QUESTION-SQL ALIGNMENT: The expected_sql MUST answer EXACTLY what the question asks — '
    'no more, no less. Specifically:\n'
    '- Do NOT add WHERE filters that the question does not mention (e.g., do not filter by '
    'booking_status=\'confirmed\' unless the question explicitly says "confirmed").\n'
    '- Do NOT add extra columns beyond what the question asks for. If the question asks for '
    '"revenue by destination", return destination and revenue — not also booking_count, '
    'avg_value, etc.\n'
    '- Do NOT add JOINs that only serve to add unrequested columns.\n'
    '- If the question is ambiguous about a filter, do NOT assume one. Write SQL that '
    'interprets the question literally.\n'
    '\n'
    'MINIMAL SQL PRINCIPLE: Write the simplest correct SQL that answers the question. '
    'Prefer fewer JOINs, fewer columns, and fewer filters. The GT SQL should be the '
    '"canonical minimal" answer that any correct system would produce.\n'
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
    '## Metric Views\n'
    '{metric_views_context}\n'
    '\n'
    '## Table-Valued Functions\n'
    '{tvfs_context}\n'
    '\n'
    '## Benchmarks to Fix\n'
    '{benchmarks_to_fix}\n'
    '\n'
    'For each benchmark:\n'
    '- If the error is a wrong table/view name, find the closest matching valid asset '
    'and rewrite the SQL.\n'
    '- If the error is a field drift issue (e.g., property_name vs property), map it to the '
    'closest valid metadata column from the provided schema context.\n'
    '- For metric views, use MEASURE() syntax for aggregate columns in SELECT/ORDER BY.\n'
    '- For table-valued functions, use the correct function call signature.\n'
    '- If no valid asset can answer the question, set "expected_sql" to null and '
    '"unfixable_reason" to explain why.\n'
    '- Preserve the original question text.\n'
    '- Apply the MINIMAL SQL PRINCIPLE: the corrected SQL should answer exactly what '
    'the question asks — no extra filters, columns, or joins.\n'
    '\n'
    'Return a JSON array of objects with: "question", "expected_sql" (corrected or null), '
    '"expected_asset", "category", "required_tables", "required_columns", "expected_facts", '
    '"unfixable_reason" (null if fixed).\n'
    '\n'
    'No markdown, just JSON.'
)

BENCHMARK_ALIGNMENT_CHECK_PROMPT = (
    'You are a Databricks SQL quality reviewer. Given a benchmark question and its '
    'ground-truth SQL, determine whether the SQL answers EXACTLY what the question asks.\n'
    '\n'
    'Questions and SQL to review:\n'
    '{benchmarks_json}\n'
    '\n'
    'For each benchmark, check for these issues:\n'
    '1. EXTRA_FILTER: SQL adds WHERE conditions not mentioned in the question '
    '(e.g., filtering by status, date range, or flag the question never specifies).\n'
    '2. EXTRA_COLUMNS: SQL returns columns the question did not ask for.\n'
    '3. MISSING_AGGREGATION: The question implies aggregation (e.g., "total", "average", '
    '"by category") but the SQL returns unaggregated rows.\n'
    '4. WRONG_INTERPRETATION: The SQL answers a materially different question.\n'
    '\n'
    'Be strict about EXTRA_FILTER — if the question says "revenue by destination" without '
    'mentioning a status, booking_status filters are EXTRA. If the question says "for '
    'confirmed bookings", the filter is required.\n'
    '\n'
    'Be lenient about EXTRA_COLUMNS — adding 1-2 contextual columns (e.g., a name alongside '
    'an ID) is acceptable. Only flag if the SQL returns many unrequested columns.\n'
    '\n'
    'Return a JSON array (one object per benchmark) with:\n'
    '{{"question": "...", "aligned": true/false, '
    '"issues": ["ISSUE_TYPE: description", ...]}}\n'
    '\n'
    'No markdown, just JSON.'
)

BENCHMARK_COVERAGE_GAP_PROMPT = (
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
    '## Uncovered Assets (MUST be targeted)\n'
    '{uncovered_assets}\n'
    '\n'
    'The benchmark suite already covers some assets but the ones listed under '
    '"Uncovered Assets" have ZERO benchmark questions. Your job is to fill '
    'these coverage gaps.\n'
    '\n'
    'CRITICAL CONSTRAINT: Your expected_sql MUST ONLY reference the tables, metric views, '
    'and functions listed in "VALID Data Assets" above. Do NOT invent, hallucinate, or guess '
    'table/view names. Every FROM clause, JOIN, and function call must reference a real asset '
    'from the list above.\n'
    'STRICT COLUMN CONSTRAINT: required_columns and SQL-selected/filter columns must come only '
    'from the provided metadata context. Do NOT invent field names or aliases that are not '
    'resolvable to listed metadata columns.\n'
    '\n'
    'QUESTION-SQL ALIGNMENT: The expected_sql MUST answer EXACTLY what the question asks — '
    'no more, no less. Specifically:\n'
    '- Do NOT add WHERE filters that the question does not mention.\n'
    '- Do NOT add extra columns beyond what the question asks for.\n'
    '- Do NOT add JOINs that only serve to add unrequested columns.\n'
    '- If the question is ambiguous about a filter, do NOT assume one.\n'
    '\n'
    'MINIMAL SQL PRINCIPLE: Write the simplest correct SQL that answers the question. '
    'Prefer fewer JOINs, fewer columns, and fewer filters.\n'
    '\n'
    'Generate 1-2 benchmark questions PER uncovered asset. Each question MUST reference '
    'the specified uncovered asset in its FROM clause, JOIN, or function call.\n'
    '\n'
    'For each question, provide:\n'
    '1. "question": The natural language question a business user would ask\n'
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
    '## Already Covered Questions (do NOT duplicate these)\n'
    '{existing_questions}\n'
    '\n'
    'Return a JSON array of question objects. No markdown, just JSON.'
)

# ── 5b. Proposal Generation Prompts ───────────────────────────────────

PROPOSAL_GENERATION_PROMPT = (
    'You are a Databricks metadata optimization expert. Your job is to fix a Genie Space\n'
    'so that it generates correct SQL for user questions.\n'
    '\n'
    '## Failure Analysis\n'
    '- Root cause: {failure_type}\n'
    '- Blamed objects: {blame_set}\n'
    '- Affected questions ({severity}): {affected_questions}\n'
    '\n'
    '## SQL Diffs (Expected vs Generated)\n'
    '{sql_diffs}\n'
    '\n'
    '## Current Metadata for Blamed Objects\n'
    '{current_metadata}\n'
    '\n'
    '## Target Change Type\n'
    '{patch_type_description}\n'
    '\n'
    'Analyze the SQL diffs above carefully. Identify EXACTLY what metadata change\n'
    '(column description, table description, or instruction) would guide Genie\n'
    'to produce the expected SQL instead of the generated SQL.\n'
    '\n'
    'Be specific — reference actual table/column names from the SQL.\n'
    'Do NOT generate generic instructions. Generate a targeted metadata fix.\n'
    '\n'
    'IMPORTANT: Instruction budget remaining: {instruction_char_budget} chars. Keep additions under 500 chars.\n'
    '\n'
    'Return JSON: {{"proposed_value": "...", "rationale": "..."}}'
)

LEVER_1_2_COLUMN_PROMPT = (
    'IMPORTANT: Your response must be ONLY a JSON object with the schema shown at the end.\n'
    'Do not write analysis or commentary -- put your reasoning in the "rationale" field.\n'
    '\n'
    'You are a Databricks Genie Space metadata expert. Your job is to fix column\n'
    'descriptions and synonyms so that Genie generates correct SQL for user questions.\n'
    '\n'
    '## Failure Analysis\n'
    '- Root cause: {failure_type}\n'
    '- Blamed objects: {blame_set}\n'
    '- Affected questions: {affected_questions}\n'
    '\n'
    '## SQL Diffs (Expected vs Generated)\n'
    '{sql_diffs}\n'
    '\n'
    '## Full Genie Space Schema (all tables, columns, descriptions, synonyms)\n'
    '{full_schema_context}\n'
    '\n'
    '## Column Config Format\n'
    'Genie Space column configs use this exact structure:\n'
    '```json\n'
    '{{"column_name": "store_number", "description": ["Store business key for human readability"], "synonyms": ["store id", "location number"]}}\n'
    '```\n'
    '- `description` is a list of strings. If the column already has an adequate\n'
    '  description or should inherit its description from Unity Catalog, set to null.\n'
    '- `synonyms` are alternative names users might use to refer to this column.\n'
    '- Do NOT add a description to columns that already have a correct one.\n'
    '  Prefer adding synonyms when the issue is naming/aliasing.\n'
    '- Do NOT repeat existing synonyms.\n'
    '\n'
    'Analyze the SQL diffs carefully. Identify which columns Genie confused or\n'
    'could not resolve, then propose targeted description or synonym changes.\n'
    '\n'
    'Return JSON with one entry per column that needs fixing:\n'
    '{{"changes": [\n'
    '  {{"table": "<fully_qualified_table_name>", "column": "<column_name>",\n'
    '    "description": ["<new description>"] or null,\n'
    '    "synonyms": ["<term1>", "<term2>"] or null}}\n'
    '], "rationale": "..."}}\n'
)

LEVER_4_JOIN_SPEC_PROMPT = (
    'You are a Databricks Genie Space join optimization expert.\n'
    '\n'
    '## SQL Diffs showing join issues\n'
    '{sql_diffs}\n'
    '\n'
    '## Current Join Specs\n'
    '{current_join_specs}\n'
    '\n'
    '## Table Relationships\n'
    '{table_relationships}\n'
    '\n'
    '## Full Schema Context (tables, columns, data types, descriptions)\n'
    '{full_schema_context}\n'
    '\n'
    'Analyze the SQL diffs to determine which tables need to be joined and how.\n'
    'Compare the expected SQL JOIN clauses with the generated SQL to identify\n'
    'the missing or incorrect join specification.\n'
    '\n'
    'IMPORTANT: The join columns MUST have compatible data types. Check the\n'
    'column types in the schema context above before proposing a join.\n'
    'For example, joining an INT column to a STRING column is invalid.\n'
    '\n'
    'Generate a valid join_spec JSON matching the Genie Space API format:\n'
    '- "left": {{"identifier": "<fully_qualified_table>", "alias": "<short_table_name>"}}\n'
    '- "right": {{"identifier": "<fully_qualified_table>", "alias": "<short_table_name>"}}\n'
    '- "sql": ["<join_condition_using_aliases>", "--rt=<relationship_type>--"]\n'
    '\n'
    'The alias should be the unqualified table name (last segment of the identifier).\n'
    'The join condition must use backtick-quoted aliases, e.g.:\n'
    '  "`fact_sales`.`product_key` = `dim_product`.`product_key`"\n'
    'The relationship_type annotation must be one of:\n'
    '  --rt=FROM_RELATIONSHIP_TYPE_MANY_TO_ONE--\n'
    '  --rt=FROM_RELATIONSHIP_TYPE_ONE_TO_MANY--\n'
    '  --rt=FROM_RELATIONSHIP_TYPE_ONE_TO_ONE--\n'
    '\n'
    'Return JSON: {{"join_spec": {{...}}, "rationale": "..."}}'
)

LEVER_4_JOIN_DISCOVERY_PROMPT = (
    'You are a Databricks Genie Space join optimization expert.\n'
    'Your task is to identify MISSING join relationships between tables.\n'
    '\n'
    '## Full Schema Context (tables, columns, data types, descriptions)\n'
    '{full_schema_context}\n'
    '\n'
    '## Currently Defined Join Specs\n'
    '{current_join_specs}\n'
    '\n'
    '## Heuristic Candidate Hints\n'
    'The following table pairs have been flagged by automated analysis as\n'
    'potential join candidates. Each hint includes the reason it was flagged.\n'
    'These are HINTS only — you must validate them using the schema context.\n'
    '\n'
    '{discovery_hints}\n'
    '\n'
    '## Instructions\n'
    'Review the heuristic hints above alongside the full schema context.\n'
    'For each hint, decide whether a join relationship actually exists by checking:\n'
    '1. Column data types MUST be compatible (e.g. INT=INT, BIGINT=INT, STRING=STRING).\n'
    '   Do NOT propose joining columns with incompatible types (e.g. INT to STRING).\n'
    '2. Column names and/or descriptions suggest a foreign-key relationship.\n'
    '3. The join is not already defined in the current join specs.\n'
    '\n'
    'Also look for any additional missing joins NOT covered by the hints.\n'
    '\n'
    'For each valid join, generate a join_spec in Genie Space API format:\n'
    '- "left": {{"identifier": "<fully_qualified_table>", "alias": "<short_table_name>"}}\n'
    '- "right": {{"identifier": "<fully_qualified_table>", "alias": "<short_table_name>"}}\n'
    '- "sql": ["<join_condition_using_aliases>", "--rt=<relationship_type>--"]\n'
    '\n'
    'The alias should be the unqualified table name (last segment of the identifier).\n'
    'The join condition must use backtick-quoted aliases.\n'
    'The relationship_type annotation must be one of:\n'
    '  --rt=FROM_RELATIONSHIP_TYPE_MANY_TO_ONE--\n'
    '  --rt=FROM_RELATIONSHIP_TYPE_ONE_TO_MANY--\n'
    '  --rt=FROM_RELATIONSHIP_TYPE_ONE_TO_ONE--\n'
    '\n'
    'Return JSON: {{"join_specs": [{{...}}, ...], "rationale": "..."}}\n'
    'If no valid joins are found, return: {{"join_specs": [], "rationale": "..."}}'
)

LEVER_5_INSTRUCTION_PROMPT = (
    'You are a Databricks Genie Space instruction expert.\n'
    '\n'
    '## SQL Diffs showing routing/disambiguation issues\n'
    '{sql_diffs}\n'
    '\n'
    '## Current Text Instructions\n'
    '{current_instructions}\n'
    '\n'
    '## Existing Example SQL Queries\n'
    '{existing_example_sqls}\n'
    '\n'
    '## Available Assets\n'
    'Tables: {table_names}\n'
    'Metric Views: {mv_names}\n'
    'TVFs: {tvf_names}\n'
    '\n'
    '## Instruction Type Priority (MUST follow this hierarchy)\n'
    '1. **SQL expressions** — Use for defining business metrics, filters, or dimensions '
    '(e.g., revenue, active_customers, gross_margin). These are handled by levers 1-4; '
    'choose this ONLY when the fix is a column-level semantic definition that earlier levers missed.\n'
    '2. **Example SQL queries** — Use to teach Genie how to handle ambiguous, multi-part, '
    'or complex question patterns. Provide a representative question and its correct SQL. '
    'Genie uses these to match similar user prompts and learn query patterns.\n'
    '3. **Text instructions** — Use ONLY as a last resort when SQL expressions and example '
    'SQL cannot address the need (e.g., clarification prompts, formatting rules, '
    'cross-cutting behavioral guidance).\n'
    '\n'
    'Analyze the SQL diffs and choose the HIGHEST-PRIORITY instruction type that fixes the issue.\n'
    '\n'
    '## ROUTING FAILURES MUST USE EXAMPLE SQL\n'
    'When the failure involves asset routing (wrong table, TVF, or metric view chosen),\n'
    'you MUST return instruction_type "example_sql". Do NOT fall back to text_instruction\n'
    'for routing issues — example SQL is orders of magnitude more effective because Genie\n'
    'pattern-matches against it directly.\n'
    '\n'
    '## Rules for Text Instructions (when you must use them)\n'
    '- Use structured markdown: ## headers, - bullets, short lines.\n'
    '- Be SPECIFIC: reference actual table names, column names, TVFs, or metric views\n'
    '  from the Available Assets section above.\n'
    '- NEVER generate generic domain guidance (e.g., "Revenue refers to gross revenue\n'
    '  unless stated otherwise"). Every instruction MUST reference a specific asset\n'
    '  from this Genie Space.\n'
    '- NEVER conflict with existing example SQL or text instructions.\n'
    '- Target ~50 lines max with clear section breaks.\n'
    '- Instruction budget remaining: {instruction_char_budget} chars.\n'
    '\n'
    '## Rules for Example SQL\n'
    '- The question must be a realistic user prompt that matches the failure pattern.\n'
    '- The SQL must be correct, executable, and consistent with existing instructions.\n'
    '- The SQL MUST reference tables, TVFs, or metric views from Available Assets above.\n'
    '- Do NOT duplicate an existing example SQL question (see above).\n'
    '- Use named parameter markers (`:param_name`) when the query filters on a value '
    'the user is likely to vary (e.g., date range, threshold, category). '
    'Parameterized queries produce **trusted asset** responses in Genie.\n'
    '- For each parameter, provide: name (matching the `:param_name` in SQL), '
    'type_hint (STRING, INTEGER, DATE, or DECIMAL), and a sensible default_value.\n'
    '- Include a usage_guidance string describing when Genie should match this query.\n'
    '- If no parameters are needed, set "parameters" to [] and still provide usage_guidance.\n'
    '\n'
    '## Anti-Hallucination Guard\n'
    'CRITICAL: If you have no blamed objects, no counterfactual fix, and cannot identify\n'
    'a specific asset to reference, return:\n'
    '{{"instruction_type": "text_instruction", "instruction_text": "", '
    '"rationale": "No actionable fix identified"}}\n'
    'An empty instruction_text signals that no proposal should be generated.\n'
    '\n'
    'Return JSON with one of these formats based on instruction_type:\n'
    '\n'
    'For example_sql:\n'
    '{{"instruction_type": "example_sql", "example_question": "...", "example_sql": "...", '
    '"parameters": [{{"name": "param_name", "type_hint": "STRING|INTEGER|DATE|DECIMAL", '
    '"default_value": "..."}}], '
    '"usage_guidance": "When to use this query", "rationale": "..."}}\n'
    '\n'
    'For text_instruction:\n'
    '{{"instruction_type": "text_instruction", "instruction_text": "...", "rationale": "..."}}\n'
    '\n'
    'For sql_expression (delegate to levers 1-4):\n'
    '{{"instruction_type": "sql_expression", "target_table": "...", "target_column": "...", '
    '"expression": "...", "rationale": "..."}}'
)

LEVER_5_HOLISTIC_PROMPT = (
    'You are a Databricks Genie Space instruction architect.\n'
    'Your task is to synthesize ALL evaluation learnings into a single, coherent\n'
    'instruction document for this Genie Space, plus targeted example SQL queries.\n'
    '\n'
    '## Genie Space Purpose\n'
    '{space_description}\n'
    '\n'
    '## Evaluation Summary\n'
    '{eval_summary}\n'
    '\n'
    '## Failure Clusters from Evaluation\n'
    'The following clusters represent systematic issues found during benchmark evaluation.\n'
    'Each cluster groups related failures by root cause and blamed objects.\n'
    'Clusters tagged "Correct-but-Suboptimal" produced correct results but used fragile or\n'
    'non-standard approaches -- use these for best-practice guidance in instructions, not fixes.\n'
    '{cluster_briefs}\n'
    '\n'
    '## Changes Already Applied by Earlier Levers\n'
    'Levers 1-4 have already applied the following fixes in this optimization iteration.\n'
    'Your instructions should COMPLEMENT these fixes, not duplicate them.\n'
    '{lever_summary}\n'
    '\n'
    '## Current Text Instructions\n'
    '{current_instructions}\n'
    '\n'
    '## Existing Example SQL Queries\n'
    '{existing_example_sqls}\n'
    '\n'
    '## Available Assets\n'
    'Tables: {table_names}\n'
    'Metric Views: {mv_names}\n'
    'TVFs: {tvf_names}\n'
    '\n'
    '## Output Requirements\n'
    '\n'
    '### 1. Instruction Document (COMPLETE REWRITE)\n'
    'Write a complete instruction document that replaces the current instructions entirely.\n'
    'Preserve any WORKING guidance from Current Text Instructions, but restructure and\n'
    'improve it based on all evaluation learnings.\n'
    '\n'
    'Follow this template structure (AgentSkills-inspired progressive disclosure):\n'
    '\n'
    '```\n'
    '## Purpose\n'
    'One paragraph: what this Genie Space does and who it serves.\n'
    '\n'
    '## Asset Routing\n'
    '- When user asks about {topic}, use {specific_table_or_tvf_or_mv}\n'
    '- {additional routing rules referencing ACTUAL asset names from Available Assets}\n'
    '\n'
    '## Query Patterns\n'
    '- {pattern}: {SQL guidance with actual column names}\n'
    '- Focus on patterns that evaluation showed Genie gets wrong\n'
    '\n'
    '## Business Definitions\n'
    '- {term}: defined as {specific_column} from {specific_table}\n'
    '- Only include definitions that prevent evaluation failures\n'
    '\n'
    '## Disambiguation\n'
    '- When {ambiguous scenario from eval failures}, prefer {specific approach}\n'
    '\n'
    '## Formatting & Behavior\n'
    '- {any cross-cutting behavioral guidance}\n'
    '```\n'
    '\n'
    'Rules for the instruction document:\n'
    '- EVERY bullet must reference a specific asset from Available Assets.\n'
    '- NEVER include generic domain guidance that does not reference an actual asset.\n'
    '- Be concise: target 30-80 lines. Prefer bullets over paragraphs.\n'
    '- Use ## headers and - bullets for structure.\n'
    '- Instruction budget: {instruction_char_budget} chars MAXIMUM.\n'
    '- Omit sections that have no actionable content (e.g., skip "Formatting" if\n'
    '  no formatting issues were found in evaluation).\n'
    '\n'
    '### 2. Example SQL Proposals\n'
    'For failure clusters involving ROUTING issues (wrong table, wrong TVF, wrong MV)\n'
    'or ambiguous question patterns, propose example SQL queries. Example SQL is the\n'
    'most effective lever for routing — Genie pattern-matches against it directly.\n'
    '\n'
    'Rules for example SQL:\n'
    '- The question must be a realistic user prompt matching a failure pattern.\n'
    '- The SQL must be correct, executable, and reference assets from Available Assets.\n'
    '- Do NOT duplicate existing example SQL questions (see above).\n'
    '- Use named parameter markers (`:param_name`) when the query filters on a\n'
    '  user-variable value. Parameterized queries produce trusted asset responses.\n'
    '- For each parameter: name, type_hint (STRING|INTEGER|DATE|DECIMAL), default_value.\n'
    '- Include usage_guidance describing when Genie should match this query.\n'
    '\n'
    '## Anti-Hallucination Guard\n'
    'CRITICAL: If you cannot identify specific assets to reference or specific\n'
    'evaluation failures to address, return empty instruction_text and no example SQL.\n'
    'An empty instruction_text signals that no changes should be made.\n'
    '\n'
    'Return a single JSON object:\n'
    '{{\n'
    '  "instruction_text": "## Purpose\\n...",\n'
    '  "example_sql_proposals": [\n'
    '    {{\n'
    '      "example_question": "What is the total revenue by destination?",\n'
    '      "example_sql": "SELECT ...",\n'
    '      "parameters": [{{"name": "...", "type_hint": "STRING", "default_value": "..."}}],\n'
    '      "usage_guidance": "Use when user asks about revenue breakdown by destination"\n'
    '    }}\n'
    '  ],\n'
    '  "rationale": "Explanation of key changes made and why"\n'
    '}}\n'
    '\n'
    'If no example SQL proposals are needed, set "example_sql_proposals" to [].\n'
    'If no instruction changes are needed, set "instruction_text" to "".'
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
    "rewrite_instruction",
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
    5: "Genie Space Instructions",
}

DEFAULT_LEVER_ORDER = [1, 2, 3, 4, 5]

MAX_VALUE_DICTIONARY_COLUMNS = 120
"""Maximum number of string columns per Genie Space that can have
enable_entity_matching=true. Enforced by auto_apply_prompt_matching()."""

ENABLE_PROMPT_MATCHING_AUTO_APPLY = True
"""When True, format assistance and entity matching are applied as a
best-practice hygiene step between baseline evaluation and the lever loop."""

CATEGORICAL_COLUMN_PATTERNS = [
    "industry", "type", "status", "state", "country", "region",
    "department", "category", "segment", "code", "tier", "level",
    "stage", "phase", "class", "group", "channel", "source", "priority",
    "currency", "unit", "role", "gender", "brand", "vendor", "supplier",
]

FREE_TEXT_COLUMN_PATTERNS = [
    "description", "comment", "notes", "address", "email", "url",
    "path", "body", "message", "content", "text", "summary", "detail",
    "narrative", "reason", "explanation",
]

NUMERIC_DATA_TYPES = {
    "DOUBLE", "FLOAT", "DECIMAL", "INT", "INTEGER", "BIGINT",
    "SMALLINT", "TINYINT", "LONG", "SHORT", "BYTE", "NUMBER",
}

MEASURE_NAME_PREFIXES = [
    "avg_", "sum_", "count_", "total_", "pct_", "ratio_",
    "min_", "max_", "num_", "mean_", "median_", "stddev_",
]

# ── 12. Delta Table Names ─────────────────────────────────────────────

TABLE_RUNS = "genie_opt_runs"
TABLE_STAGES = "genie_opt_stages"
TABLE_ITERATIONS = "genie_opt_iterations"
TABLE_PATCHES = "genie_opt_patches"
TABLE_ASI = "genie_eval_asi_results"
TABLE_PROVENANCE = "genie_opt_provenance"

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
MAX_INSTRUCTION_TEXT_CHARS = 2000
MAX_HOLISTIC_INSTRUCTION_CHARS = 8000

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
    # Lever 6: Genie Space Instructions (text)
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
    "rewrite_instruction": {
        "type": "rewrite_instruction",
        "scope": "genie_config",
        "risk_level": "medium",
        "affects": ["instructions"],
    },
    # Lever 6: Genie Space Example SQL (preferred over text instructions)
    "add_example_sql": {
        "type": "add_example_sql",
        "scope": "genie_config",
        "risk_level": "low",
        "affects": ["instructions", "example_question_sqls"],
    },
    "update_example_sql": {
        "type": "update_example_sql",
        "scope": "genie_config",
        "risk_level": "medium",
        "affects": ["instructions", "example_question_sqls"],
    },
    "remove_example_sql": {
        "type": "remove_example_sql",
        "scope": "genie_config",
        "risk_level": "medium",
        "affects": ["instructions", "example_question_sqls"],
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

# ── 18. Conflict Rules (23 pairs) ─────────────────────────────────────

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
    # Example SQL conflict pairs
    ("add_example_sql", "remove_example_sql"),
    ("add_example_sql", "update_example_sql"),
    ("update_example_sql", "remove_example_sql"),
    # Cross-type conflicts: example SQL vs text instructions on same routing
    ("add_example_sql", "add_instruction"),
    ("add_example_sql", "update_instruction"),
    # Holistic rewrite conflicts with any other instruction mutation
    ("rewrite_instruction", "add_instruction"),
    ("rewrite_instruction", "update_instruction"),
    ("rewrite_instruction", "remove_instruction"),
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
    # Lever 2: Metric Views — route aggregation/measure issues to column descriptions
    ("wrong_aggregation", 2): "update_column_description",
    ("wrong_measure", 2): "update_column_description",
    ("missing_filter", 2): "update_mv_yaml",
    ("missing_temporal_filter", 2): "update_mv_yaml",
    # Lever 3: Table-Valued Functions
    ("tvf_parameter_error", 3): "add_tvf_parameter",
    ("repeatability_issue", 3): "add_tvf_parameter",
    # Lever 4: Join Specifications
    ("wrong_join", 4): "update_join_spec",
    ("missing_join_spec", 4): "add_join_spec",
    ("wrong_join_spec", 4): "update_join_spec",
    # Lever 5: Genie Space Instructions (example SQL preferred over text)
    ("asset_routing_error", 5): "add_example_sql",
    ("missing_instruction", 5): "add_example_sql",
    ("ambiguous_question", 5): "add_example_sql",
    ("missing_filter", 5): "add_example_sql",
}
