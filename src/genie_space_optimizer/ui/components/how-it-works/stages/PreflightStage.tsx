"use client";

import {
  Globe,
  Database,
  BrainCircuit,
  CheckCircle2,
  UserCheck,
} from "lucide-react";
import { Badge } from "@/components/ui/badge";
import { ExpandableCard } from "../shared/ExpandableCard";
import { AnimatedChecklist } from "../shared/AnimatedChecklist";
import { DataModelCard } from "../shared/DataModelCard";
import { BENCHMARK_MODEL_FIELDS } from "../data";

const LLM_CONTEXT_BLOCKS = [
  "domain",
  "valid_assets_context",
  "tables_context",
  "column_allowlist",
  "metric_views_context",
  "tvfs_context",
  "instructions_context",
  "sample_questions_context",
  "data_profile_context",
];

const PROFILING_STEPS = [
  { id: "1", label: "COUNT(*) per table (up to 20 tables)" },
  { id: "2", label: "TABLESAMPLE 100 rows — cardinality, min/max per column" },
  { id: "3", label: "COLLECT_SET for low-cardinality columns (≤20 distinct values)" },
  { id: "4", label: "FK constraint extraction via REST API" },
  { id: "5", label: "Join overlap analysis via sampled FK/PK joins" },
];

const VALIDATION_PHASES = [
  {
    id: "1",
    label: "Phase 1: EXPLAIN — catches syntax errors and unresolvable columns",
  },
  {
    id: "2",
    label: "Phase 2: Table Existence — SELECT * FROM {table} LIMIT 0 for each reference",
  },
  {
    id: "3",
    label: "Phase 3: Execution Sanity — SELECT FROM (sql) LIMIT 1 verifies non-zero results",
  },
  {
    id: "4",
    label: "Phase 4: Alignment Check — LLM validates question matches SQL intent",
  },
];

export function PreflightStage() {
  return (
    <div className="space-y-4">
      <ExpandableCard
        title="Fetch Space Config"
        icon={Globe}
        accentColor="border-l-blue-500"
      >
        <p className="text-sm text-muted-foreground">
          Fetches current Genie Space configuration via REST API, stores as
          config_snapshot.
        </p>
      </ExpandableCard>

      <ExpandableCard
        title="Collect UC Metadata & Data Profile"
        icon={Database}
        accentColor="border-l-indigo-500"
      >
        <AnimatedChecklist items={PROFILING_STEPS} />
        <div className="mt-4 rounded-lg border border-db-gray-border bg-db-gray-bg/50 p-3 text-sm text-muted-foreground">
          Why profile data? The LLM uses actual cardinality, value ranges, and
          distinct values to generate realistic benchmark questions with valid
          filter conditions.
        </div>
      </ExpandableCard>

      <ExpandableCard
        title="Generate Benchmarks"
        icon={BrainCircuit}
        accentColor="border-l-purple-500"
      >
        <div className="space-y-4">
          <div>
            <span className="mb-2 block text-sm font-medium">LLM Context</span>
            <div className="flex flex-wrap gap-2">
              {LLM_CONTEXT_BLOCKS.map((block) => (
                <Badge key={block} variant="secondary" className="font-mono text-xs">
                  {block}
                </Badge>
              ))}
            </div>
          </div>
          <DataModelCard
            title="Benchmark Data Model"
            fields={BENCHMARK_MODEL_FIELDS}
          />
          <p className="text-sm text-muted-foreground">
            Coverage gap filling targets uncovered assets. Benchmarks are split
            into train and held_out for evaluation.
          </p>
        </div>
      </ExpandableCard>

      <ExpandableCard
        title="Validate Benchmarks"
        icon={CheckCircle2}
        accentColor="border-l-green-500"
      >
        <AnimatedChecklist items={VALIDATION_PHASES} />
        <p className="mt-4 text-sm text-muted-foreground">
          If less than 5 valid benchmarks remain, regeneration is triggered.
        </p>
      </ExpandableCard>

      <ExpandableCard
        title="Load Human Feedback & Setup Experiment"
        icon={UserCheck}
        accentColor="border-l-amber-500"
      >
        <p className="text-sm text-muted-foreground">
          Corrections from prior sessions: benchmark SQL fixes (genie_correct
          verdicts), judge overrides, and quarantines (excluding questions from
          accuracy). Creates or resolves the MLflow experiment, registers the
          evaluation dataset in Unity Catalog, and sets up the initial
          instruction version.
        </p>
      </ExpandableCard>
    </div>
  );
}
