"use client";

import {
  Search,
  Layers,
  ArrowUpDown,
  BrainCircuit,
  Lightbulb,
  Code,
  Wrench,
  ShieldCheck,
  RotateCcw,
  BookOpen,
  CircleDot,
} from "lucide-react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Tabs, TabsList, TabsTrigger, TabsContent } from "@/components/ui/tabs";
import {
  Tooltip,
  TooltipTrigger,
  TooltipContent,
  TooltipProvider,
} from "@/components/ui/tooltip";
import { Separator } from "@/components/ui/separator";
import { cn } from "@/lib/utils";
import { ExpandableCard } from "../shared/ExpandableCard";
import { DataModelCard } from "../shared/DataModelCard";
import { GateFunnel } from "../shared/GateFunnel";
import { LoopDiagram } from "../shared/LoopDiagram";
import {
  LEVERS,
  GATE_DEFINITIONS,
  CONVERGENCE_CONDITIONS,
  CLUSTER_IMPACT_WEIGHTS,
} from "../data";

const LOOP_STEPS = [
  { label: "Extract Failures", icon: Search },
  { label: "Cluster", icon: Layers },
  { label: "Rank", icon: ArrowUpDown },
  { label: "Strategize (LLM)", icon: BrainCircuit },
  { label: "Generate Proposals", icon: Lightbulb },
  { label: "Convert to Patches", icon: Code },
  { label: "Apply Patches", icon: Wrench },
  { label: "3-Gate Eval", icon: ShieldCheck },
  { label: "Accept/Rollback", icon: RotateCcw },
  { label: "Reflect", icon: BookOpen },
  { label: "Check Convergence", icon: CircleDot },
];

const RESOLUTION_CASCADE = [
  { step: 1, description: "ASI metadata — if asi_failure_type present and not \"other\"", resolution: "asi_metadata" },
  { step: 2, description: "Rationale pattern — keyword matching on judge rationale", resolution: "rationale_pattern" },
  { step: 3, description: "SQL diff classification — compare expected vs generated SQL", resolution: "sql_diff" },
];

const CONTEXT_BLOCKS = [
  "progress_summary",
  "priority_analysis",
  "reflection_history",
  "proven_patterns",
  "persistent_failures",
  "human_suggestions",
  "schema",
  "failure_clusters",
  "soft_signal_clusters",
  "structured_metadata",
  "join_specifications",
  "current_instructions",
  "existing_example_sqls",
];

const CAUSAL_TOP_4 = [
  ["schema_accuracy", 4.0],
  ["logical_accuracy", 3.0],
  ["completeness", 1.5],
  ["result_correctness", 1.0],
] as const;

function getSeverityTop5(weights: Record<string, number>) {
  return Object.entries(weights)
    .sort(([, a], [, b]) => b - a)
    .slice(0, 5);
}

export function LeverLoopStage() {
  const severityTop5 = getSeverityTop5(CLUSTER_IMPACT_WEIGHTS.severity);

  return (
    <TooltipProvider>
      <div className="space-y-6">
        <div>
          <h3 className="mb-3 text-lg font-semibold">The Iteration Cycle</h3>
          <div className="overflow-x-auto pb-2">
            <LoopDiagram steps={LOOP_STEPS} className="min-w-max" />
          </div>
        </div>

        <ExpandableCard
          title="Failure Extraction & Clustering"
          accentColor="border-l-red-500"
        >
          <div className="space-y-4">
            <p className="text-sm">
              Questions where any judge verdict is &quot;no&quot; (after arbiter
              adjustment) are extracted as failures.
            </p>
            <div className="space-y-2">
              <p className="text-sm font-medium">Root-cause resolution cascade</p>
              <ol className="list-inside list-decimal space-y-2 text-sm">
                {RESOLUTION_CASCADE.map(({ step, description, resolution }) => (
                  <li key={step} className="flex flex-wrap items-center gap-2">
                    <span>{description}</span>
                    <span className="text-muted-foreground">→</span>
                    <Badge variant="secondary">{resolution}</Badge>
                  </li>
                ))}
              </ol>
            </div>
            <p className="text-sm">
              Cluster key:{" "}
              <code className="rounded bg-muted px-1.5 py-0.5 font-mono text-xs">
                (dominant_root_cause, blame_set_str)
              </code>
            </p>
            <DataModelCard
              title="Failure Cluster"
              fields={[
                { name: "cluster_id", type: "string" },
                { name: "root_cause", type: "string" },
                { name: "question_ids", type: "string[]" },
                {
                  name: "confidence",
                  type: "number",
                  example: "min(0.9, 0.5 + 0.1 × count)",
                },
                { name: "asi_blame_set", type: "string[]" },
                { name: "asi_counterfactual_fixes", type: "string[]" },
              ]}
            />
          </div>
        </ExpandableCard>

        <ExpandableCard
          title="Cluster Ranking"
          accentColor="border-l-orange-500"
        >
          <div className="space-y-4">
            <pre className="overflow-x-auto rounded-lg border border-db-gray-border bg-db-gray-bg p-4 text-sm">
              impact = question_count × causal_weight × severity_weight ×
              fixability
            </pre>
            <div className="grid gap-4 sm:grid-cols-3">
              <div className="space-y-2">
                <p className="text-sm font-medium">
                  Causal weights (top 4 by judge)
                </p>
                <div className="rounded-lg border border-db-gray-border">
                  <table className="w-full text-sm">
                    <thead>
                      <tr className="border-b border-db-gray-border bg-db-gray-bg">
                        <th className="px-3 py-2 text-left font-medium">Judge</th>
                        <th className="px-3 py-2 text-right font-medium">
                          Weight
                        </th>
                      </tr>
                    </thead>
                    <tbody>
                      {CAUSAL_TOP_4.map(([judge, weight]) => (
                        <tr
                          key={judge}
                          className="border-b border-db-gray-border last:border-0"
                        >
                          <td className="px-3 py-2 font-mono">{judge}</td>
                          <td className="px-3 py-2 text-right">{weight}</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              </div>
              <div className="space-y-2">
                <p className="text-sm font-medium">
                  Severity weights (top 5 by failure type)
                </p>
                <div className="rounded-lg border border-db-gray-border">
                  <table className="w-full text-sm">
                    <thead>
                      <tr className="border-b border-db-gray-border bg-db-gray-bg">
                        <th className="px-3 py-2 text-left font-medium">Type</th>
                        <th className="px-3 py-2 text-right font-medium">
                          Weight
                        </th>
                      </tr>
                    </thead>
                    <tbody>
                      {severityTop5.map(([type, weight]) => (
                        <tr
                          key={type}
                          className="border-b border-db-gray-border last:border-0"
                        >
                          <td className="px-3 py-2 font-mono">{type}</td>
                          <td className="px-3 py-2 text-right">{weight}</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              </div>
              <div className="space-y-2">
                <p className="text-sm font-medium">Fixability</p>
                <div className="rounded-lg border border-db-gray-border">
                  <table className="w-full text-sm">
                    <thead>
                      <tr className="border-b border-db-gray-border bg-db-gray-bg">
                        <th className="px-3 py-2 text-left font-medium">
                          Condition
                        </th>
                        <th className="px-3 py-2 text-right font-medium">
                          Weight
                        </th>
                      </tr>
                    </thead>
                    <tbody>
                      <tr className="border-b border-db-gray-border">
                        <td className="px-3 py-2">With counterfactual</td>
                        <td className="px-3 py-2 text-right">
                          {
                            CLUSTER_IMPACT_WEIGHTS.fixability.withCounterfactual
                          }
                        </td>
                      </tr>
                      <tr>
                        <td className="px-3 py-2">Without</td>
                        <td className="px-3 py-2 text-right">
                          {CLUSTER_IMPACT_WEIGHTS.fixability.without}
                        </td>
                      </tr>
                    </tbody>
                  </table>
                </div>
              </div>
            </div>
          </div>
        </ExpandableCard>

        <ExpandableCard
          title="Adaptive Strategist"
          accentColor="border-l-purple-500"
        >
          <div className="space-y-4">
            <p className="text-sm">
              An LLM receives the full optimization context and produces exactly
              one action group per iteration.
            </p>
            <div className="flex flex-wrap gap-2">
              {CONTEXT_BLOCKS.map((block) => (
                <Badge key={block} variant="outline">
                  {block}
                </Badge>
              ))}
            </div>
            <DataModelCard
              title="Action Group"
              fields={[
                { name: "id", type: "string", example: '"AG1"' },
                { name: "root_cause_summary", type: "string" },
                { name: "source_cluster_ids", type: "string[]" },
                { name: "lever_directives", type: "object" },
                { name: "coordination_notes", type: "string" },
                {
                  name: "escalation",
                  type: "string?",
                  example: "null | remove_tvf | gt_repair | flag_for_review",
                },
              ]}
            />
          </div>
        </ExpandableCard>

        <div>
          <h3 className="mb-3 text-lg font-semibold">The 5 Levers</h3>
          <Tabs defaultValue={`lever-${LEVERS[0]?.number ?? 1}`}>
            <TabsList className="mb-4 flex h-auto flex-wrap gap-2">
              {LEVERS.map((lever) => (
                <Tooltip key={lever.number} delayDuration={0}>
                  <TooltipTrigger asChild>
                    <TabsTrigger
                      value={`lever-${lever.number}`}
                      className="flex flex-col items-start gap-1 px-4 py-2 text-left"
                    >
                      <span className="font-medium">Lever {lever.number}</span>
                      <span className="text-xs font-normal text-muted-foreground">
                        {lever.name}
                      </span>
                    </TabsTrigger>
                  </TooltipTrigger>
                  <TooltipContent side="bottom" className="max-w-xs">
                    {lever.description}
                  </TooltipContent>
                </Tooltip>
              ))}
            </TabsList>
            {LEVERS.map((lever) => (
              <TabsContent
                key={lever.number}
                value={`lever-${lever.number}`}
                className="mt-0"
              >
                <Card>
                  <CardHeader>
                    <CardTitle className="text-base">
                      Lever {lever.number}: {lever.name}
                    </CardTitle>
                  </CardHeader>
                  <CardContent className="space-y-4">
                    <p className="text-sm text-muted-foreground">
                      {lever.description}
                    </p>
                    {lever.ownedSections.length > 0 && (
                      <div>
                        <p className="mb-2 text-sm font-medium">
                          Owned sections
                        </p>
                        <div className="flex flex-wrap gap-1">
                          {lever.ownedSections.map((section) => (
                            <Badge key={section} variant="secondary">
                              {section}
                            </Badge>
                          ))}
                        </div>
                      </div>
                    )}
                    <div>
                      <p className="mb-2 text-sm font-medium">Patch types</p>
                      <div className="flex flex-wrap gap-1">
                        {lever.patchTypes.map((pt) => (
                          <Badge key={pt} variant="outline">
                            {pt}
                          </Badge>
                        ))}
                      </div>
                    </div>
                    <div>
                      <p className="mb-2 text-sm font-medium">
                        Associated failure types
                      </p>
                      <div className="flex flex-wrap gap-1">
                        {lever.failureTypes.map((ft) => (
                          <Badge
                            key={ft}
                            variant="secondary"
                            className={cn(
                              "border-red-200 bg-red-50 text-red-800 dark:border-red-900/50 dark:bg-red-950/30 dark:text-red-300"
                            )}
                          >
                            {ft}
                          </Badge>
                        ))}
                      </div>
                    </div>
                  </CardContent>
                </Card>
              </TabsContent>
            ))}
          </Tabs>
        </div>

        <ExpandableCard
          title="3-Gate Evaluation"
          accentColor="border-l-green-500"
          defaultOpen
        >
          <div className="space-y-4">
            <div className="flex justify-center">
              <GateFunnel />
            </div>
            <Separator />
            <div className="space-y-4">
              {GATE_DEFINITIONS.map((gate) => (
                <div
                  key={gate.name}
                  className="rounded-lg border border-db-gray-border bg-db-gray-bg/50 p-4"
                >
                  <p className="font-semibold">{gate.name}</p>
                  <dl className="mt-2 grid gap-2 text-sm sm:grid-cols-[auto_1fr]">
                    <dt className="text-muted-foreground">
                      Question selection:
                    </dt>
                    <dd>{gate.questionSelection}</dd>
                    <dt className="text-muted-foreground">Pass criteria:</dt>
                    <dd>{gate.passCriteria}</dd>
                    <dt className="text-muted-foreground">Tolerance:</dt>
                    <dd className="font-mono">{gate.tolerance}</dd>
                    <dt className="text-muted-foreground">Description:</dt>
                    <dd>{gate.description}</dd>
                  </dl>
                </div>
              ))}
            </div>
          </div>
        </ExpandableCard>

        <ExpandableCard
          title="Reflection Buffer"
          accentColor="border-l-teal-500"
        >
          <div className="space-y-4">
            <DataModelCard
              title="Reflection Entry"
              fields={[
                { name: "iteration", type: "number" },
                { name: "ag_id", type: "string" },
                { name: "accepted", type: "boolean" },
                { name: "levers", type: "number[]" },
                { name: "accuracy_delta", type: "number" },
                { name: "rollback_reason", type: "string?" },
                { name: "do_not_retry", type: "string[]" },
                { name: "still_failing", type: "string[]" },
              ]}
            />
            <p
              className={cn(
                "rounded-lg border border-teal-200 bg-teal-50 p-4 text-sm dark:border-teal-900/50 dark:bg-teal-950/30"
              )}
            >
              The reflection buffer prevents the optimizer from repeating failed
              strategies — it&apos;s the institutional memory of the
              optimization run.
            </p>
          </div>
        </ExpandableCard>

        <ExpandableCard
          title="Convergence Decision Tree"
          accentColor="border-l-gray-500"
        >
          <div className="space-y-3">
            {CONVERGENCE_CONDITIONS.map((condition) => (
              <div
                key={condition.name}
                className="rounded-lg border border-db-gray-border p-3"
              >
                <div className="flex flex-wrap items-center gap-2">
                  <span className="font-semibold">{condition.name}</span>
                  <span className="text-muted-foreground">→</span>
                  <Badge variant="secondary">{condition.resultStatus}</Badge>
                </div>
                <p className="mt-1 font-mono text-xs text-muted-foreground">
                  {condition.formula}
                </p>
                <p className="mt-1 text-sm">{condition.description}</p>
              </div>
            ))}
          </div>
        </ExpandableCard>
      </div>
    </TooltipProvider>
  );
}
