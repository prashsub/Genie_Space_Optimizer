"use client";

import type { ReactNode } from "react";
import { ArrowDown } from "lucide-react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { StageScreen } from "../StageScreen";
import { DataModelCard } from "../shared/DataModelCard";
import {
  FICTIONAL_EXAMPLE,
  FAILURE_TAXONOMY,
  PERSISTENT_FAILURE_CLASSIFICATIONS,
  ESCALATION_TYPES,
  CLUSTER_IMPACT_WEIGHTS,
} from "../data";
import { cn } from "@/lib/utils";

const LEVER_COLORS: Record<number, string> = {
  0: "bg-gray-100 text-gray-800 border-gray-200 dark:bg-gray-800 dark:text-gray-200 dark:border-gray-700",
  1: "bg-blue-100 text-blue-800 border-blue-200 dark:bg-blue-900/30 dark:text-blue-200 dark:border-blue-800",
  2: "bg-indigo-100 text-indigo-800 border-indigo-200 dark:bg-indigo-900/30 dark:text-indigo-200 dark:border-indigo-800",
  3: "bg-violet-100 text-violet-800 border-violet-200 dark:bg-violet-900/30 dark:text-violet-200 dark:border-violet-800",
  4: "bg-emerald-100 text-emerald-800 border-emerald-200 dark:bg-emerald-900/30 dark:text-emerald-200 dark:border-emerald-800",
  5: "bg-amber-100 text-amber-800 border-amber-200 dark:bg-amber-900/30 dark:text-amber-200 dark:border-amber-800",
};

function FailureTriageVisual() {
  const { cluster, benchmark } = FICTIONAL_EXAMPLE;
  const questionBadges = [
    benchmark.question,
    ...cluster.affectedQuestions.slice(1, 3),
  ];

  const clusterCards = [
    { id: "C001", rootCause: "wrong_column", count: 3 },
    { id: "C002", rootCause: "missing_join_spec", count: 2 },
  ];

  return (
    <div className="space-y-4">
      <Card>
        <CardHeader className="py-3">
          <CardTitle className="text-sm font-medium">
            Failing questions
          </CardTitle>
        </CardHeader>
        <CardContent className="flex flex-wrap gap-2 pt-0">
          {questionBadges.map((q, i) => (
            <Badge key={i} variant="outline" className="text-xs font-normal">
              {q.length > 50 ? `${q.slice(0, 50)}…` : q}
            </Badge>
          ))}
        </CardContent>
      </Card>

      <div className="flex justify-center">
        <ArrowDown className="h-5 w-5 text-muted-foreground" />
      </div>

      <div className="grid grid-cols-1 gap-2 sm:grid-cols-2">
        {clusterCards.map((c) => (
          <Card
            key={c.id}
            className={cn(
              "border-l-4",
              c.rootCause === "wrong_column"
                ? "border-l-blue-500"
                : "border-l-emerald-500"
            )}
          >
            <CardContent className="py-3">
              <Badge variant="secondary" className="font-mono">
                {c.id}: {c.rootCause} ({c.count} questions)
              </Badge>
            </CardContent>
          </Card>
        ))}
      </div>

      <Card>
        <CardHeader className="py-3">
          <CardTitle className="text-sm font-medium">Impact</CardTitle>
        </CardHeader>
        <CardContent className="pt-0">
          <code className="rounded bg-muted px-2 py-1 text-xs">
            question_count × causal_weight × severity_weight × fixability
          </code>
        </CardContent>
      </Card>
    </div>
  );
}

export function FailureAnalysisStage() {
  const learnMore: { id: string; title: string; content: ReactNode }[] = [
    {
      id: "root-cause-resolution",
      title: "Root-Cause Resolution",
      content: (
        <ol className="list-inside list-decimal space-y-2">
          <li>ASI metadata — if asi_failure_type present and not &quot;other&quot;</li>
          <li>Rationale pattern — keyword matching on judge rationale</li>
          <li>SQL diff classification — compare expected vs generated SQL</li>
        </ol>
      ),
    },
    {
      id: "cluster-ranking",
      title: "Cluster Ranking",
      content: (
        <div className="space-y-4">
          <p className="font-mono text-sm">
            impact = question_count × causal_weight × severity_weight ×
            fixability
          </p>
          <div className="grid gap-4 sm:grid-cols-2">
            <div>
              <p className="mb-2 text-sm font-medium">Causal weights (judge)</p>
              <ul className="space-y-1 text-sm">
                {Object.entries(CLUSTER_IMPACT_WEIGHTS.causal).map(
                  ([k, v]) =>
                    v >= 1 && (
                      <li key={k}>
                        <code>{k}</code>: {v}
                      </li>
                    )
                )}
              </ul>
            </div>
            <div>
              <p className="mb-2 text-sm font-medium">Fixability</p>
              <ul className="space-y-1 text-sm">
                <li>
                  With counterfactual:{" "}
                  {CLUSTER_IMPACT_WEIGHTS.fixability.withCounterfactual}
                </li>
                <li>Without: {CLUSTER_IMPACT_WEIGHTS.fixability.without}</li>
              </ul>
            </div>
          </div>
        </div>
      ),
    },
    {
      id: "adaptive-strategist",
      title: "Adaptive Strategist",
      content: (
        <div className="space-y-4">
          <p className="text-sm">
            13 context blocks passed to the LLM strategist:
          </p>
          <div className="flex flex-wrap gap-2">
            {[
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
            ].map((block) => (
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
      ),
    },
    {
      id: "failure-taxonomy",
      title: "Failure Taxonomy",
      content: (
        <div className="space-y-2">
          <p className="text-sm">
            22 failure types mapped to levers, color-coded:
          </p>
          <div className="flex flex-wrap gap-2">
            {FAILURE_TAXONOMY.map((ft) => (
              <Badge
                key={ft.name}
                variant="secondary"
                className={cn(
                  "border",
                  LEVER_COLORS[ft.lever] ?? "border-gray-300"
                )}
              >
                {ft.name} (L{ft.lever})
              </Badge>
            ))}
          </div>
        </div>
      ),
    },
    {
      id: "persistent-failure-detection",
      title: "Persistent Failure Detection",
      content: (
        <ul className="space-y-3">
          {PERSISTENT_FAILURE_CLASSIFICATIONS.map((c) => (
            <li
              key={c.name}
              className="rounded-lg border border-db-gray-border p-3"
            >
              <div className="flex items-center gap-2">
                <Badge
                  variant="outline"
                  className={
                    c.color === "amber"
                      ? "border-amber-300 bg-amber-50 dark:bg-amber-950/30"
                      : c.color === "orange"
                        ? "border-orange-300 bg-orange-50 dark:bg-orange-950/30"
                        : "border-red-300 bg-red-50 dark:bg-red-950/30"
                  }
                >
                  {c.name}
                </Badge>
              </div>
              <p className="mt-1 font-mono text-xs text-muted-foreground">
                {c.condition}
              </p>
              <p className="mt-1 text-sm">{c.meaning}</p>
            </li>
          ))}
        </ul>
      ),
    },
    {
      id: "arbiter-corrections",
      title: "Arbiter Corrections Pipeline",
      content: (
        <div className="space-y-4">
          <div>
            <p className="font-medium">Phase 1: genie_correct confirmations</p>
            <p className="text-sm text-muted-foreground">
              Threshold: 2 evaluations must say genie_correct before
              auto-correcting benchmark SQL.
            </p>
          </div>
          <div>
            <p className="font-medium">Phase 2: neither_correct repair/quarantine</p>
            <p className="text-sm text-muted-foreground">
              Repair threshold: 2. Quarantine threshold: 3 consecutive
              neither_correct verdicts.
            </p>
          </div>
        </div>
      ),
    },
    {
      id: "escalation-flagging",
      title: "Escalation & Flagging",
      content: (
        <div className="space-y-4">
          <div>
            <p className="mb-2 font-medium">3 escalation types</p>
            <ul className="space-y-2">
              {ESCALATION_TYPES.map((e) => (
                <li key={e.type} className="rounded border p-3 text-sm">
                  <code className="font-semibold">{e.type}</code>
                  <p className="mt-1 text-muted-foreground">
                    Trigger: {e.trigger}
                  </p>
                  <p className="mt-1">Action: {e.action}</p>
                </li>
              ))}
            </ul>
          </div>
          <p className="text-sm">
            End-of-run sweep flags persistent failures. High-risk patches are
            queued for human approval.
          </p>
        </div>
      ),
    },
  ];

  return (
    <StageScreen
      title="Failure Analysis"
      subtitle="Diagnose and cluster root causes"
      pipelineGroup="leverLoop"
      visual={<FailureTriageVisual />}
      explanation={
        <>
          After evaluation, the system extracts every failing question and
          diagnoses why it failed using a 3-tier root-cause resolution cascade.
          Failures with the same root cause and blamed objects form clusters,
          which are ranked by impact to prioritize what to fix first.
        </>
      }
      learnMore={learnMore}
    />
  );
}
