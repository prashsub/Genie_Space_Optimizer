"use client";

import { ArrowRight } from "lucide-react";
import { Card, CardContent } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { cn } from "@/lib/utils";
import { ExpandableCard } from "../shared/ExpandableCard";
import { ScoreGauge } from "../shared/ScoreGauge";
import { DataModelCard } from "../shared/DataModelCard";
import { JUDGES, JUDGE_CATEGORY_COLORS, ASI_MODEL_FIELDS } from "../data";

const EVALUATION_STEPS = [
  "make_predict_fn() queries Genie Space for each benchmark",
  "mlflow.genai.evaluate() runs all 9 scorers",
  "Retry logic: up to 4 retries with 10s backoff, falls back to sequential",
  "Per-judge scores normalized from 0-1 to 0-100",
];

const FLOW_NODES = [
  "Benchmarks",
  "Genie Space API",
  "9 Judges",
  "Aggregated Scores",
];

const METHOD_BADGE_VARIANTS: Record<string, string> = {
  CODE: "bg-blue-100 text-blue-800 border-blue-200",
  LLM: "bg-indigo-100 text-indigo-800 border-indigo-200",
  CONDITIONAL_LLM: "bg-amber-100 text-amber-800 border-amber-200",
};

export function BaselineStage() {
  return (
    <div className="space-y-8">
      <section>
        <h3 className="mb-4 text-lg font-semibold">Evaluation Orchestration</h3>
        <div className="flex flex-wrap items-center gap-2">
          {FLOW_NODES.map((label, i) => (
            <div key={label} className="flex items-center gap-2">
              <div className="rounded-lg border border-db-gray-border bg-db-gray-bg px-3 py-2 text-sm font-medium">
                {label}
              </div>
              {i < FLOW_NODES.length - 1 && (
                <ArrowRight className="h-4 w-4 shrink-0 text-muted-foreground" />
              )}
            </div>
          ))}
        </div>
        <ol className="mt-6 grid list-none gap-3">
          {EVALUATION_STEPS.map((step, i) => (
            <li
              key={i}
              className="flex gap-3 rounded-lg border border-db-gray-border bg-white p-3"
            >
              <span className="flex h-6 w-6 shrink-0 items-center justify-center rounded-full bg-db-blue text-xs font-semibold text-white">
                {i + 1}
              </span>
              <span className="text-sm">{step}</span>
            </li>
          ))}
        </ol>
      </section>

      <section>
        <h3 className="mb-4 text-lg font-semibold">Judge Overview</h3>
        <div className="grid grid-cols-3 gap-3">
          {JUDGES.map((judge) => (
            <Card key={judge.name} className="overflow-hidden">
              <CardContent className="p-3">
                <div className="mb-2 flex items-center justify-between gap-2">
                  <span className="truncate text-sm font-semibold">
                    {judge.displayName}
                  </span>
                  <Badge
                    variant="outline"
                    className={cn(
                      "shrink-0 text-xs",
                      METHOD_BADGE_VARIANTS[judge.method] ??
                        "bg-gray-100 text-gray-800 border-gray-200"
                    )}
                  >
                    {judge.method.replace("_", " ")}
                  </Badge>
                </div>
                <ScoreGauge
                  value={judge.threshold >= 0 ? judge.threshold : 0}
                  label="Threshold"
                  threshold={judge.threshold >= 0 ? judge.threshold : undefined}
                  color={
                    judge.threshold >= 0 ? "bg-db-blue" : "bg-db-gray-border"
                  }
                />
              </CardContent>
            </Card>
          ))}
        </div>
      </section>

      <section>
        <ExpandableCard
          title="Arbiter-Adjusted Accuracy"
          subtitle="How a row counts as correct when arbiter disagrees with result_correctness"
          accentColor={JUDGE_CATEGORY_COLORS.arbiter ?? "border-l-amber-500"}
        >
          <div className="space-y-4 text-sm">
            <p>
              A row is correct if{" "}
              <code className="rounded bg-db-gray-bg px-1.5 py-0.5">
                result_correctness == &quot;yes&quot;
              </code>{" "}
              OR (
              <code className="rounded bg-db-gray-bg px-1.5 py-0.5">
                result_correctness == &quot;no&quot;
              </code>{" "}
              AND arbiter in{" "}
              <code className="rounded bg-db-gray-bg px-1.5 py-0.5">
                genie_correct
              </code>
              ,{" "}
              <code className="rounded bg-db-gray-bg px-1.5 py-0.5">
                both_correct
              </code>
              ).
            </p>
            <p>
              Quarantined, temporal-stale, and both_empty/genie_result_unavailable
              rows are excluded from the denominator.
            </p>
            <p>
              When arbiter says Genie correct: logical_accuracy,
              semantic_equivalence, completeness, and schema_accuracy also pass.
            </p>
          </div>
        </ExpandableCard>
      </section>

      <section>
        <ExpandableCard
          title="Actionable Side Information (ASI)"
          subtitle="Structured judge feedback that drives the optimization loop"
        >
          <div className="space-y-4">
            <DataModelCard title="ASI Model" fields={ASI_MODEL_FIELDS} />
            <p className="rounded-lg border border-db-blue/30 bg-db-blue/5 p-4 text-sm">
              ASI is what makes the optimization loop intelligent. Each judge
              doesn&apos;t just say pass/fail — it says why it failed and what
              metadata change would fix it.
            </p>
          </div>
        </ExpandableCard>
      </section>
    </div>
  );
}
