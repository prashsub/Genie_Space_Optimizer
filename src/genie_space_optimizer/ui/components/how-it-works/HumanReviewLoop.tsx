"use client";

import { AlertTriangle, Scale, Flag, ClipboardList, RefreshCw, Monitor, ArrowDown } from "lucide-react";
import { Card, CardContent } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Separator } from "@/components/ui/separator";
import { cn } from "@/lib/utils";
import { ExpandableCard } from "./shared/ExpandableCard";
import { DataModelCard } from "./shared/DataModelCard";
import {
  PERSISTENT_FAILURE_CLASSIFICATIONS,
  ESCALATION_TYPES,
  REVIEW_SESSION_FIELDS,
  HIGH_RISK_PATCH_TYPES,
  FEEDBACK_INGESTION_STEPS,
} from "./data";

const FLAGGED_ITEM_FIELDS = [
  { name: "question_id", type: "string", example: '"q_001"' },
  { name: "question_text", type: "string", example: '"What is total revenue?"' },
  { name: "reason", type: "string", example: "ADDITIVE_LEVERS_EXHAUSTED" },
  { name: "iterations_failed", type: "number", example: "3" },
  { name: "patches_tried", type: "string", example: "add_instruction, add_example_sql" },
];

export function HumanReviewLoop() {
  return (
      <section className="space-y-6">
        <div>
          <h2 className="text-2xl font-semibold">Persistent Failures & Human Review</h2>
          <p className="mt-1 text-muted-foreground">
            The optimizer identifies questions it cannot fix automatically, routes them for human
            review, and incorporates human feedback in subsequent runs — creating a virtuous
            learning cycle.
          </p>
        </div>

        <div className="space-y-4">
          <ExpandableCard
            title="Persistent Failure Detection"
            icon={AlertTriangle}
            accentColor="border-l-amber-500"
          >
            <p className="text-sm text-muted-foreground">
              Tracks verdict history across iterations. A question qualifies as &quot;persistent&quot; when
              it fails in ≥2 iterations.
            </p>
            <div className="mt-4 grid gap-3 sm:grid-cols-3">
              {PERSISTENT_FAILURE_CLASSIFICATIONS.map((item) => (
                <Card
                  key={item.name}
                  className={cn(
                    "border-l-4",
                    item.color === "amber" && "border-l-amber-500",
                    item.color === "orange" && "border-l-orange-500",
                    item.color === "red" && "border-l-red-500"
                  )}
                >
                  <CardContent className="pt-4">
                    <Badge
                      variant="outline"
                      className={cn(
                        "mb-2",
                        item.color === "amber" && "border-amber-500 text-amber-700 dark:text-amber-400",
                        item.color === "orange" && "border-orange-500 text-orange-700 dark:text-orange-400",
                        item.color === "red" && "border-red-500 text-red-700 dark:text-red-400"
                      )}
                    >
                      {item.name}
                    </Badge>
                    <p className="font-mono text-xs text-muted-foreground">{item.condition}</p>
                    <p className="mt-1 text-sm">{item.meaning}</p>
                  </CardContent>
                </Card>
              ))}
            </div>
            <p className="mt-4 text-sm text-muted-foreground">
              Each reflection entry includes still_failing, fixed_questions, and new_regressions —
              feeding the strategist&apos;s context.
            </p>
          </ExpandableCard>

          <ExpandableCard
            title="Arbiter Corrections Pipeline"
            icon={Scale}
            accentColor="border-l-blue-500"
          >
            <div className="space-y-4">
              <div>
                <h4 className="font-semibold">Phase 1: genie_correct Confirmations</h4>
                <ul className="mt-2 list-inside list-disc space-y-1 text-sm text-muted-foreground">
                  <li>
                    When arbiter says Genie is correct but ground-truth is wrong
                  </li>
                  <li>
                    Requires ≥2 independent evaluations before auto-correcting benchmark SQL
                  </li>
                  <li>
                    force_adopt_qids from escalation bypasses the threshold
                  </li>
                </ul>
              </div>
              <Separator />
              <div>
                <h4 className="font-semibold">Phase 2: neither_correct Repair / Quarantine</h4>
                <ul className="mt-2 list-inside list-disc space-y-1 text-sm text-muted-foreground">
                  <li>
                    After 2 neither_correct evals: LLM-assisted ground-truth repair attempted
                  </li>
                  <li>
                    After 3 consecutive neither_correct: question quarantined (excluded from
                    accuracy)
                  </li>
                  <li>Quarantined questions get timestamp and reason</li>
                </ul>
              </div>
            </div>
            <Card className="mt-4 border-blue-200 bg-blue-50 dark:border-blue-800 dark:bg-blue-950/40">
              <CardContent className="pt-4">
                <p className="text-sm">
                  The arbiter corrections pipeline is the optimizer&apos;s self-healing mechanism.
                </p>
              </CardContent>
            </Card>
          </ExpandableCard>

          <ExpandableCard
            title="Escalation & Flagging"
            icon={Flag}
            accentColor="border-l-red-500"
          >
            <div className="overflow-x-auto">
              <table className="w-full text-sm">
                <thead>
                  <tr className="border-b">
                    <th className="pb-2 pr-4 text-left font-medium">Type</th>
                    <th className="pb-2 pr-4 text-left font-medium">Trigger</th>
                    <th className="pb-2 text-left font-medium">Action</th>
                  </tr>
                </thead>
                <tbody>
                  {ESCALATION_TYPES.map((item) => (
                    <tr key={item.type} className="border-b last:border-0">
                      <td className="py-2 pr-4 font-mono text-xs">{item.type}</td>
                      <td className="py-2 pr-4 text-muted-foreground">{item.trigger}</td>
                      <td className="py-2">{item.action}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
            <div className="mt-4">
              <h4 className="font-semibold">Additional triggers</h4>
              <ul className="mt-2 list-inside list-disc space-y-1 text-sm text-muted-foreground">
                <li>
                  End-of-run persistent failure sweep: all PERSISTENT or ADDITIVE_LEVERS_EXHAUSTED
                  questions flagged
                </li>
                <li>
                  High-risk patch queuing: patches of type {HIGH_RISK_PATCH_TYPES.join(", ")} are
                  queued, not applied
                </li>
              </ul>
            </div>
            <div className="mt-4">
              <DataModelCard title="Flagged Item" fields={FLAGGED_ITEM_FIELDS} compact />
            </div>
          </ExpandableCard>

          <ExpandableCard
            title="Human Review Session (MLflow Labeling)"
            icon={ClipboardList}
            accentColor="border-l-purple-500"
          >
            <p className="text-sm text-muted-foreground">
              Created during finalization. Pre-populated with up to 100 evaluation traces, flagged
              traces get priority.
            </p>
            <p className="mt-2 text-sm">
              Session name format:{" "}
              <code className="rounded bg-muted px-1.5 py-0.5 font-mono text-xs">
                opt_{"{"}domain{"}"}_{"{"}run_id[:8]{"}"}_{"{"}timestamp{"}"}
              </code>
            </p>
            <div className="mt-4 overflow-x-auto">
              <table className="w-full text-sm">
                <thead>
                  <tr className="border-b">
                    <th className="pb-2 pr-4 text-left font-medium">Field</th>
                    <th className="pb-2 pr-4 text-left font-medium">Type</th>
                    <th className="pb-2 pr-4 text-left font-medium">Label</th>
                    <th className="pb-2 text-left font-medium">Options</th>
                  </tr>
                </thead>
                <tbody>
                  {REVIEW_SESSION_FIELDS.map((field) => (
                    <tr key={field.name} className="border-b last:border-0">
                      <td className="py-2 pr-4 font-mono text-xs">{field.name}</td>
                      <td className="py-2 pr-4 text-muted-foreground">{field.type}</td>
                      <td className="py-2 pr-4">{field.label}</td>
                      <td className="py-2 text-muted-foreground">
                        {field.options ? field.options.join(", ") : "—"}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </ExpandableCard>

          <ExpandableCard
            title="Feedback Ingestion (Cross-Run Learning)"
            icon={RefreshCw}
            accentColor="border-l-green-500"
          >
            <div className="flex flex-col gap-0">
              {FEEDBACK_INGESTION_STEPS.map((item, i) => (
                <div key={item.step} className="flex flex-col">
                  <div className="flex items-start gap-3">
                    <span className="flex h-6 w-6 shrink-0 items-center justify-center rounded-full bg-green-100 text-xs font-semibold text-green-800 dark:bg-green-900/50 dark:text-green-300">
                      {item.step}
                    </span>
                    <div>
                      <span className="font-semibold">{item.title}</span>
                      <p className="text-sm text-muted-foreground">{item.description}</p>
                    </div>
                  </div>
                  {i < FEEDBACK_INGESTION_STEPS.length - 1 && (
                    <div className="ml-3 mt-1 flex items-center gap-2">
                      <ArrowDown className="h-4 w-4 text-muted-foreground" />
                    </div>
                  )}
                </div>
              ))}
            </div>
            <Card className="mt-4 border-green-200 bg-green-50 dark:border-green-800 dark:bg-green-950/40">
              <CardContent className="pt-4">
                <p className="text-sm">
                  This creates a virtuous cycle: human reviewers fix the benchmarks the optimizer
                  couldn&apos;t handle, and the next run starts with a cleaner benchmark set.
                </p>
              </CardContent>
            </Card>
          </ExpandableCard>

          <ExpandableCard
            title="UI Surfaces"
            icon={Monitor}
            accentColor="border-l-teal-500"
          >
            <ul className="space-y-3 text-sm">
              <li>
                <span className="font-semibold">ReviewBanner:</span>{" "}
                <span className="text-muted-foreground">
                  Appears on completed runs when pending reviews exist. Shows counts by reason, links
                  to Review App.
                </span>
              </li>
              <li>
                <span className="font-semibold">SpaceCard badge:</span>{" "}
                <span className="text-muted-foreground">
                  Amber &apos;{`{N} review(s)`}&apos; badge on the dashboard.
                </span>
              </li>
              <li>
                <span className="font-semibold">InsightTabs:</span>{" "}
                <span className="text-muted-foreground">
                  Persistent/flagged questions get special badges. &apos;Persistent&apos; filter
                  available.
                </span>
              </li>
              <li>
                <span className="font-semibold">Suggestions panel:</span>{" "}
                <span className="text-muted-foreground">
                  Accept/Reject/Apply actions for METRIC_VIEW and FUNCTION proposals.
                </span>
              </li>
            </ul>
          </ExpandableCard>
        </div>
      </section>
  );
}
