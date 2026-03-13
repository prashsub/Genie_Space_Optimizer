"use client";

import {
  RefreshCw,
  UserCheck,
  Award,
  Database,
  Flag,
} from "lucide-react";
import { ExpandableCard } from "../shared/ExpandableCard";

const REPEATABILITY_CLASSIFICATIONS = [
  { name: "IDENTICAL", value: "100%" },
  { name: "SIGNIFICANT_VARIANCE", value: "-" },
  { name: "CRITICAL_VARIANCE", value: "-" },
];

export function FinalizeStage() {
  return (
    <div className="space-y-4">
      <ExpandableCard
        title="Repeatability Testing"
        icon={RefreshCw}
        accentColor="border-l-blue-500"
      >
        <div className="space-y-4 text-sm">
          <p>
            Runs 2 full evaluation passes. For each benchmark, collects SQL
            hashes from both passes. Computes pct = (most_common_hash_count /
            total_hashes) × 100 per question. Final repeatability = average.
            Must meet REPEATABILITY_TARGET = 90%.
          </p>
          <div className="flex flex-wrap gap-2">
            {REPEATABILITY_CLASSIFICATIONS.map(({ name, value }) => (
              <span
                key={name}
                className="rounded border border-db-gray-border bg-db-gray-bg px-2 py-1 text-xs font-medium"
              >
                {name} ({value})
              </span>
            ))}
          </div>
        </div>
      </ExpandableCard>

      <ExpandableCard
        title="Human Review Session"
        icon={UserCheck}
        accentColor="border-l-amber-500"
      >
        <p className="text-sm">
          Creates an MLflow labeling session (create_labeling_session) populated
          with evaluation traces — especially failures and regressions. Up to
          100 traces, flagged traces get priority ordering. Returns a session URL
          for the Review App.
        </p>
      </ExpandableCard>

      <ExpandableCard
        title="Champion Promotion"
        icon={Award}
        accentColor="border-l-yellow-500"
      >
        <p className="text-sm">
          promote_best_model() finds the full-eval iteration with highest
          overall_accuracy. Calls mlflow.set_logged_model_alias(model_id,
          alias='champion'). Updates run with best_iteration,
          best_accuracy, best_model_id.
        </p>
      </ExpandableCard>

      <ExpandableCard
        title="UC Model Registration"
        icon={Database}
        accentColor="border-l-green-500"
      >
        <p className="text-sm">
          Guarded by ENABLE_UC_MODEL_REGISTRATION. Saves pyfunc model with:
          space config, UC metadata, benchmark summary. Promotes to @champion
          alias only if new metrics beat existing champion (compares
          result_correctness and overall judge average).
        </p>
      </ExpandableCard>

      <ExpandableCard
        title="Terminal Status"
        icon={Flag}
        accentColor="border-l-gray-500"
      >
        <p className="text-sm">
          Decision: CONVERGED (all thresholds met), MAX_ITERATIONS (reached
          limit), STALLED (everything else), FAILED (error).
          FINALIZE_TIMEOUT_SECONDS = 6600s (~110 min) with heartbeat every 30s.
        </p>
      </ExpandableCard>
    </div>
  );
}
