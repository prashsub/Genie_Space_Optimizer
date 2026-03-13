"use client";

import { motion } from "motion/react";
import { CheckCheck, Award, Rocket, Lock } from "lucide-react";
import { Card, CardContent } from "@/components/ui/card";
import { StageScreen } from "../StageScreen";
import {
  REVIEW_SESSION_FIELDS,
  FEEDBACK_INGESTION_STEPS,
} from "../data";

export function FinalizeDeployStage() {
  const visual = (
    <div className="grid grid-cols-1 gap-4 md:grid-cols-3">
      <motion.div
        initial={{ opacity: 0, y: 8 }}
        animate={{ opacity: 1, y: 0 }}
        transition={{ delay: 0, duration: 0.25 }}
      >
        <Card className="h-full overflow-hidden">
          <CardContent className="flex flex-col items-center p-6 text-center">
            <div className="mb-3 flex gap-2">
              <div className="flex h-10 w-10 items-center justify-center rounded-full bg-db-green/20">
                <CheckCheck className="h-5 w-5 text-db-green" />
              </div>
              <div className="flex h-10 w-10 items-center justify-center rounded-full bg-db-green/20">
                <CheckCheck className="h-5 w-5 text-db-green" />
              </div>
            </div>
            <h3 className="font-semibold">Repeatability Test</h3>
            <p className="mt-1 text-sm text-muted-foreground">2 passes compared</p>
          </CardContent>
        </Card>
      </motion.div>

      <motion.div
        initial={{ opacity: 0, y: 8 }}
        animate={{ opacity: 1, y: 0 }}
        transition={{ delay: 0.08, duration: 0.25 }}
      >
        <Card className="h-full overflow-hidden">
          <CardContent className="flex flex-col items-center p-6 text-center">
            <div className="mb-3 flex h-12 w-12 items-center justify-center rounded-full bg-amber-100">
              <Award className="h-6 w-6 text-amber-600" />
            </div>
            <h3 className="font-semibold">Champion Promotion</h3>
            <p className="mt-1 text-sm text-muted-foreground">Best model promoted</p>
          </CardContent>
        </Card>
      </motion.div>

      <motion.div
        initial={{ opacity: 0, y: 8 }}
        animate={{ opacity: 1, y: 0 }}
        transition={{ delay: 0.16, duration: 0.25 }}
      >
        <Card className="h-full overflow-hidden">
          <CardContent className="flex flex-col items-center p-6 text-center">
            <div className="mb-3 flex items-center gap-2">
              <Rocket className="h-6 w-6 text-db-blue" />
              <Lock className="h-4 w-4 text-muted-foreground" />
            </div>
            <h3 className="font-semibold">Deploy (optional)</h3>
            <p className="mt-1 text-sm text-muted-foreground">Source → Target</p>
            <p className="mt-0.5 flex items-center gap-1 text-xs text-muted-foreground">
              <Lock className="h-3 w-3" /> approval gate
            </p>
          </CardContent>
        </Card>
      </motion.div>
    </div>
  );

  const explanation = (
    <p>
      The final stage validates that improvements are repeatable, promotes the best configuration to &quot;champion&quot; status, creates a human review session, and optionally deploys to a target workspace.
    </p>
  );

  const learnMore = [
    {
      id: "repeatability",
      title: "Repeatability Testing",
      content: (
        <div className="space-y-2 text-sm">
          <p>Runs 2 passes; compares SQL hashes per benchmark. REPEATABILITY_TARGET=90%.</p>
        </div>
      ),
    },
    {
      id: "champion-promotion",
      title: "Champion Promotion",
      content: (
        <div className="space-y-2 text-sm">
          <p><code className="rounded bg-db-gray-bg px-1.5 py-0.5">promote_best_model()</code> selects the iteration with highest overall_accuracy. Sets MLflow alias <code className="rounded bg-db-gray-bg px-1.5 py-0.5">champion</code>.</p>
        </div>
      ),
    },
    {
      id: "uc-model-registration",
      title: "UC Model Registration",
      content: (
        <div className="space-y-2 text-sm">
          <p>Guarded by <code className="rounded bg-db-gray-bg px-1.5 py-0.5">ENABLE_UC_MODEL_REGISTRATION</code>. Uses competitive promotion: new metrics must beat existing champion to update the alias.</p>
        </div>
      ),
    },
    {
      id: "terminal-status",
      title: "Terminal Status",
      content: (
        <div className="space-y-2 text-sm">
          <p>CONVERGED / MAX_ITERATIONS / STALLED / FAILED. FINALIZE_TIMEOUT_SECONDS=6600s (~110 min).</p>
        </div>
      ),
    },
    {
      id: "deployment",
      title: "Deployment (optional)",
      content: (
        <div className="space-y-2 text-sm">
          <p>Source workspace → approval gate (model tag check) → target workspace. Per-space jobs.</p>
        </div>
      ),
    },
    {
      id: "human-review-session",
      title: "Human Review Session",
      content: (
        <div className="space-y-2 text-sm">
          <p>MLflow labeling session. 3 review schema fields:</p>
          <ul className="list-inside list-disc space-y-1">
            {REVIEW_SESSION_FIELDS.map((f) => (
              <li key={f.name}>
                <strong>{f.name}</strong> ({f.type}): {f.label}
                {f.options && ` — ${f.options.join(" | ")}`}
              </li>
            ))}
          </ul>
        </div>
      ),
    },
    {
      id: "feedback-ingestion",
      title: "Feedback Ingestion",
      content: (
        <ol className="list-inside list-decimal space-y-2 text-sm">
          {FEEDBACK_INGESTION_STEPS.map((s) => (
            <li key={s.step}>
              <strong>{s.title}:</strong> {s.description}
            </li>
          ))}
        </ol>
      ),
    },
    {
      id: "ui-surfaces",
      title: "UI Surfaces",
      content: (
        <div className="space-y-2 text-sm">
          <p>ReviewBanner, SpaceCard badge, InsightTabs, Suggestions panel.</p>
        </div>
      ),
    },
  ];

  return (
    <StageScreen
      title="Finalize & Deploy"
      subtitle="Test, promote, and optionally deploy"
      pipelineGroup="finalize"
      visual={visual}
      explanation={explanation}
      learnMore={learnMore}
    />
  );
}
