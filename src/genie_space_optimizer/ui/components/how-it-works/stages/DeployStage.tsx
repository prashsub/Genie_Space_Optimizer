"use client";

import { ArrowRight, Lock } from "lucide-react";
import { ExpandableCard } from "../shared/ExpandableCard";

const DEPLOY_STEPS = [
  "Only runs if deploy_target was set in TriggerRequest",
  "Loads space_config.json from UC registered model artifact",
  "Checks UC model tag for approval status — if not approved, task fails",
  "Creates WorkspaceClient for target workspace",
  "Calls patch_space_config() to apply optimized config",
  "Per-space deployment jobs: gso-deploy-{space_id} with tasks: Approval_Check → cross_env_deploy",
];

export function DeployStage() {
  return (
    <div className="space-y-6">
      <div className="flex items-center justify-center gap-2 py-4">
        <div className="rounded-lg border-2 border-blue-500 bg-blue-50 px-6 py-4">
          <span className="text-sm font-semibold text-blue-800">
            Source Workspace
          </span>
        </div>
        <ArrowRight className="h-5 w-5 shrink-0 text-muted-foreground" />
        <div className="flex flex-col items-center gap-1 rounded-lg border-2 border-amber-400 bg-amber-50 px-6 py-4">
          <Lock className="h-5 w-5 text-amber-600" />
          <span className="text-sm font-semibold text-amber-800">
            Approval Gate
          </span>
        </div>
        <ArrowRight className="h-5 w-5 shrink-0 text-muted-foreground" />
        <div className="rounded-lg border-2 border-green-500 bg-green-50 px-6 py-4">
          <span className="text-sm font-semibold text-green-800">
            Target Workspace
          </span>
        </div>
      </div>

      <ExpandableCard
        title="How Deployment Works"
        accentColor="border-l-db-blue"
      >
        <ol className="list-inside list-decimal space-y-2 text-sm">
          {DEPLOY_STEPS.map((step, i) => (
            <li key={i}>{step}</li>
          ))}
        </ol>
      </ExpandableCard>
    </div>
  );
}
