"use client";

import { motion } from "motion/react";
import { Settings2, BarChart3, Code, Link2, BookOpen } from "lucide-react";
import { Card, CardContent } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { StageScreen } from "../StageScreen";
import { LEVERS, FICTIONAL_EXAMPLE } from "../data";

const LEVER_ICONS = [Settings2, BarChart3, Code, Link2, BookOpen] as const;

export function LeversStage() {
  const visual = (
    <div className="grid grid-cols-2 gap-3 sm:grid-cols-2 md:grid-cols-3 xl:grid-cols-5">
      {LEVERS.map((lever, index) => {
        const Icon = LEVER_ICONS[lever.number - 1];
        return (
          <motion.div
            key={lever.number}
            initial={{ opacity: 0, scale: 0.95 }}
            animate={{ opacity: 1, scale: 1 }}
            transition={{ delay: index * 0.05, duration: 0.25 }}
          >
            <Card className="h-full overflow-hidden">
              <CardContent className="flex flex-col gap-2 p-4">
                <div className="flex items-center gap-2">
                  <div className="flex h-9 w-9 shrink-0 items-center justify-center rounded-lg bg-db-gray-bg">
                    <Icon className="h-4 w-4 text-muted-foreground" />
                  </div>
                  <div className="min-w-0">
                    <span className="text-xs text-muted-foreground">L{lever.number}</span>
                    <h3 className="truncate font-semibold">{lever.name}</h3>
                  </div>
                </div>
                <div className="mt-1 flex flex-wrap gap-1">
                  {lever.failureTypes.map((ft) => (
                    <Badge key={ft} variant="secondary" className="text-xs">
                      {ft}
                    </Badge>
                  ))}
                </div>
              </CardContent>
            </Card>
          </motion.div>
        );
      })}
    </div>
  );

  const explanation = (
    <p>
      Each lever is a specialized tool for a specific class of problem. The strategist decides which levers to pull based on the failure clusters. Levers operate on different parts of the Genie Space metadata — from column descriptions to join specifications to instructions.
    </p>
  );

  const patch = FICTIONAL_EXAMPLE.patch;

  const learnMore = [
    {
      id: "per-lever-proposals",
      title: "Per-Lever Proposal Generation",
      content: (
        <div className="space-y-3 text-sm">
          <p><strong>L1 & L2:</strong> Use <code className="rounded bg-db-gray-bg px-1.5 py-0.5">LEVER_1_2_COLUMN_PROMPT</code> for structured metadata changes. L1: table/column descriptions, synonyms, visibility. L2: MV-specific (aggregation, filters, MEASURE).</p>
          <p><strong>L3:</strong> TVF docs + <code className="rounded bg-db-gray-bg px-1.5 py-0.5">remove_tvf</code> for persistently failing TVFs.</p>
          <p><strong>L4:</strong> <code className="rounded bg-db-gray-bg px-1.5 py-0.5">LEVER_4_JOIN_SPEC_PROMPT</code> — analyzes SQL diffs for missing/incorrect JOINs, generates Genie-format join specs.</p>
          <p><strong>L5:</strong> Instruction rewrites + example SQL. Budget-aware, respects character limits.</p>
        </div>
      ),
    },
    {
      id: "section-ownership",
      title: "Section Ownership",
      content: (
        <div className="space-y-2 text-sm">
          <p><code className="rounded bg-db-gray-bg px-1.5 py-0.5">update_section()</code> raises <code className="rounded bg-db-gray-bg px-1.5 py-0.5">LeverOwnershipError</code> if a lever tries to modify a section it doesn&apos;t own.</p>
          <p><strong>L0</strong> owns all 14 metadata sections (for non-lever updates).</p>
          <p><strong>L5</strong> owns no metadata sections — it edits instructions and example SQL only.</p>
        </div>
      ),
    },
    {
      id: "patch-lifecycle",
      title: "Patch Lifecycle",
      content: (
        <div className="space-y-3 text-sm">
          <p><code className="rounded bg-db-gray-bg px-1.5 py-0.5">proposals_to_patches()</code> converts strategist proposals into patches.</p>
          <p><strong>Patch DSL format:</strong> <code className="rounded bg-db-gray-bg px-1.5 py-0.5">action_type</code>, <code className="rounded bg-db-gray-bg px-1.5 py-0.5">target</code>, <code className="rounded bg-db-gray-bg px-1.5 py-0.5">command</code>, <code className="rounded bg-db-gray-bg px-1.5 py-0.5">rollback_command</code>, <code className="rounded bg-db-gray-bg px-1.5 py-0.5">risk_level</code>. 35 patch types.</p>
          <p><strong>Fictional example patch:</strong></p>
          <pre className="overflow-x-auto rounded-lg border border-db-gray-border bg-db-gray-bg p-3 text-xs">
            {JSON.stringify({ action_type: patch.actionType, target: patch.target, sections: patch.sections }, null, 2)}
          </pre>
        </div>
      ),
    },
    {
      id: "patch-application",
      title: "Patch Application",
      content: (
        <ol className="list-inside list-decimal space-y-2 text-sm">
          <li>Load pre_snapshot (config before patches)</li>
          <li>Apply patches in dependency order</li>
          <li>Propagate changes (format assistance, entity matching)</li>
          <li>Wait for propagation (30s default)</li>
          <li>Run Slice Gate (if enabled)</li>
          <li>Run P0 Gate</li>
          <li>Run Full Gate</li>
          <li>Accept or rollback based on gate results</li>
        </ol>
      ),
    },
  ];

  return (
    <StageScreen
      title="The 5 Levers"
      subtitle="Targeted tools for specific problem types"
      pipelineGroup="leverLoop"
      visual={visual}
      explanation={explanation}
      learnMore={learnMore}
    />
  );
}
