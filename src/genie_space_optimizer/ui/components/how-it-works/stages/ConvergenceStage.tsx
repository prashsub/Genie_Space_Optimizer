"use client";

import { motion } from "motion/react";
import { ArrowDown } from "lucide-react";
import { Card, CardContent } from "@/components/ui/card";
import { cn } from "@/lib/utils";
import { StageScreen } from "../StageScreen";
import { ExpandableCard } from "../shared/ExpandableCard";
import { RUN_STATUSES, CONVERGENCE_CONDITIONS, SAFETY_CONSTANTS } from "../data";

const STATUS_COLOR_CLASSES: Record<string, string> = {
  green: "border-l-db-green bg-db-green/5",
  red: "border-l-red-500 bg-red-50/50",
  yellow: "border-l-yellow-500 bg-yellow-50/50",
  gray: "border-l-gray-400 bg-gray-50/50",
  blue: "border-l-db-blue bg-db-blue/5",
};

export function ConvergenceStage() {
  const statusMap = new Map(RUN_STATUSES.map((s) => [s.name, s]));

  const visual = (
    <div className="space-y-6">
      <div className="text-sm font-medium text-muted-foreground">
        Run status state machine
      </div>
      <div className="flex flex-col items-center gap-4">
        {/* Row 1: QUEUED → IN_PROGRESS */}
        <div className="flex flex-wrap justify-center gap-2">
          {["QUEUED", "IN_PROGRESS"].map((name, i) => (
            <StatusNode key={name} name={name} statusMap={statusMap} delay={i * 0.05} />
          ))}
        </div>
        <ArrowDown className="h-5 w-5 text-muted-foreground" />
        {/* Row 2: CONVERGED, STALLED, MAX_ITERATIONS, FAILED, CANCELLED */}
        <div className="flex flex-wrap justify-center gap-2">
          {["CONVERGED", "STALLED", "MAX_ITERATIONS", "FAILED", "CANCELLED"].map((name, i) => (
            <StatusNode key={name} name={name} statusMap={statusMap} delay={0.15 + i * 0.05} />
          ))}
        </div>
        <ArrowDown className="h-5 w-5 text-muted-foreground" />
        {/* Row 3: APPLIED, DISCARDED */}
        <div className="flex flex-wrap justify-center gap-2">
          {["APPLIED", "DISCARDED"].map((name, i) => (
            <StatusNode key={name} name={name} statusMap={statusMap} delay={0.4 + i * 0.05} />
          ))}
        </div>
      </div>
    </div>
  );

  const explanation = (
    <p>
      The optimizer knows when to stop. Six exit conditions govern convergence, and ten safety constants act as guardrails preventing the system from making things worse. Every run ends in a well-defined terminal state.
    </p>
  );

  const learnMore = [
    {
      id: "convergence-decision-tree",
      title: "Convergence Decision Tree",
      content: (
        <div className="space-y-3">
          {CONVERGENCE_CONDITIONS.map((c) => (
            <ExpandableCard
              key={c.name}
              title={c.name}
              subtitle={`→ ${c.resultStatus}`}
              accentColor="border-l-db-blue"
            >
              <div className="space-y-2 text-sm">
                <p><code className="rounded bg-db-gray-bg px-1.5 py-0.5">{c.formula}</code></p>
                <p className="text-muted-foreground">{c.description}</p>
              </div>
            </ExpandableCard>
          ))}
        </div>
      ),
    },
    {
      id: "safety-constants",
      title: "Safety Constants",
      content: (
        <div className="space-y-2">
          {SAFETY_CONSTANTS.map((c) => (
            <div key={c.name} className="rounded-lg border border-db-gray-border bg-white p-3">
              <div className="flex items-center gap-2">
                <span className="font-mono text-sm font-semibold">{c.name}</span>
                <span className="rounded bg-db-gray-bg px-2 py-0.5 text-xs font-medium">{c.value}</span>
              </div>
              <p className="mt-1 text-sm text-muted-foreground">{c.description}</p>
            </div>
          ))}
        </div>
      ),
    },
  ];

  return (
    <StageScreen
      title="Convergence & Safety"
      subtitle="Know when to stop, prevent harm"
      pipelineGroup="finalize"
      visual={visual}
      explanation={explanation}
      learnMore={learnMore}
    />
  );
}

function StatusNode({
  name,
  statusMap,
  delay,
}: {
  name: string;
  statusMap: Map<string, (typeof RUN_STATUSES)[0]>;
  delay: number;
}) {
  const status = statusMap.get(name);
  if (!status) return null;
  const colorClass = STATUS_COLOR_CLASSES[status.color] ?? "border-l-gray-400 bg-gray-50/50";

  return (
    <motion.div
      initial={{ opacity: 0, scale: 0.95 }}
      animate={{ opacity: 1, scale: 1 }}
      transition={{ delay, duration: 0.2 }}
    >
      <Card className={cn("min-w-[120px] border-l-4", colorClass)}>
        <CardContent className="py-2 px-3 text-center">
          <span className="text-sm font-semibold">{status.name}</span>
        </CardContent>
      </Card>
    </motion.div>
  );
}
