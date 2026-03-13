"use client";

import * as React from "react";
import { motion, AnimatePresence } from "motion/react";
import { ExpandableCard } from "./shared/ExpandableCard";
import { Badge } from "@/components/ui/badge";
import { FAILURE_TAXONOMY, LEVERS } from "./data";
import { cn } from "@/lib/utils";
import { ArrowRight } from "lucide-react";

const LEVER_COLORS: Record<number, string> = {
  0: "bg-gray-500/20 text-gray-700 dark:bg-gray-500/30 dark:text-gray-300 border-gray-400/30",
  1: "bg-blue-500/20 text-blue-700 dark:bg-blue-500/30 dark:text-blue-300 border-blue-400/30",
  2: "bg-indigo-500/20 text-indigo-700 dark:bg-indigo-500/30 dark:text-indigo-300 border-indigo-400/30",
  3: "bg-purple-500/20 text-purple-700 dark:bg-purple-500/30 dark:text-purple-300 border-purple-400/30",
  4: "bg-emerald-500/20 text-emerald-700 dark:bg-emerald-500/30 dark:text-emerald-300 border-emerald-400/30",
  5: "bg-orange-500/20 text-orange-700 dark:bg-orange-500/30 dark:text-orange-300 border-orange-400/30",
};

const LEVER_NAMES: Record<number, string> = {
  0: "Not actionable",
  1: LEVERS[0]?.name ?? "Tables & Columns",
  2: LEVERS[1]?.name ?? "Metric Views",
  3: LEVERS[2]?.name ?? "Table-Valued Functions",
  4: LEVERS[3]?.name ?? "Join Specifications",
  5: LEVERS[4]?.name ?? "Instructions & Examples",
};

function getLeverCounts() {
  const counts: Record<number, number> = { 0: 0, 1: 0, 2: 0, 3: 0, 4: 0, 5: 0 };
  for (const ft of FAILURE_TAXONOMY) {
    counts[ft.lever] = (counts[ft.lever] ?? 0) + 1;
  }
  return counts;
}

const LEVER_COUNTS = getLeverCounts();

export function FailureTaxonomyExplorer() {
  const [activeLever, setActiveLever] = React.useState<number | null>(null);

  const toggleLever = (lever: number) => {
    setActiveLever((prev) => (prev === lever ? null : lever));
  };

  return (
    <section className="space-y-6">
      <div>
        <h2 className="text-2xl font-semibold tracking-tight">Failure Taxonomy</h2>
        <p className="mt-1 text-muted-foreground">
          The optimizer classifies 22 failure types across 5 optimization levers.
          Each failure type maps to a specific lever that can address it.
        </p>
      </div>

      <div className="flex flex-wrap gap-2">
        {([0, 1, 2, 3, 4, 5] as const).map((lever) => (
          <button
            key={lever}
            type="button"
            onClick={() => toggleLever(lever)}
            className={cn(
              "rounded-lg border px-4 py-2 text-sm font-medium transition-colors",
              activeLever === lever
                ? LEVER_COLORS[lever]
                : "border-border bg-muted/50 text-muted-foreground hover:bg-muted",
            )}
          >
            <span>{LEVER_NAMES[lever]}</span>
            <span className="ml-1.5 rounded-full bg-current/20 px-1.5 py-0.5 text-xs">
              {LEVER_COUNTS[lever]}
            </span>
          </button>
        ))}
      </div>

      <motion.div
        className="grid grid-cols-2 gap-3 md:grid-cols-3 lg:grid-cols-4"
        layout
      >
        <AnimatePresence mode="popLayout">
          {FAILURE_TAXONOMY.map((ft) => {
            const isMatch = activeLever === null || ft.lever === activeLever;
            return (
              <motion.div
                key={ft.name}
                layout
                initial={{ opacity: 1 }}
                animate={{
                  opacity: isMatch ? 1 : 0.3,
                }}
                transition={{ duration: 0.2 }}
                className={cn(
                  "rounded-lg border bg-card p-3 transition-opacity",
                  !isMatch && "pointer-events-none",
                )}
              >
                <div className="flex items-start justify-between gap-2">
                  <code className="text-xs font-medium">{ft.name}</code>
                  <Badge
                    variant="outline"
                    className={cn(
                      "shrink-0 text-[10px]",
                      LEVER_COLORS[ft.lever],
                    )}
                  >
                    L{ft.lever}
                  </Badge>
                </div>
                <p className="mt-1 text-xs text-muted-foreground">{ft.description}</p>
              </motion.div>
            );
          })}
        </AnimatePresence>
      </motion.div>

      <ExpandableCard title="Root-Cause Resolution" defaultOpen={false}>
        <div className="space-y-4">
          <div className="flex flex-wrap items-center gap-2">
            <div className="rounded-lg border bg-muted/50 px-4 py-2 text-sm font-medium">
              ASI Metadata
            </div>
            <ArrowRight className="h-4 w-4 shrink-0 text-muted-foreground" />
            <div className="rounded-lg border bg-muted/50 px-4 py-2 text-sm font-medium">
              Rationale Pattern
            </div>
            <ArrowRight className="h-4 w-4 shrink-0 text-muted-foreground" />
            <div className="rounded-lg border bg-muted/50 px-4 py-2 text-sm font-medium">
              SQL Diff Classification
            </div>
          </div>
          <p className="text-xs text-muted-foreground">
            ASI Metadata: check if asi_failure_type present. Rationale Pattern:
            keyword matching on judge rationale. SQL Diff Classification: compare
            expected vs generated SQL.
          </p>
          <p className="text-sm font-medium">
            Result: classified, clustered, ranked actionable item
          </p>
        </div>
      </ExpandableCard>
    </section>
  );
}
