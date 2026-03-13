"use client";

import { ExpandableCard } from "./shared/ExpandableCard";
import { Badge } from "@/components/ui/badge";
import { JUDGES, JUDGE_CATEGORY_COLORS } from "./data";
import { cn } from "@/lib/utils";

const METHOD_BADGE_CLASSES: Record<string, string> = {
  CODE: "border-transparent bg-blue-500/20 text-blue-700 dark:bg-blue-500/30 dark:text-blue-300",
  LLM: "border-transparent bg-purple-500/20 text-purple-700 dark:bg-purple-500/30 dark:text-purple-300",
  CONDITIONAL_LLM:
    "border-transparent bg-amber-500/20 text-amber-700 dark:bg-amber-500/30 dark:text-amber-300",
};

export function JudgeGrid() {
  return (
    <section className="space-y-6">
      <div>
        <h2 className="text-2xl font-semibold tracking-tight">The 9 Judges</h2>
        <p className="mt-1 text-muted-foreground">
          Each benchmark is evaluated by 9 specialized judges. LLM judges use AI
          reasoning, CODE judges use deterministic comparison, and the Arbiter
          synthesizes all verdicts.
        </p>
      </div>
      <div className="grid grid-cols-1 gap-4 md:grid-cols-2 lg:grid-cols-3">
        {JUDGES.map((judge) => (
          <ExpandableCard
            key={judge.number}
            title={judge.displayName}
            subtitle={judge.description}
            badge={{
              text: judge.method,
              className: METHOD_BADGE_CLASSES[judge.method],
            }}
            accentColor={
              JUDGE_CATEGORY_COLORS[judge.category] ?? "border-l-gray-400"
            }
          >
            <div className="space-y-4">
              <p className="text-sm text-muted-foreground">
                {judge.detailedDescription}
              </p>
              {judge.threshold >= 0 && (
                <div className="space-y-1.5">
                  <span className="text-xs font-medium">Threshold: {judge.threshold}%</span>
                  <div className="h-2 w-full overflow-hidden rounded-full bg-gray-200 dark:bg-gray-700">
                    <div
                      className="h-full rounded-full bg-primary transition-all"
                      style={{ width: `${judge.threshold}%` }}
                    />
                  </div>
                </div>
              )}
              {judge.failureTypes.length > 0 && (
                <div className="flex flex-wrap gap-1.5">
                  {judge.failureTypes.map((ft) => (
                    <Badge
                      key={ft}
                      variant="secondary"
                      className="border-red-300/50 bg-red-50 text-red-800 dark:border-red-800/50 dark:bg-red-950/50 dark:text-red-300"
                    >
                      {ft}
                    </Badge>
                  ))}
                </div>
              )}
              {judge.overrideRules.length > 0 && (
                <div>
                  <span className="text-xs font-medium">Override rules:</span>
                  <ul className="mt-1 list-inside list-disc space-y-0.5 text-sm text-muted-foreground">
                    {judge.overrideRules.map((rule, i) => (
                      <li key={i}>{rule}</li>
                    ))}
                  </ul>
                </div>
              )}
              {judge.asiFields.length > 0 && (
                <div className="flex flex-wrap gap-1.5">
                  {judge.asiFields.map((field) => (
                    <Badge
                      key={field}
                      variant="outline"
                      className="font-mono text-xs"
                    >
                      {field}
                    </Badge>
                  ))}
                </div>
              )}
              {judge.promptSnippet && (
                <blockquote
                  className={cn(
                    "rounded-lg bg-muted p-3 font-mono text-xs italic",
                  )}
                >
                  {judge.promptSnippet}
                </blockquote>
              )}
            </div>
          </ExpandableCard>
        ))}
      </div>
    </section>
  );
}
