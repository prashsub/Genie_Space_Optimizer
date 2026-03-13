"use client";

import * as React from "react";
import { motion } from "motion/react";
import { StageNode } from "./shared/StageNode";
import { FlowLine } from "./shared/FlowLine";
import { PIPELINE_STAGES } from "./data";
import { cn } from "@/lib/utils";

export interface PipelineOverviewProps {
  activeStage: string | null;
  onStageClick: (stageId: string) => void;
}

export function PipelineOverview({
  activeStage,
  onStageClick,
}: PipelineOverviewProps) {
  return (
    <motion.section
      initial={{ opacity: 0, y: 20 }}
      animate={{ opacity: 1, y: 0 }}
      transition={{ duration: 0.4 }}
      className="w-full space-y-6"
    >
      <p className="text-muted-foreground">
        Genie Space Optimizer automatically improves the quality of Databricks
        Genie Spaces — the natural-language-to-SQL interface for business
        users. It runs a closed-loop optimization pipeline: generating benchmarks,
        evaluating accuracy with 9 specialized judges, diagnosing failures, and
        applying targeted metadata patches until quality thresholds are met.
      </p>

      <div
        className={cn(
          "flex flex-wrap items-center justify-center gap-x-2 gap-y-4",
          "xl:flex-nowrap xl:justify-between xl:gap-0"
        )}
      >
        {PIPELINE_STAGES.flatMap((stage, index) => {
          const elems: React.ReactNode[] = [
            <div
              key={stage.id}
              className="flex flex-[1_1_calc(33.333%-0.5rem)] justify-center xl:flex-initial xl:flex-1 xl:min-w-0"
            >
              <StageNode
                icon={stage.icon}
                title={stage.title}
                summary={stage.summary}
                isActive={activeStage === stage.id}
                onClick={() => onStageClick(stage.id)}
              />
            </div>,
          ];
          if (index < PIPELINE_STAGES.length - 1) {
            const isRowBreak = index === 2;
            elems.push(
              <div
                key={`flow-${stage.id}`}
                className={cn(
                  "hidden shrink-0 xl:block",
                  isRowBreak ? "xl:w-full xl:max-w-8 xl:rotate-90 xl:self-stretch" : "xl:w-8"
                )}
              >
                <FlowLine className={cn("h-5", isRowBreak ? "w-8 xl:w-5" : "w-8")} />
              </div>
            );
          }
          return elems;
        })}
      </div>
    </motion.section>
  );
}
