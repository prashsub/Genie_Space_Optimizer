"use client";

import { motion } from "motion/react";
import { cn } from "@/lib/utils";
import { PIPELINE_GROUP_COLORS } from "./data";
import type { WalkthroughStage } from "./types";
import {
  Tooltip,
  TooltipTrigger,
  TooltipContent,
  TooltipProvider,
} from "@/components/ui/tooltip";

interface ProgressRailProps {
  stages: WalkthroughStage[];
  currentIndex: number;
  onNavigate: (index: number) => void;
}

export function ProgressRail({ stages, currentIndex, onNavigate }: ProgressRailProps) {
  return (
    <TooltipProvider delayDuration={200}>
      <nav aria-label="Walkthrough progress" className="flex flex-col gap-0.5">
        {stages.map((stage, i) => {
          const isCompleted = i < currentIndex;
          const isCurrent = i === currentIndex;
          const colors = PIPELINE_GROUP_COLORS[stage.pipelineGroup] ?? PIPELINE_GROUP_COLORS.neutral;

          return (
            <div key={stage.id} className="flex items-start gap-3">
              {/* Vertical line + dot */}
              <div className="flex flex-col items-center">
                <Tooltip>
                  <TooltipTrigger asChild>
                    <button
                      type="button"
                      onClick={() => onNavigate(i)}
                      className={cn(
                        "relative z-10 flex h-5 w-5 shrink-0 items-center justify-center rounded-full border-2 transition-all",
                        isCurrent && `${colors.dot} border-transparent`,
                        isCompleted && "border-db-green bg-db-green",
                        !isCurrent && !isCompleted && "border-db-gray-border bg-white",
                      )}
                      aria-current={isCurrent ? "step" : undefined}
                    >
                      {isCurrent && (
                        <motion.span
                          className="absolute inset-0 rounded-full bg-current opacity-30"
                          animate={{ scale: [1, 1.5, 1] }}
                          transition={{ duration: 2, repeat: Infinity }}
                        />
                      )}
                      {isCompleted && (
                        <svg className="h-3 w-3 text-white" viewBox="0 0 12 12" fill="none">
                          <path d="M2 6l3 3 5-5" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" />
                        </svg>
                      )}
                    </button>
                  </TooltipTrigger>
                  <TooltipContent side="right">{stage.title}</TooltipContent>
                </Tooltip>
                {/* Connector line */}
                {i < stages.length - 1 && (
                  <div
                    className={cn(
                      "w-0.5 min-h-[1.25rem]",
                      i < currentIndex ? "bg-db-green" : "bg-db-gray-border",
                    )}
                  />
                )}
              </div>

              {/* Label */}
              <button
                type="button"
                onClick={() => onNavigate(i)}
                className={cn(
                  "pt-0.5 text-left text-xs leading-tight transition-colors",
                  isCurrent ? "font-semibold text-foreground" : "text-muted-foreground hover:text-foreground",
                )}
              >
                {stage.title}
              </button>
            </div>
          );
        })}
      </nav>
    </TooltipProvider>
  );
}
