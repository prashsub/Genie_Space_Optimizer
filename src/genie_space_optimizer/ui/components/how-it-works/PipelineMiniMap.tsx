"use client";

import { cn } from "@/lib/utils";
import { PIPELINE_GROUP_COLORS } from "./data";

const PIPELINE_GROUPS = [
  { id: "preflight", label: "Preflight" },
  { id: "baseline", label: "Baseline" },
  { id: "enrichment", label: "Enrichment" },
  { id: "leverLoop", label: "Lever Loop" },
  { id: "finalize", label: "Finalize" },
] as const;

interface PipelineMiniMapProps {
  currentGroup: string;
}

export function PipelineMiniMap({ currentGroup }: PipelineMiniMapProps) {
  return (
    <div className="flex items-center justify-center gap-1.5 border-b border-db-gray-border px-4 py-2">
      {PIPELINE_GROUPS.map((group, i) => {
        const colors = PIPELINE_GROUP_COLORS[group.id] ?? PIPELINE_GROUP_COLORS.neutral;
        const isActive = currentGroup === group.id;
        return (
          <div key={group.id} className="flex items-center gap-1.5">
            <div className="flex items-center gap-1">
              <span
                className={cn(
                  "inline-block h-2.5 w-2.5 rounded-full transition-all",
                  isActive ? `${colors.dot} scale-125 ring-2 ring-offset-1 ring-current` : "bg-db-gray-border",
                )}
                title={group.label}
              />
              <span
                className={cn(
                  "hidden text-[10px] sm:inline",
                  isActive ? "font-semibold text-foreground" : "text-muted-foreground",
                )}
              >
                {group.label}
              </span>
            </div>
            {i < PIPELINE_GROUPS.length - 1 && (
              <span className="h-px w-4 bg-db-gray-border sm:w-6" />
            )}
          </div>
        );
      })}
    </div>
  );
}
