"use client";

import { motion } from "motion/react";
import { ArrowDown } from "lucide-react";
import { Card, CardContent } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import {
  Tooltip,
  TooltipContent,
  TooltipProvider,
  TooltipTrigger,
} from "@/components/ui/tooltip";
import { cn } from "@/lib/utils";
import { DATA_FLOW_NODES } from "./data";

const ROW_ORDER: string[][] = [
  ["genie-api", "uc-metadata"],
  ["benchmarks", "iterations", "asi-results"],
  ["provenance", "patches", "runs", "stages"],
  ["suggestions", "flagged", "queued-patches"],
];

const TYPE_BADGE_CLASSES: Record<string, string> = {
  input: "bg-blue-100 text-blue-800 border-blue-300",
  storage: "bg-green-100 text-green-800 border-green-300",
  process: "bg-purple-100 text-purple-800 border-purple-300",
};

const TYPE_BORDER_CLASSES: Record<string, string> = {
  input: "border-l-blue-500",
  storage: "border-l-green-500",
  process: "border-l-purple-500",
};

const nodeMap = new Map(DATA_FLOW_NODES.map((n) => [n.id, n]));

export function DataFlowDiagram() {
  return (
    <motion.section
      initial={{ opacity: 0, y: 20 }}
      animate={{ opacity: 1, y: 0 }}
      transition={{ duration: 0.4 }}
      className="w-full space-y-6"
    >
      <div>
        <h3 className="text-lg font-semibold">Data Flow & Audit Trail</h3>
        <p className="mt-1 text-sm text-muted-foreground">
          Every step is auditable. Data flows through 12 Delta tables with full
          provenance tracking.
        </p>
      </div>

      <TooltipProvider>
        <div className="flex flex-col items-center gap-4">
          {ROW_ORDER.map((rowIds, rowIndex) => (
            <div key={rowIndex}>
              <div className="flex flex-wrap justify-center gap-3">
                {rowIds.map((id) => {
                  const node = nodeMap.get(id);
                  if (!node) return null;
                  const keyColumnsContent =
                    node.keyColumns && node.keyColumns.length > 0 ? (
                      <ul className="mt-2 space-y-1 text-left">
                        {node.keyColumns.map((col) => (
                          <li key={col} className="font-mono text-xs">
                            {col}
                          </li>
                        ))}
                      </ul>
                    ) : (
                      <p className="text-xs text-muted-foreground">
                        {node.description}
                      </p>
                    );
                  return (
                    <Tooltip key={id}>
                      <TooltipTrigger asChild>
                        <Card
                          className={cn(
                            "w-40 cursor-default border-l-4 transition-shadow hover:shadow-md",
                            TYPE_BORDER_CLASSES[node.type] ?? "border-l-gray-500"
                          )}
                        >
                          <CardContent className="p-3">
                            <div className="flex items-center justify-between gap-2">
                              <span className="font-semibold text-sm">
                                {node.label}
                              </span>
                              <Badge
                                variant="outline"
                                className={cn(
                                  "shrink-0 text-[10px]",
                                  TYPE_BADGE_CLASSES[node.type] ??
                                    "bg-gray-100 text-gray-800 border-gray-300"
                                )}
                              >
                                {node.type}
                              </Badge>
                            </div>
                            {node.deltaTable && (
                              <p className="mt-1 font-mono text-xs text-muted-foreground truncate">
                                {node.deltaTable}
                              </p>
                            )}
                          </CardContent>
                        </Card>
                      </TooltipTrigger>
                      <TooltipContent
                        side="bottom"
                        className="max-w-xs p-3"
                        sideOffset={8}
                      >
                        {keyColumnsContent}
                      </TooltipContent>
                    </Tooltip>
                  );
                })}
              </div>
              {rowIndex < ROW_ORDER.length - 1 && (
                <div className="flex justify-center text-muted-foreground">
                  <ArrowDown className="h-5 w-5" />
                </div>
              )}
            </div>
          ))}
        </div>
      </TooltipProvider>
    </motion.section>
  );
}
