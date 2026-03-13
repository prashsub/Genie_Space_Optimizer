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
import { RUN_STATUSES, SAFETY_CONSTANTS } from "./data";

const STATUS_COLOR_CLASSES: Record<string, string> = {
  green: "bg-green-100 text-green-800 border-green-300",
  red: "bg-red-100 text-red-800 border-red-300",
  yellow: "bg-yellow-100 text-yellow-800 border-yellow-300",
  gray: "bg-gray-100 text-gray-800 border-gray-300",
  blue: "bg-blue-100 text-blue-800 border-blue-300",
};

const ROW_1 = ["QUEUED", "IN_PROGRESS"];
const ROW_2 = ["CONVERGED", "STALLED", "MAX_ITERATIONS", "FAILED", "CANCELLED"];
const ROW_3 = ["APPLIED", "DISCARDED"];

const statusMap = new Map(RUN_STATUSES.map((s) => [s.name, s]));

export function ConvergenceStateMachine() {
  return (
    <TooltipProvider>
      <motion.section
        initial={{ opacity: 0, y: 20 }}
        animate={{ opacity: 1, y: 0 }}
        transition={{ duration: 0.4 }}
        className="w-full space-y-8"
      >
        <div>
          <h3 className="text-lg font-semibold">Run Status Lifecycle</h3>
          <div className="mt-4 flex flex-col items-center gap-6">
            <motion.div
              className="flex flex-wrap justify-center gap-3"
              initial={{ opacity: 0 }}
              animate={{ opacity: 1 }}
              transition={{ delay: 0.1 }}
            >
              {ROW_1.map((name, i) => (
                <StatusPill key={name} name={name} delay={0.1 + i * 0.05} />
              ))}
            </motion.div>
            <div className="flex justify-center text-muted-foreground">
              <ArrowDown className="h-6 w-6" />
            </div>
            <motion.div
              className="flex flex-wrap justify-center gap-3"
              initial={{ opacity: 0 }}
              animate={{ opacity: 1 }}
              transition={{ delay: 0.25 }}
            >
              {ROW_2.map((name, i) => (
                <StatusPill key={name} name={name} delay={0.25 + i * 0.05} />
              ))}
            </motion.div>
            <div className="flex justify-center text-muted-foreground">
              <ArrowDown className="h-6 w-6" />
            </div>
            <motion.div
              className="flex flex-wrap justify-center gap-3"
              initial={{ opacity: 0 }}
              animate={{ opacity: 1 }}
              transition={{ delay: 0.55 }}
            >
              {ROW_3.map((name, i) => (
                <StatusPill key={name} name={name} delay={0.55 + i * 0.05} />
              ))}
            </motion.div>
          </div>
        </div>

        <div>
          <h3 className="text-lg font-semibold">Safety Constants</h3>
          <div className="mt-4 grid grid-cols-1 gap-3 md:grid-cols-2">
            {SAFETY_CONSTANTS.map((c) => (
              <Card key={c.name} className="overflow-hidden">
                <CardContent className="p-4">
                  <Tooltip>
                    <TooltipTrigger asChild>
                      <span className="cursor-help font-mono text-sm font-semibold">
                        {c.name}
                      </span>
                    </TooltipTrigger>
                    <TooltipContent side="top" className="max-w-sm p-3">
                      <p>{c.description}</p>
                    </TooltipContent>
                  </Tooltip>
                  <div className="mt-2">
                    <Badge
                      variant="secondary"
                      className="text-base font-semibold"
                    >
                      {c.value}
                    </Badge>
                  </div>
                  <p className="mt-2 line-clamp-2 text-sm text-muted-foreground">
                    {c.description}
                  </p>
                </CardContent>
              </Card>
            ))}
          </div>
        </div>
      </motion.section>
    </TooltipProvider>
  );
}

function StatusPill({ name, delay }: { name: string; delay: number }) {
  const status = statusMap.get(name);
  if (!status) return null;
  const colorClass =
    STATUS_COLOR_CLASSES[status.color] ?? "bg-gray-100 text-gray-800 border-gray-300";
  const transitionsText =
    status.transitions.length > 0
      ? status.transitions
          .map((t) => `${t.label} → ${t.target}`)
          .join("; ")
      : "Terminal";

  return (
    <Tooltip>
      <TooltipTrigger asChild>
        <motion.span
          initial={{ opacity: 0, y: 4 }}
          animate={{ opacity: 1, y: 0 }}
          transition={{ delay, duration: 0.2 }}
          className={cn(
            "inline-flex cursor-default rounded-full border px-3 py-1 text-sm font-medium",
            colorClass
          )}
        >
          {status.name}
        </motion.span>
      </TooltipTrigger>
      <TooltipContent side="bottom" className="max-w-xs p-3">
        <p>{status.description}</p>
        <p className="mt-2 text-xs text-muted-foreground">Transitions: {transitionsText}</p>
      </TooltipContent>
    </Tooltip>
  );
}
