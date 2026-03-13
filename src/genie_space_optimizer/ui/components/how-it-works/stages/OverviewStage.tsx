"use client";

import * as React from "react";
import { motion, useReducedMotion } from "motion/react";
import { StageScreen } from "../StageScreen";
import { StageNode } from "../shared/StageNode";
import { FlowLine } from "../shared/FlowLine";
import { PIPELINE_STAGES, DATA_FLOW_NODES } from "../data";
import { Card, CardContent } from "@/components/ui/card";

function PipelineDiagram() {
  const prefersReducedMotion = useReducedMotion();

  return (
    <div className="relative flex flex-wrap items-center justify-center gap-x-2 gap-y-6 xl:flex-nowrap xl:justify-between xl:gap-4">
      {/* Glowing pulse traveling left-to-right */}
      {!prefersReducedMotion && (
        <motion.div
          className="pointer-events-none absolute inset-0 z-10 flex items-center"
          aria-hidden
        >
          <motion.div
            className="h-16 w-16 shrink-0 rounded-full bg-db-blue/40 blur-2xl"
            animate={{ x: ["0%", "100%"] }}
            transition={{
              duration: 3,
              repeat: Infinity,
              ease: "easeInOut",
              repeatDelay: 0.5,
            }}
            style={{
              marginLeft: "calc(75px - 32px)",
              width: "calc(100% - 150px)",
            }}
          />
        </motion.div>
      )}

      <div className="relative z-0 flex w-full flex-wrap items-center justify-center gap-x-2 gap-y-4 xl:flex-nowrap xl:justify-between xl:gap-4">
        {PIPELINE_STAGES.flatMap((stage, index) => {
          const elems: React.ReactNode[] = [
            <div
              key={stage.id}
              className="flex flex-1 min-w-[120px] max-w-[150px] justify-center xl:flex-initial xl:flex-1 xl:min-w-0"
            >
              <StageNode
                icon={stage.icon}
                title={stage.title}
                summary={stage.summary}
                isActive={false}
              />
            </div>,
          ];
          if (index < PIPELINE_STAGES.length - 1) {
            elems.push(
              <div key={`flow-${stage.id}`} className="hidden w-6 shrink-0 xl:block">
                <FlowLine className="h-4 w-full" />
              </div>
            );
          }
          return elems;
        })}
      </div>
    </div>
  );
}

function DataFlowContent() {
  return (
    <div className="grid gap-3 sm:grid-cols-2 lg:grid-cols-3">
      {DATA_FLOW_NODES.map((node) => (
        <Card key={node.id} className="overflow-hidden border border-db-gray-border">
          <CardContent className="p-3">
            <p className="font-mono text-sm font-semibold">
              {node.deltaTable ?? node.label}
            </p>
            {node.keyColumns && node.keyColumns.length > 0 && (
              <p className="mt-1 text-xs text-muted-foreground">
                Key: {node.keyColumns.slice(0, 4).join(", ")}
                {node.keyColumns.length > 4 ? "…" : ""}
              </p>
            )}
            <p className="mt-2 text-xs text-muted-foreground">{node.description}</p>
          </CardContent>
        </Card>
      ))}
    </div>
  );
}

export function OverviewStage() {
  return (
    <StageScreen
      title="The Big Picture"
      subtitle="How Genie Space Optimizer works end-to-end"
      pipelineGroup="neutral"
      visual={<PipelineDiagram />}
      explanation={
        <>
          Genie Space Optimizer automatically improves the quality of Databricks
          Genie Spaces — the natural-language-to-SQL interface for business users.
          It runs a closed-loop optimization pipeline: generating benchmarks,
          evaluating accuracy with 9 specialized judges, diagnosing failures, and
          applying targeted metadata patches until quality thresholds are met.
        </>
      }
      learnMore={[
        {
          id: "data-flow",
          title: "Data Flow Transparency",
          content: <DataFlowContent />,
        },
      ]}
    />
  );
}
