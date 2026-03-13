"use client";

import type { ComponentType } from "react";
import { AnimatePresence, motion } from "motion/react";
import { Tabs, TabsList, TabsTrigger, TabsContent } from "@/components/ui/tabs";
import { PIPELINE_STAGES } from "./data";
import { PreflightStage } from "./stages/PreflightStage";
import { BaselineStage } from "./stages/BaselineStage";
import { EnrichmentStage } from "./stages/EnrichmentStage";
import { LeverLoopStage } from "./stages/LeverLoopStage";
import { FinalizeStage } from "./stages/FinalizeStage";
import { DeployStage } from "./stages/DeployStage";

const STAGE_COMPONENTS: Record<string, ComponentType> = {
  preflight: PreflightStage,
  baseline: BaselineStage,
  enrichment: EnrichmentStage,
  "lever-loop": LeverLoopStage,
  finalize: FinalizeStage,
  deploy: DeployStage,
};

export interface StageDeepDiveProps {
  activeStage: string;
  onStageChange: (stageId: string) => void;
}

export function StageDeepDive({ activeStage, onStageChange }: StageDeepDiveProps) {
  return (
    <Tabs value={activeStage} onValueChange={onStageChange}>
      <TabsList className="mb-6 flex w-full flex-wrap gap-1">
        {PIPELINE_STAGES.map((stage) => {
          const Icon = stage.icon;
          return (
            <TabsTrigger key={stage.id} value={stage.id} className="gap-1.5">
              <Icon className="h-4 w-4" />
              {stage.title}
            </TabsTrigger>
          );
        })}
      </TabsList>
      {PIPELINE_STAGES.map((stage) => {
        const Component = STAGE_COMPONENTS[stage.id];
        if (!Component) return null;
        return (
          <TabsContent key={stage.id} value={stage.id} className="mt-0">
            <AnimatePresence mode="wait">
              {activeStage === stage.id && (
                <motion.div
                  initial={{ opacity: 0 }}
                  animate={{ opacity: 1 }}
                  exit={{ opacity: 0 }}
                  transition={{ duration: 0.2 }}
                >
                  <Component />
                </motion.div>
              )}
            </AnimatePresence>
          </TabsContent>
        );
      })}
    </Tabs>
  );
}
