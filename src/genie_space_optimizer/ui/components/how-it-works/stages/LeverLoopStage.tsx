"use client";

import {
  Search,
  Layers,
  ArrowUpDown,
  BrainCircuit,
  Lightbulb,
  Code,
  Wrench,
  ShieldCheck,
  RotateCcw,
  BookOpen,
  CircleDot,
} from "lucide-react";
import { Badge } from "@/components/ui/badge";
import { StageScreen } from "../StageScreen";
import { LoopDiagram } from "../shared/LoopDiagram";

const LOOP_STEPS = [
  { label: "Extract Failures", icon: Search },
  { label: "Cluster", icon: Layers },
  { label: "Rank", icon: ArrowUpDown },
  { label: "Strategize", icon: BrainCircuit },
  { label: "Generate Proposals", icon: Lightbulb },
  { label: "Convert to Patches", icon: Code },
  { label: "Apply Patches", icon: Wrench },
  { label: "3-Gate Eval", icon: ShieldCheck },
  { label: "Accept/Rollback", icon: RotateCcw },
  { label: "Reflect", icon: BookOpen },
  { label: "Check Convergence", icon: CircleDot },
];

export function LeverLoopStage() {
  return (
    <StageScreen
      title="The Optimization Loop"
      subtitle="Iteratively improve via failure analysis and targeted patches"
      pipelineGroup="leverLoop"
      visual={
        <div className="space-y-4">
          <div className="overflow-x-auto pb-2">
            <LoopDiagram steps={LOOP_STEPS} className="min-w-max" />
          </div>
          <div className="flex justify-center">
            <Badge variant="secondary" className="text-muted-foreground">
              Iteration 1 of up to 5
            </Badge>
          </div>
        </div>
      }
      explanation={
        <>
          This is the heart of the optimizer. Each iteration analyzes
          what&apos;s failing, clusters those failures by root cause,
          generates targeted patches, and validates them through a rigorous
          3-gate evaluation. If the patches help, they stick. If not,
          they&apos;re rolled back and the system tries a different approach.
        </>
      }
    />
  );
}
