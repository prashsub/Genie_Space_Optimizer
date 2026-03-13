import { createFileRoute } from "@tanstack/react-router";
import { useRef, useState } from "react";
import { motion } from "motion/react";
import { PipelineOverview } from "@/components/how-it-works/PipelineOverview";
import { StageDeepDive } from "@/components/how-it-works/StageDeepDive";
import { DataFlowDiagram } from "@/components/how-it-works/DataFlowDiagram";
import { JudgeGrid } from "@/components/how-it-works/JudgeGrid";
import { ConvergenceStateMachine } from "@/components/how-it-works/ConvergenceStateMachine";
import { FailureTaxonomyExplorer } from "@/components/how-it-works/FailureTaxonomyExplorer";
import { HumanReviewLoop } from "@/components/how-it-works/HumanReviewLoop";

export const Route = createFileRoute("/how-it-works")({
  component: HowItWorksPage,
});

function HowItWorksPage() {
  const [activeStage, setActiveStage] = useState("preflight");
  const deepDiveRef = useRef<HTMLDivElement>(null);

  const handleStageClick = (stageId: string) => {
    setActiveStage(stageId);
    deepDiveRef.current?.scrollIntoView({ behavior: "smooth" });
  };

  const sectionProps = {
    initial: { opacity: 0, y: 30 },
    whileInView: { opacity: 1, y: 0 },
    viewport: { once: true, margin: "-100px" },
    transition: { duration: 0.4 },
  };

  return (
    <div className="space-y-12 pb-12">
      <motion.div {...sectionProps}>
        <PipelineOverview activeStage={activeStage} onStageClick={handleStageClick} />
      </motion.div>

      <motion.div ref={deepDiveRef} id="stage-deep-dive" {...sectionProps}>
        <StageDeepDive activeStage={activeStage} onStageChange={setActiveStage} />
      </motion.div>

      <motion.div {...sectionProps}>
        <DataFlowDiagram />
      </motion.div>

      <motion.div {...sectionProps}>
        <JudgeGrid />
      </motion.div>

      <motion.div {...sectionProps}>
        <ConvergenceStateMachine />
      </motion.div>

      <motion.div {...sectionProps}>
        <FailureTaxonomyExplorer />
      </motion.div>

      <motion.div {...sectionProps}>
        <HumanReviewLoop />
      </motion.div>
    </div>
  );
}
