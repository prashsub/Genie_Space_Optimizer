"use client";

import { useState, useEffect, useCallback, type ReactNode } from "react";
import { AnimatePresence, motion, useReducedMotion } from "motion/react";
import { ProgressRail } from "./ProgressRail";
import { PipelineMiniMap } from "./PipelineMiniMap";
import { StageNav } from "./StageNav";
import { WALKTHROUGH_STAGES } from "./data";

export interface WalkthroughShellProps {
  children: (stageId: string, stageIndex: number) => ReactNode;
}

export function WalkthroughShell({ children }: WalkthroughShellProps) {
  const [currentStage, setCurrentStage] = useState(0);
  const [direction, setDirection] = useState(0);
  const prefersReducedMotion = useReducedMotion();

  // URL hash sync — read on mount
  useEffect(() => {
    const hash = window.location.hash.slice(1);
    if (hash) {
      const idx = WALKTHROUGH_STAGES.findIndex((s) => s.id === hash);
      if (idx >= 0) setCurrentStage(idx);
    }
  }, []);

  // URL hash sync — write on change
  useEffect(() => {
    const stage = WALKTHROUGH_STAGES[currentStage];
    if (stage) {
      window.history.replaceState(null, "", `#${stage.id}`);
    }
  }, [currentStage]);

  const goTo = useCallback(
    (index: number) => {
      if (index < 0 || index >= WALKTHROUGH_STAGES.length) return;
      setDirection(index > currentStage ? 1 : -1);
      setCurrentStage(index);
    },
    [currentStage],
  );

  const goNext = useCallback(() => goTo(currentStage + 1), [goTo, currentStage]);
  const goPrev = useCallback(() => goTo(currentStage - 1), [goTo, currentStage]);

  // Keyboard navigation
  useEffect(() => {
    function handleKey(e: KeyboardEvent) {
      if (e.target instanceof HTMLInputElement || e.target instanceof HTMLTextAreaElement) return;
      switch (e.key) {
        case "ArrowRight":
          e.preventDefault();
          goNext();
          break;
        case "ArrowLeft":
          e.preventDefault();
          goPrev();
          break;
        case "Escape":
          e.preventDefault();
          goTo(0);
          break;
        default:
          if (/^[0-9]$/.test(e.key)) {
            e.preventDefault();
            const num = parseInt(e.key, 10);
            // 1-9 -> stages 1-9, 0 -> stage 10
            const target = num === 0 ? 10 : num;
            if (target < WALKTHROUGH_STAGES.length) goTo(target);
          }
      }
    }
    window.addEventListener("keydown", handleKey);
    return () => window.removeEventListener("keydown", handleKey);
  }, [goTo, goNext, goPrev]);

  const stage = WALKTHROUGH_STAGES[currentStage];

  const slideVariants = prefersReducedMotion
    ? { enter: { opacity: 0 }, center: { opacity: 1 }, exit: { opacity: 0 } }
    : {
        enter: (d: number) => ({ x: d > 0 ? 300 : -300, opacity: 0 }),
        center: { x: 0, opacity: 1 },
        exit: (d: number) => ({ x: d > 0 ? -300 : 300, opacity: 0 }),
      };

  return (
    <div className="flex min-h-[calc(100vh-8rem)] gap-0">
      {/* Left progress rail — hidden on small screens */}
      <aside className="hidden shrink-0 lg:block lg:w-56">
        <div className="sticky top-20 max-h-[calc(100vh-6rem)] overflow-y-auto py-4 pr-4">
          <ProgressRail
            stages={WALKTHROUGH_STAGES}
            currentIndex={currentStage}
            onNavigate={goTo}
          />
        </div>
      </aside>

      {/* Main content */}
      <div className="flex min-w-0 flex-1 flex-col">
        {/* Mini-map */}
        <PipelineMiniMap currentGroup={stage?.pipelineGroup ?? "neutral"} />

        {/* Mobile progress indicator */}
        <div className="flex items-center justify-between border-b border-db-gray-border px-4 py-2 text-xs text-muted-foreground lg:hidden">
          <button onClick={goPrev} disabled={currentStage === 0} className="disabled:opacity-30">
            &larr; Prev
          </button>
          <span>
            Stage {currentStage + 1} of {WALKTHROUGH_STAGES.length}
          </span>
          <button
            onClick={goNext}
            disabled={currentStage === WALKTHROUGH_STAGES.length - 1}
            className="disabled:opacity-30"
          >
            Next &rarr;
          </button>
        </div>

        {/* Stage content with directional transitions */}
        <div className="relative flex-1 overflow-x-hidden overflow-y-auto px-4 py-6 lg:px-8">
          <AnimatePresence mode="wait" custom={direction}>
            <motion.div
              key={stage?.id}
              custom={direction}
              variants={slideVariants}
              initial="enter"
              animate="center"
              exit="exit"
              transition={{ duration: prefersReducedMotion ? 0 : 0.3, ease: "easeInOut" }}
              className="w-full"
            >
              {stage && children(stage.id, currentStage)}
            </motion.div>
          </AnimatePresence>
        </div>

        {/* Bottom navigation */}
        <StageNav
          stages={WALKTHROUGH_STAGES}
          currentIndex={currentStage}
          onNext={goNext}
          onPrev={goPrev}
        />
      </div>
    </div>
  );
}
