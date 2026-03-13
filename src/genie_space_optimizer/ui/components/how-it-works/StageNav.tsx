"use client";

import { useState, useEffect } from "react";
import { motion, useReducedMotion } from "motion/react";
import { ChevronLeft, ChevronRight } from "lucide-react";
import { Button } from "@/components/ui/button";
import type { WalkthroughStage } from "./types";

interface StageNavProps {
  stages: WalkthroughStage[];
  currentIndex: number;
  onNext: () => void;
  onPrev: () => void;
}

export function StageNav({ stages, currentIndex, onNext, onPrev }: StageNavProps) {
  const isFirst = currentIndex === 0;
  const isLast = currentIndex === stages.length - 1;
  const nextStage = !isLast ? stages[currentIndex + 1] : null;
  const prevStage = !isFirst ? stages[currentIndex - 1] : null;
  const prefersReducedMotion = useReducedMotion();

  // Auto-advance hint: pulse the Next button after 3s on overview
  const [showPulse, setShowPulse] = useState(false);
  useEffect(() => {
    if (currentIndex !== 0) {
      setShowPulse(false);
      return;
    }
    const timer = setTimeout(() => setShowPulse(true), 3000);
    return () => clearTimeout(timer);
  }, [currentIndex]);

  return (
    <div className="flex items-center justify-between border-t border-db-gray-border px-4 py-3 lg:px-8">
      {/* Back button */}
      <div className="w-1/3">
        {prevStage && (
          <Button variant="ghost" size="sm" onClick={onPrev} className="gap-1 text-muted-foreground">
            <ChevronLeft className="h-4 w-4" />
            <span className="hidden sm:inline">{prevStage.title}</span>
            <span className="sm:hidden">Back</span>
          </Button>
        )}
      </div>

      {/* Stage counter */}
      <div className="text-center text-xs text-muted-foreground">
        Stage {currentIndex + 1} of {stages.length}
      </div>

      {/* Next button */}
      <div className="flex w-1/3 justify-end">
        {nextStage && (
          <motion.div
            animate={
              showPulse && !prefersReducedMotion
                ? { scale: [1, 1.05, 1] }
                : {}
            }
            transition={
              showPulse ? { duration: 1.5, repeat: Infinity, ease: "easeInOut" } : {}
            }
          >
            <Button variant="default" size="sm" onClick={onNext} className="gap-1">
              <span className="hidden sm:inline">Next: {nextStage.title}</span>
              <span className="sm:hidden">Next</span>
              <ChevronRight className="h-4 w-4" />
            </Button>
          </motion.div>
        )}
      </div>
    </div>
  );
}
