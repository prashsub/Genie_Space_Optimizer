"use client";

import * as React from "react";
import { motion, useReducedMotion } from "motion/react";
import { Circle, CheckCircle2 } from "lucide-react";
import { cn } from "@/lib/utils";

export interface AnimatedChecklistItem {
  id: string;
  label: string;
  description?: string;
}

export interface AnimatedChecklistProps {
  items: AnimatedChecklistItem[];
  staggerDelay?: number;
  autoPlay?: boolean;
}

export function AnimatedChecklist({
  items,
  staggerDelay = 200,
  autoPlay = true,
}: AnimatedChecklistProps) {
  const [checkedIndices, setCheckedIndices] = React.useState<Set<number>>(
    () => new Set()
  );
  const prefersReducedMotion = useReducedMotion();

  React.useEffect(() => {
    if (!autoPlay || prefersReducedMotion) {
      if (prefersReducedMotion) {
        setCheckedIndices(new Set(items.map((_, i) => i)));
      }
      return;
    }

    let current = 0;
    const timers: ReturnType<typeof setTimeout>[] = [];

    for (let i = 0; i < items.length; i++) {
      const timer = setTimeout(() => {
        setCheckedIndices((prev) => new Set([...prev, i]));
        current++;
      }, i * staggerDelay);
      timers.push(timer);
    }

    return () => timers.forEach((t) => clearTimeout(t));
  }, [items.length, staggerDelay, autoPlay, prefersReducedMotion]);

  const allChecked = prefersReducedMotion || checkedIndices.size === items.length;

  return (
    <ul className="space-y-3">
      {items.map((item, index) => {
        const isChecked = checkedIndices.has(index) || allChecked;
        return (
          <motion.li
            key={item.id}
            initial={prefersReducedMotion ? false : { opacity: 0, x: -8 }}
            animate={
              prefersReducedMotion
                ? false
                : { opacity: 1, x: 0 }
            }
            transition={{
              delay: prefersReducedMotion ? 0 : index * staggerDelay * 0.5,
              duration: prefersReducedMotion ? 0 : 0.2,
            }}
            className="flex items-start gap-3"
          >
            <span className="mt-0.5 shrink-0">
              {isChecked ? (
                <CheckCircle2 className="h-5 w-5 text-db-green" />
              ) : (
                <Circle className="h-5 w-5 text-db-gray-border" strokeWidth={2} />
              )}
            </span>
            <div className="min-w-0 flex-1">
              <span
                className={cn(
                  "font-medium",
                  isChecked && "text-foreground"
                )}
              >
                {item.label}
              </span>
              {item.description && (
                <p className="mt-0.5 text-sm text-muted-foreground">
                  {item.description}
                </p>
              )}
            </div>
          </motion.li>
        );
      })}
    </ul>
  );
}
