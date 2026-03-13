"use client";

import type { ReactNode } from "react";
import { cn } from "@/lib/utils";
import { Accordion, AccordionItem, AccordionTrigger, AccordionContent } from "@/components/ui/accordion";
import { PIPELINE_GROUP_COLORS } from "./data";

interface LearnMoreItem {
  id: string;
  title: string;
  content: ReactNode;
}

export interface StageScreenProps {
  title: string;
  subtitle: string;
  pipelineGroup: string;
  visual: ReactNode;
  explanation: ReactNode;
  learnMore?: LearnMoreItem[];
  children?: ReactNode;
}

export function StageScreen({
  title,
  subtitle,
  pipelineGroup,
  visual,
  explanation,
  learnMore,
  children,
}: StageScreenProps) {
  const colors = PIPELINE_GROUP_COLORS[pipelineGroup] ?? PIPELINE_GROUP_COLORS.neutral;

  return (
    <div className={cn("space-y-6 rounded-lg border-l-4 p-4 lg:p-6", colors.border, colors.bg)}>
      {/* Header */}
      <div>
        <h2 className="text-2xl font-bold tracking-tight">{title}</h2>
        <p className="mt-1 text-sm text-muted-foreground">{subtitle}</p>
      </div>

      {/* Primary visual (hero) */}
      <div className="min-h-[40vh]">{visual}</div>

      {/* Explanation */}
      <div className="max-w-prose text-sm leading-relaxed text-muted-foreground">
        {explanation}
      </div>

      {/* Extra children slot */}
      {children}

      {/* Learn more accordions */}
      {learnMore && learnMore.length > 0 && (
        <div className="space-y-2">
          <p className="text-xs font-medium uppercase tracking-wider text-muted-foreground">
            Learn more
          </p>
          <Accordion type="multiple" className="space-y-2">
            {learnMore.map((item) => (
              <AccordionItem
                key={item.id}
                value={item.id}
                className="rounded-lg border border-db-gray-border bg-white px-4"
              >
                <AccordionTrigger className="text-sm font-medium">
                  {item.title}
                </AccordionTrigger>
                <AccordionContent className="text-sm text-muted-foreground">
                  {item.content}
                </AccordionContent>
              </AccordionItem>
            ))}
          </Accordion>
        </div>
      )}
    </div>
  );
}
