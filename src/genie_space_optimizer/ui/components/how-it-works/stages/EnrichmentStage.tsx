"use client";

import { ArrowRight, Merge } from "lucide-react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { cn } from "@/lib/utils";
import { ExpandableCard } from "../shared/ExpandableCard";
import { BeforeAfterSplit } from "../shared/BeforeAfterSplit";
import { ENRICHMENT_SUBSTAGES } from "../data";

const ACCENT_COLORS = [
  "border-l-blue-500",
  "border-l-indigo-500",
  "border-l-purple-500",
  "border-l-emerald-500",
  "border-l-orange-500",
  "border-l-amber-500",
  "border-l-teal-500",
  "border-l-cyan-500",
  "border-l-pink-500",
];

const COLUMN_DESCRIPTIONS_BEFORE = {
  title: "Before",
  items: [
    "revenue_amount — (no description)",
    "region_code — (no description)",
    "order_date — (no description)",
  ],
};

const COLUMN_DESCRIPTIONS_AFTER = {
  title: "After",
  items: [
    "DEFINITION: Total gross revenue in USD\nSYNONYMS: revenue, total_sales",
    "DEFINITION: Geographic sales region\nVALUES: US, EMEA, APAC",
    "DEFINITION: Date the order was placed\nSYNONYMS: purchase_date",
  ],
};

const JOIN_DISCOVERY_FLOW = [
  "FK Constraints (Tier 0)",
  "merge",
  "Execution-Proven JOINs (Tier 1)",
  "Type Corroboration",
  "Validated Join Specs",
];

const SUMMARY_ITEMS = [
  { label: "Columns enriched", value: "~15-30" },
  { label: "Join specs discovered", value: "3-8" },
  { label: "Instructions seeded", value: "yes/no" },
  { label: "Example SQLs added", value: "2-5" },
];

export function EnrichmentStage() {
  return (
    <div className="space-y-8">
      <p className="rounded-lg border border-amber-200 bg-amber-50 p-4 text-sm text-amber-900">
        If baseline already meets all quality thresholds, enrichment is skipped
        entirely.
      </p>

      <div className="space-y-4">
        {ENRICHMENT_SUBSTAGES.map((substage, idx) => {
          const Icon = substage.icon;
          const accentColor =
            ACCENT_COLORS[idx % ACCENT_COLORS.length] ?? "border-l-blue-500";

          return (
            <ExpandableCard
              key={substage.id}
              title={substage.title}
              subtitle={substage.description}
              icon={Icon}
              accentColor={accentColor}
            >
              <div className="space-y-4">
                <ul className="list-inside list-disc space-y-1 text-sm">
                  {substage.details.map((d, i) => (
                    <li key={i}>{d}</li>
                  ))}
                </ul>

                {substage.id === "column-descriptions" && (
                  <>
                    <BeforeAfterSplit
                      before={COLUMN_DESCRIPTIONS_BEFORE}
                      after={COLUMN_DESCRIPTIONS_AFTER}
                    />
                    <p className="text-sm text-muted-foreground">
                      Column classification: dimension (→ definition, values,
                      synonyms) | measure (→ definition, aggregation, grain_note,
                      synonyms) | key (→ definition, join, synonyms)
                    </p>
                  </>
                )}

                {substage.id === "join-discovery" && (
                  <div className="flex flex-wrap items-center gap-2">
                    {JOIN_DISCOVERY_FLOW.map((node, i) => (
                      <div
                        key={i}
                        className="flex items-center gap-2"
                      >
                        {node === "merge" ? (
                          <Merge className="h-4 w-4 text-muted-foreground" />
                        ) : (
                          <div className="rounded-lg border border-db-gray-border bg-db-gray-bg px-3 py-2 text-sm font-medium">
                            {node}
                          </div>
                        )}
                        {i < JOIN_DISCOVERY_FLOW.length - 1 && (
                          <ArrowRight className="h-4 w-4 shrink-0 text-muted-foreground" />
                        )}
                      </div>
                    ))}
                  </div>
                )}
              </div>
            </ExpandableCard>
          );
        })}
      </div>

      <Card>
        <CardHeader>
          <CardTitle className="text-base">
            Typical Enrichment Results
          </CardTitle>
        </CardHeader>
        <CardContent>
          <div className="grid gap-3 sm:grid-cols-2">
            {SUMMARY_ITEMS.map(({ label, value }) => (
              <div
                key={label}
                className="flex justify-between rounded border border-db-gray-border px-3 py-2"
              >
                <span className="text-sm text-muted-foreground">{label}:</span>
                <span className="text-sm font-medium">{value}</span>
              </div>
            ))}
          </div>
        </CardContent>
      </Card>

      <p
        className={cn(
          "rounded-lg border border-db-blue/30 bg-db-blue/5 p-4 text-sm"
        )}
      >
        All enrichment changes use lever=0 (proactive enrichment), which has the
        widest section ownership. The lever loop (levers 1-5) can later refine
        but not overwrite these foundational descriptions.
      </p>
    </div>
  );
}
