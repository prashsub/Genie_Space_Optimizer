"use client";

import { ArrowDownRight } from "lucide-react";
import { StageScreen } from "../StageScreen";
import { GateFunnel } from "../shared/GateFunnel";
import { GATE_DEFINITIONS } from "../data";

export function ThreeGatesStage() {
  const visual = (
    <div className="flex flex-col items-center gap-6">
      <GateFunnel className="w-full max-w-md" />
      <div className="flex items-center gap-2 rounded-lg border border-amber-200 bg-amber-50 px-4 py-2 text-sm">
        <ArrowDownRight className="h-5 w-5 text-amber-600 shrink-0" aria-hidden />
        <span className="text-amber-800">
          <strong>Fast fail:</strong> Slice Gate failure skips P0 and Full gates — saving compute and catching regressions early
        </span>
      </div>
    </div>
  );

  const explanation = (
    <p>
      Every set of patches must pass through three progressively expensive evaluation gates. If the cheap Slice Gate catches a regression, the expensive Full Gate is never run — saving compute and catching problems early. This builds trust: the system won&apos;t make things worse.
    </p>
  );

  const sliceGate = GATE_DEFINITIONS[0];
  const p0Gate = GATE_DEFINITIONS[1];
  const fullGate = GATE_DEFINITIONS[2];

  const learnMore = [
    {
      id: "slice-gate",
      title: "Slice Gate",
      content: (
        <div className="space-y-2 text-sm">
          <p>{sliceGate?.description}</p>
          <p><strong>Question selection:</strong> {sliceGate?.questionSelection}</p>
          <p><strong>Pass criteria:</strong> {sliceGate?.passCriteria}</p>
          <p><strong>Tolerance:</strong> {sliceGate?.tolerance}</p>
        </div>
      ),
    },
    {
      id: "p0-gate",
      title: "P0 Gate",
      content: (
        <div className="space-y-2 text-sm">
          <p>{p0Gate?.description}</p>
          <p><strong>Question selection:</strong> {p0Gate?.questionSelection}</p>
          <p><strong>Pass criteria:</strong> {p0Gate?.passCriteria}</p>
          <p><strong>Tolerance:</strong> {p0Gate?.tolerance}</p>
        </div>
      ),
    },
    {
      id: "full-gate",
      title: "Full Gate",
      content: (
        <div className="space-y-2 text-sm">
          <p>{fullGate?.description}</p>
          <p><strong>Question selection:</strong> {fullGate?.questionSelection}</p>
          <p><strong>Pass criteria:</strong> {fullGate?.passCriteria}</p>
          <p><strong>Tolerance:</strong> {fullGate?.tolerance}</p>
          <p><strong>Accuracy guard:</strong> detect_regressions with REGRESSION_THRESHOLD=5%.</p>
        </div>
      ),
    },
    {
      id: "rollback",
      title: "Rollback Mechanism",
      content: (
        <div className="space-y-2 text-sm">
          <p><strong>Primary:</strong> Restore from <code className="rounded bg-db-gray-bg px-1.5 py-0.5">pre_snapshot</code> (the config state before patches were applied).</p>
          <p><strong>Fallback:</strong> Individual <code className="rounded bg-db-gray-bg px-1.5 py-0.5">rollback_commands</code> per patch if snapshot restore fails.</p>
        </div>
      ),
    },
    {
      id: "reflection-buffer",
      title: "Reflection Buffer",
      content: (
        <div className="space-y-2 text-sm">
          <p><strong>Entry shape:</strong> 17 fields per reflection entry.</p>
          <p><strong>Rendering for strategist:</strong> REFLECTION_WINDOW_FULL=3 — the strategist sees the last 3 iterations of reflection context.</p>
          <p><strong>DO NOT RETRY block:</strong> Failed strategies are recorded so the strategist won&apos;t retry the same approach.</p>
        </div>
      ),
    },
  ];

  return (
    <StageScreen
      title="The 3-Gate Pattern"
      subtitle="Validate improvements, catch regressions"
      pipelineGroup="leverLoop"
      visual={visual}
      explanation={explanation}
      learnMore={learnMore}
    />
  );
}
