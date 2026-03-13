"use client";

import { ArrowRight } from "lucide-react";
import { motion } from "motion/react";
import { cn } from "@/lib/utils";
import { StageScreen } from "../StageScreen";
import { ScoreGauge } from "../shared/ScoreGauge";
import { JUDGES, JUDGE_CATEGORY_COLORS } from "../data";

const FLOW_NODES = [
  "Benchmarks",
  "Genie Space API",
  "9 Judges",
  "Aggregated Scores",
];

const EVALUATION_ORCHESTRATION_STEPS = [
  "make_predict_fn closure — captures Genie Space config and benchmark dataset",
  "run_evaluation DataFrame — builds input for mlflow.genai.evaluate",
  "mlflow.genai.evaluate with 9 scorers — runs all judges per question",
  "Retry logic: EVAL_MAX_ATTEMPTS=4 with exponential sleep on transient failures",
  "Sequential fallback on total failure — falls back to per-question evaluation",
];

/** Abbreviations for judge icons (for layoutId animation to JudgesStage) */
const JUDGE_ABBREVS: Record<string, string> = {
  syntax_validity: "SV",
  schema_accuracy: "SA",
  logical_accuracy: "LA",
  semantic_equivalence: "SE",
  completeness: "CO",
  response_quality: "RQ",
  result_correctness: "RC",
  asset_routing: "AR",
  arbiter: "AB",
};

export function BaselineStage() {
  return (
    <StageScreen
      title="Baseline Evaluation"
      subtitle="Establish quality with 9 judges"
      pipelineGroup="baseline"
      visual={
        <div className="flex flex-col items-center gap-8">
          {/* Animated flow: 4 nodes connected by arrows */}
          <div className="flex flex-wrap items-center justify-center gap-2">
            {FLOW_NODES.map((label, i) => (
              <div key={label} className="flex items-center gap-2">
                <motion.div
                  className="rounded-lg border border-db-gray-border bg-db-gray-bg px-4 py-2.5 text-sm font-medium"
                  initial={{ opacity: 0, scale: 0.9 }}
                  animate={{ opacity: 1, scale: 1 }}
                  transition={{ delay: i * 0.15, duration: 0.3 }}
                >
                  {label}
                </motion.div>
                {i < FLOW_NODES.length - 1 && (
                  <motion.div
                    initial={{ opacity: 0 }}
                    animate={{ opacity: 1 }}
                    transition={{ delay: i * 0.15 + 0.2 }}
                  >
                    <ArrowRight className="h-4 w-4 shrink-0 text-muted-foreground" />
                  </motion.div>
                )}
              </div>
            ))}
          </div>

          {/* Baseline accuracy ScoreGauge */}
          <div className="w-full max-w-xs">
            <ScoreGauge
              value={72}
              label="Baseline accuracy"
              threshold={72}
              color="bg-purple-500"
            />
          </div>

          {/* 9 judge icons — layoutId for animation to JudgesStage */}
          <div className="flex flex-wrap items-center justify-center gap-2">
            {JUDGES.map((judge, i) => (
              <motion.div
                key={judge.name}
                layoutId={`judge-${judge.name}`}
                className={cn(
                  "flex h-9 w-9 shrink-0 items-center justify-center rounded-full text-xs font-semibold text-white",
                  JUDGE_CATEGORY_COLORS[judge.category]?.replace("border-l-", "bg-") ?? "bg-gray-400"
                )}
                initial={{ opacity: 0, scale: 0.8 }}
                animate={{ opacity: 1, scale: 1 }}
                transition={{ delay: 0.5 + i * 0.05, duration: 0.25 }}
              >
                {JUDGE_ABBREVS[judge.name] ?? judge.number}
              </motion.div>
            ))}
          </div>
        </div>
      }
      explanation={
        <p>
          Every benchmark question is sent to the Genie Space, and the response is
          evaluated by 9 specialized judges. The result is a baseline accuracy
          score that tells us how good the space is before optimization.
        </p>
      }
      learnMore={[
        {
          id: "evaluation-orchestration",
          title: "Evaluation Orchestration",
          content: (
            <ol className="list-decimal space-y-2 pl-4">
              {EVALUATION_ORCHESTRATION_STEPS.map((step, i) => (
                <li key={i}>{step}</li>
              ))}
            </ol>
          ),
        },
      ]}
    />
  );
}
