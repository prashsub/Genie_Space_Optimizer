import { createFileRoute, useNavigate } from "@tanstack/react-router";
import { useGetRun } from "@/lib/api";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Progress } from "@/components/ui/progress";
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert";
import { Skeleton } from "@/components/ui/skeleton";
import { PipelineStepCard } from "@/components/PipelineStepCard";
import { LeverProgress } from "@/components/LeverProgress";
import { Suspense, useMemo } from "react";
import {
  ArrowLeft,
  ArrowRight,
  AlertTriangle,
  XCircle,
} from "lucide-react";
import { ErrorBoundary } from "react-error-boundary";

export const Route = createFileRoute("/runs/$runId")({
  component: () => (
    <ErrorBoundary fallback={<PipelineError />}>
      <Suspense fallback={<PipelineSkeleton />}>
        <PipelineView />
      </Suspense>
    </ErrorBoundary>
  ),
});

function PipelineError() {
  return (
    <Alert variant="destructive">
      <AlertTitle>Failed to load run</AlertTitle>
      <AlertDescription>
        Could not fetch pipeline status. The run may not exist.
      </AlertDescription>
    </Alert>
  );
}

function PipelineSkeleton() {
  return (
    <div className="space-y-4">
      <Skeleton className="h-8 w-48" />
      <Skeleton className="h-4 w-full" />
      <div className="space-y-3">
        {[1, 2, 3, 4, 5].map((i) => (
          <Skeleton key={i} className="h-20 rounded-lg" />
        ))}
      </div>
    </div>
  );
}

const TERMINAL_STATUSES = ["COMPLETED", "FAILED", "CANCELLED", "CONVERGED", "STALLED", "MAX_ITERATIONS", "APPLIED", "DISCARDED"];

export function estimateTimeRemaining(
  benchmarkCount: number,
  elapsedMs: number,
): string {
  const estimatedTotal = benchmarkCount * 12_000;
  const remaining = Math.max(0, estimatedTotal - elapsedMs);
  if (remaining === 0) return "Almost done…";
  return `~${Math.ceil(remaining / 60_000)}m remaining`;
}

function PipelineView() {
  const { runId } = Route.useParams();
  const navigate = useNavigate();

  const { data: runResponse } = useGetRun({
    params: { run_id: runId },
    query: {
      refetchInterval: (query: { state: { data?: { data?: { status?: string } } } }) => {
        const wrapped = query.state.data;
        const run = wrapped?.data ?? wrapped;
        const status = (run as { status?: string })?.status;
        if (status && TERMINAL_STATUSES.includes(status)) return false;
        return 5000;
      },
    },
  });

  const run = useMemo(() => {
    if (!runResponse) return null;
    return (runResponse as { data?: unknown }).data ?? runResponse;
  }, [runResponse]) as {
    runId: string;
    spaceId: string;
    spaceName: string;
    status: string;
    startedAt: string;
    steps: {
      stepNumber: number;
      name: string;
      status: string;
      durationSeconds?: number | null;
      summary?: string | null;
    }[];
    levers: {
      lever: number;
      name: string;
      status: string;
      patchCount: number;
      scoreBefore?: number | null;
      scoreAfter?: number | null;
      scoreDelta?: number | null;
      rollbackReason?: string | null;
    }[];
    baselineScore?: number | null;
    optimizedScore?: number | null;
    convergenceReason?: string | null;
  } | null;

  if (!run) return null;

  const completedSteps = run.steps.filter(
    (s) => s.status === "completed",
  ).length;
  const progressPct = (completedSteps / 5) * 100;
  const isCompleted = ["COMPLETED", "CONVERGED", "STALLED", "MAX_ITERATIONS"].includes(run.status);
  const isFailed = run.status === "FAILED";
  const isCancelled = run.status === "CANCELLED";

  return (
    <div className="space-y-6">
      <div className="space-y-1">
        <button
          className="mb-2 flex items-center gap-1 text-sm text-muted-foreground hover:text-foreground"
          onClick={() =>
            navigate({
              to: "/spaces/$spaceId",
              params: { spaceId: run.spaceId },
            })
          }
        >
          <ArrowLeft className="h-4 w-4" />
          Back to {run.spaceName || "Space"}
        </button>
        <h1 className="text-2xl font-bold">Optimization Pipeline</h1>
        <p className="text-sm text-muted-foreground">
          Run {run.runId.slice(0, 8)}… &middot;{" "}
          {run.status === "RUNNING" ? "In Progress" : run.status}
        </p>
      </div>

      <div className="space-y-1">
        <div className="flex items-center justify-between text-xs text-muted-foreground">
          <span>Progress</span>
          <span>
            {completedSteps}/5 steps
          </span>
        </div>
        <Progress value={progressPct} className="h-2" />
      </div>

      {isCompleted &&
        run.baselineScore != null &&
        run.optimizedScore != null && (
          <div className="grid grid-cols-1 gap-4 sm:grid-cols-3">
            <Card className="border-db-gray-border bg-white">
              <CardHeader className="pb-1">
                <CardTitle className="text-xs font-medium uppercase tracking-wide text-muted-foreground">
                  Baseline
                </CardTitle>
              </CardHeader>
              <CardContent>
                <span className="text-2xl font-bold">
                  {run.baselineScore.toFixed(1)}%
                </span>
              </CardContent>
            </Card>
            <Card className="border-db-blue/30 bg-white">
              <CardHeader className="pb-1">
                <CardTitle className="text-xs font-medium uppercase tracking-wide text-db-blue">
                  Optimized
                </CardTitle>
              </CardHeader>
              <CardContent>
                <span className="text-2xl font-bold text-db-blue">
                  {run.optimizedScore.toFixed(1)}%
                </span>
              </CardContent>
            </Card>
            <Card className="border-db-gray-border bg-white">
              <CardHeader className="pb-1">
                <CardTitle className="text-xs font-medium uppercase tracking-wide text-muted-foreground">
                  Improvement
                </CardTitle>
              </CardHeader>
              <CardContent>
                <span
                  className={`text-2xl font-bold ${
                    run.optimizedScore > run.baselineScore
                      ? "text-db-green"
                      : "text-muted-foreground"
                  }`}
                >
                  {run.optimizedScore > run.baselineScore ? "+" : ""}
                  {(run.optimizedScore - run.baselineScore).toFixed(1)}%
                </span>
              </CardContent>
            </Card>
          </div>
        )}

      <div className="space-y-3">
        {run.steps.map((step) => (
          <PipelineStepCard
            key={step.stepNumber}
            stepNumber={step.stepNumber}
            name={step.name}
            status={step.status}
            durationSeconds={step.durationSeconds}
            summary={step.summary}
          >
            {step.stepNumber === 4 &&
              (step.status === "running" || step.status === "completed") &&
              run.levers.length > 0 && (
                <LeverProgress levers={run.levers} />
              )}
          </PipelineStepCard>
        ))}
      </div>

      {isCompleted && (
        <div className="flex justify-center">
          <Button
            size="lg"
            className="bg-db-red hover:bg-db-red/90"
            onClick={() =>
              navigate({
                to: "/runs/$runId/comparison",
                params: { runId: run.runId },
              })
            }
          >
            View Results
            <ArrowRight className="ml-2 h-4 w-4" />
          </Button>
        </div>
      )}

      {isFailed && (
        <Alert variant="destructive">
          <XCircle className="h-4 w-4" />
          <AlertTitle>Pipeline Failed</AlertTitle>
          <AlertDescription>
            {run.convergenceReason || "An error occurred during optimization."}
            <button
              className="mt-2 block text-sm underline"
              onClick={() =>
                navigate({
                  to: "/spaces/$spaceId",
                  params: { spaceId: run.spaceId },
                })
              }
            >
              Return to Space Detail
            </button>
          </AlertDescription>
        </Alert>
      )}

      {isCancelled && (
        <Alert>
          <AlertTriangle className="h-4 w-4" />
          <AlertTitle>Pipeline Cancelled</AlertTitle>
          <AlertDescription>
            This optimization run was cancelled.
            <button
              className="mt-2 block text-sm underline"
              onClick={() =>
                navigate({
                  to: "/spaces/$spaceId",
                  params: { spaceId: run.spaceId },
                })
              }
            >
              Return to Space Detail
            </button>
          </AlertDescription>
        </Alert>
      )}
    </div>
  );
}
