import { createFileRoute, useNavigate } from "@tanstack/react-router";
import { useGetRun } from "@/lib/api";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Progress } from "@/components/ui/progress";
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert";
import { Skeleton } from "@/components/ui/skeleton";
import { Badge } from "@/components/ui/badge";
import { PipelineStepCard } from "@/components/PipelineStepCard";
import { LeverProgress } from "@/components/LeverProgress";
import { ResourceLinks } from "@/components/ResourceLinks";
import { Suspense, useEffect, useMemo, useState } from "react";
import {
  ArrowLeft,
  ArrowRight,
  AlertTriangle,
  XCircle,
  Clock,
  Loader2,
  Zap,
  TrendingUp,
  ExternalLink,
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
          <Skeleton key={i} className="h-24 rounded-lg" />
        ))}
      </div>
    </div>
  );
}

const TERMINAL_STATUSES = [
  "COMPLETED", "FAILED", "CANCELLED", "CONVERGED",
  "STALLED", "MAX_ITERATIONS", "APPLIED", "DISCARDED",
];

const STEP_DESCRIPTIONS: Record<number, string> = {
  1: "Reads the Genie Space configuration — tables, instructions, sample questions, and functions — to understand the current setup.",
  2: "Queries Unity Catalog for column-level metadata, tags, descriptions, and data profiles used to inform optimization decisions.",
  3: "Runs all benchmark questions through the Genie API with 8 evaluation judges to establish the current accuracy baseline.",
  4: "Applies up to 6 optimization levers (descriptions, instructions, synonyms, filters, etc.) and evaluates each change.",
  5: "Final evaluation pass on the optimized configuration with repeatability checks to confirm stable improvements.",
};

function useElapsedTime(startedAt: string | undefined, isActive: boolean) {
  const [elapsed, setElapsed] = useState(0);
  useEffect(() => {
    if (!isActive || !startedAt) {
      setElapsed(0);
      return;
    }
    const parsed = Date.parse(startedAt);
    if (Number.isNaN(parsed)) {
      setElapsed(0);
      return;
    }
    const tick = () =>
      setElapsed(Math.max(0, Math.floor((Date.now() - parsed) / 1000)));
    tick();
    const id = setInterval(tick, 1000);
    return () => clearInterval(id);
  }, [startedAt, isActive]);
  return elapsed;
}

function formatElapsed(seconds: number): string {
  const safe = Math.max(0, Math.floor(seconds));
  const h = Math.floor(safe / 3600);
  const m = Math.floor((safe % 3600) / 60);
  const s = safe % 60;
  const pad2 = (v: number) => String(v).padStart(2, "0");
  return `${pad2(h)}:${pad2(m)}:${pad2(s)}`;
}

function statusDisplayLabel(status: string): string {
  const map: Record<string, string> = {
    QUEUED: "Queued",
    RUNNING: "In Progress",
    IN_PROGRESS: "In Progress",
    CONVERGED: "Converged",
    STALLED: "Stalled",
    MAX_ITERATIONS: "Max Iterations Reached",
    FAILED: "Failed",
    CANCELLED: "Cancelled",
    APPLIED: "Applied",
    DISCARDED: "Discarded",
  };
  return map[status] || status;
}

function StatusBanner({
  status,
  elapsed,
  completedSteps,
}: {
  status: string;
  elapsed: number;
  completedSteps: number;
}) {
  if (status === "QUEUED") {
    return (
      <div className="rounded-lg border border-amber-200 bg-amber-50 p-4">
        <div className="flex items-start gap-3">
          <div className="relative mt-0.5">
            <div className="absolute inset-0 animate-ping rounded-full bg-amber-400/30" />
            <Clock className="relative h-5 w-5 text-amber-600" />
          </div>
          <div className="flex-1">
            <h3 className="text-sm font-semibold text-amber-900">
              Queued &mdash; Waiting for Resources
            </h3>
            <p className="mt-0.5 text-xs text-amber-700">
              Your optimization run has been submitted and will start once
              serverless compute is allocated. This typically takes 1-2 minutes.
            </p>
          </div>
          {elapsed > 0 && (
            <Badge variant="outline" className="shrink-0 border-amber-300 text-amber-700">
              {formatElapsed(elapsed)}
            </Badge>
          )}
        </div>
      </div>
    );
  }

  if (status === "RUNNING" || status === "IN_PROGRESS") {
    return (
      <div className="rounded-lg border border-db-blue/20 bg-db-blue/5 p-4">
        <div className="flex items-center gap-3">
          <Loader2 className="h-5 w-5 animate-spin text-db-blue" />
          <div className="flex-1">
            <h3 className="text-sm font-semibold text-db-blue">
              Optimization In Progress
            </h3>
            <p className="text-xs text-db-blue/70">
              Step {completedSteps + 1} of 5 &middot; Polling for updates every 5 seconds
            </p>
          </div>
          <Badge variant="outline" className="shrink-0 border-db-blue/30 text-db-blue">
            {formatElapsed(elapsed)}
          </Badge>
        </div>
      </div>
    );
  }

  return null;
}

function PipelineView() {
  const { runId } = Route.useParams();
  const navigate = useNavigate();

  const { data: runResponse } = useGetRun({
    params: { run_id: runId },
    query: {
      refetchInterval: (query) => {
        const wrapped = query.state.data as { data?: { status?: string } } | undefined;
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
      inputs?: Record<string, unknown> | null;
      outputs?: Record<string, unknown> | null;
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
      patches?: {
        patchType?: string;
        scope?: string;
        riskLevel?: string;
        targetObject?: string;
        rolledBack?: boolean;
        rollbackReason?: string | null;
        command?: unknown;
        patch?: unknown;
        appliedAt?: string | null;
      }[];
      iterations?: {
        iteration: number;
        status: string;
        patchCount?: number;
        patchTypes?: string[];
        scoreBefore?: number | null;
        scoreAfter?: number | null;
        scoreDelta?: number | null;
        sliceAccuracy?: number | null;
        p0Accuracy?: number | null;
        fullAccuracy?: number | null;
        totalQuestions?: number | null;
        correctCount?: number | null;
        judgeScores?: Record<string, number | null>;
        mlflowRunId?: string | null;
        rollbackReason?: string | null;
        patches?: {
          patchType?: string;
          scope?: string;
          riskLevel?: string;
          targetObject?: string;
          rolledBack?: boolean;
          rollbackReason?: string | null;
          command?: unknown;
          appliedAt?: string | null;
        }[];
        stageEvents?: {
          stage?: string;
          status?: string;
          durationSeconds?: number | null;
          errorMessage?: string | null;
        }[];
      }[];
    }[];
    baselineScore?: number | null;
    optimizedScore?: number | null;
    convergenceReason?: string | null;
    links?: { label: string; url: string; category: string }[];
  } | null;

  const isActive = run
    ? !TERMINAL_STATUSES.includes(run.status)
    : false;
  const elapsed = useElapsedTime(run?.startedAt, isActive);
  const workflowLink = run?.links?.find((link) => link.category === "job")?.url;

  if (!run) return null;

  const completedSteps = run.steps.filter(
    (s) => s.status === "completed",
  ).length;
  const runningStep = run.steps.find((s) => s.status === "running");
  const progressPct = run.steps.reduce((acc, s) => {
    if (s.status === "completed") return acc + 20;
    if (s.status === "running") return acc + 10;
    return acc;
  }, 0);
  const isCompleted = ["COMPLETED", "CONVERGED", "STALLED", "MAX_ITERATIONS"].includes(run.status);
  const isFailed = run.status === "FAILED";
  const isCancelled = run.status === "CANCELLED";

  return (
    <div className="space-y-6">
      {/* Header */}
      <div className="flex items-start justify-between gap-4">
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
          <div className="flex items-center gap-3">
            <h1 className="text-2xl font-bold">Optimization Pipeline</h1>
            <Badge
              variant="outline"
              className={
                isActive
                  ? "border-db-blue/30 text-db-blue"
                  : isCompleted
                    ? "border-db-green/30 text-db-green"
                    : isFailed
                      ? "border-db-red-error/30 text-db-red-error"
                      : "text-muted-foreground"
              }
            >
              {isActive && <Loader2 className="mr-1 h-3 w-3 animate-spin" />}
              {statusDisplayLabel(run.status)}
            </Badge>
          </div>
          <p className="text-sm text-muted-foreground">
            Run <code className="rounded bg-muted px-1 py-0.5 font-mono text-xs">{run.runId.slice(0, 8)}</code>
            {run.startedAt && (
              <> &middot; Started {new Date(run.startedAt).toLocaleString()}</>
            )}
          </p>
        </div>
        {workflowLink && (
          <Button variant="outline" size="sm" asChild>
            <a href={workflowLink} target="_blank" rel="noopener noreferrer">
              Open in Workflows
              <ExternalLink className="ml-2 h-3.5 w-3.5" />
            </a>
          </Button>
        )}
      </div>

      {/* Status Banner */}
      <StatusBanner
        status={run.status}
        elapsed={elapsed}
        completedSteps={completedSteps}
      />

      {/* Progress */}
      <div className="space-y-2">
        <div className="flex items-center justify-between text-xs text-muted-foreground">
          <span className="flex items-center gap-1.5">
            {isActive && <Loader2 className="h-3 w-3 animate-spin" />}
            {isActive
              ? runningStep
                ? `Running: ${runningStep.name}`
                : "Waiting to start…"
              : isCompleted
                ? "All steps complete"
                : "Progress"}
          </span>
          <span className="tabular-nums font-medium">
            {completedSteps}/5 steps
          </span>
        </div>
        <div className="relative">
          <Progress
            value={progressPct}
            className={`h-2.5 ${isActive ? "[&>div]:bg-db-blue" : isCompleted ? "[&>div]:bg-db-green" : ""}`}
          />
          {isActive && progressPct > 0 && progressPct < 100 && (
            <div
              className="absolute top-0 h-2.5 animate-pulse rounded-full bg-db-blue/30"
              style={{ width: `${progressPct}%` }}
            />
          )}
        </div>
      </div>

      {/* Score Summary Cards (shown when completed) */}
      {isCompleted &&
        run.baselineScore != null &&
        run.optimizedScore != null && (
          <div className="grid grid-cols-1 gap-4 sm:grid-cols-3">
            <Card className="border-db-gray-border bg-white">
              <CardHeader className="pb-1">
                <CardTitle className="flex items-center gap-1.5 text-xs font-medium uppercase tracking-wide text-muted-foreground">
                  <FlaskConicalIcon />
                  Baseline
                </CardTitle>
              </CardHeader>
              <CardContent>
                <span className="text-2xl font-bold">
                  {run.baselineScore.toFixed(1)}%
                </span>
              </CardContent>
            </Card>
            <Card className="border-db-blue/30 bg-db-blue/5">
              <CardHeader className="pb-1">
                <CardTitle className="flex items-center gap-1.5 text-xs font-medium uppercase tracking-wide text-db-blue">
                  <Zap className="h-3.5 w-3.5" />
                  Optimized
                </CardTitle>
              </CardHeader>
              <CardContent>
                <span className="text-2xl font-bold text-db-blue">
                  {run.optimizedScore.toFixed(1)}%
                </span>
              </CardContent>
            </Card>
            <Card className={`border-db-gray-border bg-white ${
              run.optimizedScore > run.baselineScore ? "border-db-green/30 bg-db-green/5" : ""
            }`}>
              <CardHeader className="pb-1">
                <CardTitle className="flex items-center gap-1.5 text-xs font-medium uppercase tracking-wide text-muted-foreground">
                  <TrendingUp className="h-3.5 w-3.5" />
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

      {/* Pipeline Steps */}
      <div className="relative space-y-3">
        {/* Vertical connector line */}
        <div className="absolute left-[2.05rem] top-4 -z-10 h-[calc(100%-2rem)] w-px bg-gradient-to-b from-db-gray-border via-db-gray-border to-transparent" />

        {run.steps.map((step) => (
          <PipelineStepCard
            key={step.stepNumber}
            stepNumber={step.stepNumber}
            name={step.name}
            status={step.status}
            description={STEP_DESCRIPTIONS[step.stepNumber]}
            durationSeconds={step.durationSeconds}
            summary={step.summary}
          >
            <StepInsights step={step} links={run.links ?? []} />
            {step.stepNumber === 4 &&
              run.levers.length > 0 && (
                <LeverProgress levers={run.levers} links={run.links ?? []} />
              )}
          </PipelineStepCard>
        ))}
      </div>

      {/* Resource Links */}
      {run.links && run.links.length > 0 && (
        <ResourceLinks links={run.links} />
      )}

      {/* Completed CTA */}
      {isCompleted && (
        <div className="flex justify-center pt-2">
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
            View Results & Compare
            <ArrowRight className="ml-2 h-4 w-4" />
          </Button>
        </div>
      )}

      {/* Failed Alert */}
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

      {/* Cancelled Alert */}
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

function FlaskConicalIcon() {
  return (
    <svg className="h-3.5 w-3.5" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
      <path d="M10 2v7.527a2 2 0 0 1-.211.896L4.72 20.55a1 1 0 0 0 .9 1.45h12.76a1 1 0 0 0 .9-1.45l-5.069-10.127A2 2 0 0 1 14 9.527V2" />
      <path d="M8.5 2h7" />
    </svg>
  );
}

function StepInsights({
  step,
  links,
}: {
  step: {
    stepNumber: number;
    outputs?: Record<string, unknown> | null;
  };
  links: { label: string; url: string; category: string }[];
}) {
  const outputs = step.outputs ?? {};
  if (!Object.keys(outputs).length) return null;

  if (step.stepNumber === 2) {
    const columnsCollected = outputs.columnsCollected as number | undefined;
    const tagsCollected = outputs.tagsCollected as number | undefined;
    const routinesCollected = outputs.routinesCollected as number | undefined;
    const columnSamples = (outputs.columnSamples as string[] | undefined) ?? [];
    const tagSamples = (outputs.tagSamples as string[] | undefined) ?? [];
    const routineSamples = (outputs.routineSamples as string[] | undefined) ?? [];
    const tableRefCount = outputs.tableRefCount as number | undefined;
    const referencedSchemaCount = outputs.referencedSchemaCount as number | undefined;
    const referencedSchemas = (outputs.referencedSchemas as string[] | undefined) ?? [];
    const collectionScope = outputs.collectionScope as string | undefined;
    const metadataSource = outputs.metadataSource as
      | { columns?: string; tags?: string; routines?: string }
      | undefined;

    return (
      <div className="space-y-2 text-xs">
        <div className="flex flex-wrap gap-2">
          <Badge variant="secondary">Columns: {columnsCollected ?? "?"}</Badge>
          <Badge variant="secondary">Tags: {tagsCollected ?? "?"}</Badge>
          <Badge variant="secondary">Routines: {routinesCollected ?? "?"}</Badge>
          {tableRefCount != null && (
            <Badge variant="secondary">Referenced assets: {tableRefCount}</Badge>
          )}
          {referencedSchemaCount != null && (
            <Badge variant="secondary">Referenced schemas: {referencedSchemaCount}</Badge>
          )}
        </div>
        {collectionScope && (
          <p className="text-muted-foreground">
            Collection scope: {collectionScope === "genie_assets" ? "Genie referenced assets" : "Catalog/schema fallback"}
          </p>
        )}
        {metadataSource && (
          <p className="text-muted-foreground">
            Source - columns: {metadataSource.columns ?? "unknown"}, tags: {metadataSource.tags ?? "unknown"}, routines: {metadataSource.routines ?? "unknown"}
          </p>
        )}
        {referencedSchemas.length > 0 && (
          <p className="text-muted-foreground">
            Schemas: {referencedSchemas.slice(0, 6).join(", ")}
          </p>
        )}
        {columnSamples.length > 0 && (
          <p className="text-muted-foreground">Sample columns: {columnSamples.slice(0, 8).join(", ")}</p>
        )}
        {tagSamples.length > 0 && (
          <p className="text-muted-foreground">Sample tags: {tagSamples.slice(0, 5).join(", ")}</p>
        )}
        {routineSamples.length > 0 && (
          <p className="text-muted-foreground">Sample routines: {routineSamples.slice(0, 5).join(", ")}</p>
        )}
      </div>
    );
  }

  if (step.stepNumber === 3) {
    const judgeScores = (outputs.judgeScores as Record<string, number | null> | undefined) ?? {};
    const sampleQuestions = (outputs.sampleQuestions as Record<string, unknown>[] | undefined) ?? [];
    const invalidBenchmarkCount = outputs.invalidBenchmarkCount as number | undefined;
    const permissionBlockedCount = outputs.permissionBlockedCount as number | undefined;
    const unresolvedColumnCount = outputs.unresolvedColumnCount as number | undefined;
    const harnessRetryCount = outputs.harnessRetryCount as number | undefined;
    const evalUrlFromOutput = outputs.evaluationRunUrl as string | undefined;
    const evalUrlFromLinks = links.find(
      (l) => l.category === "mlflow" && l.label.toLowerCase().includes("baseline"),
    )?.url;
    const evalUrl = evalUrlFromOutput ?? evalUrlFromLinks;

    return (
      <div className="space-y-3">
        <div className="flex flex-wrap gap-2">
          {invalidBenchmarkCount != null && (
            <Badge variant="secondary">Invalid benchmarks: {invalidBenchmarkCount}</Badge>
          )}
          {permissionBlockedCount != null && (
            <Badge variant="secondary">Permission blocked: {permissionBlockedCount}</Badge>
          )}
          {unresolvedColumnCount != null && (
            <Badge variant="secondary">Unresolved columns: {unresolvedColumnCount}</Badge>
          )}
          {harnessRetryCount != null && (
            <Badge variant="secondary">Harness retries: {harnessRetryCount}</Badge>
          )}
        </div>

        {!!Object.keys(judgeScores).length && (
          <div className="space-y-1">
            <p className="text-xs font-medium text-muted-foreground">Judge scores</p>
            <div className="flex flex-wrap gap-2">
              {Object.entries(judgeScores).map(([judge, score]) => (
                <Badge key={judge} variant="secondary">
                  {judge}: {score != null ? `${score.toFixed(1)}%` : "n/a"}
                </Badge>
              ))}
            </div>
          </div>
        )}

        {sampleQuestions.length > 0 && (
          <div className="space-y-1">
            <p className="text-xs font-medium text-muted-foreground">Sample evaluated questions</p>
            <div className="space-y-1">
              {sampleQuestions.slice(0, 3).map((row, idx) => (
                <div key={idx} className="rounded border bg-muted/30 px-2 py-1 text-xs">
                  <p className="font-medium">{String(row.question ?? "Question")}</p>
                  <p className="text-muted-foreground">
                    result_correctness: {String(row.resultCorrectness ?? "n/a")}
                    {row.matchType ? ` · match: ${String(row.matchType)}` : ""}
                  </p>
                </div>
              ))}
            </div>
          </div>
        )}

        {evalUrl && (
          <Button variant="outline" size="sm" asChild>
            <a href={evalUrl} target="_blank" rel="noopener noreferrer">
              Open baseline evaluation run
              <ExternalLink className="ml-1.5 h-3.5 w-3.5" />
            </a>
          </Button>
        )}
      </div>
    );
  }

  return null;
}
