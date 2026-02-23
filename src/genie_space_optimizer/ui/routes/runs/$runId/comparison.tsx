import { createFileRoute, useNavigate } from "@tanstack/react-router";
import {
  useGetComparisonSuspense,
  useApplyOptimization,
  useDiscardOptimization,
} from "@/lib/api";
import selector from "@/lib/selector";
import { Button } from "@/components/ui/button";
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert";
import { Skeleton } from "@/components/ui/skeleton";
import { ScoreCard } from "@/components/ScoreCard";
import { ConfigDiff } from "@/components/ConfigDiff";
import { Suspense } from "react";
import { ArrowLeft, Check, X } from "lucide-react";
import { ErrorBoundary } from "react-error-boundary";
import { toast } from "sonner";

export const Route = createFileRoute("/runs/$runId/comparison")({
  component: () => (
    <ErrorBoundary fallback={<ComparisonError />}>
      <Suspense fallback={<ComparisonSkeleton />}>
        <ComparisonView />
      </Suspense>
    </ErrorBoundary>
  ),
});

function ComparisonError() {
  return (
    <Alert variant="destructive">
      <AlertTitle>Failed to load comparison</AlertTitle>
      <AlertDescription>
        Could not load comparison data for this run.
      </AlertDescription>
    </Alert>
  );
}

function ComparisonSkeleton() {
  return (
    <div className="space-y-6">
      <Skeleton className="h-8 w-64" />
      <div className="grid grid-cols-1 gap-4 sm:grid-cols-3">
        {[1, 2, 3].map((i) => (
          <Skeleton key={i} className="h-24 rounded-lg" />
        ))}
      </div>
      <Skeleton className="h-64 rounded-lg" />
    </div>
  );
}

function ComparisonView() {
  const { runId } = Route.useParams();
  const navigate = useNavigate();
  const { data: comparison } = useGetComparisonSuspense({
    params: { run_id: runId },
    ...selector(),
  });
  const applyMut = useApplyOptimization();
  const discardMut = useDiscardOptimization();

  function handleApply() {
    applyMut.mutate(
      { params: { run_id: runId } },
      {
        onSuccess: () => {
          toast.success("Optimization applied!", {
            description: "Changes have been saved to your Genie Space.",
          });
          navigate({ to: "/" });
        },
        onError: () => {
          toast.error("Failed to apply optimization");
        },
      },
    );
  }

  function handleDiscard() {
    discardMut.mutate(
      { params: { run_id: runId } },
      {
        onSuccess: () => {
          toast.info("Optimization discarded", {
            description: "Changes have been rolled back.",
          });
          navigate({ to: "/" });
        },
        onError: () => {
          toast.error("Failed to discard optimization");
        },
      },
    );
  }

  if (!comparison) return null;

  const actionDisabled = applyMut.isPending || discardMut.isPending;

  return (
    <div className="space-y-6">
      <div className="flex items-start justify-between gap-4">
        <div className="space-y-1">
          <button
            className="mb-2 flex items-center gap-1 text-sm text-muted-foreground hover:text-foreground"
            onClick={() =>
              navigate({ to: "/runs/$runId", params: { runId } })
            }
          >
            <ArrowLeft className="h-4 w-4" />
            Back to Pipeline
          </button>
          <h1 className="text-2xl font-bold">Optimization Results</h1>
          <p className="text-sm text-muted-foreground">
            Compare baseline vs. optimized configuration for{" "}
            {comparison.spaceName}
          </p>
        </div>
        <div className="flex items-center gap-2">
          <Button
            variant="outline"
            onClick={handleDiscard}
            disabled={actionDisabled}
          >
            <X className="mr-2 h-4 w-4" />
            {discardMut.isPending ? "Discarding…" : "Discard"}
          </Button>
          <Button
            onClick={handleApply}
            disabled={actionDisabled}
            className="bg-db-green hover:bg-db-green/90"
          >
            <Check className="mr-2 h-4 w-4" />
            {applyMut.isPending ? "Applying…" : "Apply Changes"}
          </Button>
        </div>
      </div>

      <ScoreCard
        baselineScore={comparison.baselineScore}
        optimizedScore={comparison.optimizedScore}
        improvementPct={comparison.improvementPct}
        perDimensionScores={comparison.perDimensionScores}
      />

      <ConfigDiff
        original={comparison.original}
        optimized={comparison.optimized}
      />

      <div className="flex justify-end gap-2 border-t border-db-gray-border pt-4">
        <Button
          variant="outline"
          onClick={handleDiscard}
          disabled={actionDisabled}
        >
          <X className="mr-2 h-4 w-4" />
          {discardMut.isPending ? "Discarding…" : "Discard"}
        </Button>
        <Button
          onClick={handleApply}
          disabled={actionDisabled}
          className="bg-db-green hover:bg-db-green/90"
        >
          <Check className="mr-2 h-4 w-4" />
          {applyMut.isPending ? "Applying…" : "Apply Changes"}
        </Button>
      </div>
    </div>
  );
}
