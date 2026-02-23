import { createFileRoute, useNavigate } from "@tanstack/react-router";
import {
  useGetSpaceDetailSuspense,
  useStartOptimization,
} from "@/lib/api";
import selector from "@/lib/selector";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Skeleton } from "@/components/ui/skeleton";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert";
import { Suspense } from "react";
import {
  ArrowLeft,
  Rocket,
  FileText,
  HelpCircle,
  Database,
  History,
} from "lucide-react";
import { ErrorBoundary } from "react-error-boundary";

export const Route = createFileRoute("/spaces/$spaceId")({
  component: () => (
    <ErrorBoundary fallback={<SpaceDetailError />}>
      <Suspense fallback={<SpaceDetailSkeleton />}>
        <SpaceDetail />
      </Suspense>
    </ErrorBoundary>
  ),
});

function SpaceDetailError() {
  return (
    <Alert variant="destructive">
      <AlertTitle>Failed to load space</AlertTitle>
      <AlertDescription>
        Could not load space details. The space may not exist or the server is
        unavailable.
      </AlertDescription>
    </Alert>
  );
}

function SpaceDetailSkeleton() {
  return (
    <div className="space-y-6">
      <Skeleton className="h-8 w-64" />
      <Skeleton className="h-4 w-96" />
      <div className="grid grid-cols-1 gap-4 lg:grid-cols-2">
        <Skeleton className="h-48 rounded-lg" />
        <Skeleton className="h-48 rounded-lg" />
      </div>
      <Skeleton className="h-64 rounded-lg" />
    </div>
  );
}

function SpaceDetail() {
  const { spaceId } = Route.useParams();
  const navigate = useNavigate();
  const { data: space } = useGetSpaceDetailSuspense({
    params: { space_id: spaceId },
    ...selector(),
  });
  const startOpt = useStartOptimization();

  function handleOptimize() {
    startOpt.mutate(
      { params: { space_id: spaceId } },
      {
        onSuccess: (res) => {
          const runId = res.data?.runId ?? (res as { runId?: string }).runId;
          if (runId) {
            navigate({ to: "/runs/$runId", params: { runId } });
          }
        },
      },
    );
  }

  if (!space) return null;

  return (
    <div className="space-y-6">
      <div className="flex items-start justify-between gap-4">
        <div className="space-y-1">
          <button
            className="mb-2 flex items-center gap-1 text-sm text-muted-foreground hover:text-foreground"
            onClick={() => navigate({ to: "/" })}
          >
            <ArrowLeft className="h-4 w-4" />
            Back to Dashboard
          </button>
          <h1 className="text-2xl font-bold">{space.name}</h1>
          <p className="text-sm text-muted-foreground">{space.description}</p>
        </div>
        <Button
          onClick={handleOptimize}
          disabled={startOpt.isPending}
          className="bg-db-red hover:bg-db-red/90"
        >
          <Rocket className="mr-2 h-4 w-4" />
          {startOpt.isPending ? "Starting…" : "Start Optimization"}
        </Button>
      </div>

      <div className="grid grid-cols-1 gap-4 lg:grid-cols-2">
        <Card className="border-db-gray-border bg-white">
          <CardHeader className="flex flex-row items-center gap-2 pb-2">
            <FileText className="h-4 w-4 text-muted-foreground" />
            <CardTitle className="text-sm font-semibold">
              Instructions
            </CardTitle>
          </CardHeader>
          <CardContent>
            <pre className="whitespace-pre-wrap text-sm text-muted-foreground">
              {space.instructions || "No instructions configured."}
            </pre>
          </CardContent>
        </Card>

        <Card className="border-db-gray-border bg-white">
          <CardHeader className="flex flex-row items-center gap-2 pb-2">
            <HelpCircle className="h-4 w-4 text-muted-foreground" />
            <CardTitle className="text-sm font-semibold">
              Sample Questions
            </CardTitle>
          </CardHeader>
          <CardContent>
            {space.sampleQuestions.length === 0 ? (
              <p className="text-sm italic text-muted-foreground">
                No sample questions configured.
              </p>
            ) : (
              <ul className="list-inside list-disc space-y-1 text-sm text-muted-foreground">
                {space.sampleQuestions.map((q: string, i: number) => (
                  <li key={i}>{q}</li>
                ))}
              </ul>
            )}
          </CardContent>
        </Card>
      </div>

      <Card className="border-db-gray-border bg-white">
        <CardHeader className="flex flex-row items-center gap-2">
          <Database className="h-4 w-4 text-muted-foreground" />
          <CardTitle className="text-sm font-semibold">
            Referenced Tables
          </CardTitle>
        </CardHeader>
        <CardContent>
          {space.tables.length === 0 ? (
            <p className="text-sm italic text-muted-foreground">
              No tables referenced.
            </p>
          ) : (
            <Table>
              <TableHeader>
                <TableRow>
                  <TableHead>Table Name</TableHead>
                  <TableHead>Catalog</TableHead>
                  <TableHead>Schema</TableHead>
                  <TableHead className="text-right">Columns</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {space.tables.map((t) => (
                    <TableRow key={t.name}>
                      <TableCell className="font-medium">{t.name}</TableCell>
                      <TableCell>{t.catalog}</TableCell>
                      <TableCell>{t.schema_name}</TableCell>
                      <TableCell className="text-right">
                        {t.columnCount}
                      </TableCell>
                    </TableRow>
                  ),
                )}
              </TableBody>
            </Table>
          )}
        </CardContent>
      </Card>

      {space.optimizationHistory.length > 0 && (
        <Card className="border-db-gray-border bg-white">
          <CardHeader className="flex flex-row items-center gap-2">
            <History className="h-4 w-4 text-muted-foreground" />
            <CardTitle className="text-sm font-semibold">
              Optimization History
            </CardTitle>
          </CardHeader>
          <CardContent>
            <Table>
              <TableHeader>
                <TableRow>
                  <TableHead>Run ID</TableHead>
                  <TableHead>Status</TableHead>
                  <TableHead className="text-right">Baseline</TableHead>
                  <TableHead className="text-right">Optimized</TableHead>
                  <TableHead className="text-right">Date</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {space.optimizationHistory.map((run) => (
                    <TableRow
                      key={run.runId}
                      className="cursor-pointer hover:bg-muted/50"
                      onClick={() =>
                        navigate({
                          to: "/runs/$runId",
                          params: { runId: run.runId },
                        })
                      }
                    >
                      <TableCell className="font-mono text-xs">
                        {run.runId.slice(0, 8)}…
                      </TableCell>
                      <TableCell>
                        <Badge variant="outline">{run.status}</Badge>
                      </TableCell>
                      <TableCell className="text-right">
                        {run.baselineScore != null
                          ? `${run.baselineScore.toFixed(1)}%`
                          : "—"}
                      </TableCell>
                      <TableCell className="text-right">
                        {run.optimizedScore != null
                          ? `${run.optimizedScore.toFixed(1)}%`
                          : "—"}
                      </TableCell>
                      <TableCell className="text-right text-xs text-muted-foreground">
                        {run.timestamp
                          ? new Date(run.timestamp).toLocaleDateString()
                          : ""}
                      </TableCell>
                    </TableRow>
                  ),
                )}
              </TableBody>
            </Table>
          </CardContent>
        </Card>
      )}
    </div>
  );
}
