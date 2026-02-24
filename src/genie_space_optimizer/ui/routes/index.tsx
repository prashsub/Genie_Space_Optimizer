import { createFileRoute, useNavigate } from "@tanstack/react-router";
import { useListSpacesSuspense, useGetActivitySuspense } from "@/lib/api";
import selector from "@/lib/selector";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { Badge } from "@/components/ui/badge";
import { Skeleton } from "@/components/ui/skeleton";
import { SpaceCard } from "@/components/SpaceCard";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import { Suspense, useMemo, useState, useEffect } from "react";
import { LayoutGrid, Activity, BarChart3, Search } from "lucide-react";
import { ErrorBoundary } from "react-error-boundary";
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert";

type SpaceSummary = {
  id: string;
  name: string;
  description: string;
  tableCount: number;
  lastModified: string;
  qualityScore?: number | null;
};

type ActivityItem = {
  runId: string;
  spaceName: string;
  status: string;
  initiatedBy?: string | null;
  optimizedScore?: number | null;
  timestamp?: string | null;
};

function toSpaceSummaryArray(value: unknown): SpaceSummary[] {
  if (!Array.isArray(value)) return [];
  return value
    .filter((item): item is Record<string, unknown> => !!item && typeof item === "object")
    .map((item) => ({
      id: String(item.id ?? ""),
      name: String(item.name ?? ""),
      description: String(item.description ?? ""),
      tableCount: typeof item.tableCount === "number" ? item.tableCount : 0,
      lastModified: item.lastModified != null ? String(item.lastModified) : "",
      qualityScore: typeof item.qualityScore === "number" ? item.qualityScore : null,
    }));
}

function toActivityItemArray(value: unknown): ActivityItem[] {
  if (!Array.isArray(value)) return [];
  return value
    .filter((item): item is Record<string, unknown> => !!item && typeof item === "object")
    .map((item) => ({
      runId: String(item.runId ?? ""),
      spaceName: String(item.spaceName ?? ""),
      status: String(item.status ?? ""),
      initiatedBy: item.initiatedBy != null ? String(item.initiatedBy) : "",
      optimizedScore: typeof item.optimizedScore === "number" ? item.optimizedScore : null,
      timestamp: item.timestamp != null ? String(item.timestamp) : "",
    }))
    .filter((item) => item.runId.length > 0);
}

export const Route = createFileRoute("/")({
  component: () => (
    <ErrorBoundary fallback={<DashboardError />}>
      <Suspense fallback={<DashboardSkeleton />}>
        <DashboardContent />
      </Suspense>
    </ErrorBoundary>
  ),
});

function DashboardError() {
  return (
    <Alert variant="destructive">
      <AlertTitle>Failed to load dashboard</AlertTitle>
      <AlertDescription>
        Could not fetch spaces. Please check your connection and try again.
      </AlertDescription>
    </Alert>
  );
}

function DashboardSkeleton() {
  return (
    <div className="space-y-6">
      <div className="grid grid-cols-1 gap-4 sm:grid-cols-3">
        {[1, 2, 3].map((i) => (
          <Skeleton key={i} className="h-24 rounded-lg" />
        ))}
      </div>
      <Skeleton className="h-10 w-full rounded-md" />
      <div className="grid grid-cols-1 gap-4 sm:grid-cols-2 lg:grid-cols-3">
        {[1, 2, 3, 4, 5, 6].map((i) => (
          <Skeleton key={i} className="h-36 rounded-lg" />
        ))}
      </div>
    </div>
  );
}

function DashboardContent() {
  const { data } = useListSpacesSuspense(selector());
  const spaces = toSpaceSummaryArray(data);
  return (
    <Dashboard
      spaces={spaces}
    />
  );
}

function ActivitySection({ navigate }: { navigate: ReturnType<typeof useNavigate> }) {
  const { data } = useGetActivitySuspense(selector());
  const activity = toActivityItemArray(data);
  if (!activity || activity.length === 0) return null;

  return (
    <Card className="border-db-gray-border bg-white">
      <CardHeader>
        <CardTitle className="text-sm font-semibold">
          Recent Activity
        </CardTitle>
      </CardHeader>
      <CardContent>
        <Table>
          <TableHeader>
            <TableRow>
              <TableHead>Space</TableHead>
              <TableHead>Status</TableHead>
              <TableHead>Initiated By</TableHead>
              <TableHead className="text-right">Score</TableHead>
              <TableHead className="text-right">Time</TableHead>
            </TableRow>
          </TableHeader>
          <TableBody>
            {activity.map((item) => (
                <TableRow
                  key={item.runId}
                  className="cursor-pointer hover:bg-muted/50"
                  onClick={() =>
                    navigate({
                      to: "/runs/$runId",
                      params: { runId: item.runId },
                    })
                  }
                >
                  <TableCell className="font-medium">
                    {item.spaceName}
                  </TableCell>
                  <TableCell>
                    <Badge variant="outline">{item.status}</Badge>
                  </TableCell>
                  <TableCell>{item.initiatedBy}</TableCell>
                  <TableCell className="text-right">
                    {item.optimizedScore != null
                      ? `${item.optimizedScore.toFixed(1)}%`
                      : "\u2014"}
                  </TableCell>
                  <TableCell className="text-right text-xs text-muted-foreground">
                    {item.timestamp
                      ? new Date(item.timestamp).toLocaleDateString()
                      : ""}
                  </TableCell>
                </TableRow>
              ),
            )}
          </TableBody>
        </Table>
      </CardContent>
    </Card>
  );
}

function Dashboard({ spaces }: { spaces: SpaceSummary[] }) {
  const [search, setSearch] = useState("");
  const [debouncedSearch, setDebouncedSearch] = useState("");
  const navigate = useNavigate();

  useEffect(() => {
    const t = setTimeout(() => setDebouncedSearch(search), 300);
    return () => clearTimeout(t);
  }, [search]);

  const filtered = useMemo(() => {
    const q = debouncedSearch.toLowerCase();
    if (!q) return spaces;
    return spaces.filter(
      (s) =>
        s.name.toLowerCase().includes(q) ||
        s.description.toLowerCase().includes(q),
    );
  }, [spaces, debouncedSearch]);

  const totalSpaces = spaces.length;
  const avgScore = useMemo(() => {
    if (spaces.length === 0) return 0;
    const scored = spaces.filter((s) => s.qualityScore != null);
    if (scored.length === 0) return 0;
    const sum = scored.reduce((acc, s) => acc + (s.qualityScore ?? 0), 0);
    return sum / scored.length;
  }, [spaces]);

  return (
    <div className="space-y-6">
      <div className="grid grid-cols-1 gap-4 sm:grid-cols-3">
        <Card className="border-db-gray-border bg-white">
          <CardHeader className="flex flex-row items-center justify-between pb-2">
            <CardTitle className="text-xs font-medium uppercase tracking-wide text-muted-foreground">
              Total Spaces
            </CardTitle>
            <LayoutGrid className="h-4 w-4 text-muted-foreground" />
          </CardHeader>
          <CardContent>
            <span className="text-2xl font-bold">{totalSpaces}</span>
          </CardContent>
        </Card>

        <Card className="border-db-gray-border bg-white">
          <CardHeader className="flex flex-row items-center justify-between pb-2">
            <CardTitle className="text-xs font-medium uppercase tracking-wide text-muted-foreground">
              Recent Runs
            </CardTitle>
            <Activity className="h-4 w-4 text-muted-foreground" />
          </CardHeader>
          <CardContent>
            <span className="text-2xl font-bold">{"\u2014"}</span>
          </CardContent>
        </Card>

        <Card className="border-db-gray-border bg-white">
          <CardHeader className="flex flex-row items-center justify-between pb-2">
            <CardTitle className="text-xs font-medium uppercase tracking-wide text-muted-foreground">
              Avg Quality
            </CardTitle>
            <BarChart3 className="h-4 w-4 text-muted-foreground" />
          </CardHeader>
          <CardContent>
            <span className="text-2xl font-bold">
              {avgScore > 0 ? `${avgScore.toFixed(1)}%` : "\u2014"}
            </span>
          </CardContent>
        </Card>
      </div>

      <div className="relative">
        <Search className="absolute left-3 top-1/2 h-4 w-4 -translate-y-1/2 text-muted-foreground" />
        <Input
          placeholder="Search spaces by name or description\u2026"
          value={search}
          onChange={(e) => setSearch(e.target.value)}
          className="pl-9"
        />
      </div>

      <div className="grid grid-cols-1 gap-4 sm:grid-cols-2 lg:grid-cols-3">
        {filtered.map((space) => (
            <SpaceCard
              key={space.id}
              {...space}
              onClick={() => {
                void navigate({ to: "/spaces/$spaceId", params: { spaceId: space.id } });
              }}
            />
          ),
        )}
        {filtered.length === 0 && (
          <p className="col-span-full py-12 text-center text-sm text-muted-foreground">
            {debouncedSearch
              ? "No spaces match your search."
              : "No Genie Spaces found."}
          </p>
        )}
      </div>

      <ErrorBoundary fallback={null}>
        <Suspense fallback={null}>
          <ActivitySection navigate={navigate} />
        </Suspense>
      </ErrorBoundary>
    </div>
  );
}
