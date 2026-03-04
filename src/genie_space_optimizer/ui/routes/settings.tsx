import { createFileRoute } from "@tanstack/react-router";
import {
  useGetPermissionDashboardSuspense,
  type SpacePermissions,
  type SchemaPermission,
} from "@/lib/api";
import selector from "@/lib/selector";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { Skeleton } from "@/components/ui/skeleton";
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert";
import {
  Accordion,
  AccordionContent,
  AccordionItem,
  AccordionTrigger,
} from "@/components/ui/accordion";
import { ProcessFlow } from "@/components/ProcessFlow";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import { Suspense, useState } from "react";
import { ErrorBoundary } from "react-error-boundary";
import {
  Shield,
  CheckCircle2,
  AlertTriangle,
  Copy,
  Check,
  BookOpen,
  Server,
  Info,
  ExternalLink,
} from "lucide-react";
import { toast } from "sonner";

export const Route = createFileRoute("/settings")({
  component: () => (
    <ErrorBoundary fallback={<SettingsError />}>
      <Suspense fallback={<SettingsSkeleton />}>
        <SettingsContent />
      </Suspense>
    </ErrorBoundary>
  ),
});

function SettingsError() {
  return (
    <Alert variant="destructive" className="mt-6">
      <AlertTriangle className="h-4 w-4" />
      <AlertTitle>Failed to load settings</AlertTitle>
      <AlertDescription>
        Could not load permission settings. Please check your connection and try
        again.
      </AlertDescription>
    </Alert>
  );
}

function SettingsSkeleton() {
  return (
    <div className="space-y-6">
      <Skeleton className="h-8 w-48" />
      <Skeleton className="h-[200px] w-full" />
      <Skeleton className="h-[300px] w-full" />
    </div>
  );
}

function SettingsContent() {
  const { data } = useGetPermissionDashboardSuspense(selector());
  const dashboard = data as any;
  const spaces: SpacePermissions[] = dashboard?.spaces ?? [];
  const spPrincipalId: string = dashboard?.spPrincipalId ?? "";
  const spPrincipalDisplayName: string =
    dashboard?.spPrincipalDisplayName ?? "";
  const frameworkCatalog: string = dashboard?.frameworkCatalog ?? "";
  const frameworkSchema: string = dashboard?.frameworkSchema ?? "";
  const experimentBasePath: string = dashboard?.experimentBasePath ?? "";
  const jobName: string = dashboard?.jobName ?? "";
  const jobUrl: string | null = dashboard?.jobUrl ?? null;
  const workspaceHost: string | null = dashboard?.workspaceHost ?? null;

  const actionNeeded = spaces.filter((s) => s.status !== "ready");
  const defaultOpen = actionNeeded.map((s) => s.spaceId);

  return (
    <Tabs defaultValue="permissions" className="space-y-6">
      <TabsList>
        <TabsTrigger value="permissions" className="gap-1.5">
          <Shield className="h-3.5 w-3.5" />
          Permissions
        </TabsTrigger>
        <TabsTrigger value="how-it-works" className="gap-1.5">
          <BookOpen className="h-3.5 w-3.5" />
          How It Works
        </TabsTrigger>
      </TabsList>

      <TabsContent value="permissions">
        <div className="space-y-6">
          <div>
            <h1 className="text-2xl font-bold text-foreground">
              Permission Settings
            </h1>
            <p className="mt-1 text-sm text-muted-foreground">
              Review service principal access per Genie Space. Where access is
              missing, use the provided SQL commands or sharing instructions to
              grant it.
            </p>
          </div>

          <SPIdentityCard
            spId={spPrincipalId}
            spDisplayName={spPrincipalDisplayName}
          />

          {spaces.length === 0 ? (
            <Card>
              <CardContent className="py-8 text-center text-sm text-muted-foreground">
                No Genie Spaces found. Spaces where you have Can Edit or Can
                Manage will appear here.
              </CardContent>
            </Card>
          ) : (
            <Accordion
              type="multiple"
              defaultValue={defaultOpen}
              className="space-y-3"
            >
              {spaces.map((space) => (
                <SpacePermissionCard
                  key={space.spaceId}
                  space={space}
                  workspaceHost={workspaceHost}
                />
              ))}
            </Accordion>
          )}

          <FrameworkResourcesCard
            catalog={frameworkCatalog}
            schema={frameworkSchema}
            experimentBasePath={experimentBasePath}
            jobName={jobName}
            jobUrl={jobUrl}
          />
        </div>
      </TabsContent>

      <TabsContent value="how-it-works">
        <ProcessFlow />
      </TabsContent>
    </Tabs>
  );
}

function SPIdentityCard({
  spId,
  spDisplayName,
}: {
  spId: string;
  spDisplayName?: string;
}) {
  const [copied, setCopied] = useState(false);
  const [showClientId, setShowClientId] = useState(false);

  const handleCopy = (text: string) => {
    navigator.clipboard.writeText(text);
    setCopied(true);
    setTimeout(() => setCopied(false), 2000);
  };

  return (
    <Card>
      <CardHeader className="pb-3">
        <CardTitle className="flex items-center gap-2 text-base">
          <Shield className="h-4 w-4 text-blue-600" />
          Service Principal
        </CardTitle>
      </CardHeader>
      <CardContent>
        <div className="flex items-center gap-2">
          <p className="text-base font-semibold text-foreground">
            {spDisplayName || spId}
          </p>
          <Button
            variant="ghost"
            size="sm"
            className="h-8 w-8 p-0"
            onClick={() => handleCopy(spDisplayName || spId)}
          >
            {copied ? (
              <Check className="h-3.5 w-3.5 text-green-600" />
            ) : (
              <Copy className="h-3.5 w-3.5" />
            )}
          </Button>
        </div>
        <p className="mt-1 text-xs text-muted-foreground">
          This service principal runs background optimization jobs. Use the
          instructions below to grant it the required access.
        </p>
        {spDisplayName && (
          <div className="mt-2">
            <Button
              variant="link"
              size="sm"
              className="h-auto p-0 text-xs text-muted-foreground"
              onClick={() => setShowClientId(!showClientId)}
            >
              {showClientId ? "Hide" : "Show"} Client ID
            </Button>
            {showClientId && (
              <div className="mt-1 flex items-center gap-2">
                <code className="rounded-md bg-muted px-2.5 py-1.5 text-xs font-mono">
                  {spId}
                </code>
                <Button
                  variant="ghost"
                  size="sm"
                  className="h-7 w-7 p-0"
                  onClick={() => handleCopy(spId)}
                >
                  <Copy className="h-3 w-3" />
                </Button>
              </div>
            )}
          </div>
        )}
      </CardContent>
    </Card>
  );
}

function StatusBadge({ status }: { status: string }) {
  if (status === "ready") {
    return (
      <Badge
        variant="outline"
        className="border-green-200 bg-green-50 text-green-700"
      >
        <CheckCircle2 className="mr-1 h-3 w-3" />
        Ready
      </Badge>
    );
  }
  if (status === "action_needed") {
    return (
      <Badge
        variant="outline"
        className="border-amber-200 bg-amber-50 text-amber-700"
      >
        <AlertTriangle className="mr-1 h-3 w-3" />
        Action Needed
      </Badge>
    );
  }
  return (
    <Badge
      variant="outline"
      className="border-red-200 bg-red-50 text-red-700"
    >
      <Shield className="mr-1 h-3 w-3" />
      Not Configured
    </Badge>
  );
}

function SpacePermissionCard({
  space,
  workspaceHost,
}: {
  space: SpacePermissions;
  workspaceHost?: string | null;
}) {
  const spaceUrl = workspaceHost
    ? `${workspaceHost.split("?")[0]}/genie/rooms/${space.spaceId}${workspaceHost.includes("?") ? `?${workspaceHost.split("?")[1]}` : ""}`
    : null;

  return (
    <AccordionItem
      value={space.spaceId}
      className="rounded-lg border bg-card"
    >
      <AccordionTrigger className="px-4 hover:no-underline">
        <div className="flex flex-1 items-center justify-between pr-2">
          <div className="text-left">
            <div className="flex items-center gap-1.5">
              <p className="font-semibold">{space.title}</p>
              {spaceUrl && (
                <a
                  href={spaceUrl}
                  target="_blank"
                  rel="noopener noreferrer"
                  className="text-blue-600 hover:text-blue-800"
                  onClick={(e) => e.stopPropagation()}
                >
                  <ExternalLink className="h-3.5 w-3.5" />
                </a>
              )}
            </div>
            <p className="text-xs text-muted-foreground">{space.spaceId}</p>
          </div>
          <StatusBadge status={space.status} />
        </div>
      </AccordionTrigger>
      <AccordionContent className="px-4">
        <div className="space-y-4">
          {/* Space Access */}
          <div className="rounded border p-3">
            <p className="mb-2 text-sm font-medium">Space Access</p>
            <div className="flex items-center gap-2">
              <span className="text-sm">SP CAN_MANAGE</span>
              {space.spHasManage ? (
                <Badge
                  variant="outline"
                  className="border-green-200 bg-green-50 text-green-700"
                >
                  <CheckCircle2 className="mr-1 h-3 w-3" />
                  Granted
                </Badge>
              ) : (
                <Badge
                  variant="outline"
                  className="border-red-200 bg-red-50 text-red-700"
                >
                  Not Granted
                </Badge>
              )}
            </div>
            {!space.spHasManage && space.spGrantInstructions && (
              <div className="mt-2 flex items-start gap-2 rounded bg-amber-50 border border-amber-200 px-3 py-2">
                <Info className="mt-0.5 h-4 w-4 shrink-0 text-amber-600" />
                <p className="text-xs text-amber-800">
                  {space.spGrantInstructions}
                </p>
              </div>
            )}
          </div>

          {/* Data Access */}
          {(space.schemas ?? []).length > 0 && (
            <div className="rounded border p-3">
              <p className="mb-2 text-sm font-medium">Data Access</p>
              <Table>
                <TableHeader>
                  <TableRow>
                    <TableHead>Schema</TableHead>
                    <TableHead className="text-center">
                      Read (SELECT)
                    </TableHead>
                    <TableHead className="text-center">
                      <span>Write (MODIFY)</span>
                      <p className="text-[10px] font-normal text-muted-foreground">
                        Optional — only for UC write backs
                      </p>
                    </TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {(space.schemas ?? []).map((s) => (
                    <TableRow key={`${s.catalog}.${s.schema_name}`}>
                      <TableCell className="font-mono text-xs">
                        {s.catalog}.{s.schema_name}
                      </TableCell>
                      <TableCell className="text-center">
                        <GrantCell
                          granted={s.readGranted}
                          grantCommand={s.readGrantCommand}
                        />
                      </TableCell>
                      <TableCell className="text-center">
                        <GrantCell
                          granted={s.writeGranted}
                          grantCommand={s.writeGrantCommand}
                          isOptional
                        />
                      </TableCell>
                    </TableRow>
                  ))}
                </TableBody>
              </Table>
            </div>
          )}
        </div>
      </AccordionContent>
    </AccordionItem>
  );
}

function SqlHint({ sql }: { sql: string }) {
  const [copied, setCopied] = useState(false);
  const handleCopy = () => {
    navigator.clipboard.writeText(sql);
    setCopied(true);
    setTimeout(() => setCopied(false), 2000);
    toast.info("SQL copied to clipboard");
  };
  return (
    <div className="mt-1 flex items-start gap-1.5 rounded bg-muted/60 px-2 py-1.5 text-left max-w-xs">
      <code className="flex-1 whitespace-pre-wrap break-all text-[10px] font-mono leading-tight text-muted-foreground">
        {sql}
      </code>
      <Button
        variant="ghost"
        size="sm"
        className="h-5 w-5 shrink-0 p-0"
        onClick={handleCopy}
      >
        {copied ? (
          <Check className="h-3 w-3 text-green-600" />
        ) : (
          <Copy className="h-3 w-3" />
        )}
      </Button>
    </div>
  );
}

function GrantCell({
  granted,
  grantCommand,
  isOptional,
}: {
  granted: boolean;
  grantCommand?: string | null;
  isOptional?: boolean;
}) {
  if (granted) {
    return (
      <Badge
        variant="outline"
        className="border-green-200 bg-green-50 text-green-700"
      >
        <CheckCircle2 className="mr-1 h-3 w-3" />
        Granted
      </Badge>
    );
  }

  if (grantCommand) {
    return (
      <div className="flex flex-col items-center gap-0.5">
        <Badge
          variant="outline"
          className="border-red-200 bg-red-50 text-red-700"
        >
          Not Granted
        </Badge>
        <SqlHint sql={grantCommand} />
      </div>
    );
  }

  return (
    <Badge variant="outline" className="text-gray-400">
      {isOptional ? "Optional" : "—"}
    </Badge>
  );
}

function FrameworkResourcesCard({
  catalog,
  schema,
  experimentBasePath,
  jobName,
  jobUrl,
}: {
  catalog: string;
  schema: string;
  experimentBasePath: string;
  jobName: string;
  jobUrl?: string | null;
}) {
  return (
    <Card>
      <CardHeader className="pb-3">
        <CardTitle className="flex items-center gap-2 text-base">
          <Server className="h-4 w-4 text-purple-600" />
          Framework Resources
        </CardTitle>
      </CardHeader>
      <CardContent>
        <div className="space-y-2 text-sm">
          <div className="flex justify-between">
            <span className="text-muted-foreground">Optimization tables</span>
            <code className="font-mono text-xs">
              {catalog}.{schema}
            </code>
          </div>
          <div className="flex justify-between">
            <span className="text-muted-foreground">MLflow experiments</span>
            <code className="font-mono text-xs">{experimentBasePath}</code>
          </div>
          <div className="flex justify-between">
            <span className="text-muted-foreground">Background job</span>
            {jobUrl ? (
              <a
                href={jobUrl}
                target="_blank"
                rel="noopener noreferrer"
                className="font-mono text-xs text-blue-600 hover:underline"
              >
                {jobName} ↗
              </a>
            ) : (
              <code className="font-mono text-xs">{jobName}</code>
            )}
          </div>
        </div>
      </CardContent>
    </Card>
  );
}
