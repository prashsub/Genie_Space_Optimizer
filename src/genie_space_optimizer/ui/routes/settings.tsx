import { createFileRoute } from "@tanstack/react-router";
import {
  useGetPermissionDashboardSuspense,
  useGrantDataAccess,
  useRevokeDataAccess,
  useGrantSpaceAccess,
  useRevokeSpaceAccess,
  getPermissionDashboardKey,
  getDataAccessKey,
  type SpacePermissions,
  type SchemaPermission,
} from "@/lib/api";
import selector from "@/lib/selector";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Badge } from "@/components/ui/badge";
import { Skeleton } from "@/components/ui/skeleton";
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert";
import {
  AlertDialog,
  AlertDialogAction,
  AlertDialogCancel,
  AlertDialogContent,
  AlertDialogDescription,
  AlertDialogFooter,
  AlertDialogHeader,
  AlertDialogTitle,
} from "@/components/ui/alert-dialog";
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
import { useIsFetching } from "@tanstack/react-query";
import {
  Shield,
  Plus,
  Trash2,
  CheckCircle2,
  AlertTriangle,
  Copy,
  Check,
  BookOpen,
  Server,
} from "lucide-react";
import { toast } from "sonner";
import { useQueryClient } from "@tanstack/react-query";

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
  const queryClient = useQueryClient();
  const isRefreshing =
    useIsFetching({ queryKey: getPermissionDashboardKey() }) > 0;

  const dashboard = data as any;
  const spaces: SpacePermissions[] = dashboard?.spaces ?? [];
  const spPrincipalId: string = dashboard?.spPrincipalId ?? "";
  const spPrincipalDisplayName: string =
    dashboard?.spPrincipalDisplayName ?? "";
  const frameworkCatalog: string = dashboard?.frameworkCatalog ?? "";
  const frameworkSchema: string = dashboard?.frameworkSchema ?? "";
  const experimentBasePath: string = dashboard?.experimentBasePath ?? "";
  const jobName: string = dashboard?.jobName ?? "";

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
              Manage service principal access per Genie Space. Grant read access
              for optimization, write access for UC artifact updates.
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
                  spPrincipalId={spPrincipalId}
                  spDisplayName={spPrincipalDisplayName}
                  queryClient={queryClient}
                  isRefreshing={isRefreshing}
                />
              ))}
            </Accordion>
          )}

          <ManualGrantCard queryClient={queryClient} />

          <FrameworkResourcesCard
            catalog={frameworkCatalog}
            schema={frameworkSchema}
            experimentBasePath={experimentBasePath}
            jobName={jobName}
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
          This service principal runs background optimization jobs. Grant it
          access to your Genie Spaces and data schemas below.
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
  spPrincipalId: _spPrincipalId,
  spDisplayName: _spDisplayName,
  queryClient,
  isRefreshing,
}: {
  space: SpacePermissions;
  spPrincipalId: string;
  spDisplayName: string;
  queryClient: ReturnType<typeof useQueryClient>;
  isRefreshing: boolean;
}) {
  const grantDataMutation = useGrantDataAccess();
  const revokeDataMutation = useRevokeDataAccess();
  const grantSpaceMutation = useGrantSpaceAccess();
  const revokeSpaceMutation = useRevokeSpaceAccess();

  const isBusy =
    grantDataMutation.isPending ||
    revokeDataMutation.isPending ||
    grantSpaceMutation.isPending ||
    revokeSpaceMutation.isPending ||
    isRefreshing;

  const [confirmAction, setConfirmAction] = useState<{
    type:
      | "grantRead"
      | "grantWrite"
      | "revokeRead"
      | "revokeWrite"
      | "grantSpace"
      | "revokeSpace"
      | "grantAllRead"
      | "grantAllReadWrite"
      | "revokeAll";
    schema?: SchemaPermission;
  } | null>(null);

  const invalidate = () => {
    queryClient.invalidateQueries({ queryKey: getPermissionDashboardKey() });
    queryClient.invalidateQueries({ queryKey: getDataAccessKey() });
  };

  const handleGrantSpace = () => {
    grantSpaceMutation.mutate(
      { params: {}, data: { space_id: space.spaceId } },
      {
        onSuccess: () => {
          toast.success(`Granted SP access to ${space.title}`);
          invalidate();
        },
        onError: (err) => {
          toast.error(
            (err as any)?.body?.detail || err.message || "Grant failed",
          );
        },
      },
    );
  };

  const handleRevokeSpace = () => {
    revokeSpaceMutation.mutate(
      { params: { space_id: space.spaceId } },
      {
        onSuccess: () => {
          toast.success(`Revoked SP access from ${space.title}`);
          invalidate();
        },
        onError: (err) => {
          toast.error(
            (err as any)?.body?.detail || err.message || "Revoke failed",
          );
        },
      },
    );
  };

  const handleGrantData = (
    schema: SchemaPermission,
    grantType: "read" | "write",
  ) => {
    grantDataMutation.mutate(
      {
        params: {},
        data: {
          catalog: schema.catalog,
          schema_name: schema.schema_name,
          grant_type: grantType,
        },
      },
      {
        onSuccess: () => {
          toast.success(
            `Granted ${grantType} access to ${schema.catalog}.${schema.schema_name}`,
          );
          invalidate();
        },
        onError: (err) => {
          toast.error(
            (err as any)?.body?.detail || err.message || "Grant failed",
            { duration: 10000 },
          );
        },
      },
    );
  };

  const handleRevokeData = (grantId: string, schema: SchemaPermission) => {
    revokeDataMutation.mutate(
      { params: { grant_id: grantId } },
      {
        onSuccess: () => {
          toast.success(
            `Revoked access to ${schema.catalog}.${schema.schema_name}`,
          );
          invalidate();
        },
        onError: (err) => {
          toast.error(
            (err as any)?.body?.detail || err.message || "Revoke failed",
          );
        },
      },
    );
  };

  const schemas = space.schemas ?? [];
  const ungrantedRead = schemas.filter((s) => !s.readGranted && s.canGrantRead);
  const ungrantedWrite = schemas.filter(
    (s) => !s.writeGranted && s.canGrantWrite,
  );
  const revocableRead = schemas.filter((s) => s.readGranted && s.readGrantId);
  const revocableWrite = schemas.filter(
    (s) => s.writeGranted && s.writeGrantId,
  );
  const totalRevocable = revocableRead.length + revocableWrite.length;

  const handleBulkGrantRead = () => {
    if (!space.spHasManage && space.userCanGrantManage) {
      handleGrantSpace();
    }
    for (const s of ungrantedRead) handleGrantData(s, "read");
  };

  const handleBulkGrantReadWrite = () => {
    if (!space.spHasManage && space.userCanGrantManage) {
      handleGrantSpace();
    }
    for (const s of ungrantedRead) handleGrantData(s, "read");
    for (const s of ungrantedWrite) handleGrantData(s, "write");
  };

  const handleBulkRevokeAll = () => {
    for (const s of revocableRead) handleRevokeData(s.readGrantId!, s);
    for (const s of revocableWrite) handleRevokeData(s.writeGrantId!, s);
  };

  const handleConfirmedAction = () => {
    if (!confirmAction) return;
    const { type, schema } = confirmAction;
    setConfirmAction(null);
    switch (type) {
      case "grantSpace":
        handleGrantSpace();
        break;
      case "revokeSpace":
        handleRevokeSpace();
        break;
      case "grantRead":
        if (schema) handleGrantData(schema, "read");
        break;
      case "grantWrite":
        if (schema) handleGrantData(schema, "write");
        break;
      case "revokeRead":
        if (schema?.readGrantId) handleRevokeData(schema.readGrantId, schema);
        break;
      case "revokeWrite":
        if (schema?.writeGrantId) handleRevokeData(schema.writeGrantId, schema);
        break;
      case "grantAllRead":
        handleBulkGrantRead();
        break;
      case "grantAllReadWrite":
        handleBulkGrantReadWrite();
        break;
      case "revokeAll":
        handleBulkRevokeAll();
        break;
    }
  };

  const confirmTitle = () => {
    if (!confirmAction) return "";
    switch (confirmAction.type) {
      case "grantSpace":
        return "Grant SP Access to Space";
      case "revokeSpace":
        return "Revoke SP Access from Space";
      case "grantRead":
        return "Grant Read Access";
      case "grantWrite":
        return "Grant Write Access (MODIFY)";
      case "revokeRead":
        return "Revoke Read Access";
      case "revokeWrite":
        return "Revoke Write Access";
      case "grantAllRead":
        return "Grant All Read Permissions";
      case "grantAllReadWrite":
        return "Grant All Read + Write Permissions";
      case "revokeAll":
        return "Revoke All App-Managed Permissions";
    }
  };

  const confirmDescription = () => {
    if (!confirmAction) return "";
    const s = confirmAction.schema;
    switch (confirmAction.type) {
      case "grantSpace":
        return `This will grant the service principal CAN_MANAGE on "${space.title}". The SP will be able to read and modify the Genie Space configuration.`;
      case "revokeSpace":
        return `This will revoke the service principal's CAN_MANAGE on "${space.title}". The optimizer will no longer be able to modify this space.`;
      case "grantRead":
        return `This will execute GRANT USE CATALOG, USE SCHEMA, SELECT, EXECUTE on ${s?.catalog}.${s?.schema_name} to the service principal.`;
      case "grantWrite":
        return `This will grant MODIFY on ${s?.catalog}.${s?.schema_name}. The SP will be able to write column descriptions, table descriptions, and TVF definitions directly to Unity Catalog.`;
      case "revokeRead":
        return `This will revoke SELECT/EXECUTE on ${s?.catalog}.${s?.schema_name}. The optimizer will no longer be able to read data from this schema.`;
      case "revokeWrite":
        return `This will revoke MODIFY on ${s?.catalog}.${s?.schema_name}. UC artifact writes will no longer work for this schema.`;
      case "grantAllRead":
        return `This will grant: ${!space.spHasManage ? "SP CAN_MANAGE on the space + " : ""}read access (SELECT/EXECUTE) on ${ungrantedRead.length} schema(s). Write access is not included.`;
      case "grantAllReadWrite":
        return `This will grant: ${!space.spHasManage ? "SP CAN_MANAGE on the space + " : ""}read access on ${ungrantedRead.length} schema(s) + write access (MODIFY) on ${ungrantedWrite.length} schema(s). Write access allows the optimizer to modify UC artifacts directly.`;
      case "revokeAll":
        return `This will revoke ${totalRevocable} app-managed data grant(s). Externally managed grants and SP space access are not affected.`;
    }
  };

  return (
    <>
      <AccordionItem
        value={space.spaceId}
        className={`rounded-lg border bg-card transition-opacity ${isBusy ? "opacity-70" : ""}`}
      >
        <AccordionTrigger className="px-4 hover:no-underline">
          <div className="flex flex-1 items-center justify-between pr-2">
            <div className="text-left">
              <p className="font-semibold">{space.title}</p>
              <p className="text-xs text-muted-foreground">{space.spaceId}</p>
            </div>
            <div className="flex items-center gap-2">
              {isBusy && (
                <span className="text-xs text-muted-foreground animate-pulse">
                  Updating…
                </span>
              )}
              <StatusBadge status={space.status} />
            </div>
          </div>
        </AccordionTrigger>
        <AccordionContent className="px-4">
          <div className="space-y-4">
            {/* Space Access */}
            <div className="rounded border p-3">
              <p className="mb-2 text-sm font-medium">Space Access</p>
              <div className="flex items-center justify-between">
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
                      className="border-amber-200 bg-amber-50 text-amber-700"
                    >
                      Not Granted
                    </Badge>
                  )}
                </div>
                <div>
                  {!space.spHasManage && space.userCanGrantManage && (
                    <Button
                      size="sm"
                      variant="outline"
                      disabled={isBusy}
                      onClick={() =>
                        setConfirmAction({ type: "grantSpace" })
                      }
                    >
                      <Plus className="mr-1 h-3 w-3" />
                      Grant
                    </Button>
                  )}
                  {space.spHasManage && (
                    <Button
                      size="sm"
                      variant="ghost"
                      className="text-red-600 hover:bg-red-50 hover:text-red-700"
                      disabled={isBusy}
                      onClick={() =>
                        setConfirmAction({ type: "revokeSpace" })
                      }
                    >
                      <Trash2 className="mr-1 h-3 w-3" />
                      Revoke
                    </Button>
                  )}
                </div>
              </div>
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
                        Write (MODIFY)
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
                            grantId={s.readGrantId}
                            canGrant={s.canGrantRead}
                            grantLabel="Grant"
                            disabled={isBusy}
                            onGrant={() =>
                              setConfirmAction({ type: "grantRead", schema: s })
                            }
                            onRevoke={() =>
                              setConfirmAction({
                                type: "revokeRead",
                                schema: s,
                              })
                            }
                            variant="green"
                          />
                        </TableCell>
                        <TableCell className="text-center">
                          <GrantCell
                            granted={s.writeGranted}
                            grantId={s.writeGrantId}
                            canGrant={s.canGrantWrite}
                            grantLabel="Grant Write"
                            disabled={isBusy}
                            onGrant={() =>
                              setConfirmAction({
                                type: "grantWrite",
                                schema: s,
                              })
                            }
                            onRevoke={() =>
                              setConfirmAction({
                                type: "revokeWrite",
                                schema: s,
                              })
                            }
                            variant="amber"
                          />
                        </TableCell>
                      </TableRow>
                    ))}
                  </TableBody>
                </Table>
              </div>
            )}

            {/* Bulk Actions */}
            <div className="flex flex-wrap items-center justify-end gap-2 border-t pt-3">
              {(ungrantedRead.length > 0 ||
                (!space.spHasManage && space.userCanGrantManage)) && (
                <Button
                  variant="default"
                  size="sm"
                  disabled={isBusy}
                  onClick={() =>
                    setConfirmAction({ type: "grantAllRead" })
                  }
                >
                  <Plus className="mr-1 h-3 w-3" />
                  Grant All Read
                  {!space.spHasManage ? " + Space" : ""}
                </Button>
              )}
              {(ungrantedRead.length > 0 || ungrantedWrite.length > 0) && (
                <Button
                  variant="default"
                  size="sm"
                  disabled={isBusy}
                  className="bg-amber-600 hover:bg-amber-700"
                  onClick={() =>
                    setConfirmAction({ type: "grantAllReadWrite" })
                  }
                >
                  <Plus className="mr-1 h-3 w-3" />
                  Grant All Read + Write
                </Button>
              )}
              {totalRevocable > 0 && (
                <Button
                  variant="ghost"
                  size="sm"
                  disabled={isBusy}
                  className="text-red-600 hover:bg-red-50 hover:text-red-700"
                  onClick={() => setConfirmAction({ type: "revokeAll" })}
                >
                  <Trash2 className="mr-1 h-3 w-3" />
                  Revoke All
                </Button>
              )}
            </div>
          </div>
        </AccordionContent>
      </AccordionItem>

      <AlertDialog
        open={confirmAction !== null}
        onOpenChange={(open) => {
          if (!open) setConfirmAction(null);
        }}
      >
        <AlertDialogContent>
          <AlertDialogHeader>
            <AlertDialogTitle>{confirmTitle()}</AlertDialogTitle>
            <AlertDialogDescription>
              {confirmDescription()}
            </AlertDialogDescription>
          </AlertDialogHeader>
          <AlertDialogFooter>
            <AlertDialogCancel disabled={isBusy}>Cancel</AlertDialogCancel>
            <AlertDialogAction onClick={handleConfirmedAction} disabled={isBusy}>
              {isBusy ? "Processing…" : "Confirm"}
            </AlertDialogAction>
          </AlertDialogFooter>
        </AlertDialogContent>
      </AlertDialog>
    </>
  );
}

function GrantCell({
  granted,
  grantId,
  canGrant,
  grantLabel,
  onGrant,
  onRevoke,
  variant,
  disabled,
}: {
  granted: boolean;
  grantId?: string | null;
  canGrant: boolean;
  grantLabel: string;
  onGrant: () => void;
  onRevoke: () => void;
  variant: "green" | "amber";
  disabled?: boolean;
}) {
  const badgeCls =
    variant === "green"
      ? "border-green-200 bg-green-50 text-green-700"
      : "border-amber-200 bg-amber-50 text-amber-700";
  const grantBtnCls =
    variant === "amber"
      ? "border-amber-300 text-amber-700 hover:bg-amber-50"
      : "";

  if (granted) {
    if (grantId) {
      return (
        <div className="flex items-center justify-center gap-1">
          <Badge variant="outline" className={badgeCls}>
            Granted
          </Badge>
          <Button
            size="sm"
            variant="ghost"
            className="h-6 w-6 p-0 text-red-500"
            disabled={disabled}
            onClick={onRevoke}
          >
            <Trash2 className="h-3 w-3" />
          </Button>
        </div>
      );
    }
    return (
      <div
        className="flex items-center justify-center gap-1"
        title="This grant was created outside the app (e.g. via SQL or catalog admin). It cannot be revoked from here."
      >
        <Badge variant="outline" className={badgeCls}>
          Granted
        </Badge>
        <Badge
          variant="outline"
          className="border-blue-200 bg-blue-50 text-blue-600 text-[10px] px-1"
        >
          External
        </Badge>
      </div>
    );
  }

  if (canGrant) {
    return (
      <Button
        size="sm"
        variant="outline"
        className={grantBtnCls}
        disabled={disabled}
        onClick={onGrant}
      >
        <Plus className="mr-1 h-3 w-3" />
        {grantLabel}
      </Button>
    );
  }

  return (
    <Badge variant="outline" className="text-gray-400">
      {variant === "amber" ? "Optional" : "No Permission"}
    </Badge>
  );
}

function ManualGrantCard({
  queryClient,
}: {
  queryClient: ReturnType<typeof useQueryClient>;
}) {
  const [catalog, setCatalog] = useState("");
  const [schema, setSchema] = useState("");
  const grantMutation = useGrantDataAccess();

  const handleSubmit = (e: React.FormEvent) => {
    e.preventDefault();
    if (!catalog.trim() || !schema.trim()) {
      toast.error("Both catalog and schema are required.");
      return;
    }
    grantMutation.mutate(
      {
        params: {},
        data: { catalog: catalog.trim(), schema_name: schema.trim() },
      },
      {
        onSuccess: () => {
          toast.success(`Granted access to ${catalog}.${schema}`);
          setCatalog("");
          setSchema("");
          queryClient.invalidateQueries({
            queryKey: getPermissionDashboardKey(),
          });
          queryClient.invalidateQueries({ queryKey: getDataAccessKey() });
        },
        onError: (err) => {
          const detail = (err as any)?.body?.detail || err.message;
          toast.error(detail, { duration: 10000 });
        },
      },
    );
  };

  return (
    <Card>
      <CardHeader className="pb-3">
        <CardTitle className="flex items-center gap-2 text-base">
          <Plus className="h-4 w-4 text-green-600" />
          Manual Schema Grant
        </CardTitle>
      </CardHeader>
      <CardContent>
        <p className="mb-3 text-sm text-muted-foreground">
          Grant access to a schema not yet tied to a Genie Space. You need
          MANAGE privilege on the catalog/schema.
        </p>
        <form onSubmit={handleSubmit} className="flex items-end gap-3">
          <div className="flex-1">
            <label className="mb-1 block text-xs font-medium text-muted-foreground">
              Catalog
            </label>
            <Input
              placeholder="e.g. my_catalog"
              value={catalog}
              onChange={(e) => setCatalog(e.target.value)}
            />
          </div>
          <div className="flex-1">
            <label className="mb-1 block text-xs font-medium text-muted-foreground">
              Schema
            </label>
            <Input
              placeholder="e.g. gold_schema"
              value={schema}
              onChange={(e) => setSchema(e.target.value)}
            />
          </div>
          <Button type="submit" disabled={grantMutation.isPending}>
            {grantMutation.isPending ? "Granting..." : "Grant Read Access"}
          </Button>
        </form>
      </CardContent>
    </Card>
  );
}

function FrameworkResourcesCard({
  catalog,
  schema,
  experimentBasePath,
  jobName,
}: {
  catalog: string;
  schema: string;
  experimentBasePath: string;
  jobName: string;
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
            <code className="font-mono text-xs">{jobName}</code>
          </div>
        </div>
      </CardContent>
    </Card>
  );
}
