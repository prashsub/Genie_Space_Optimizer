import { createFileRoute } from "@tanstack/react-router";
import {
  useGetDataAccessSuspense,
  useGrantDataAccess,
  useRevokeDataAccess,
  getDataAccessKey,
  type DataAccessGrant,
  type DetectedSchema,
} from "@/lib/api";
import selector from "@/lib/selector";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Badge } from "@/components/ui/badge";
import { Skeleton } from "@/components/ui/skeleton";
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert";
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
  Plus,
  Trash2,
  CheckCircle2,
  AlertTriangle,
  Database,
  Copy,
  Check,
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
        Could not load data access settings. Please check your connection and
        try again.
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
  const { data } = useGetDataAccessSuspense(selector());
  const queryClient = useQueryClient();

  const grants = (data as any)?.grants ?? [];
  const detectedSchemas = (data as any)?.detectedSchemas ?? [];
  const spPrincipalId = (data as any)?.spPrincipalId ?? "";
  const spPrincipalDisplayName = (data as any)?.spPrincipalDisplayName ?? "";

  return (
    <div className="space-y-6">
      <div>
        <h1 className="text-2xl font-bold text-foreground">
          Data Access Settings
        </h1>
        <p className="mt-1 text-sm text-muted-foreground">
          Grant the optimizer service principal read access to catalogs and
          schemas so it can read metadata and execute queries during
          optimization.
        </p>
      </div>

      <SPIdentityCard
        spId={spPrincipalId}
        spDisplayName={spPrincipalDisplayName}
      />

      <DetectedSchemasCard
        schemas={detectedSchemas}
        spPrincipalId={spPrincipalId}
        spDisplayName={spPrincipalDisplayName}
        queryClient={queryClient}
      />

      <ManualGrantCard queryClient={queryClient} />

      <ActiveGrantsCard
        grants={grants}
        queryClient={queryClient}
      />
    </div>
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
          Grants give it SELECT, EXECUTE, USE CATALOG, and USE SCHEMA on
          target schemas.
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

function _buildGrantSql(
  spDisplayName: string,
  spId: string,
  catalog: string,
  schema: string,
) {
  const principal = spDisplayName || spId;
  return [
    `GRANT USE CATALOG ON CATALOG \`${catalog}\` TO \`${principal}\`;`,
    `GRANT USE SCHEMA ON SCHEMA \`${catalog}\`.\`${schema}\` TO \`${principal}\`;`,
    `GRANT SELECT ON SCHEMA \`${catalog}\`.\`${schema}\` TO \`${principal}\`;`,
    `GRANT EXECUTE ON SCHEMA \`${catalog}\`.\`${schema}\` TO \`${principal}\`;`,
  ].join("\n");
}

function CopyableSql({ sql }: { sql: string }) {
  const [copied, setCopied] = useState(false);

  return (
    <div className="mt-2 flex items-start gap-2">
      <pre className="flex-1 overflow-x-auto rounded-md bg-muted px-3 py-2 text-xs font-mono whitespace-pre-wrap">
        {sql}
      </pre>
      <Button
        variant="ghost"
        size="sm"
        className="mt-1 h-7 w-7 shrink-0 p-0"
        onClick={() => {
          navigator.clipboard.writeText(sql);
          setCopied(true);
          setTimeout(() => setCopied(false), 2000);
        }}
      >
        {copied ? (
          <Check className="h-3.5 w-3.5 text-green-600" />
        ) : (
          <Copy className="h-3.5 w-3.5" />
        )}
      </Button>
    </div>
  );
}

function DetectedSchemasCard({
  schemas,
  spPrincipalId,
  spDisplayName,
  queryClient,
}: {
  schemas: DetectedSchema[];
  spPrincipalId: string;
  spDisplayName: string;
  queryClient: ReturnType<typeof useQueryClient>;
}) {
  const grantMutation = useGrantDataAccess();
  const [expandedRow, setExpandedRow] = useState<string | null>(null);

  if (schemas.length === 0) {
    return null;
  }

  const handleGrant = (schema: DetectedSchema) => {
    grantMutation.mutate(
      { params: {}, data: { catalog: schema.catalog, schema_name: schema.schema_name } },
      {
        onSuccess: () => {
          toast.success(
            `Granted access to ${schema.catalog}.${schema.schema_name}`,
          );
          queryClient.invalidateQueries({
            queryKey: getDataAccessKey(),
          });
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
          <Database className="h-4 w-4 text-orange-600" />
          Detected Schemas from Genie Spaces
        </CardTitle>
      </CardHeader>
      <CardContent>
        <p className="mb-3 text-sm text-muted-foreground">
          These schemas are referenced by existing Genie spaces. Grant access so
          the optimizer can work with them.
        </p>
        <Table>
          <TableHeader>
            <TableRow>
              <TableHead>Catalog</TableHead>
              <TableHead>Schema</TableHead>
              <TableHead className="text-center">Spaces</TableHead>
              <TableHead className="text-center">Status</TableHead>
              <TableHead className="text-right">Action</TableHead>
            </TableRow>
          </TableHeader>
          <TableBody>
            {schemas.map((s) => {
              const rowKey = `${s.catalog}.${s.schema_name}`;
              const isExpanded = expandedRow === rowKey;
              return (
                <>
                  <TableRow key={rowKey}>
                    <TableCell className="font-mono text-sm">
                      {s.catalog}
                    </TableCell>
                    <TableCell className="font-mono text-sm">
                      {s.schema_name}
                    </TableCell>
                    <TableCell className="text-center">
                      {s.spaceCount}
                    </TableCell>
                    <TableCell className="text-center">
                      {s.granted ? (
                        <Badge
                          variant="outline"
                          className="border-green-200 bg-green-50 text-green-700"
                        >
                          <CheckCircle2 className="mr-1 h-3 w-3" />
                          Granted
                        </Badge>
                      ) : s.canGrant ? (
                        <Badge
                          variant="outline"
                          className="border-amber-200 bg-amber-50 text-amber-700"
                        >
                          <AlertTriangle className="mr-1 h-3 w-3" />
                          Not Granted
                        </Badge>
                      ) : (
                        <Badge
                          variant="outline"
                          className="border-gray-200 bg-gray-50 text-gray-500"
                        >
                          <Shield className="mr-1 h-3 w-3" />
                          No Permission
                        </Badge>
                      )}
                    </TableCell>
                    <TableCell className="text-right">
                      {!s.granted && s.canGrant && (
                        <Button
                          size="sm"
                          variant="outline"
                          disabled={grantMutation.isPending}
                          onClick={() => handleGrant(s)}
                        >
                          <Plus className="mr-1 h-3 w-3" />
                          Grant
                        </Button>
                      )}
                      {!s.granted && !s.canGrant && (
                        <Button
                          size="sm"
                          variant="ghost"
                          className="text-xs text-muted-foreground"
                          onClick={() =>
                            setExpandedRow(isExpanded ? null : rowKey)
                          }
                        >
                          {isExpanded ? "Hide SQL" : "Show SQL"}
                        </Button>
                      )}
                    </TableCell>
                  </TableRow>
                  {isExpanded && !s.granted && !s.canGrant && (
                    <TableRow key={`${rowKey}-sql`}>
                      <TableCell colSpan={5} className="bg-muted/30 py-2">
                        <p className="mb-1 text-xs text-muted-foreground">
                          You don't have MANAGE privilege on this schema. Ask a
                          catalog owner to run the following SQL, or grant access
                          via this page:
                        </p>
                        <CopyableSql
                          sql={_buildGrantSql(
                            spDisplayName,
                            spPrincipalId,
                            s.catalog,
                            s.schema_name,
                          )}
                        />
                      </TableCell>
                    </TableRow>
                  )}
                </>
              );
            })}
          </TableBody>
        </Table>
      </CardContent>
    </Card>
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
            queryKey: getDataAccessKey(),
          });
        },
        onError: (err) => {
          const detail =
            (err as any)?.body?.detail || err.message;
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
          Grant Access to a Schema
        </CardTitle>
      </CardHeader>
      <CardContent>
        <p className="mb-3 text-sm text-muted-foreground">
          Enter a catalog and schema you have MANAGE privilege on. The app will
          execute GRANT statements using your identity.
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
            {grantMutation.isPending ? "Granting..." : "Grant Access"}
          </Button>
        </form>
      </CardContent>
    </Card>
  );
}

function ActiveGrantsCard({
  grants,
  queryClient,
}: {
  grants: DataAccessGrant[];
  queryClient: ReturnType<typeof useQueryClient>;
}) {
  const revokeMutation = useRevokeDataAccess();

  const handleRevoke = (grant: DataAccessGrant) => {
    if (
      !confirm(
        `Revoke access to ${grant.catalog}.${grant.schema_name}? The optimizer will no longer be able to optimize spaces that reference this schema.`,
      )
    ) {
      return;
    }
    revokeMutation.mutate(
      { params: { grant_id: grant.id } },
      {
        onSuccess: () => {
          toast.success(
            `Revoked access to ${grant.catalog}.${grant.schema_name}`,
          );
          queryClient.invalidateQueries({
            queryKey: getDataAccessKey(),
          });
        },
        onError: (err) => {
          toast.error(`Revoke failed: ${err.message}`);
        },
      },
    );
  };

  return (
    <Card>
      <CardHeader className="pb-3">
        <CardTitle className="flex items-center gap-2 text-base">
          <Shield className="h-4 w-4 text-green-600" />
          Active Grants
        </CardTitle>
      </CardHeader>
      <CardContent>
        {grants.length === 0 ? (
          <p className="py-8 text-center text-sm text-muted-foreground">
            No grants configured yet. The optimizer needs access to catalog
            schemas referenced by your Genie spaces.
          </p>
        ) : (
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead>Catalog</TableHead>
                <TableHead>Schema</TableHead>
                <TableHead>Granted By</TableHead>
                <TableHead>Granted At</TableHead>
                <TableHead className="text-right">Action</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {grants.map((g) => {
                const isUcDetected = (g as any).source === "uc";
                return (
                  <TableRow key={g.id}>
                    <TableCell className="font-mono text-sm">
                      {g.catalog}
                    </TableCell>
                    <TableCell className="font-mono text-sm">
                      {g.schema_name}
                    </TableCell>
                    <TableCell className="text-sm">
                      {isUcDetected ? (
                        <span className="text-muted-foreground italic">
                          {g.grantedBy}
                        </span>
                      ) : (
                        g.grantedBy
                      )}
                    </TableCell>
                    <TableCell className="text-sm text-muted-foreground">
                      {g.grantedAt
                        ? new Date(g.grantedAt).toLocaleDateString()
                        : "\u2014"}
                    </TableCell>
                    <TableCell className="text-right">
                      {isUcDetected ? (
                        <Badge
                          variant="outline"
                          className="border-blue-200 bg-blue-50 text-blue-700 text-xs"
                        >
                          External
                        </Badge>
                      ) : (
                        <Button
                          size="sm"
                          variant="ghost"
                          className="text-red-600 hover:bg-red-50 hover:text-red-700"
                          disabled={revokeMutation.isPending}
                          onClick={() => handleRevoke(g)}
                        >
                          <Trash2 className="mr-1 h-3 w-3" />
                          Revoke
                        </Button>
                      )}
                    </TableCell>
                  </TableRow>
                );
              })}
            </TableBody>
          </Table>
        )}
      </CardContent>
    </Card>
  );
}
