import { Badge } from "@/components/ui/badge";
import { Loader2 } from "lucide-react";

interface LeverItem {
  lever: number;
  name: string;
  status: string;
  patchCount: number;
  scoreBefore?: number | null;
  scoreAfter?: number | null;
  scoreDelta?: number | null;
  rollbackReason?: string | null;
}

interface LeverProgressProps {
  levers: LeverItem[];
}

const leverStatusConfig: Record<
  string,
  { label: string; className: string; icon?: React.ReactNode }
> = {
  accepted: {
    label: "Accepted",
    className: "bg-db-green text-white",
  },
  rolled_back: {
    label: "Rolled Back",
    className: "bg-db-orange text-white",
  },
  running: {
    label: "Running",
    className: "bg-db-blue text-white",
    icon: <Loader2 className="mr-1 h-3 w-3 animate-spin" />,
  },
  evaluating: {
    label: "Evaluating",
    className: "bg-db-blue/80 text-white",
    icon: <Loader2 className="mr-1 h-3 w-3 animate-spin" />,
  },
  skipped: {
    label: "Skipped",
    className: "bg-muted text-muted-foreground",
  },
  pending: {
    label: "Pending",
    className: "bg-muted/50 text-muted-foreground/60",
  },
  failed: {
    label: "Failed",
    className: "bg-db-red-error text-white",
  },
};

function formatDelta(delta: number | null | undefined): React.ReactNode {
  if (delta == null) return null;
  const sign = delta >= 0 ? "+" : "";
  const color = delta >= 0 ? "text-db-green" : "text-db-red-error";
  return (
    <span className={`text-xs font-medium ${color}`}>
      {sign}
      {delta.toFixed(1)}%
    </span>
  );
}

export function LeverProgress({ levers }: LeverProgressProps) {
  if (!levers.length) return null;

  return (
    <div className="space-y-2">
      <h4 className="text-xs font-semibold uppercase tracking-wide text-muted-foreground">
        Optimization Levers
      </h4>
      <div className="space-y-1.5">
        {levers.map((lever) => {
          const cfg = leverStatusConfig[lever.status] ?? leverStatusConfig.pending;
          return (
            <div
              key={lever.lever}
              className="flex items-center gap-3 rounded-md border border-db-gray-border bg-db-gray-bg/50 px-3 py-2"
            >
              <span className="w-40 truncate text-sm font-medium">
                {lever.name}
              </span>
              <Badge className={`${cfg.className} flex items-center`}>
                {cfg.icon}
                {cfg.label}
              </Badge>
              {lever.patchCount > 0 && (
                <span className="text-xs text-muted-foreground">
                  {lever.patchCount} patch{lever.patchCount !== 1 ? "es" : ""}
                </span>
              )}
              {formatDelta(lever.scoreDelta)}
              {lever.rollbackReason && (
                <span className="ml-auto text-xs italic text-db-orange">
                  {lever.rollbackReason}
                </span>
              )}
            </div>
          );
        })}
      </div>
    </div>
  );
}
