import { useState } from "react";
import {
  Card,
  CardContent,
  CardHeader,
  CardTitle,
} from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import {
  Collapsible,
  CollapsibleContent,
  CollapsibleTrigger,
} from "@/components/ui/collapsible";
import {
  Circle,
  Loader2,
  CheckCircle2,
  XCircle,
  ChevronDown,
} from "lucide-react";

interface PipelineStepCardProps {
  stepNumber: number;
  name: string;
  status: string;
  durationSeconds?: number | null;
  summary?: string | null;
  children?: React.ReactNode;
}

const statusConfig: Record<
  string,
  { icon: React.ReactNode; badge: string; color: string }
> = {
  pending: {
    icon: <Circle className="h-5 w-5 text-muted-foreground/50" />,
    badge: "Pending",
    color: "bg-muted text-muted-foreground",
  },
  running: {
    icon: <Loader2 className="h-5 w-5 animate-spin text-db-blue" />,
    badge: "Running",
    color: "bg-db-blue text-white",
  },
  completed: {
    icon: <CheckCircle2 className="h-5 w-5 text-db-green" />,
    badge: "Complete",
    color: "bg-db-green text-white",
  },
  failed: {
    icon: <XCircle className="h-5 w-5 text-db-red-error" />,
    badge: "Failed",
    color: "bg-db-red-error text-white",
  },
};

function formatDuration(seconds: number): string {
  if (seconds < 60) return `${Math.round(seconds)}s`;
  const mins = Math.floor(seconds / 60);
  const secs = Math.round(seconds % 60);
  return secs > 0 ? `${mins}m ${secs}s` : `${mins}m`;
}

export function PipelineStepCard({
  stepNumber,
  name,
  status,
  durationSeconds,
  summary,
  children,
}: PipelineStepCardProps) {
  const [open, setOpen] = useState(status === "running" || status === "failed");
  const cfg = statusConfig[status] ?? statusConfig.pending;
  const isExpandable =
    status === "completed" || status === "running" || status === "failed";

  return (
    <Collapsible open={open} onOpenChange={setOpen}>
      <Card
        className={`border transition-colors ${
          status === "running"
            ? "border-db-blue/40 shadow-sm"
            : status === "failed"
              ? "border-db-red-error/40"
              : "border-db-gray-border"
        } bg-white`}
      >
        <CollapsibleTrigger asChild disabled={!isExpandable}>
          <CardHeader
            className={`flex cursor-pointer flex-row items-center gap-3 pb-2 ${
              !isExpandable ? "cursor-default" : ""
            }`}
          >
            {cfg.icon}
            <div className="flex flex-1 items-center gap-3">
              <span className="text-xs font-medium text-muted-foreground">
                Step {stepNumber}
              </span>
              <CardTitle className="text-sm font-semibold">{name}</CardTitle>
            </div>
            <div className="flex items-center gap-2">
              {durationSeconds != null && (
                <span className="text-xs text-muted-foreground">
                  {formatDuration(durationSeconds)}
                </span>
              )}
              <Badge className={cfg.color}>{cfg.badge}</Badge>
              {isExpandable && (
                <ChevronDown
                  className={`h-4 w-4 text-muted-foreground transition-transform ${
                    open ? "rotate-180" : ""
                  }`}
                />
              )}
            </div>
          </CardHeader>
        </CollapsibleTrigger>

        <CollapsibleContent>
          <CardContent className="space-y-3 pt-0">
            {summary && (
              <p className="text-sm text-muted-foreground">{summary}</p>
            )}
            {children}
          </CardContent>
        </CollapsibleContent>
      </Card>
    </Collapsible>
  );
}
