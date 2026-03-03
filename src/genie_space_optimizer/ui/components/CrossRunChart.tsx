"use client";

import {
  BarChart,
  Bar,
  XAxis,
  YAxis,
  CartesianGrid,
} from "recharts";
import {
  ChartContainer,
  ChartTooltip,
  ChartLegend,
  ChartLegendContent,
  type ChartConfig,
} from "@/components/ui/chart";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";

export interface RunHistory {
  runId: string;
  status: string;
  baselineScore?: number | null;
  optimizedScore?: number | null;
  timestamp: string;
}

export interface CrossRunChartProps {
  runs: RunHistory[];
}

const chartConfig = {
  baseline: { label: "Baseline", color: "hsl(var(--chart-4))" },
  optimized: { label: "Optimized", color: "hsl(var(--chart-1))" },
} satisfies ChartConfig;

function formatDate(timestamp: string): string {
  return new Date(timestamp).toLocaleDateString("en-US", {
    month: "short",
    day: "numeric",
  });
}

export function CrossRunChart({ runs }: CrossRunChartProps) {
  const chartData = runs
    .filter((r) => r.baselineScore != null || r.optimizedScore != null)
    .map((r) => {
      const baseline = r.baselineScore ?? 0;
      const optimized = r.optimizedScore ?? 0;
      return {
        runId: r.runId,
        date: formatDate(r.timestamp),
        baseline,
        optimized,
        delta: optimized - baseline,
      };
    });

  if (chartData.length === 0) {
    return (
      <Card>
        <CardHeader>
          <CardTitle>Optimization Trend</CardTitle>
        </CardHeader>
        <CardContent>
          <p className="text-sm text-muted-foreground">
            No optimization data available yet
          </p>
        </CardContent>
      </Card>
    );
  }

  const CustomTooltipContent = ({
    active,
    payload,
  }: {
    active?: boolean;
    payload?: Array<{
      payload: (typeof chartData)[number];
      dataKey: string;
      value: number;
    }>;
  }) => {
    if (!active || !payload?.length) return null;
    const data = payload[0]?.payload;
    if (!data) return null;

    return (
      <div className="grid min-w-[8rem] gap-1.5 rounded-lg border border-border/50 bg-background px-2.5 py-1.5 text-xs shadow-xl">
        <div className="font-medium">
          Run: {data.runId.slice(0, 8)} • {data.date}
        </div>
        <div className="grid gap-1">
          <div className="flex justify-between gap-4">
            <span className="text-muted-foreground">Baseline</span>
            <span className="font-mono font-medium tabular-nums">
              {data.baseline.toFixed(2)}%
            </span>
          </div>
          <div className="flex justify-between gap-4">
            <span className="text-muted-foreground">Optimized</span>
            <span className="font-mono font-medium tabular-nums">
              {data.optimized.toFixed(2)}%
            </span>
          </div>
          <div className="flex justify-between gap-4">
            <span className="text-muted-foreground">Delta</span>
            <span
              className={`font-mono font-medium tabular-nums ${data.delta >= 0 ? "text-green-600" : "text-red-600"}`}
            >
              {data.delta >= 0 ? "+" : ""}
              {data.delta.toFixed(2)}%
            </span>
          </div>
        </div>
      </div>
    );
  };

  return (
    <Card>
      <CardHeader>
        <CardTitle>Optimization Trend</CardTitle>
      </CardHeader>
      <CardContent>
        <ChartContainer config={chartConfig} className="h-[300px] w-full">
          <BarChart data={chartData} margin={{ top: 8, right: 8, bottom: 8, left: 8 }}>
            <CartesianGrid vertical={false} />
            <XAxis
              dataKey="date"
              tickLine={false}
              tickMargin={10}
              axisLine={false}
            />
            <YAxis
              domain={[0, 100]}
              tickFormatter={(v) => `${v}%`}
              tickLine={false}
              axisLine={false}
            />
            <ChartTooltip content={<CustomTooltipContent />} />
            <ChartLegend
              content={(props) => (
                <ChartLegendContent
                  payload={props.payload ?? []}
                  verticalAlign={props.verticalAlign ?? "bottom"}
                />
              )}
            />
            <Bar
              dataKey="baseline"
              fill="hsl(var(--chart-4))"
              radius={[0, 0, 0, 0]}
            />
            <Bar
              dataKey="optimized"
              fill="hsl(var(--chart-1))"
              radius={[0, 0, 0, 0]}
            />
          </BarChart>
        </ChartContainer>
      </CardContent>
    </Card>
  );
}
