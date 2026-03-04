"use client";

import {
  CartesianGrid,
  Line,
  LineChart,
  ReferenceLine,
  XAxis,
  YAxis,
} from "recharts";
import {
  ChartContainer,
  ChartTooltip,
  ChartTooltipContent,
  type ChartConfig,
} from "@/components/ui/chart";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { useIterations } from "@/lib/transparency-api";
import type { IterationSummary } from "@/lib/transparency-api";

const chartConfig = {
  accuracy: { label: "Accuracy", color: "hsl(var(--chart-1))" },
} satisfies ChartConfig;

type ChartPoint = {
  iteration: number;
  lever: number | null;
  leverLabel: string;
  accuracy: number;
  totalQuestions: number;
};

function buildChartData(iterations: IterationSummary[]): ChartPoint[] {
  const full = iterations
    .filter((it) => it.evalScope === "full")
    .filter((it) => it.iteration === 0 || it.overallAccuracy > 0)
    .sort((a, b) => a.iteration - b.iteration);

  return full.map((it) => ({
    iteration: it.iteration,
    lever: it.lever,
    leverLabel:
      it.iteration === 0
        ? "Baseline"
        : it.lever != null && it.lever > 0
          ? `Lever ${it.lever}`
          : `Iteration ${it.iteration}`,
    accuracy: it.overallAccuracy <= 1 ? it.overallAccuracy * 100 : it.overallAccuracy,
    totalQuestions: it.totalQuestions,
  }));
}

export function IterationChart({ runId }: { runId: string }) {
  const { data, isLoading, isError, error } = useIterations(runId);

  if (isLoading) {
    return (
      <Card>
        <CardHeader>
          <CardTitle>Score Progression</CardTitle>
        </CardHeader>
        <CardContent>
          <div className="flex aspect-video items-center justify-center text-muted-foreground">
            Loading...
          </div>
        </CardContent>
      </Card>
    );
  }

  if (isError) {
    return (
      <Card>
        <CardHeader>
          <CardTitle>Score Progression</CardTitle>
        </CardHeader>
        <CardContent>
          <div className="flex aspect-video items-center justify-center text-destructive">
            {error?.message ?? "Failed to load iteration data"}
          </div>
        </CardContent>
      </Card>
    );
  }

  const chartData = data ? buildChartData(data) : [];

  if (chartData.length < 2) {
    return (
      <Card>
        <CardHeader>
          <CardTitle>Score Progression</CardTitle>
        </CardHeader>
        <CardContent>
          <div className="flex aspect-video items-center justify-center text-muted-foreground">
            Not enough data to show progression
          </div>
        </CardContent>
      </Card>
    );
  }

  return (
    <Card>
      <CardHeader>
        <CardTitle>Score Progression</CardTitle>
      </CardHeader>
      <CardContent>
        <ChartContainer config={chartConfig} className="h-[300px] w-full">
          <LineChart
            data={chartData}
            margin={{ top: 5, right: 10, left: 10, bottom: 0 }}
          >
            <CartesianGrid strokeDasharray="3 3" />
            <XAxis
              dataKey="iteration"
              tickFormatter={(val) => {
                const pt = chartData.find((p) => p.iteration === val);
                return pt?.leverLabel ?? (val === 0 ? "Baseline" : String(val));
              }}
            />
            <YAxis domain={[0, 100]} tickFormatter={(v) => `${v}%`} />
            <ReferenceLine
              y={80}
              stroke="hsl(var(--muted-foreground))"
              strokeDasharray="4 4"
              label={{ value: "Target", position: "right" }}
            />
            <ChartTooltip
              content={
                <ChartTooltipContent
                  formatter={(value, _name, _item, _index, payload) => {
                    const pt = payload as unknown as ChartPoint;
                    if (!pt) return null;
                    return (
                      <div className="grid gap-1 text-xs">
                        <div>
                          <span className="text-muted-foreground">
                            Iteration:
                          </span>{" "}
                          {pt.iteration}
                        </div>
                        <div>
                          <span className="text-muted-foreground">
                            Accuracy:
                          </span>{" "}
                          {typeof value === "number"
                            ? `${value.toFixed(1)}%`
                            : value}
                        </div>
                        <div>
                          <span className="text-muted-foreground">Lever:</span>{" "}
                          {pt.leverLabel}
                        </div>
                        <div>
                          <span className="text-muted-foreground">
                            Total questions:
                          </span>{" "}
                          {pt.totalQuestions}
                        </div>
                      </div>
                    );
                  }}
                />
              }
            />
            <Line
              type="monotone"
              dataKey="accuracy"
              stroke="hsl(var(--chart-1))"
              strokeWidth={2}
              dot={{ r: 4 }}
              activeDot={{ r: 6 }}
            />
          </LineChart>
        </ChartContainer>
      </CardContent>
    </Card>
  );
}
