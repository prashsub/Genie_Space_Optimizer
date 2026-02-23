# BUILD-07: React Frontend (APX — TanStack Router + shadcn/ui)

> **APX project.** The frontend lives in `src/genie_space_optimizer/ui/`. APX manages it with `bun` (not npm), uses TanStack Router (not React Router), shadcn/ui for components, and **auto-generates TypeScript API hooks** from the FastAPI OpenAPI schema. You do NOT write `fetch()` calls — you use the generated hooks. See `APX-BUILD-GUIDE.md` Step 10.

## Context

You are building the **Genie Space Optimizer**, a Databricks App (APX). This step creates the React SPA that users interact with. The SPA communicates with the FastAPI backend via auto-generated TypeScript hooks under `/api/genie/`.

**Read these reference docs before starting:**
- `docs/genie_space_optimizer/05-databricks-app-frontend.md` — **THE** primary reference:
  - Tech stack (Section 1)
  - App structure (Section 2)
  - Navigation flow (Section 3)
  - All 4 screen layouts with wireframes (Sections 4-7)
  - API endpoints (Section 9)
  - TypeScript types (Section 9 "Response Models")
  - **Visual walkthroughs** (Section 15) — exact UI state at every pipeline phase, lever sub-progress, error states, rendering decision tree
- Design intent: `design_prd.md` (user journeys) and `ui_design.md` (design principles)
- `docs/genie_space_optimizer/APX-BUILD-GUIDE.md` Step 10 — APX frontend patterns

**Depends on (already built in BUILD-06):**
- FastAPI backend endpoints with `operation_id` → auto-generated hooks

---

## APX Frontend Patterns

| Pattern | APX Way | NOT This |
|---------|---------|----------|
| Routing | TanStack Router file-based (`ui/routes/`) | React Router v6 `<Routes>` |
| Components | `apx components add card` (shadcn/ui) | Custom CSS from scratch |
| API calls | Auto-generated hooks: `useListSpaces()` | Manual `fetch()` wrappers |
| Params | `useParams({ from: "/spaces/$spaceId" })` | `useParams()` from react-router |
| Navigation | `useNavigate()` from `@tanstack/react-router` | `useNavigate()` from react-router |
| Package manager | `bun add` | `npm install` |
| Build | `apx build` (produces `__dist__/`) | `npm run build` |
| Types | Generated from OpenAPI (in `lib/api/`) | Manual TypeScript interfaces |

### Auto-Generated Hooks

When `apx dev start` runs, it watches the FastAPI OpenAPI schema and auto-generates typed React Query hooks in `ui/lib/api/`. For every endpoint with `operation_id`, you get a hook:

| Backend `operation_id` | Generated Hook | Usage |
|------------------------|---------------|-------|
| `listSpaces` | `useListSpaces()` | Dashboard |
| `getSpaceDetail` | `useGetSpaceDetail({ spaceId })` | Space Detail |
| `startOptimization` | `useStartOptimization()` | Space Detail button |
| `getRun` | `useGetRun({ runId })` | Pipeline View |
| `getComparison` | `useGetComparison({ runId })` | Comparison View |
| `applyOptimization` | `useApplyOptimization()` | Comparison View |
| `discardOptimization` | `useDiscardOptimization()` | Comparison View |
| `getActivity` | `useGetActivity()` | Dashboard |

You use these directly — no `fetch()` code needed.

---

## What to Build

### 1. Add shadcn/ui components

```bash
apx components add card badge button input table collapsible progress tabs toast separator skeleton alert
```

### 2. Route pages (TanStack Router file-based)

Create in `src/genie_space_optimizer/ui/routes/`:

```
routes/
├── __root.tsx              # Layout (header, sidebar, footer)
├── index.tsx               # Dashboard (/)
├── spaces/
│   └── $spaceId.tsx        # Space Detail (/spaces/:spaceId)
└── runs/
    ├── $runId.tsx           # Pipeline View (/runs/:runId)
    └── $runId/
        └── comparison.tsx   # Comparison View (/runs/:runId/comparison)
```

### 3. Layout — `routes/__root.tsx`

- Dark header (#1B3139) with app title "Genie Space Optimizer" and Databricks logo
- Red accent (#FF3621) for active nav / branding
- Content area with `<Outlet />`
- Footer with copyright

### 4. Screen 1: Dashboard — `routes/index.tsx`

Reference: `05-databricks-app-frontend.md` Section 4

```typescript
import { useListSpaces, useGetActivity } from "@/lib/api";
import { Card } from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { Table } from "@/components/ui/table";
import { useNavigate } from "@tanstack/react-router";
import { useState, useMemo } from "react";

export default function Dashboard() {
  const { data: spaces, isLoading: spacesLoading } = useListSpaces();
  const { data: activity } = useGetActivity();
  const [search, setSearch] = useState("");
  const navigate = useNavigate();

  const filtered = useMemo(() => {
    if (!spaces) return [];
    const q = search.toLowerCase();
    return spaces.filter(s =>
      s.name.toLowerCase().includes(q) || s.description.toLowerCase().includes(q)
    );
  }, [spaces, search]);

  // Render:
  // 1. Stats Bar: 3 Cards — Total Spaces, Recent Runs, Avg Quality
  // 2. SearchBar: Input with debounced onChange (300ms)
  // 3. Space Grid: 3-col responsive grid of SpaceCard. Click → navigate("/spaces/" + id)
  // 4. Activity Table: recent runs from activity hook
}
```

**SpaceCard component** (`ui/components/SpaceCard.tsx`):
- Uses shadcn `Card` + `Badge`
- Shows: name, description (truncated), tableCount, lastModified, qualityScore badge
- Badge color: green ≥80, yellow 60-79, red <60
- Click → navigate to `/spaces/:id`

### 5. Screen 2: Space Detail — `routes/spaces/$spaceId.tsx`

Reference: `05-databricks-app-frontend.md` Section 5

```typescript
import { useGetSpaceDetail, useStartOptimization } from "@/lib/api";
import { useNavigate, useParams } from "@tanstack/react-router";

export default function SpaceDetail() {
  const { spaceId } = useParams({ from: "/spaces/$spaceId" });
  const { data: space, isLoading } = useGetSpaceDetail({ spaceId });
  const { mutate: startOpt, isPending } = useStartOptimization();
  const navigate = useNavigate();

  function handleOptimize() {
    startOpt({ spaceId }, {
      onSuccess: (res) => navigate({ to: `/runs/${res.runId}` }),
    });
  }

  // Render:
  // 1. Header: space name, description, "Start Optimization" Button (disabled while isPending)
  // 2. Config panels (2-col): Left = instructions, Right = sample questions list
  // 3. Tables panel: full-width Table of referenced UC tables with column counts
  // 4. History: past runs Table. Click row → navigate("/runs/" + runId)
}
```

### 6. Screen 3: Pipeline View — `routes/runs/$runId.tsx`

Reference: `05-databricks-app-frontend.md` Sections 6 and **15 (Visual Walkthroughs)**

This is the most complex screen. Use the rendering decision tree from Section 15.10.

```typescript
import { useGetRun } from "@/lib/api";
import { useParams, useNavigate } from "@tanstack/react-router";

export default function PipelineView() {
  const { runId } = useParams({ from: "/runs/$runId" });
  const navigate = useNavigate();

  // POLLING: React Query refetchInterval — poll every 5s until terminal state
  const { data: run } = useGetRun({ runId }, {
    refetchInterval: (query) => {
      const status = query.state.data?.status;
      if (status && ["COMPLETED", "FAILED", "CANCELLED"].includes(status)) return false;
      return 5000;
    },
  });

  // Render:
  // 1. Progress bar: completedSteps / 5, gradient red → green
  // 2. Score summary (when COMPLETED): 3 Cards — Baseline, Optimized, Improvement
  // 3. 5 PipelineStepCards: vertically stacked, expandable via shadcn Collapsible
  // 4. Terminal state buttons:
  //    - COMPLETED → "View Results →" Button → navigate("/runs/" + runId + "/comparison")
  //    - FAILED → Alert with error + "Return to Space Detail" link
  //    - CANCELLED → Alert with cancellation notice
}
```

**PipelineStepCard component** (`ui/components/PipelineStepCard.tsx`):

Uses shadcn `Collapsible` + `Card` + `Badge`. States from Section 15.2-15.6:
- **pending:** Gray circle icon, grayed text
- **running:** Blue spinner (animated), elapsed timer ticking, "What's happening" detail text
- **completed:** Green check icon, duration, expandable summary with data
- **failed:** Red X icon, error message, troubleshooting hints

**Step 4 special handling — Lever sub-progress:**

When Step 4 (Configuration Generation) is expanded and status is `running`, render a `LeverProgress` component using the `levers` array from the API response. Each lever row shows:
- Lever name (e.g., "Table Descriptions")
- Status badge: accepted (green), rolled_back (orange), running (blue spinner), evaluating (blue), skipped (gray), pending (light gray)
- Patch count
- Score delta (+3.2% green or -1.5% red)
- Rollback reason (if rolled_back)

See `05-databricks-app-frontend.md` Section 15.5 for exact layout.

**LeverProgress component** (`ui/components/LeverProgress.tsx`):
```typescript
interface LeverProgressProps {
  levers: LeverStatus[];
}
// Render vertical list of lever rows with status indicators
```

**Time estimation** (Steps 3 and 5):
```typescript
function estimateTimeRemaining(benchmarkCount: number, elapsedMs: number): string {
  const estimatedTotal = benchmarkCount * 12_000; // ~12s per benchmark
  const remaining = Math.max(0, estimatedTotal - elapsedMs);
  return `~${Math.ceil(remaining / 60_000)}m remaining`;
}
```

### 7. Screen 4: Comparison View — `routes/runs/$runId/comparison.tsx`

Reference: `05-databricks-app-frontend.md` Section 7

```typescript
import { useGetComparison, useApplyOptimization, useDiscardOptimization } from "@/lib/api";
import { useParams, useNavigate } from "@tanstack/react-router";
import { useToast } from "@/components/ui/use-toast";

export default function ComparisonView() {
  const { runId } = useParams({ from: "/runs/$runId/comparison" });
  const { data, isLoading } = useGetComparison({ runId });
  const { mutate: apply, isPending: applying } = useApplyOptimization();
  const { mutate: discard, isPending: discarding } = useDiscardOptimization();
  const navigate = useNavigate();
  const { toast } = useToast();

  function handleApply() {
    apply({ runId }, {
      onSuccess: () => {
        toast({ title: "Optimization applied!", description: "Changes saved." });
        navigate({ to: "/" });
      },
    });
  }
  function handleDiscard() {
    discard({ runId }, {
      onSuccess: () => {
        toast({ title: "Optimization discarded", description: "Changes rolled back." });
        navigate({ to: "/" });
      },
    });
  }

  // Render:
  // 1. Header: Title, description, Apply/Discard Buttons (top)
  // 2. ScoreCard: 3 stat Cards + per-dimension score bars
  // 3. ConfigDiff: 3 Tabs (Instructions, Sample Questions, Table Descriptions)
  //    - Side-by-side, additions highlighted green, removals highlighted red
  // 4. Apply/Discard Buttons (bottom — duplicated for convenience)
}
```

**ScoreCard component** (`ui/components/ScoreCard.tsx`):
- 3 stat cards: Baseline Score, Optimized Score, Improvement %
- Per-dimension bars: dual horizontal bars (baseline gray, optimized colored)

**ConfigDiff component** (`ui/components/ConfigDiff.tsx`):
- Uses shadcn `Tabs` for Instructions / Sample Questions / Table Descriptions
- Side-by-side layout: Original (left) vs Optimized (right)
- Green background for new/changed lines, red for removed

---

## Design System

Extend Tailwind config with Databricks brand colors:

```javascript
// tailwind.config.js (or equivalent in APX project)
theme: {
  extend: {
    colors: {
      'db-dark': '#1B3139',
      'db-red': '#FF3621',
      'db-gray-bg': '#F5F5F5',
      'db-gray-border': '#E5E5E5',
      'db-green': '#00A972',
      'db-blue': '#2272B4',
      'db-orange': '#F2994A',
      'db-red-error': '#EB5757',
    },
  },
},
```

Typography: System font stack. Headings semibold. Body regular.
Cards: White bg, 1px border, 8px radius, subtle shadow on hover.

---

## File Structure

```
src/genie_space_optimizer/ui/
├── main.tsx
├── components/
│   ├── ui/                    # shadcn/ui (auto-managed by apx components add)
│   │   ├── card.tsx
│   │   ├── badge.tsx
│   │   ├── button.tsx
│   │   ├── input.tsx
│   │   ├── table.tsx
│   │   ├── collapsible.tsx
│   │   ├── progress.tsx
│   │   ├── tabs.tsx
│   │   ├── toast.tsx
│   │   ├── skeleton.tsx
│   │   └── alert.tsx
│   ├── SpaceCard.tsx          # Dashboard card for each Genie Space
│   ├── PipelineStepCard.tsx   # Pipeline View step with status/timing/summary
│   ├── LeverProgress.tsx      # Step 4 lever sub-progress display
│   ├── ScoreCard.tsx          # Comparison View score summary + dimension bars
│   └── ConfigDiff.tsx         # Side-by-side config comparison with highlighting
├── routes/
│   ├── __root.tsx             # Layout (header, nav, footer)
│   ├── index.tsx              # Dashboard
│   ├── spaces/
│   │   └── $spaceId.tsx       # Space Detail
│   └── runs/
│       ├── $runId.tsx         # Pipeline View
│       └── $runId/
│           └── comparison.tsx # Comparison View
└── lib/
    └── api/                   # Auto-generated from OpenAPI — DO NOT EDIT
        ├── client.ts
        └── hooks.ts
```

---

## Acceptance Criteria

1. All 4 screens render correctly with skeleton loaders during fetch
2. Dashboard search filters spaces in real-time (debounced 300ms)
3. Pipeline View polls every 5s, stops on terminal state
4. Pipeline View shows lever sub-progress in Step 4 (accepted/running/rolled_back/skipped)
5. Pipeline View shows estimated time remaining during Steps 3 and 5
6. Comparison View renders side-by-side diff with additions green, removals red
7. Apply/Discard buttons work, show toast, navigate to Dashboard
8. Responsive: 3-col desktop, 2-col tablet, 1-col mobile
9. All interactive elements keyboard accessible (Tab, Enter, Escape)
10. Loading and error states handled gracefully for all API calls
11. **No manual `fetch()` calls** — all API access via auto-generated hooks
12. **No React Router imports** — all routing via TanStack Router

---

## Predecessor Prompts
- BUILD-06 (Backend) — provides all API endpoints with operation_id

## Successor Prompts
- BUILD-08 (Deployment) — bundles React build output with backend
