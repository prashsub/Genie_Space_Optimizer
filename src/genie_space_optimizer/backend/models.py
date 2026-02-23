"""Pydantic response models for the Genie Space Optimizer API."""

from __future__ import annotations

from pydantic import BaseModel

from .. import __version__


class VersionOut(BaseModel):
    version: str

    @classmethod
    def from_metadata(cls):
        return cls(version=__version__)


# ── Space Models ────────────────────────────────────────────────────────


class SpaceSummary(BaseModel):
    id: str
    name: str
    description: str
    tableCount: int
    lastModified: str
    qualityScore: float | None = None


class TableInfo(BaseModel):
    name: str
    catalog: str
    schema_name: str
    description: str
    columnCount: int
    rowCount: int | None = None


class RunSummary(BaseModel):
    runId: str
    status: str
    baselineScore: float | None = None
    optimizedScore: float | None = None
    timestamp: str


class SpaceDetail(BaseModel):
    id: str
    name: str
    description: str
    instructions: str
    sampleQuestions: list[str]
    tables: list[TableInfo]
    optimizationHistory: list[RunSummary]


class OptimizeResponse(BaseModel):
    runId: str
    jobRunId: str


# ── Pipeline Models ─────────────────────────────────────────────────────


class PipelineStep(BaseModel):
    stepNumber: int
    name: str
    status: str
    durationSeconds: float | None = None
    summary: str | None = None
    inputs: dict | None = None
    outputs: dict | None = None


class LeverStatus(BaseModel):
    lever: int
    name: str
    status: str
    patchCount: int = 0
    scoreBefore: float | None = None
    scoreAfter: float | None = None
    scoreDelta: float | None = None
    rollbackReason: str | None = None
    patches: list[dict] = []


class PipelineRun(BaseModel):
    runId: str
    spaceId: str
    spaceName: str
    status: str
    startedAt: str
    completedAt: str | None = None
    initiatedBy: str
    baselineScore: float | None = None
    optimizedScore: float | None = None
    steps: list[PipelineStep]
    levers: list[LeverStatus] = []
    convergenceReason: str | None = None


# ── Comparison Models ───────────────────────────────────────────────────


class DimensionScore(BaseModel):
    dimension: str
    baseline: float
    optimized: float
    delta: float


class TableDescription(BaseModel):
    tableName: str
    description: str


class SpaceConfiguration(BaseModel):
    instructions: str
    sampleQuestions: list[str]
    tableDescriptions: list[TableDescription]


class ComparisonData(BaseModel):
    runId: str
    spaceId: str
    spaceName: str
    baselineScore: float
    optimizedScore: float
    improvementPct: float
    perDimensionScores: list[DimensionScore]
    original: SpaceConfiguration
    optimized: SpaceConfiguration


# ── Action Models ───────────────────────────────────────────────────────


class ActionResponse(BaseModel):
    status: str
    runId: str
    message: str


# ── Activity Models ─────────────────────────────────────────────────────


class ActivityItem(BaseModel):
    runId: str
    spaceId: str
    spaceName: str
    status: str
    initiatedBy: str
    baselineScore: float | None = None
    optimizedScore: float | None = None
    timestamp: str
