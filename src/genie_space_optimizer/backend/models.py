"""Pydantic response models for the Genie Space Optimizer API."""

from __future__ import annotations

import math
from typing import Any

from pydantic import BaseModel, model_serializer

from .. import __version__


def _scrub_floats(obj: Any) -> Any:
    """Recursively replace NaN / Inf floats with None."""
    if isinstance(obj, float):
        return None if not math.isfinite(obj) else obj
    if isinstance(obj, dict):
        return {k: _scrub_floats(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [_scrub_floats(v) for v in obj]
    return obj


class SafeModel(BaseModel):
    """BaseModel that converts NaN/Inf to None during serialization."""

    @model_serializer(mode="wrap")
    def _nan_safe_serialize(self, handler: Any) -> Any:
        return _scrub_floats(handler(self))


class VersionOut(BaseModel):
    version: str

    @classmethod
    def from_metadata(cls):
        return cls(version=__version__)


# ── Space Models ────────────────────────────────────────────────────────


class SpaceSummary(SafeModel):
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


class FunctionInfo(BaseModel):
    name: str
    catalog: str
    schema_name: str


class JoinInfo(BaseModel):
    leftTable: str
    rightTable: str
    relationshipType: str | None = None
    joinColumns: list[str] = []


class RunSummary(SafeModel):
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
    benchmarkQuestions: list[str] = []
    tables: list[TableInfo]
    joins: list[JoinInfo] = []
    functions: list[FunctionInfo] = []
    optimizationHistory: list[RunSummary]
    hasActiveRun: bool = False


class OptimizeResponse(BaseModel):
    runId: str
    jobRunId: str
    jobUrl: str | None = None


# ── Pipeline Models ─────────────────────────────────────────────────────


class PipelineStep(SafeModel):
    stepNumber: int
    name: str
    status: str
    durationSeconds: float | None = None
    summary: str | None = None
    inputs: dict | None = None
    outputs: dict | None = None


class LeverStatus(SafeModel):
    lever: int
    name: str
    status: str
    patchCount: int = 0
    scoreBefore: float | None = None
    scoreAfter: float | None = None
    scoreDelta: float | None = None
    rollbackReason: str | None = None
    patches: list[dict] = []
    iterations: list[dict] = []


class PipelineLink(BaseModel):
    label: str
    url: str
    category: str


class PipelineRun(SafeModel):
    runId: str
    spaceId: str
    spaceName: str
    status: str
    startedAt: str
    completedAt: str | None = None
    initiatedBy: str = "system"
    baselineScore: float | None = None
    optimizedScore: float | None = None
    steps: list[PipelineStep]
    levers: list[LeverStatus] = []
    convergenceReason: str | None = None
    links: list[PipelineLink] = []


# ── Comparison Models ───────────────────────────────────────────────────


class DimensionScore(SafeModel):
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


class ComparisonData(SafeModel):
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


class ActivityItem(SafeModel):
    runId: str
    spaceId: str
    spaceName: str
    status: str
    initiatedBy: str = "system"
    baselineScore: float | None = None
    optimizedScore: float | None = None
    timestamp: str


# ── Data Access Grant Models ───────────────────────────────────────────


class DataAccessGrant(BaseModel):
    id: str
    catalog: str
    schema_name: str = ""
    grantedBy: str
    grantedAt: str
    status: str = "active"


class DataAccessGrantRequest(BaseModel):
    catalog: str
    schema_name: str


class DetectedSchema(BaseModel):
    catalog: str
    schema_name: str
    spaceCount: int
    granted: bool
    canGrant: bool = False


class DataAccessOverview(BaseModel):
    grants: list[DataAccessGrant]
    detectedSchemas: list[DetectedSchema]
    spPrincipalId: str
    spPrincipalDisplayName: str | None = None


class MissingGrantDetail(BaseModel):
    catalog: str
    schema_name: str
    grantCommand: str
