"""Pydantic response models for the Genie Space Optimizer API."""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, model_serializer

from .. import __version__
from .utils import scrub_nan_inf


class SafeModel(BaseModel):
    """BaseModel that converts NaN/Inf to None during serialization."""

    @model_serializer(mode="wrap")
    def _nan_safe_serialize(self, handler: Any) -> Any:
        return scrub_nan_inf(handler(self))


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


# ── Trigger API Models ──────────────────────────────────────────────────


class TriggerRequest(BaseModel):
    space_id: str
    apply_mode: str = "genie_config"


class TriggerResponse(BaseModel):
    runId: str
    jobRunId: str
    jobUrl: str | None = None
    status: str


class RunStatusResponse(SafeModel):
    runId: str
    status: str
    spaceId: str
    startedAt: str | None = None
    completedAt: str | None = None
    baselineScore: float | None = None
    optimizedScore: float | None = None
    convergenceReason: str | None = None


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
    source: str = "app"


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


# ── ASI (Judge Feedback) Models ───────────────────────────────────────


class AsiResult(SafeModel):
    questionId: str
    judge: str
    value: str
    failureType: str | None = None
    severity: str | None = None
    confidence: float | None = None
    blameSet: list[str] = []
    counterfactualFix: str | None = None
    wrongClause: str | None = None
    expectedValue: str | None = None
    actualValue: str | None = None


class AsiSummary(SafeModel):
    runId: str
    iteration: int
    totalResults: int
    passCount: int
    failCount: int
    failureTypeDistribution: dict[str, int] = {}
    blameDistribution: dict[str, int] = {}
    judgePassRates: dict[str, float] = {}
    results: list[AsiResult] = []


# ── Provenance Models ─────────────────────────────────────────────────


class ProvenanceRecord(SafeModel):
    questionId: str
    signalType: str
    judge: str
    judgeVerdict: str
    resolvedRootCause: str
    resolutionMethod: str
    blameSet: list[str] = []
    counterfactualFix: str | None = None
    clusterId: str
    proposalId: str | None = None
    patchType: str | None = None
    gateType: str | None = None
    gateResult: str | None = None


class ProvenanceSummary(SafeModel):
    runId: str
    iteration: int
    lever: int
    totalRecords: int
    clusterCount: int
    proposalCount: int
    rootCauseDistribution: dict[str, int] = {}
    gateResults: dict[str, int] = {}
    records: list[ProvenanceRecord] = []


# ── Iteration Models ──────────────────────────────────────────────────


class IterationSummary(SafeModel):
    iteration: int
    lever: int | None = None
    evalScope: str
    overallAccuracy: float
    totalQuestions: int
    correctCount: int
    repeatabilityPct: float | None = None
    thresholdsMet: bool
    judgeScores: dict[str, float | None] = {}
