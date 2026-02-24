import { useQuery, useSuspenseQuery, useMutation } from "@tanstack/react-query";
import type { UseQueryOptions, UseSuspenseQueryOptions, UseMutationOptions } from "@tanstack/react-query";
export class ApiError extends Error {
    status: number;
    statusText: string;
    body: unknown;
    constructor(status: number, statusText: string, body: unknown){
        super(`HTTP ${status}: ${statusText}`);
        this.name = "ApiError";
        this.status = status;
        this.statusText = statusText;
        this.body = body;
    }
}
export interface ActionResponse {
    message: string;
    runId: string;
    status: string;
}
export type ActivityItem = unknown;
export type ComparisonData = unknown;
export interface ComplexValue {
    display?: string | null;
    primary?: boolean | null;
    ref?: string | null;
    type?: string | null;
    value?: string | null;
}
export interface DataAccessGrant {
    catalog: string;
    grantedAt: string;
    grantedBy: string;
    id: string;
    schema_name?: string;
    status?: string;
}
export interface DataAccessGrantRequest {
    catalog: string;
    schema_name: string;
}
export interface DataAccessOverview {
    detectedSchemas: DetectedSchema[];
    grants: DataAccessGrant[];
    spPrincipalDisplayName?: string | null;
    spPrincipalId: string;
}
export interface DetectedSchema {
    canGrant?: boolean;
    catalog: string;
    granted: boolean;
    schema_name: string;
    spaceCount: number;
}
export type DimensionScore = unknown;
export interface FunctionInfo {
    catalog: string;
    name: string;
    schema_name: string;
}
export interface HTTPValidationError {
    detail?: ValidationError[];
}
export interface JoinInfo {
    joinColumns?: string[];
    leftTable: string;
    relationshipType?: string | null;
    rightTable: string;
}
export type LeverStatus = unknown;
export interface Name {
    family_name?: string | null;
    given_name?: string | null;
}
export interface OptimizeResponse {
    jobRunId: string;
    jobUrl?: string | null;
    runId: string;
}
export interface PipelineLink {
    category: string;
    label: string;
    url: string;
}
export type PipelineRun = unknown;
export type PipelineStep = unknown;
export type RunSummary = unknown;
export interface SpaceConfiguration {
    instructions: string;
    sampleQuestions: string[];
    tableDescriptions: TableDescription[];
}
export interface SpaceDetail {
    benchmarkQuestions?: string[];
    description: string;
    functions?: FunctionInfo[];
    hasActiveRun?: boolean;
    id: string;
    instructions: string;
    joins?: JoinInfo[];
    name: string;
    optimizationHistory: RunSummary[];
    sampleQuestions: string[];
    tables: TableInfo[];
}
export type SpaceSummary = unknown;
export interface TableDescription {
    description: string;
    tableName: string;
}
export interface TableInfo {
    catalog: string;
    columnCount: number;
    description: string;
    name: string;
    rowCount?: number | null;
    schema_name: string;
}
export interface User {
    active?: boolean | null;
    display_name?: string | null;
    emails?: ComplexValue[] | null;
    entitlements?: ComplexValue[] | null;
    external_id?: string | null;
    groups?: ComplexValue[] | null;
    id?: string | null;
    name?: Name | null;
    roles?: ComplexValue[] | null;
    schemas?: UserSchema[] | null;
    user_name?: string | null;
}
export const UserSchema = {
    "urn:ietf:params:scim:schemas:core:2.0:User": "urn:ietf:params:scim:schemas:core:2.0:User",
    "urn:ietf:params:scim:schemas:extension:workspace:2.0:User": "urn:ietf:params:scim:schemas:extension:workspace:2.0:User"
} as const;
export type UserSchema = typeof UserSchema[keyof typeof UserSchema];
export interface ValidationError {
    ctx?: Record<string, unknown>;
    input?: unknown;
    loc: (string | number)[];
    msg: string;
    type: string;
}
export interface VersionOut {
    version: string;
}
export interface GetActivityParams {
    space_id?: string | null;
    limit?: number;
}
export const getActivity = async (params?: GetActivityParams, options?: RequestInit): Promise<{
    data: ActivityItem[];
}> =>{
    const searchParams = new URLSearchParams();
    if (params?.space_id != null) searchParams.set("space_id", String(params?.space_id));
    if (params?.limit != null) searchParams.set("limit", String(params?.limit));
    const queryString = searchParams.toString();
    const url = queryString ? `/api/genie/activity?${queryString}` : "/api/genie/activity";
    const res = await fetch(url, {
        ...options,
        method: "GET"
    });
    if (!res.ok) {
        const body = await res.text();
        let parsed: unknown;
        try {
            parsed = JSON.parse(body);
        } catch  {
            parsed = body;
        }
        throw new ApiError(res.status, res.statusText, parsed);
    }
    return {
        data: await res.json()
    };
};
export const getActivityKey = (params?: GetActivityParams)=>{
    return [
        "/api/genie/activity",
        params
    ] as const;
};
export function useGetActivity<TData = {
    data: ActivityItem[];
}>(options?: {
    params?: GetActivityParams;
    query?: Omit<UseQueryOptions<{
        data: ActivityItem[];
    }, ApiError, TData>, "queryKey" | "queryFn">;
}) {
    return useQuery({
        queryKey: getActivityKey(options?.params),
        queryFn: ()=>getActivity(options?.params),
        ...options?.query
    });
}
export function useGetActivitySuspense<TData = {
    data: ActivityItem[];
}>(options?: {
    params?: GetActivityParams;
    query?: Omit<UseSuspenseQueryOptions<{
        data: ActivityItem[];
    }, ApiError, TData>, "queryKey" | "queryFn">;
}) {
    return useSuspenseQuery({
        queryKey: getActivityKey(options?.params),
        queryFn: ()=>getActivity(options?.params),
        ...options?.query
    });
}
export interface CurrentUserParams {
    "X-Forwarded-Host"?: string | null;
    "X-Forwarded-Preferred-Username"?: string | null;
    "X-Forwarded-User"?: string | null;
    "X-Forwarded-Email"?: string | null;
    "X-Request-Id"?: string | null;
    "X-Forwarded-Access-Token"?: string | null;
}
export const currentUser = async (params?: CurrentUserParams, options?: RequestInit): Promise<{
    data: User;
}> =>{
    const res = await fetch("/api/genie/current-user", {
        ...options,
        method: "GET",
        headers: {
            ...(params?.["X-Forwarded-Host"] != null && {
                "X-Forwarded-Host": params["X-Forwarded-Host"]
            }),
            ...(params?.["X-Forwarded-Preferred-Username"] != null && {
                "X-Forwarded-Preferred-Username": params["X-Forwarded-Preferred-Username"]
            }),
            ...(params?.["X-Forwarded-User"] != null && {
                "X-Forwarded-User": params["X-Forwarded-User"]
            }),
            ...(params?.["X-Forwarded-Email"] != null && {
                "X-Forwarded-Email": params["X-Forwarded-Email"]
            }),
            ...(params?.["X-Request-Id"] != null && {
                "X-Request-Id": params["X-Request-Id"]
            }),
            ...(params?.["X-Forwarded-Access-Token"] != null && {
                "X-Forwarded-Access-Token": params["X-Forwarded-Access-Token"]
            }),
            ...options?.headers
        }
    });
    if (!res.ok) {
        const body = await res.text();
        let parsed: unknown;
        try {
            parsed = JSON.parse(body);
        } catch  {
            parsed = body;
        }
        throw new ApiError(res.status, res.statusText, parsed);
    }
    return {
        data: await res.json()
    };
};
export const currentUserKey = (params?: CurrentUserParams)=>{
    return [
        "/api/genie/current-user",
        params
    ] as const;
};
export function useCurrentUser<TData = {
    data: User;
}>(options?: {
    params?: CurrentUserParams;
    query?: Omit<UseQueryOptions<{
        data: User;
    }, ApiError, TData>, "queryKey" | "queryFn">;
}) {
    return useQuery({
        queryKey: currentUserKey(options?.params),
        queryFn: ()=>currentUser(options?.params),
        ...options?.query
    });
}
export function useCurrentUserSuspense<TData = {
    data: User;
}>(options?: {
    params?: CurrentUserParams;
    query?: Omit<UseSuspenseQueryOptions<{
        data: User;
    }, ApiError, TData>, "queryKey" | "queryFn">;
}) {
    return useSuspenseQuery({
        queryKey: currentUserKey(options?.params),
        queryFn: ()=>currentUser(options?.params),
        ...options?.query
    });
}
export interface GetRunParams {
    run_id: string;
    "X-Forwarded-Host"?: string | null;
    "X-Forwarded-Preferred-Username"?: string | null;
    "X-Forwarded-User"?: string | null;
    "X-Forwarded-Email"?: string | null;
    "X-Request-Id"?: string | null;
    "X-Forwarded-Access-Token"?: string | null;
}
export const getRun = async (params: GetRunParams, options?: RequestInit): Promise<{
    data: PipelineRun;
}> =>{
    const res = await fetch(`/api/genie/runs/${params.run_id}`, {
        ...options,
        method: "GET",
        headers: {
            ...(params?.["X-Forwarded-Host"] != null && {
                "X-Forwarded-Host": params["X-Forwarded-Host"]
            }),
            ...(params?.["X-Forwarded-Preferred-Username"] != null && {
                "X-Forwarded-Preferred-Username": params["X-Forwarded-Preferred-Username"]
            }),
            ...(params?.["X-Forwarded-User"] != null && {
                "X-Forwarded-User": params["X-Forwarded-User"]
            }),
            ...(params?.["X-Forwarded-Email"] != null && {
                "X-Forwarded-Email": params["X-Forwarded-Email"]
            }),
            ...(params?.["X-Request-Id"] != null && {
                "X-Request-Id": params["X-Request-Id"]
            }),
            ...(params?.["X-Forwarded-Access-Token"] != null && {
                "X-Forwarded-Access-Token": params["X-Forwarded-Access-Token"]
            }),
            ...options?.headers
        }
    });
    if (!res.ok) {
        const body = await res.text();
        let parsed: unknown;
        try {
            parsed = JSON.parse(body);
        } catch  {
            parsed = body;
        }
        throw new ApiError(res.status, res.statusText, parsed);
    }
    return {
        data: await res.json()
    };
};
export const getRunKey = (params?: GetRunParams)=>{
    return [
        "/api/genie/runs/{run_id}",
        params
    ] as const;
};
export function useGetRun<TData = {
    data: PipelineRun;
}>(options: {
    params: GetRunParams;
    query?: Omit<UseQueryOptions<{
        data: PipelineRun;
    }, ApiError, TData>, "queryKey" | "queryFn">;
}) {
    return useQuery({
        queryKey: getRunKey(options.params),
        queryFn: ()=>getRun(options.params),
        ...options?.query
    });
}
export function useGetRunSuspense<TData = {
    data: PipelineRun;
}>(options: {
    params: GetRunParams;
    query?: Omit<UseSuspenseQueryOptions<{
        data: PipelineRun;
    }, ApiError, TData>, "queryKey" | "queryFn">;
}) {
    return useSuspenseQuery({
        queryKey: getRunKey(options.params),
        queryFn: ()=>getRun(options.params),
        ...options?.query
    });
}
export interface ApplyOptimizationParams {
    run_id: string;
    "X-Forwarded-Host"?: string | null;
    "X-Forwarded-Preferred-Username"?: string | null;
    "X-Forwarded-User"?: string | null;
    "X-Forwarded-Email"?: string | null;
    "X-Request-Id"?: string | null;
    "X-Forwarded-Access-Token"?: string | null;
}
export const applyOptimization = async (params: ApplyOptimizationParams, options?: RequestInit): Promise<{
    data: ActionResponse;
}> =>{
    const res = await fetch(`/api/genie/runs/${params.run_id}/apply`, {
        ...options,
        method: "POST",
        headers: {
            ...(params?.["X-Forwarded-Host"] != null && {
                "X-Forwarded-Host": params["X-Forwarded-Host"]
            }),
            ...(params?.["X-Forwarded-Preferred-Username"] != null && {
                "X-Forwarded-Preferred-Username": params["X-Forwarded-Preferred-Username"]
            }),
            ...(params?.["X-Forwarded-User"] != null && {
                "X-Forwarded-User": params["X-Forwarded-User"]
            }),
            ...(params?.["X-Forwarded-Email"] != null && {
                "X-Forwarded-Email": params["X-Forwarded-Email"]
            }),
            ...(params?.["X-Request-Id"] != null && {
                "X-Request-Id": params["X-Request-Id"]
            }),
            ...(params?.["X-Forwarded-Access-Token"] != null && {
                "X-Forwarded-Access-Token": params["X-Forwarded-Access-Token"]
            }),
            ...options?.headers
        }
    });
    if (!res.ok) {
        const body = await res.text();
        let parsed: unknown;
        try {
            parsed = JSON.parse(body);
        } catch  {
            parsed = body;
        }
        throw new ApiError(res.status, res.statusText, parsed);
    }
    return {
        data: await res.json()
    };
};
export function useApplyOptimization(options?: {
    mutation?: UseMutationOptions<{
        data: ActionResponse;
    }, ApiError, {
        params: ApplyOptimizationParams;
    }>;
}) {
    return useMutation({
        mutationFn: (vars)=>applyOptimization(vars.params),
        ...options?.mutation
    });
}
export interface GetComparisonParams {
    run_id: string;
    "X-Forwarded-Host"?: string | null;
    "X-Forwarded-Preferred-Username"?: string | null;
    "X-Forwarded-User"?: string | null;
    "X-Forwarded-Email"?: string | null;
    "X-Request-Id"?: string | null;
    "X-Forwarded-Access-Token"?: string | null;
}
export const getComparison = async (params: GetComparisonParams, options?: RequestInit): Promise<{
    data: ComparisonData;
}> =>{
    const res = await fetch(`/api/genie/runs/${params.run_id}/comparison`, {
        ...options,
        method: "GET",
        headers: {
            ...(params?.["X-Forwarded-Host"] != null && {
                "X-Forwarded-Host": params["X-Forwarded-Host"]
            }),
            ...(params?.["X-Forwarded-Preferred-Username"] != null && {
                "X-Forwarded-Preferred-Username": params["X-Forwarded-Preferred-Username"]
            }),
            ...(params?.["X-Forwarded-User"] != null && {
                "X-Forwarded-User": params["X-Forwarded-User"]
            }),
            ...(params?.["X-Forwarded-Email"] != null && {
                "X-Forwarded-Email": params["X-Forwarded-Email"]
            }),
            ...(params?.["X-Request-Id"] != null && {
                "X-Request-Id": params["X-Request-Id"]
            }),
            ...(params?.["X-Forwarded-Access-Token"] != null && {
                "X-Forwarded-Access-Token": params["X-Forwarded-Access-Token"]
            }),
            ...options?.headers
        }
    });
    if (!res.ok) {
        const body = await res.text();
        let parsed: unknown;
        try {
            parsed = JSON.parse(body);
        } catch  {
            parsed = body;
        }
        throw new ApiError(res.status, res.statusText, parsed);
    }
    return {
        data: await res.json()
    };
};
export const getComparisonKey = (params?: GetComparisonParams)=>{
    return [
        "/api/genie/runs/{run_id}/comparison",
        params
    ] as const;
};
export function useGetComparison<TData = {
    data: ComparisonData;
}>(options: {
    params: GetComparisonParams;
    query?: Omit<UseQueryOptions<{
        data: ComparisonData;
    }, ApiError, TData>, "queryKey" | "queryFn">;
}) {
    return useQuery({
        queryKey: getComparisonKey(options.params),
        queryFn: ()=>getComparison(options.params),
        ...options?.query
    });
}
export function useGetComparisonSuspense<TData = {
    data: ComparisonData;
}>(options: {
    params: GetComparisonParams;
    query?: Omit<UseSuspenseQueryOptions<{
        data: ComparisonData;
    }, ApiError, TData>, "queryKey" | "queryFn">;
}) {
    return useSuspenseQuery({
        queryKey: getComparisonKey(options.params),
        queryFn: ()=>getComparison(options.params),
        ...options?.query
    });
}
export interface DiscardOptimizationParams {
    run_id: string;
    "X-Forwarded-Host"?: string | null;
    "X-Forwarded-Preferred-Username"?: string | null;
    "X-Forwarded-User"?: string | null;
    "X-Forwarded-Email"?: string | null;
    "X-Request-Id"?: string | null;
    "X-Forwarded-Access-Token"?: string | null;
}
export const discardOptimization = async (params: DiscardOptimizationParams, options?: RequestInit): Promise<{
    data: ActionResponse;
}> =>{
    const res = await fetch(`/api/genie/runs/${params.run_id}/discard`, {
        ...options,
        method: "POST",
        headers: {
            ...(params?.["X-Forwarded-Host"] != null && {
                "X-Forwarded-Host": params["X-Forwarded-Host"]
            }),
            ...(params?.["X-Forwarded-Preferred-Username"] != null && {
                "X-Forwarded-Preferred-Username": params["X-Forwarded-Preferred-Username"]
            }),
            ...(params?.["X-Forwarded-User"] != null && {
                "X-Forwarded-User": params["X-Forwarded-User"]
            }),
            ...(params?.["X-Forwarded-Email"] != null && {
                "X-Forwarded-Email": params["X-Forwarded-Email"]
            }),
            ...(params?.["X-Request-Id"] != null && {
                "X-Request-Id": params["X-Request-Id"]
            }),
            ...(params?.["X-Forwarded-Access-Token"] != null && {
                "X-Forwarded-Access-Token": params["X-Forwarded-Access-Token"]
            }),
            ...options?.headers
        }
    });
    if (!res.ok) {
        const body = await res.text();
        let parsed: unknown;
        try {
            parsed = JSON.parse(body);
        } catch  {
            parsed = body;
        }
        throw new ApiError(res.status, res.statusText, parsed);
    }
    return {
        data: await res.json()
    };
};
export function useDiscardOptimization(options?: {
    mutation?: UseMutationOptions<{
        data: ActionResponse;
    }, ApiError, {
        params: DiscardOptimizationParams;
    }>;
}) {
    return useMutation({
        mutationFn: (vars)=>discardOptimization(vars.params),
        ...options?.mutation
    });
}
export interface GetDataAccessParams {
    "X-Forwarded-Host"?: string | null;
    "X-Forwarded-Preferred-Username"?: string | null;
    "X-Forwarded-User"?: string | null;
    "X-Forwarded-Email"?: string | null;
    "X-Request-Id"?: string | null;
    "X-Forwarded-Access-Token"?: string | null;
}
export const getDataAccess = async (params?: GetDataAccessParams, options?: RequestInit): Promise<{
    data: DataAccessOverview;
}> =>{
    const res = await fetch("/api/genie/settings/data-access", {
        ...options,
        method: "GET",
        headers: {
            ...(params?.["X-Forwarded-Host"] != null && {
                "X-Forwarded-Host": params["X-Forwarded-Host"]
            }),
            ...(params?.["X-Forwarded-Preferred-Username"] != null && {
                "X-Forwarded-Preferred-Username": params["X-Forwarded-Preferred-Username"]
            }),
            ...(params?.["X-Forwarded-User"] != null && {
                "X-Forwarded-User": params["X-Forwarded-User"]
            }),
            ...(params?.["X-Forwarded-Email"] != null && {
                "X-Forwarded-Email": params["X-Forwarded-Email"]
            }),
            ...(params?.["X-Request-Id"] != null && {
                "X-Request-Id": params["X-Request-Id"]
            }),
            ...(params?.["X-Forwarded-Access-Token"] != null && {
                "X-Forwarded-Access-Token": params["X-Forwarded-Access-Token"]
            }),
            ...options?.headers
        }
    });
    if (!res.ok) {
        const body = await res.text();
        let parsed: unknown;
        try {
            parsed = JSON.parse(body);
        } catch  {
            parsed = body;
        }
        throw new ApiError(res.status, res.statusText, parsed);
    }
    return {
        data: await res.json()
    };
};
export const getDataAccessKey = (params?: GetDataAccessParams)=>{
    return [
        "/api/genie/settings/data-access",
        params
    ] as const;
};
export function useGetDataAccess<TData = {
    data: DataAccessOverview;
}>(options?: {
    params?: GetDataAccessParams;
    query?: Omit<UseQueryOptions<{
        data: DataAccessOverview;
    }, ApiError, TData>, "queryKey" | "queryFn">;
}) {
    return useQuery({
        queryKey: getDataAccessKey(options?.params),
        queryFn: ()=>getDataAccess(options?.params),
        ...options?.query
    });
}
export function useGetDataAccessSuspense<TData = {
    data: DataAccessOverview;
}>(options?: {
    params?: GetDataAccessParams;
    query?: Omit<UseSuspenseQueryOptions<{
        data: DataAccessOverview;
    }, ApiError, TData>, "queryKey" | "queryFn">;
}) {
    return useSuspenseQuery({
        queryKey: getDataAccessKey(options?.params),
        queryFn: ()=>getDataAccess(options?.params),
        ...options?.query
    });
}
export interface GrantDataAccessParams {
    "X-Forwarded-Host"?: string | null;
    "X-Forwarded-Preferred-Username"?: string | null;
    "X-Forwarded-User"?: string | null;
    "X-Forwarded-Email"?: string | null;
    "X-Request-Id"?: string | null;
    "X-Forwarded-Access-Token"?: string | null;
}
export const grantDataAccess = async (data: DataAccessGrantRequest, params?: GrantDataAccessParams, options?: RequestInit): Promise<{
    data: DataAccessGrant;
}> =>{
    const res = await fetch("/api/genie/settings/data-access", {
        ...options,
        method: "POST",
        headers: {
            "Content-Type": "application/json",
            ...(params?.["X-Forwarded-Host"] != null && {
                "X-Forwarded-Host": params["X-Forwarded-Host"]
            }),
            ...(params?.["X-Forwarded-Preferred-Username"] != null && {
                "X-Forwarded-Preferred-Username": params["X-Forwarded-Preferred-Username"]
            }),
            ...(params?.["X-Forwarded-User"] != null && {
                "X-Forwarded-User": params["X-Forwarded-User"]
            }),
            ...(params?.["X-Forwarded-Email"] != null && {
                "X-Forwarded-Email": params["X-Forwarded-Email"]
            }),
            ...(params?.["X-Request-Id"] != null && {
                "X-Request-Id": params["X-Request-Id"]
            }),
            ...(params?.["X-Forwarded-Access-Token"] != null && {
                "X-Forwarded-Access-Token": params["X-Forwarded-Access-Token"]
            }),
            ...options?.headers
        },
        body: JSON.stringify(data)
    });
    if (!res.ok) {
        const body = await res.text();
        let parsed: unknown;
        try {
            parsed = JSON.parse(body);
        } catch  {
            parsed = body;
        }
        throw new ApiError(res.status, res.statusText, parsed);
    }
    return {
        data: await res.json()
    };
};
export function useGrantDataAccess(options?: {
    mutation?: UseMutationOptions<{
        data: DataAccessGrant;
    }, ApiError, {
        params: GrantDataAccessParams;
        data: DataAccessGrantRequest;
    }>;
}) {
    return useMutation({
        mutationFn: (vars)=>grantDataAccess(vars.data, vars.params),
        ...options?.mutation
    });
}
export interface RevokeDataAccessParams {
    grant_id: string;
    "X-Forwarded-Host"?: string | null;
    "X-Forwarded-Preferred-Username"?: string | null;
    "X-Forwarded-User"?: string | null;
    "X-Forwarded-Email"?: string | null;
    "X-Request-Id"?: string | null;
    "X-Forwarded-Access-Token"?: string | null;
}
export const revokeDataAccess = async (params: RevokeDataAccessParams, options?: RequestInit): Promise<{
    data: DataAccessGrant;
}> =>{
    const res = await fetch(`/api/genie/settings/data-access/${params.grant_id}`, {
        ...options,
        method: "DELETE",
        headers: {
            ...(params?.["X-Forwarded-Host"] != null && {
                "X-Forwarded-Host": params["X-Forwarded-Host"]
            }),
            ...(params?.["X-Forwarded-Preferred-Username"] != null && {
                "X-Forwarded-Preferred-Username": params["X-Forwarded-Preferred-Username"]
            }),
            ...(params?.["X-Forwarded-User"] != null && {
                "X-Forwarded-User": params["X-Forwarded-User"]
            }),
            ...(params?.["X-Forwarded-Email"] != null && {
                "X-Forwarded-Email": params["X-Forwarded-Email"]
            }),
            ...(params?.["X-Request-Id"] != null && {
                "X-Request-Id": params["X-Request-Id"]
            }),
            ...(params?.["X-Forwarded-Access-Token"] != null && {
                "X-Forwarded-Access-Token": params["X-Forwarded-Access-Token"]
            }),
            ...options?.headers
        }
    });
    if (!res.ok) {
        const body = await res.text();
        let parsed: unknown;
        try {
            parsed = JSON.parse(body);
        } catch  {
            parsed = body;
        }
        throw new ApiError(res.status, res.statusText, parsed);
    }
    return {
        data: await res.json()
    };
};
export function useRevokeDataAccess(options?: {
    mutation?: UseMutationOptions<{
        data: DataAccessGrant;
    }, ApiError, {
        params: RevokeDataAccessParams;
    }>;
}) {
    return useMutation({
        mutationFn: (vars)=>revokeDataAccess(vars.params),
        ...options?.mutation
    });
}
export interface ListSpacesParams {
    "X-Forwarded-Host"?: string | null;
    "X-Forwarded-Preferred-Username"?: string | null;
    "X-Forwarded-User"?: string | null;
    "X-Forwarded-Email"?: string | null;
    "X-Request-Id"?: string | null;
    "X-Forwarded-Access-Token"?: string | null;
}
export const listSpaces = async (params?: ListSpacesParams, options?: RequestInit): Promise<{
    data: SpaceSummary[];
}> =>{
    const res = await fetch("/api/genie/spaces", {
        ...options,
        method: "GET",
        headers: {
            ...(params?.["X-Forwarded-Host"] != null && {
                "X-Forwarded-Host": params["X-Forwarded-Host"]
            }),
            ...(params?.["X-Forwarded-Preferred-Username"] != null && {
                "X-Forwarded-Preferred-Username": params["X-Forwarded-Preferred-Username"]
            }),
            ...(params?.["X-Forwarded-User"] != null && {
                "X-Forwarded-User": params["X-Forwarded-User"]
            }),
            ...(params?.["X-Forwarded-Email"] != null && {
                "X-Forwarded-Email": params["X-Forwarded-Email"]
            }),
            ...(params?.["X-Request-Id"] != null && {
                "X-Request-Id": params["X-Request-Id"]
            }),
            ...(params?.["X-Forwarded-Access-Token"] != null && {
                "X-Forwarded-Access-Token": params["X-Forwarded-Access-Token"]
            }),
            ...options?.headers
        }
    });
    if (!res.ok) {
        const body = await res.text();
        let parsed: unknown;
        try {
            parsed = JSON.parse(body);
        } catch  {
            parsed = body;
        }
        throw new ApiError(res.status, res.statusText, parsed);
    }
    return {
        data: await res.json()
    };
};
export const listSpacesKey = (params?: ListSpacesParams)=>{
    return [
        "/api/genie/spaces",
        params
    ] as const;
};
export function useListSpaces<TData = {
    data: SpaceSummary[];
}>(options?: {
    params?: ListSpacesParams;
    query?: Omit<UseQueryOptions<{
        data: SpaceSummary[];
    }, ApiError, TData>, "queryKey" | "queryFn">;
}) {
    return useQuery({
        queryKey: listSpacesKey(options?.params),
        queryFn: ()=>listSpaces(options?.params),
        ...options?.query
    });
}
export function useListSpacesSuspense<TData = {
    data: SpaceSummary[];
}>(options?: {
    params?: ListSpacesParams;
    query?: Omit<UseSuspenseQueryOptions<{
        data: SpaceSummary[];
    }, ApiError, TData>, "queryKey" | "queryFn">;
}) {
    return useSuspenseQuery({
        queryKey: listSpacesKey(options?.params),
        queryFn: ()=>listSpaces(options?.params),
        ...options?.query
    });
}
export interface GetSpaceDetailParams {
    space_id: string;
    "X-Forwarded-Host"?: string | null;
    "X-Forwarded-Preferred-Username"?: string | null;
    "X-Forwarded-User"?: string | null;
    "X-Forwarded-Email"?: string | null;
    "X-Request-Id"?: string | null;
    "X-Forwarded-Access-Token"?: string | null;
}
export const getSpaceDetail = async (params: GetSpaceDetailParams, options?: RequestInit): Promise<{
    data: SpaceDetail;
}> =>{
    const res = await fetch(`/api/genie/spaces/${params.space_id}`, {
        ...options,
        method: "GET",
        headers: {
            ...(params?.["X-Forwarded-Host"] != null && {
                "X-Forwarded-Host": params["X-Forwarded-Host"]
            }),
            ...(params?.["X-Forwarded-Preferred-Username"] != null && {
                "X-Forwarded-Preferred-Username": params["X-Forwarded-Preferred-Username"]
            }),
            ...(params?.["X-Forwarded-User"] != null && {
                "X-Forwarded-User": params["X-Forwarded-User"]
            }),
            ...(params?.["X-Forwarded-Email"] != null && {
                "X-Forwarded-Email": params["X-Forwarded-Email"]
            }),
            ...(params?.["X-Request-Id"] != null && {
                "X-Request-Id": params["X-Request-Id"]
            }),
            ...(params?.["X-Forwarded-Access-Token"] != null && {
                "X-Forwarded-Access-Token": params["X-Forwarded-Access-Token"]
            }),
            ...options?.headers
        }
    });
    if (!res.ok) {
        const body = await res.text();
        let parsed: unknown;
        try {
            parsed = JSON.parse(body);
        } catch  {
            parsed = body;
        }
        throw new ApiError(res.status, res.statusText, parsed);
    }
    return {
        data: await res.json()
    };
};
export const getSpaceDetailKey = (params?: GetSpaceDetailParams)=>{
    return [
        "/api/genie/spaces/{space_id}",
        params
    ] as const;
};
export function useGetSpaceDetail<TData = {
    data: SpaceDetail;
}>(options: {
    params: GetSpaceDetailParams;
    query?: Omit<UseQueryOptions<{
        data: SpaceDetail;
    }, ApiError, TData>, "queryKey" | "queryFn">;
}) {
    return useQuery({
        queryKey: getSpaceDetailKey(options.params),
        queryFn: ()=>getSpaceDetail(options.params),
        ...options?.query
    });
}
export function useGetSpaceDetailSuspense<TData = {
    data: SpaceDetail;
}>(options: {
    params: GetSpaceDetailParams;
    query?: Omit<UseSuspenseQueryOptions<{
        data: SpaceDetail;
    }, ApiError, TData>, "queryKey" | "queryFn">;
}) {
    return useSuspenseQuery({
        queryKey: getSpaceDetailKey(options.params),
        queryFn: ()=>getSpaceDetail(options.params),
        ...options?.query
    });
}
export interface StartOptimizationParams {
    space_id: string;
    apply_mode?: string;
    "X-Forwarded-Host"?: string | null;
    "X-Forwarded-Preferred-Username"?: string | null;
    "X-Forwarded-User"?: string | null;
    "X-Forwarded-Email"?: string | null;
    "X-Request-Id"?: string | null;
    "X-Forwarded-Access-Token"?: string | null;
}
export const startOptimization = async (params: StartOptimizationParams, options?: RequestInit): Promise<{
    data: OptimizeResponse;
}> =>{
    const searchParams = new URLSearchParams();
    if (params?.apply_mode != null) searchParams.set("apply_mode", String(params?.apply_mode));
    const queryString = searchParams.toString();
    const url = queryString ? `/api/genie/spaces/${params.space_id}/optimize?${queryString}` : `/api/genie/spaces/${params.space_id}/optimize`;
    const res = await fetch(url, {
        ...options,
        method: "POST",
        headers: {
            ...(params?.["X-Forwarded-Host"] != null && {
                "X-Forwarded-Host": params["X-Forwarded-Host"]
            }),
            ...(params?.["X-Forwarded-Preferred-Username"] != null && {
                "X-Forwarded-Preferred-Username": params["X-Forwarded-Preferred-Username"]
            }),
            ...(params?.["X-Forwarded-User"] != null && {
                "X-Forwarded-User": params["X-Forwarded-User"]
            }),
            ...(params?.["X-Forwarded-Email"] != null && {
                "X-Forwarded-Email": params["X-Forwarded-Email"]
            }),
            ...(params?.["X-Request-Id"] != null && {
                "X-Request-Id": params["X-Request-Id"]
            }),
            ...(params?.["X-Forwarded-Access-Token"] != null && {
                "X-Forwarded-Access-Token": params["X-Forwarded-Access-Token"]
            }),
            ...options?.headers
        }
    });
    if (!res.ok) {
        const body = await res.text();
        let parsed: unknown;
        try {
            parsed = JSON.parse(body);
        } catch  {
            parsed = body;
        }
        throw new ApiError(res.status, res.statusText, parsed);
    }
    return {
        data: await res.json()
    };
};
export function useStartOptimization(options?: {
    mutation?: UseMutationOptions<{
        data: OptimizeResponse;
    }, ApiError, {
        params: StartOptimizationParams;
    }>;
}) {
    return useMutation({
        mutationFn: (vars)=>startOptimization(vars.params),
        ...options?.mutation
    });
}
export const version = async (options?: RequestInit): Promise<{
    data: VersionOut;
}> =>{
    const res = await fetch("/api/genie/version", {
        ...options,
        method: "GET"
    });
    if (!res.ok) {
        const body = await res.text();
        let parsed: unknown;
        try {
            parsed = JSON.parse(body);
        } catch  {
            parsed = body;
        }
        throw new ApiError(res.status, res.statusText, parsed);
    }
    return {
        data: await res.json()
    };
};
export const versionKey = ()=>{
    return [
        "/api/genie/version"
    ] as const;
};
export function useVersion<TData = {
    data: VersionOut;
}>(options?: {
    query?: Omit<UseQueryOptions<{
        data: VersionOut;
    }, ApiError, TData>, "queryKey" | "queryFn">;
}) {
    return useQuery({
        queryKey: versionKey(),
        queryFn: ()=>version(),
        ...options?.query
    });
}
export function useVersionSuspense<TData = {
    data: VersionOut;
}>(options?: {
    query?: Omit<UseSuspenseQueryOptions<{
        data: VersionOut;
    }, ApiError, TData>, "queryKey" | "queryFn">;
}) {
    return useSuspenseQuery({
        queryKey: versionKey(),
        queryFn: ()=>version(),
        ...options?.query
    });
}
