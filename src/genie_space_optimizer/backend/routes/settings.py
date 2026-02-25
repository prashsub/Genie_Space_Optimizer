"""Settings endpoints: SP data-access grant management."""

from __future__ import annotations

import logging
import uuid
from datetime import datetime, timezone

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.catalog import (
    Privilege,
    SecurableType,
)
from fastapi import HTTPException

from ..core import Dependencies, create_router
from ..models import (
    DataAccessGrant,
    DataAccessGrantRequest,
    DataAccessOverview,
    DetectedSchema,
)
from ..utils import get_sp_principal as _get_sp_principal
from .._spark import get_spark

router = create_router()
logger = logging.getLogger(__name__)

_ALL_PRIV = Privilege.ALL_PRIVILEGES


def _get_sp_display_name(ws: WorkspaceClient) -> str:
    """Return the app SP display name when available."""
    try:
        me = ws.current_user.me()
        for attr in ("display_name", "user_name"):
            value = getattr(me, attr, None)
            if value and str(value).strip():
                return str(value).strip()
    except Exception:
        logger.debug("Could not resolve service principal display name", exc_info=True)
    return ""


def _sp_sql_principal(sp_ws: WorkspaceClient) -> str:
    """Return the principal name to use in SQL GRANT/REVOKE statements.

    UC SQL expects the display name or application_id, not the OAuth client_id.
    """
    name = _get_sp_display_name(sp_ws)
    if not name:
        name = _get_sp_principal(sp_ws)
    return name.replace("`", "")


def _load_grants(spark, catalog: str, schema: str) -> list[dict]:
    from genie_space_optimizer.optimization.state import TABLE_DATA_ACCESS_GRANTS
    from genie_space_optimizer.common.delta_helpers import run_query

    fqn = f"`{catalog}`.`{schema}`.`{TABLE_DATA_ACCESS_GRANTS}`"
    try:
        df = run_query(spark, f"SELECT * FROM {fqn} WHERE status = 'active' ORDER BY granted_at DESC")
        return df.to_dict("records") if not df.empty else []
    except Exception:
        return []


def _probe_user_manage_privileges(
    sp_ws: WorkspaceClient,
    schemas: set[tuple[str, str]],
    user_aliases: set[str],
) -> set[tuple[str, str]]:
    """Return the subset of (catalog, schema) pairs where the user can grant.

    Uses the SP client (M2M auth, no scope restrictions) to call
    get_effective, then filters results by the current user's identity.
    A user can grant if they have MANAGE or ALL_PRIVILEGES on the catalog or schema.
    """
    if not schemas or not user_aliases:
        return set()

    by_catalog: dict[str, list[str]] = {}
    for cat, sch in schemas:
        by_catalog.setdefault(cat, []).append(sch)

    manageable: set[tuple[str, str]] = set()

    for cat, schema_list in by_catalog.items():
        cat_has_manage = False
        try:
            cat_grants = sp_ws.grants.get_effective(
                securable_type=SecurableType.CATALOG.value,
                full_name=cat,
            )
            user_privs = _effective_privileges_for_principal(
                cat_grants.privilege_assignments, user_aliases,
            )
            if Privilege.MANAGE in user_privs or _ALL_PRIV in user_privs:
                cat_has_manage = True
        except Exception:
            logger.debug("Could not probe catalog-level grants on %s", cat, exc_info=True)

        if cat_has_manage:
            for sch in schema_list:
                manageable.add((cat.lower(), sch.lower()))
            continue

        for sch in schema_list:
            try:
                sch_grants = sp_ws.grants.get_effective(
                    securable_type=SecurableType.SCHEMA.value,
                    full_name=f"{cat}.{sch}",
                )
                user_privs = _effective_privileges_for_principal(
                    sch_grants.privilege_assignments, user_aliases,
                )
                if Privilege.MANAGE in user_privs or _ALL_PRIV in user_privs:
                    manageable.add((cat.lower(), sch.lower()))
            except Exception:
                logger.debug(
                    "Could not probe schema-level grants on %s.%s", cat, sch, exc_info=True
                )
    return manageable


def _get_sp_principal_aliases(sp_ws: WorkspaceClient) -> set[str]:
    """Return known principal aliases for the app service principal."""
    aliases: set[str] = set()
    aliases.add(_get_sp_principal(sp_ws).lower())
    try:
        me = sp_ws.current_user.me()
        for attr in ("user_name", "display_name", "application_id", "id"):
            value = getattr(me, attr, None)
            if value:
                aliases.add(str(value).lower())
    except Exception:
        logger.debug("Could not resolve SP aliases from current_user.me()", exc_info=True)
    return aliases


def _effective_privileges_for_principal(
    privilege_assignments,
    principal_aliases: set[str],
) -> set[Privilege]:
    """Extract effective privileges for a specific principal from assignment rows."""
    effective: set[Privilege] = set()
    if not privilege_assignments:
        return effective
    for assignment in privilege_assignments:
        principal = str(getattr(assignment, "principal", "") or "").lower()
        if principal and principal not in principal_aliases:
            continue
        for granted in getattr(assignment, "privileges", None) or []:
            priv = getattr(granted, "privilege", None)
            if priv:
                effective.add(priv)
    return effective


def _probe_sp_required_access(
    sp_ws: WorkspaceClient,
    schemas: set[tuple[str, str]],
) -> set[tuple[str, str]]:
    """Return schemas where the SP already has required privileges."""
    if not schemas:
        return set()

    by_catalog: dict[str, list[str]] = {}
    for cat, sch in schemas:
        by_catalog.setdefault(cat, []).append(sch)

    aliases = _get_sp_principal_aliases(sp_ws)
    granted: set[tuple[str, str]] = set()

    for cat, schema_list in by_catalog.items():
        catalog_privs: set[Privilege] = set()
        try:
            cat_eff = sp_ws.grants.get_effective(
                securable_type=SecurableType.CATALOG.value,
                full_name=cat,
            )
            catalog_privs = _effective_privileges_for_principal(
                cat_eff.privilege_assignments,
                aliases,
            )
        except Exception:
            logger.debug("Could not read effective grants for catalog %s", cat, exc_info=True)

        for sch in schema_list:
            schema_privs: set[Privilege] = set()
            try:
                sch_eff = sp_ws.grants.get_effective(
                    securable_type=SecurableType.SCHEMA.value,
                    full_name=f"{cat}.{sch}",
                )
                schema_privs = _effective_privileges_for_principal(
                    sch_eff.privilege_assignments,
                    aliases,
                )
            except Exception:
                logger.debug(
                    "Could not read effective grants for schema %s.%s",
                    cat,
                    sch,
                    exc_info=True,
                )

            all_privs = catalog_privs | schema_privs

            has_catalog_access = bool(all_privs) or (_ALL_PRIV in all_privs)
            has_schema_access = (Privilege.USE_SCHEMA in all_privs) or (_ALL_PRIV in all_privs)
            has_select = (Privilege.SELECT in all_privs) or (_ALL_PRIV in all_privs)

            if has_catalog_access and has_schema_access and has_select:
                granted.add((cat.lower(), sch.lower()))

    return granted


def _detect_schemas_from_spaces(
    sp_ws: WorkspaceClient,
    grants: list[dict],
    user_aliases: set[str],
) -> tuple[list[DetectedSchema], set[tuple[str, str]]]:
    """Discover catalog.schema pairs referenced by existing Genie spaces.

    Returns (detected_schemas, sp_effective_granted) so the caller can build
    synthetic Active Grants for externally-managed UC access.
    """
    from genie_space_optimizer.common.genie_client import list_spaces, fetch_space_config
    from genie_space_optimizer.common.uc_metadata import extract_genie_space_table_refs

    granted_set_from_ledger = {
        (g.get("target_catalog", "").lower(), g.get("target_schema", "").lower())
        for g in grants
    }

    schema_space_count: dict[tuple[str, str], int] = {}
    try:
        space_list = list_spaces(sp_ws)
    except Exception:
        space_list = []

    for space in space_list:
        space_id = space.get("id", "")
        if not space_id:
            continue
        try:
            config = fetch_space_config(sp_ws, space_id)
            refs = extract_genie_space_table_refs(config)
            for ref in refs:
                cat = ref[0] if isinstance(ref, (list, tuple)) else ""
                sch = ref[1] if isinstance(ref, (list, tuple)) and len(ref) > 1 else ""
                if cat and sch:
                    key = (cat.lower(), sch.lower())
                    schema_space_count[key] = schema_space_count.get(key, 0) + 1
        except Exception:
            continue

    sp_effective_granted = _probe_sp_required_access(sp_ws, set(schema_space_count.keys()))
    granted_set = granted_set_from_ledger.union(sp_effective_granted)

    ungranted = {k for k in schema_space_count if k not in granted_set}
    manageable = _probe_user_manage_privileges(sp_ws, ungranted, user_aliases)

    detected: list[DetectedSchema] = []
    for (cat, sch), count in sorted(schema_space_count.items()):
        is_granted = (cat, sch) in granted_set
        detected.append(DetectedSchema(
            catalog=cat,
            schema_name=sch,
            spaceCount=count,
            granted=is_granted,
            canGrant=not is_granted and (cat, sch) in manageable,
        ))
    return detected, sp_effective_granted


@router.get(
    "/settings/data-access",
    response_model=DataAccessOverview,
    operation_id="getDataAccess",
)
def get_data_access(
    sp_ws: Dependencies.Client,
    config: Dependencies.Config,
    headers: Dependencies.Headers,
):
    """List active data-access grants and auto-detected schemas."""
    spark = get_spark()
    grants_rows = _load_grants(spark, config.catalog, config.schema_name)

    ledger_grants = [
        DataAccessGrant(
            id=str(g.get("grant_id", "")),
            catalog=str(g.get("target_catalog", "")),
            schema_name=str(g.get("target_schema", "")),
            grantedBy=str(g.get("granted_by", "")),
            grantedAt=str(g.get("granted_at", "")),
            status=str(g.get("status", "active")),
            source="app",
        )
        for g in grants_rows
    ]

    user_aliases: set[str] = set()
    for val in (headers.user_email, headers.user_name):
        if val:
            user_aliases.add(val.lower())

    detected, sp_effective_granted = _detect_schemas_from_spaces(
        sp_ws, grants_rows, user_aliases,
    )

    ledger_keys = {
        (g.get("target_catalog", "").lower(), g.get("target_schema", "").lower())
        for g in grants_rows
    }
    uc_grants = [
        DataAccessGrant(
            id=f"uc-{cat}-{sch}",
            catalog=cat,
            schema_name=sch,
            grantedBy="Detected from UC",
            grantedAt="",
            status="active",
            source="uc",
        )
        for cat, sch in sorted(sp_effective_granted)
        if (cat, sch) not in ledger_keys
    ]

    sp_id = _get_sp_principal(sp_ws)
    sp_display_name = _get_sp_display_name(sp_ws)

    return DataAccessOverview(
        grants=ledger_grants + uc_grants,
        detectedSchemas=detected,
        spPrincipalId=sp_id,
        spPrincipalDisplayName=sp_display_name or None,
    )


@router.post(
    "/settings/data-access",
    response_model=DataAccessGrant,
    operation_id="grantDataAccess",
)
def grant_data_access(
    body: DataAccessGrantRequest,
    ws: Dependencies.UserClient,
    sp_ws: Dependencies.Client,
    config: Dependencies.Config,
    headers: Dependencies.Headers,
):
    """Grant the app SP read access to a catalog.schema using the caller's UC privileges.

    Executes GRANT SQL via the Statement Execution API (covered by the ``sql``
    OBO scope) instead of the UC REST API which requires an unavailable scope.
    """
    from genie_space_optimizer.optimization.state import (
        ensure_optimization_tables,
    )

    spark = get_spark()
    ensure_optimization_tables(spark, config.catalog, config.schema_name)

    sp_name = _sp_sql_principal(sp_ws)
    current_user = headers.user_email or headers.user_name or "unknown"

    cat = body.catalog.strip().strip("`")
    sch = body.schema_name.strip().strip("`")
    if not cat or not sch:
        raise HTTPException(status_code=400, detail="Both catalog and schema_name are required.")

    if not config.warehouse_id:
        raise HTTPException(status_code=500, detail="No SQL warehouse configured for grant execution.")

    grant_statements = [
        f"GRANT USE CATALOG ON CATALOG `{cat}` TO `{sp_name}`",
        f"GRANT USE SCHEMA ON SCHEMA `{cat}`.`{sch}` TO `{sp_name}`",
        f"GRANT SELECT ON SCHEMA `{cat}`.`{sch}` TO `{sp_name}`",
        f"GRANT EXECUTE ON SCHEMA `{cat}`.`{sch}` TO `{sp_name}`",
    ]

    errors: list[str] = []
    for stmt in grant_statements:
        try:
            ws.statement_execution.execute_statement(
                warehouse_id=config.warehouse_id,
                statement=stmt,
                wait_timeout="30s",
            )
            logger.info("Executed: %s", stmt)
        except Exception as exc:
            msg = str(exc)
            logger.warning("Grant SQL failed — %s — %s", stmt, msg)
            errors.append(f"`{stmt}`: {msg[:300]}")

    if errors:
        raise HTTPException(
            status_code=403,
            detail=(
                f"Some grants failed on `{cat}`.`{sch}`. "
                f"You may not have MANAGE privilege on this catalog/schema.\n\n"
                + "\n".join(errors)
            ),
        )

    grant_id = str(uuid.uuid4())
    now = datetime.now(timezone.utc).isoformat()
    from genie_space_optimizer.optimization.state import TABLE_DATA_ACCESS_GRANTS

    fqn = f"`{config.catalog}`.`{config.schema_name}`.`{TABLE_DATA_ACCESS_GRANTS}`"
    safe_user = current_user.replace("'", "''")
    spark.sql(
        f"INSERT INTO {fqn} VALUES ("
        f"'{grant_id}', '{cat}', '{sch}', '{safe_user}', "
        f"TIMESTAMP '{now}', NULL, 'active')"
    )

    return DataAccessGrant(
        id=grant_id,
        catalog=cat,
        schema_name=sch,
        grantedBy=current_user,
        grantedAt=now,
        status="active",
    )


@router.delete(
    "/settings/data-access/{grant_id}",
    response_model=DataAccessGrant,
    operation_id="revokeDataAccess",
)
def revoke_data_access(
    grant_id: str,
    ws: Dependencies.UserClient,
    sp_ws: Dependencies.Client,
    config: Dependencies.Config,
):
    """Revoke previously granted SP access to a catalog.schema."""
    spark = get_spark()
    grants = _load_grants(spark, config.catalog, config.schema_name)
    grant = next((g for g in grants if str(g.get("grant_id")) == grant_id), None)
    if not grant:
        raise HTTPException(status_code=404, detail=f"Grant {grant_id} not found.")

    sp_name = _sp_sql_principal(sp_ws)
    cat = str(grant["target_catalog"])
    sch = str(grant["target_schema"])

    if not config.warehouse_id:
        raise HTTPException(status_code=500, detail="No SQL warehouse configured for revoke execution.")

    revoke_statements = [
        f"REVOKE USE CATALOG ON CATALOG `{cat}` FROM `{sp_name}`",
        f"REVOKE USE SCHEMA ON SCHEMA `{cat}`.`{sch}` FROM `{sp_name}`",
        f"REVOKE SELECT ON SCHEMA `{cat}`.`{sch}` FROM `{sp_name}`",
        f"REVOKE EXECUTE ON SCHEMA `{cat}`.`{sch}` FROM `{sp_name}`",
    ]

    for stmt in revoke_statements:
        try:
            ws.statement_execution.execute_statement(
                warehouse_id=config.warehouse_id,
                statement=stmt,
                wait_timeout="30s",
            )
            logger.info("Executed: %s", stmt)
        except Exception as exc:
            logger.warning("Revoke SQL failed (best-effort) — %s — %s", stmt, exc)

    now = datetime.now(timezone.utc).isoformat()
    from genie_space_optimizer.optimization.state import TABLE_DATA_ACCESS_GRANTS

    fqn = f"`{config.catalog}`.`{config.schema_name}`.`{TABLE_DATA_ACCESS_GRANTS}`"
    spark.sql(
        f"UPDATE {fqn} SET status = 'revoked', revoked_at = TIMESTAMP '{now}' "
        f"WHERE grant_id = '{grant_id}'"
    )

    return DataAccessGrant(
        id=grant_id,
        catalog=cat,
        schema_name=sch,
        grantedBy=str(grant.get("granted_by", "")),
        grantedAt=str(grant.get("granted_at", "")),
        status="revoked",
    )
