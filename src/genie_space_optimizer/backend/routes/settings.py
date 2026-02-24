"""Settings endpoints: SP data-access grant management."""

from __future__ import annotations

import logging
import os
import uuid
from datetime import datetime, timezone

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.catalog import (
    Privilege,
    PermissionsChange,
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
from .._spark import get_spark

router = create_router()
logger = logging.getLogger(__name__)

_CATALOG_PRIVILEGES = [Privilege.USE_CATALOG]
_SCHEMA_PRIVILEGES = [Privilege.USE_SCHEMA, Privilege.SELECT, Privilege.EXECUTE]
_ALL_PRIV = Privilege.ALL_PRIVILEGES


def _get_sp_principal(ws: WorkspaceClient) -> str:
    """Return the app's service principal client ID."""
    cid = ws.config.client_id or os.getenv("DATABRICKS_CLIENT_ID", "")
    if not cid:
        raise HTTPException(
            status_code=500,
            detail="Cannot determine app service principal ID.",
        )
    return cid


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
    user_ws: WorkspaceClient,
    schemas: set[tuple[str, str]],
) -> set[tuple[str, str]]:
    """Return the subset of (catalog, schema) pairs where the user can grant.

    Uses the UC REST API (ws.grants.get) to check effective privileges.
    A user can grant if they have MANAGE or OWN on the catalog or schema.
    """
    if not schemas:
        return set()

    by_catalog: dict[str, list[str]] = {}
    for cat, sch in schemas:
        by_catalog.setdefault(cat, []).append(sch)

    manageable: set[tuple[str, str]] = set()

    for cat, schema_list in by_catalog.items():
        cat_has_manage = False
        try:
            cat_grants = user_ws.grants.get_effective(
                securable_type=SecurableType.CATALOG.value,
                full_name=cat,
            )
            if cat_grants.privilege_assignments:
                for pa in cat_grants.privilege_assignments:
                    privs = {p.privilege for p in (pa.privileges or [])}
                    if Privilege.MANAGE in privs or Privilege.ALL_PRIVILEGES in privs:
                        cat_has_manage = True
                        break
        except Exception:
            logger.debug("Could not probe catalog-level grants on %s", cat, exc_info=True)

        if cat_has_manage:
            for sch in schema_list:
                manageable.add((cat.lower(), sch.lower()))
            continue

        for sch in schema_list:
            try:
                sch_grants = user_ws.grants.get_effective(
                    securable_type=SecurableType.SCHEMA.value,
                    full_name=f"{cat}.{sch}",
                )
                if sch_grants.privilege_assignments:
                    for pa in sch_grants.privilege_assignments:
                        privs = {p.privilege for p in (pa.privileges or [])}
                        if Privilege.MANAGE in privs or Privilege.ALL_PRIVILEGES in privs:
                            manageable.add((cat.lower(), sch.lower()))
                            break
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

            use_catalog = (
                (Privilege.USE_CATALOG in catalog_privs)
                or (Privilege.USE_CATALOG in schema_privs)
                or (_ALL_PRIV in catalog_privs)
                or (_ALL_PRIV in schema_privs)
            )
            use_schema = (Privilege.USE_SCHEMA in schema_privs) or (_ALL_PRIV in schema_privs)
            can_select = (Privilege.SELECT in schema_privs) or (_ALL_PRIV in schema_privs)
            can_execute = (Privilege.EXECUTE in schema_privs) or (_ALL_PRIV in schema_privs)

            if use_catalog and use_schema and can_select and can_execute:
                granted.add((cat.lower(), sch.lower()))

    return granted


def _detect_schemas_from_spaces(
    sp_ws: WorkspaceClient,
    user_ws: WorkspaceClient,
    grants: list[dict],
) -> list[DetectedSchema]:
    """Discover catalog.schema pairs referenced by existing Genie spaces."""
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
    manageable = _probe_user_manage_privileges(user_ws, ungranted)

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
    return detected


@router.get(
    "/settings/data-access",
    response_model=DataAccessOverview,
    operation_id="getDataAccess",
)
def get_data_access(
    ws: Dependencies.UserClient,
    sp_ws: Dependencies.Client,
    config: Dependencies.Config,
):
    """List active data-access grants and auto-detected schemas."""
    spark = get_spark()
    grants_rows = _load_grants(spark, config.catalog, config.schema_name)
    grants = [
        DataAccessGrant(
            id=str(g.get("grant_id", "")),
            catalog=str(g.get("target_catalog", "")),
            schema_name=str(g.get("target_schema", "")),
            grantedBy=str(g.get("granted_by", "")),
            grantedAt=str(g.get("granted_at", "")),
            status=str(g.get("status", "active")),
        )
        for g in grants_rows
    ]

    detected = _detect_schemas_from_spaces(sp_ws, ws, grants_rows)
    sp_id = _get_sp_principal(sp_ws)
    sp_display_name = _get_sp_display_name(sp_ws)

    return DataAccessOverview(
        grants=grants,
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
    """Grant the app SP read access to a catalog.schema using the caller's UC privileges."""
    from genie_space_optimizer.optimization.state import (
        ensure_optimization_tables,
    )

    spark = get_spark()
    ensure_optimization_tables(spark, config.catalog, config.schema_name)

    sp_id = _get_sp_principal(sp_ws)
    current_user = headers.user_email or headers.user_name or "unknown"

    cat = body.catalog.strip().strip("`")
    sch = body.schema_name.strip().strip("`")
    if not cat or not sch:
        raise HTTPException(status_code=400, detail="Both catalog and schema_name are required.")

    errors: list[str] = []

    try:
        ws.grants.update(
            securable_type=SecurableType.CATALOG.value,
            full_name=cat,
            changes=[PermissionsChange(add=_CATALOG_PRIVILEGES, principal=sp_id)],
        )
        logger.info("Granted USE CATALOG on %s to %s", cat, sp_id)
    except Exception as exc:
        msg = str(exc)
        logger.warning("Catalog grant failed on %s — %s", cat, msg)
        errors.append(f"USE CATALOG on `{cat}`: {msg[:300]}")

    try:
        ws.grants.update(
            securable_type=SecurableType.SCHEMA.value,
            full_name=f"{cat}.{sch}",
            changes=[PermissionsChange(add=_SCHEMA_PRIVILEGES, principal=sp_id)],
        )
        logger.info("Granted schema privileges on %s.%s to %s", cat, sch, sp_id)
    except Exception as exc:
        msg = str(exc)
        logger.warning("Schema grant failed on %s.%s — %s", cat, sch, msg)
        errors.append(f"Schema privileges on `{cat}`.`{sch}`: {msg[:300]}")

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

    sp_id = _get_sp_principal(sp_ws)
    cat = str(grant["target_catalog"])
    sch = str(grant["target_schema"])

    try:
        ws.grants.update(
            securable_type=SecurableType.CATALOG.value,
            full_name=cat,
            changes=[PermissionsChange(remove=_CATALOG_PRIVILEGES, principal=sp_id)],
        )
        logger.info("Revoked USE CATALOG on %s from %s", cat, sp_id)
    except Exception as exc:
        logger.warning("Catalog revoke failed (best-effort) on %s — %s", cat, exc)

    try:
        ws.grants.update(
            securable_type=SecurableType.SCHEMA.value,
            full_name=f"{cat}.{sch}",
            changes=[PermissionsChange(remove=_SCHEMA_PRIVILEGES, principal=sp_id)],
        )
        logger.info("Revoked schema privileges on %s.%s from %s", cat, sch, sp_id)
    except Exception as exc:
        logger.warning("Schema revoke failed (best-effort) on %s.%s — %s", cat, sch, exc)

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
