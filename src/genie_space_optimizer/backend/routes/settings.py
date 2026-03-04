"""Settings endpoints: SP data-access grant management and permission dashboard."""

from __future__ import annotations

import logging
import uuid
from datetime import datetime, timezone

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.catalog import (
    Privilege,
    SecurableType,
)
from databricks.sdk.service.iam import AccessControlRequest, PermissionLevel
from fastapi import HTTPException

from ..core import Dependencies, create_router
from ..models import (
    DataAccessGrant,
    DataAccessGrantRequest,
    DataAccessOverview,
    DetectedSchema,
    PermissionDashboard,
    SchemaPermission,
    SpaceAccessGrantRequest,
    SpacePermissions,
)
from ..utils import get_sp_principal as _get_sp_principal
from .._spark import get_spark

router = create_router()
logger = logging.getLogger(__name__)

_ALL_PRIV = Privilege.ALL_PRIVILEGES


def _table_has_column(spark, fqn: str, col: str) -> bool:
    """Check whether *col* exists in the Delta table at *fqn*."""
    try:
        cols = {row["col_name"].lower() for row in spark.sql(f"DESCRIBE TABLE {fqn}").collect()}
        return col.lower() in cols
    except Exception:
        return False


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
) -> tuple[set[tuple[str, str]], set[tuple[str, str]]]:
    """Return schemas where the SP has read / write privileges.

    Returns ``(read_granted, write_granted)`` — sets of ``(catalog, schema)``
    pairs where the SP has the corresponding access level.
    """
    if not schemas:
        return set(), set()

    by_catalog: dict[str, list[str]] = {}
    for cat, sch in schemas:
        by_catalog.setdefault(cat, []).append(sch)

    aliases = _get_sp_principal_aliases(sp_ws)
    read_granted: set[tuple[str, str]] = set()
    write_granted: set[tuple[str, str]] = set()

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
                    cat, sch, exc_info=True,
                )

            all_privs = catalog_privs | schema_privs

            has_catalog_access = bool(all_privs) or (_ALL_PRIV in all_privs)
            has_schema_access = (Privilege.USE_SCHEMA in all_privs) or (_ALL_PRIV in all_privs)
            has_select = (Privilege.SELECT in all_privs) or (_ALL_PRIV in all_privs)

            key = (cat.lower(), sch.lower())
            if has_catalog_access and has_schema_access and has_select:
                read_granted.add(key)
            has_modify = (Privilege.MODIFY in all_privs) or (_ALL_PRIV in all_privs)
            if has_modify:
                write_granted.add(key)

    return read_granted, write_granted


def _detect_schemas_from_spaces(
    sp_ws: WorkspaceClient,
    grants: list[dict],
    user_aliases: set[str],
    obo_ws: WorkspaceClient | None = None,
) -> tuple[list[DetectedSchema], set[tuple[str, str]], set[tuple[str, str]]]:
    """Discover catalog.schema pairs referenced by existing Genie spaces.

    Returns ``(detected_schemas, sp_read_granted, sp_write_granted)`` so the
    caller can build synthetic Active Grants for externally-managed UC access.
    """
    from genie_space_optimizer.common.genie_client import list_spaces, fetch_space_config
    from genie_space_optimizer.common.uc_metadata import extract_genie_space_table_refs

    read_ledger = {
        (g.get("target_catalog", "").lower(), g.get("target_schema", "").lower())
        for g in grants if g.get("grant_type", "read") == "read"
    }
    write_ledger = {
        (g.get("target_catalog", "").lower(), g.get("target_schema", "").lower())
        for g in grants if g.get("grant_type") == "write"
    }

    schema_space_count: dict[tuple[str, str], int] = {}
    clients = [obo_ws, sp_ws] if obo_ws else [sp_ws]
    space_list: list[dict] = []
    for client in clients:
        try:
            space_list = list_spaces(client)
            break
        except Exception:
            continue

    for space in space_list:
        space_id = space.get("id", "")
        if not space_id:
            continue
        cfg: dict = {}
        for client in clients:
            try:
                cfg = fetch_space_config(client, space_id)
                break
            except Exception:
                continue
        try:
            refs = extract_genie_space_table_refs(cfg)
            for ref in refs:
                cat = ref[0] if isinstance(ref, (list, tuple)) else ""
                sch = ref[1] if isinstance(ref, (list, tuple)) and len(ref) > 1 else ""
                if cat and sch:
                    key = (cat.lower(), sch.lower())
                    schema_space_count[key] = schema_space_count.get(key, 0) + 1
        except Exception:
            continue

    sp_read_granted, sp_write_granted = _probe_sp_required_access(
        sp_ws, set(schema_space_count.keys()),
    )
    read_set = read_ledger.union(sp_read_granted)
    write_set = write_ledger.union(sp_write_granted)

    ungranted_read = {k for k in schema_space_count if k not in read_set}
    ungranted_write = {k for k in schema_space_count if k not in write_set}
    manageable = _probe_user_manage_privileges(
        sp_ws, ungranted_read | ungranted_write, user_aliases,
    )

    detected: list[DetectedSchema] = []
    for (cat, sch), count in sorted(schema_space_count.items()):
        is_read = (cat, sch) in read_set
        is_write = (cat, sch) in write_set
        detected.append(DetectedSchema(
            catalog=cat,
            schema_name=sch,
            spaceCount=count,
            granted=is_read,
            canGrant=not is_read and (cat, sch) in manageable,
            writeGranted=is_write,
            canGrantWrite=not is_write and (cat, sch) in manageable,
        ))
    return detected, sp_read_granted, sp_write_granted


@router.get(
    "/settings/data-access",
    response_model=DataAccessOverview,
    operation_id="getDataAccess",
)
def get_data_access(
    ws: Dependencies.UserClient,
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
            grantType=str(g.get("grant_type", "read")),
        )
        for g in grants_rows
    ]

    user_aliases: set[str] = set()
    for val in (headers.user_email, headers.user_name):
        if val:
            user_aliases.add(val.lower())

    detected, sp_read_granted, sp_write_granted = _detect_schemas_from_spaces(
        sp_ws, grants_rows, user_aliases, obo_ws=ws,
    )

    ledger_keys = {
        (g.get("target_catalog", "").lower(), g.get("target_schema", "").lower())
        for g in grants_rows
    }
    uc_grants: list[DataAccessGrant] = []
    for cat, sch in sorted(sp_read_granted):
        if (cat, sch) not in ledger_keys:
            uc_grants.append(DataAccessGrant(
                id=f"uc-read-{cat}-{sch}",
                catalog=cat, schema_name=sch,
                grantedBy="Detected from UC", grantedAt="",
                status="active", source="uc", grantType="read",
            ))
    for cat, sch in sorted(sp_write_granted):
        if (cat, sch) not in ledger_keys:
            uc_grants.append(DataAccessGrant(
                id=f"uc-write-{cat}-{sch}",
                catalog=cat, schema_name=sch,
                grantedBy="Detected from UC", grantedAt="",
                status="active", source="uc", grantType="write",
            ))

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
    """Grant the app SP access to a catalog.schema using the caller's UC privileges.

    ``grant_type`` controls what is granted:
      - ``"read"`` (default): ``USE CATALOG``, ``USE SCHEMA``, ``SELECT``, ``EXECUTE``
      - ``"write"``: additionally ``MODIFY`` on the schema
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
    grant_type = body.grant_type or "read"
    if grant_type not in ("read", "write"):
        raise HTTPException(status_code=400, detail="grant_type must be 'read' or 'write'.")
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
    if grant_type == "write":
        grant_statements.append(
            f"GRANT MODIFY ON SCHEMA `{cat}`.`{sch}` TO `{sp_name}`",
        )

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

    has_grant_type = _table_has_column(spark, fqn, "grant_type")
    if has_grant_type:
        spark.sql(
            f"INSERT INTO {fqn} "
            f"(grant_id, target_catalog, target_schema, granted_by, granted_at, revoked_at, status, grant_type) "
            f"VALUES ('{grant_id}', '{cat}', '{sch}', '{safe_user}', "
            f"TIMESTAMP '{now}', NULL, 'active', '{grant_type}')"
        )
    else:
        spark.sql(
            f"INSERT INTO {fqn} "
            f"(grant_id, target_catalog, target_schema, granted_by, granted_at, revoked_at, status) "
            f"VALUES ('{grant_id}', '{cat}', '{sch}', '{safe_user}', "
            f"TIMESTAMP '{now}', NULL, 'active')"
        )

    return DataAccessGrant(
        id=grant_id,
        catalog=cat,
        schema_name=sch,
        grantedBy=current_user,
        grantedAt=now,
        status="active",
        grantType=grant_type,
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
    grant_type = str(grant.get("grant_type", "read"))

    if not config.warehouse_id:
        raise HTTPException(status_code=500, detail="No SQL warehouse configured for revoke execution.")

    revoke_statements = [
        f"REVOKE USE CATALOG ON CATALOG `{cat}` FROM `{sp_name}`",
        f"REVOKE USE SCHEMA ON SCHEMA `{cat}`.`{sch}` FROM `{sp_name}`",
        f"REVOKE SELECT ON SCHEMA `{cat}`.`{sch}` FROM `{sp_name}`",
        f"REVOKE EXECUTE ON SCHEMA `{cat}`.`{sch}` FROM `{sp_name}`",
    ]
    if grant_type == "write":
        revoke_statements.append(
            f"REVOKE MODIFY ON SCHEMA `{cat}`.`{sch}` FROM `{sp_name}`",
        )

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
        grantType=grant_type,
    )


# ── Genie Space SP Access ────────────────────────────────────────────


def _sp_can_manage_space(
    sp_ws: WorkspaceClient, space_id: str, sp_aliases: set[str],
) -> bool:
    """Check whether the app SP has CAN_MANAGE on a Genie Space."""
    try:
        perms = sp_ws.permissions.get("genie", space_id)
        for acl in perms.access_control_list or []:
            principal = (acl.user_name or acl.group_name or "").lower()
            if principal not in sp_aliases:
                sn = getattr(acl, "service_principal_name", "") or ""
                if sn.lower() not in sp_aliases:
                    continue
            for p in acl.all_permissions or []:
                level = str(p.permission_level).replace("PermissionLevel.", "")
                if level == "CAN_MANAGE":
                    return True
        return False
    except Exception:
        logger.debug("Could not check SP permissions for space %s", space_id)
        return False


def _user_can_manage_space(
    sp_ws: WorkspaceClient,
    space_id: str,
    user_email: str,
) -> bool:
    """Check whether *user_email* has CAN_MANAGE (not just CAN_EDIT).

    Uses the SP client for the ACL lookup since OBO may lack the Permissions
    API scope.
    """
    try:
        my_email = user_email.lower()
        perms = sp_ws.permissions.get("genie", space_id)
        for acl in perms.access_control_list or []:
            principal = (acl.user_name or acl.group_name or "").lower()
            is_me = principal == my_email or acl.group_name == "admins"
            if not is_me:
                continue
            for p in acl.all_permissions or []:
                if str(p.permission_level).replace("PermissionLevel.", "") == "CAN_MANAGE":
                    return True
        return False
    except Exception:
        return False


@router.post(
    "/settings/space-access",
    response_model=dict,
    operation_id="grantSpaceAccess",
)
def grant_space_access(
    body: SpaceAccessGrantRequest,
    ws: Dependencies.UserClient,
    sp_ws: Dependencies.Client,
):
    """Grant the app SP CAN_MANAGE on a Genie Space.

    Tries the OBO client first (user authority), then falls back to the SP
    client.  OBO often lacks the Permissions API scope, so the SP fallback
    is the common path.
    """
    sp_id = _get_sp_principal(sp_ws)
    sp_display = _get_sp_display_name(sp_ws)
    principal_name = sp_display or sp_id
    acl = [
        AccessControlRequest(
            service_principal_name=principal_name,
            permission_level=PermissionLevel.CAN_MANAGE,
        )
    ]

    last_err: Exception | None = None
    for label, client in [("OBO", ws), ("SP", sp_ws)]:
        try:
            client.permissions.update("genie", body.space_id, access_control_list=acl)
            logger.info("Granted SP CAN_MANAGE on space %s via %s", body.space_id, label)
            return {"space_id": body.space_id, "status": "granted"}
        except Exception as exc:
            logger.info("Grant space access via %s failed: %s", label, exc)
            last_err = exc

    raise HTTPException(
        status_code=403,
        detail=(
            f"Could not grant SP access to space {body.space_id}. "
            "You may need to add the service principal manually via the "
            f"Genie Space sharing dialog. Error: {last_err}"
        ),
    )


@router.delete(
    "/settings/space-access/{space_id}",
    response_model=dict,
    operation_id="revokeSpaceAccess",
)
def revoke_space_access(
    space_id: str,
    ws: Dependencies.UserClient,
    sp_ws: Dependencies.Client,
):
    """Revoke the app SP's CAN_MANAGE from a Genie Space.

    Uses the raw REST API because the SDK ``PermissionLevel`` enum does not
    include ``NO_PERMISSIONS``.  Tries the SP client first (it already has
    CAN_MANAGE so it can modify the ACL), with OBO as fallback.
    """
    sp_id = _get_sp_principal(sp_ws)
    sp_display = _get_sp_display_name(sp_ws)
    principal_name = sp_display or sp_id
    body = {
        "access_control_list": [
            {
                "service_principal_name": principal_name,
                "permission_level": "NO_PERMISSIONS",
            }
        ]
    }

    last_err: Exception | None = None
    for label, client in [("SP", sp_ws), ("OBO", ws)]:
        try:
            client.api_client.do("PATCH", f"/api/2.0/permissions/genie/{space_id}", body=body)
            logger.info("Revoked SP CAN_MANAGE from space %s via %s", space_id, label)
            return {"space_id": space_id, "status": "revoked"}
        except Exception as exc:
            logger.info("Revoke space access via %s failed: %s", label, exc)
            last_err = exc

    raise HTTPException(
        status_code=403,
        detail=f"Failed to revoke SP access from space {space_id}: {last_err}",
    )


# ── Permission Dashboard ─────────────────────────────────────────────


@router.get(
    "/settings/permissions",
    response_model=PermissionDashboard,
    operation_id="getPermissionDashboard",
)
def get_permission_dashboard(
    ws: Dependencies.UserClient,
    sp_ws: Dependencies.Client,
    config: Dependencies.Config,
    headers: Dependencies.Headers,
):
    """Per-space permission overview combining space access and data access."""
    from genie_space_optimizer.common.genie_client import (
        list_spaces, fetch_space_config, user_can_edit_space,
    )
    from genie_space_optimizer.common.uc_metadata import (
        extract_genie_space_table_refs,
        get_unique_schemas,
    )

    spark = get_spark()
    grants_rows = _load_grants(spark, config.catalog, config.schema_name)
    sp_aliases = _get_sp_principal_aliases(sp_ws)

    caller_email = headers.user_email or headers.user_name or ""

    all_spaces: list[dict] = []
    for client_label, client in [("OBO", ws), ("SP", sp_ws)]:
        try:
            all_spaces = list_spaces(client)
            logger.info("Listed %d spaces via %s client", len(all_spaces), client_label)
            break
        except Exception:
            logger.info("list_spaces via %s failed, trying next", client_label)

    user_spaces = [
        s for s in all_spaces
        if user_can_edit_space(ws, s["id"], user_email=caller_email, acl_client=sp_ws)
    ]

    user_aliases: set[str] = set()
    for val in (headers.user_email, headers.user_name):
        if val:
            user_aliases.add(val.lower())

    all_schemas: set[tuple[str, str]] = set()
    space_schemas: dict[str, list[tuple[str, str]]] = {}
    for space in user_spaces:
        sid = space["id"]
        cfg: dict = {}
        for c_label, c in [("OBO", ws), ("SP", sp_ws)]:
            try:
                cfg = fetch_space_config(c, sid)
                break
            except Exception:
                continue
        try:
            refs = extract_genie_space_table_refs(cfg)
        except Exception:
            refs = []
        unique = get_unique_schemas(refs)
        normalized = [(c.lower(), s.lower()) for c, s in unique]
        for key in normalized:
            all_schemas.add(key)
        space_schemas[sid] = normalized

    sp_read_granted, sp_write_granted = _probe_sp_required_access(sp_ws, all_schemas)

    read_ledger: dict[tuple[str, str], str] = {}
    write_ledger: dict[tuple[str, str], str] = {}
    for g in grants_rows:
        key = (g.get("target_catalog", "").lower(), g.get("target_schema", "").lower())
        gt = g.get("grant_type", "read")
        gid = str(g.get("grant_id", ""))
        if gt == "write":
            write_ledger[key] = gid
        else:
            read_ledger[key] = gid
    read_set = set(read_ledger.keys()) | sp_read_granted
    write_set = set(write_ledger.keys()) | sp_write_granted

    ungranted = all_schemas - read_set - write_set
    manageable = _probe_user_manage_privileges(sp_ws, ungranted | all_schemas, user_aliases)

    space_perms: list[SpacePermissions] = []
    for space in user_spaces:
        sid = space["id"]
        title = space.get("title", sid)
        sp_has_manage = _sp_can_manage_space(sp_ws, sid, sp_aliases)
        user_can_grant_manage = _user_can_manage_space(sp_ws, sid, caller_email)

        schemas_out: list[SchemaPermission] = []
        for cat, sch in space_schemas.get(sid, []):
            key = (cat, sch)
            schemas_out.append(SchemaPermission(
                catalog=cat,
                schema_name=sch,
                readGranted=key in read_set,
                writeGranted=key in write_set,
                canGrantRead=key not in read_set and key in manageable,
                canGrantWrite=key not in write_set and key in manageable,
                readGrantId=read_ledger.get(key),
                writeGrantId=write_ledger.get(key),
            ))

        all_read = all(s.readGranted for s in schemas_out) if schemas_out else True
        if not sp_has_manage or not all_read:
            status = "not_configured" if not sp_has_manage and not all_read else "action_needed"
        else:
            status = "ready"

        space_perms.append(SpacePermissions(
            spaceId=sid,
            title=title,
            spHasManage=sp_has_manage,
            userCanGrantManage=user_can_grant_manage,
            schemas=schemas_out,
            status=status,
        ))

    sp_id = _get_sp_principal(sp_ws)
    sp_display_name = _get_sp_display_name(sp_ws)

    return PermissionDashboard(
        spaces=space_perms,
        spPrincipalId=sp_id,
        spPrincipalDisplayName=sp_display_name or None,
        frameworkCatalog=config.catalog,
        frameworkSchema=config.schema_name,
        experimentBasePath="/Shared/genie-space-optimizer/",
        jobName="genie-space-optimizer-runner",
    )
