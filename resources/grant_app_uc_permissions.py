import argparse
import json
import subprocess
import sys

# SP privileges — backend reads state tables for the UI; it no longer runs
# user-facing SQL, MLflow, or Prompt Registry operations (those run as user).
SP_CATALOG_PRIVILEGES = {"USE_CATALOG"}
SP_SCHEMA_PRIVILEGES = {
    "USE_SCHEMA",
    "SELECT",
}

# User-group privileges — optimization jobs run as the triggering user, so
# users need write access to the optimizer's state schema for Delta tables,
# MLflow Prompt Registry, and model logging.
USER_CATALOG_PRIVILEGES = {"USE_CATALOG"}
USER_SCHEMA_PRIVILEGES = {
    "USE_SCHEMA",
    "SELECT",
    "MODIFY",
    "CREATE_TABLE",
    "CREATE_FUNCTION",
    "CREATE_MODEL",
    "EXECUTE",
    "MANAGE",
}

# Backwards-compat aliases used by _verify_required_privileges for SP checks.
CATALOG_PRIVILEGES = SP_CATALOG_PRIVILEGES
SCHEMA_PRIVILEGES = SP_SCHEMA_PRIVILEGES


def _run(cmd: list[str]) -> str:
    result = subprocess.run(cmd, capture_output=True, text=True, check=False)
    if result.returncode != 0:
        stderr = (result.stderr or "").strip()
        raise RuntimeError(
            f"Command failed ({result.returncode}): {' '.join(cmd)}\n{stderr}",
        )
    return (result.stdout or "").strip()


def _run_json(cmd: list[str]) -> dict:
    out = _run(cmd)
    if not out:
        return {}
    return json.loads(out)


def _is_app_missing(err: Exception) -> bool:
    msg = str(err).lower()
    return "does not exist" in msg or "resource_does_not_exist" in msg


def _update_grants(
    *,
    profile: str,
    securable_type: str,
    full_name: str,
    principal: str,
    add: list[str],
) -> dict:
    payload = {
        "changes": [
            {
                "principal": principal,
                "add": add,
            },
        ],
    }
    return _run_json(
        [
            "databricks",
            "grants",
            "update",
            securable_type,
            full_name,
            "--profile",
            profile,
            "--json",
            json.dumps(payload),
            "-o",
            "json",
        ],
    )


def _get_grants(
    *,
    profile: str,
    securable_type: str,
    full_name: str,
) -> dict:
    return _run_json(
        [
            "databricks",
            "grants",
            "get",
            securable_type,
            full_name,
            "--profile",
            profile,
            "-o",
            "json",
        ],
    )


def _extract_principal_privileges(grants: dict, principal: str) -> set[str]:
    assignments = grants.get("privilege_assignments") or []
    target = principal.strip().lower()
    for assignment in assignments:
        if not isinstance(assignment, dict):
            continue
        assignee = str(assignment.get("principal") or "").strip().lower()
        if assignee != target:
            continue
        values: set[str] = set()
        for priv in assignment.get("privileges") or []:
            if isinstance(priv, str):
                values.add(priv.strip().upper())
            elif isinstance(priv, dict):
                raw = priv.get("privilege") or priv.get("name") or priv.get("value")
                if raw:
                    values.add(str(raw).strip().upper())
        return values
    return set()


def _verify_required_privileges(
    *,
    profile: str,
    principal: str,
    catalog: str,
    schema: str,
) -> None:
    schema_fqn = f"{catalog}.{schema}"

    catalog_grants = _get_grants(
        profile=profile,
        securable_type="catalog",
        full_name=catalog,
    )
    schema_grants = _get_grants(
        profile=profile,
        securable_type="schema",
        full_name=schema_fqn,
    )

    have_catalog = _extract_principal_privileges(catalog_grants, principal)
    have_schema = _extract_principal_privileges(schema_grants, principal)

    missing_catalog = sorted(CATALOG_PRIVILEGES - have_catalog)
    missing_schema = sorted(SCHEMA_PRIVILEGES - have_schema)
    if missing_catalog or missing_schema:
        raise RuntimeError(
            "Grant verification failed for app service principal "
            f"{principal}. Missing catalog privileges={missing_catalog or '[]'} "
            f"on {catalog}; missing schema privileges={missing_schema or '[]'} "
            f"on {schema_fqn}."
        )


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Grant required Unity Catalog privileges to Databricks App SP.",
    )
    parser.add_argument("--profile", required=True)
    parser.add_argument("--app-name", required=True)
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--schema", required=True)
    args = parser.parse_args()

    try:
        app = _run_json(
            [
                "databricks",
                "apps",
                "get",
                args.app_name,
                "--profile",
                args.profile,
                "-o",
                "json",
            ],
        )
    except Exception as err:
        if _is_app_missing(err):
            # First deploy can create the app after this build step.
            print(
                f"[grant-app-sp] App '{args.app_name}' not found yet, skipping grants for now. "
                "This is expected on first deployment. Re-run deploy once app is created so "
                "UC + Prompt Registry grants can be applied.",
            )
            return 0
        raise

    principal = (
        app.get("service_principal_client_id")
        or app.get("service_principal_name")
        or ""
    ).strip()
    if not principal:
        raise RuntimeError(
            "Could not resolve app service principal from `databricks apps get` output.",
        )

    schema_fqn = f"{args.catalog}.{args.schema}"

    # ── SP grants (read-only on optimizer schema) ──────────────────────
    _update_grants(
        profile=args.profile,
        securable_type="catalog",
        full_name=args.catalog,
        principal=principal,
        add=sorted(SP_CATALOG_PRIVILEGES),
    )
    _update_grants(
        profile=args.profile,
        securable_type="schema",
        full_name=schema_fqn,
        principal=principal,
        add=sorted(SP_SCHEMA_PRIVILEGES),
    )
    _verify_required_privileges(
        profile=args.profile,
        principal=principal,
        catalog=args.catalog,
        schema=args.schema,
    )
    print(
        f"[grant-app-sp] SP grants applied: principal={principal} "
        f"on {schema_fqn}",
    )

    # ── User-group grants (jobs run_as user need write access) ─────────
    _update_grants(
        profile=args.profile,
        securable_type="catalog",
        full_name=args.catalog,
        principal="users",
        add=sorted(USER_CATALOG_PRIVILEGES),
    )
    _update_grants(
        profile=args.profile,
        securable_type="schema",
        full_name=schema_fqn,
        principal="users",
        add=sorted(USER_SCHEMA_PRIVILEGES),
    )
    print(
        f"[grant-app-sp] User-group grants applied: group=users "
        f"on {schema_fqn}",
    )

    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print(f"[grant-app-sp] ERROR: {exc}", file=sys.stderr)
        raise
