# Genie Space Optimizer — deployment helpers
# Usage:
#   make deploy          Full clean + bundle deploy + app deploy + verify (default)
#   make verify          Check that workspace has the expected wheel
#   make clean-wheels    Remove stale wheels from the workspace

PROFILE       ?= genie-test
APP_NAME      ?= genie-space-optimizer
WS_BUILD_PATH  = /Workspace/Users/prashanth.subrahmanyam@databricks.com/.bundle/genie-space-optimizer/dev/files/.build

.PHONY: deploy verify clean-wheels

# ── Default target ──────────────────────────────────────────────────────
# 1. Clean stale wheels from workspace
# 2. Bundle deploy: builds wheel, removes .gitignore, syncs files to workspace
# 3. App deploy: creates a new snapshot and restarts the app with the new code
# 4. Verify: confirms the wheel is present on the workspace

deploy: clean-wheels _bundle_deploy _app_deploy verify

# ── Bundle Deploy (build + sync files to workspace) ────────────────────

_bundle_deploy:
	databricks bundle deploy --profile $(PROFILE)

# ── App Deploy (create snapshot + restart app) ─────────────────────────

_app_deploy:
	databricks apps deploy $(APP_NAME) --profile $(PROFILE)

# ── Verify ──────────────────────────────────────────────────────────────

verify:
	@echo "Workspace wheels:"
	@databricks workspace list "$(WS_BUILD_PATH)" --profile $(PROFILE) 2>&1 | grep -i ".whl" \
		|| (echo "ERROR: No wheel found on workspace at $(WS_BUILD_PATH)" && exit 1)
	@echo "Verification passed."

# ── Cleanup stale wheels ───────────────────────────────────────────────

clean-wheels:
	@echo "Removing stale wheels from workspace..."
	@for whl in $$(databricks workspace list "$(WS_BUILD_PATH)" --profile $(PROFILE) 2>/dev/null \
		| grep "\.whl" | awk '{print $$NF}'); do \
		echo "  Deleting: $$(basename $$whl)"; \
		databricks workspace delete "$$whl" --profile $(PROFILE) 2>/dev/null || true; \
	done
	@echo "Cleanup done."
