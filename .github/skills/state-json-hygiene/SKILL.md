# State JSON Hygiene

Purpose: help AI coding agents modify runtime JSON state safely with minimal churn.

## Use This Skill When
- Editing logic that reads/writes runtime JSON files in repo root.
- Touching state artifacts like `alerts.json`, `status.json`, `kelly_state.json`, `updn_traded.json`, `elite_wallets.json`.
- Updating serialization/deserialization behavior.

## Guardrails
- Treat runtime JSON as mutable artifacts, not hand-formatted docs.
- Avoid unnecessary key reordering or full-file rewrites.
- Preserve tolerant load behavior (fallback defaults on read failures).
- Do not add sensitive data to state files.

## Required Pre-Edit Checks
1. Read [AGENTS.md](../../../AGENTS.md) and [README.md](../../../README.md).
2. Identify all files touched by the code path.
3. Confirm whether writes are single-threaded or lock-protected.

## Edit Pattern
1. Make narrowly scoped changes to read/write logic only.
2. Keep existing format style unless change requires migration.
3. If schema changes are necessary, include backward-compatible defaults.
4. Add brief comments only for non-obvious migration or fallback logic.

## Validation Checklist
- Import check for edited module(s).
- Run relevant entrypoint and trigger one state write path.
- Confirm resulting JSON remains parseable and expected fields exist.
- Confirm no unrelated formatting churn in untouched state files.

## Common Pitfalls
- Concurrent writes can corrupt files if lock assumptions are changed.
- Missing defaults can break startup on old state files.
- Large periodic writes can create noisy diffs and hide real changes.

## Escalation Rule
If a change modifies state schema or lifecycle semantics, call it out explicitly and request confirmation before broad rollout.
