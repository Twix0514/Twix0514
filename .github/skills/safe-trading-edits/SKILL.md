# Safe Trading Edits

Purpose: guide AI coding agents when modifying trading/runtime code so changes stay safe, minimal, and verifiable.

## Use This Skill When
- Editing `bot.py`, `agent_system.py`, `kelly.py`, `position_tracker.py`, `wallet_scanner.py`, `edge.py`, or `watchdog.py`.
- Touching risk thresholds, order flow, sizing, or state persistence.
- Updating network/API handling where transient failures are common.

## Guardrails
- Keep edits surgical; avoid broad refactors in runtime-critical files.
- Do not change strategy behavior unless the request explicitly asks for it.
- Preserve fail-soft error handling (returning defaults instead of raising in hot paths).
- Never hardcode or expose secrets; prefer environment-variable flow.
- Never run fund-moving scripts unless explicitly requested and confirmed:
  - `transfer_usdc.py`
  - `withdraw_proxy.py`

## Required Pre-Edit Checks
1. Read [AGENTS.md](../../../AGENTS.md) and [README.md](../../../README.md).
2. Confirm which engine/module is in scope (UPDN/LIVE/DRIFT/WX or agent gate).
3. Identify impacted state files and avoid formatting churn in JSON runtime artifacts.

## Edit Pattern
1. Make the smallest possible change.
2. Preserve existing lock/thread assumptions in `bot.py` around order placement.
3. Keep Windows compatibility in commands and scripts.
4. Add only succinct comments for non-obvious logic.

## Validation Checklist
- Import check for edited modules.
- Run only the relevant entrypoint for the change:
  - `python bot.py` for trading loop changes
  - `python watchdog.py` for restart/lock handling
  - `python server.py` for proxy/static behavior
  - `python app.py --status` for dashboard/report-side changes
- Verify expected log/state effects in:
  - `bot_stdout.log`, `bot_stderr.log`, `alerts.json`, `status.json`

## Common Pitfalls
- `arb.py` has unresolved merge markers; avoid using it as a clean reference.
- `watchdog.py` lock file (`bot.lock`) may become stale after crashes.
- `requirements.txt` is intentionally minimal; runtime imports can require extra packages.

## Escalation Rule
If a requested change materially alters risk management, position sizing, or order execution behavior, stop and ask for explicit confirmation before implementing.
