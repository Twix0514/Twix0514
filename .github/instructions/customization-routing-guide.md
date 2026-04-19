# Customization Routing Guide

Use this guide to choose the right local customization quickly.

## Routing Rules
- Trading/runtime code edits (`bot.py`, `agent_system.py`, `kelly.py`, `position_tracker.py`): use skill [.github/skills/safe-trading-edits/SKILL.md](../skills/safe-trading-edits/SKILL.md)
- Proxy host or `/proxy` behavior changes (`server.py`): use skill [.github/skills/proxy-allowlist-updates/SKILL.md](../skills/proxy-allowlist-updates/SKILL.md)
- Runtime JSON state handling changes: use skill [.github/skills/state-json-hygiene/SKILL.md](../skills/state-json-hygiene/SKILL.md)
- Post-edit smoke verification: use agent [.github/agents/runtime-smoke-checker.md](../agents/runtime-smoke-checker.md)
- Pre-merge trading risk review: use agent [.github/agents/trading-change-risk-reviewer.md](../agents/trading-change-risk-reviewer.md)
- Dependency mismatch/startup import failures: use agent [.github/agents/dependency-drift-checker.md](../agents/dependency-drift-checker.md) and prompt [.github/prompts/dependency-drift-check.prompt.md](../prompts/dependency-drift-check.prompt.md)
- Incident/debug triage from logs/state: use prompt [.github/prompts/incident-triage.prompt.md](../prompts/incident-triage.prompt.md)
- Watchdog restart or stale lock issues: use prompt [.github/prompts/watchdog-lock-recovery.prompt.md](../prompts/watchdog-lock-recovery.prompt.md)
- First-time setup/readiness checks: use prompt [.github/prompts/first-run-bootstrap-check.prompt.md](../prompts/first-run-bootstrap-check.prompt.md)
- Validation planning after a change: use prompt [.github/prompts/post-change-validation-matrix.prompt.md](../prompts/post-change-validation-matrix.prompt.md)
- Commit/PR/release writeups: use instruction [.github/instructions/commit-and-release-hygiene.md](commit-and-release-hygiene.md)

## Selection Heuristic
1. Pick one primary customization by changed file area.
2. Add one verification customization (agent or prompt) for confidence.
3. For risky trading behavior deltas, always include risk review before merge.

## Escalation
If a requested change weakens safety controls (risk limits, allowlists, or secret handling), stop and request explicit confirmation.
