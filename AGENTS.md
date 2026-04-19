# AGENTS.md

Instructions for AI coding agents working in this repository.

## Project Snapshot
- This is a Python-first Polymarket trading/analytics workspace.
- There is no formal build system, test suite, or linter config.
- Existing high-level usage notes are in [README.md](README.md).

## Quick Start
1. Create/activate a Python environment.
2. Install dependencies:
   - `pip install -r requirements.txt`
   - `pip install web3 py_clob_client websocket-client numpy scipy scikit-learn plotly anthropic`
3. Set runtime secrets via environment variables (preferred):
   - `POLY_PRIVATE_KEY`, `POLY_FUNDER`, `POLY_PROXY`, `ANTHROPIC_API_KEY`, optionally `ADMIN_PATH`
4. Run one entrypoint at a time based on task:
   - Trading bot: `python bot.py`
   - Watchdog supervisor: `python watchdog.py` or [start_watchdog.bat](start_watchdog.bat)
   - Local proxy/static server: `python server.py`
   - Analytics/report app: `python app.py`

## Core Entry Points
- [bot.py](bot.py): Main trading runtime (UPDN, LIVE, DRIFT, WX engines), risk checks, order flow.
- [watchdog.py](watchdog.py): Restarts [bot.py](bot.py) on failure with backoff.
- [agent_system.py](agent_system.py): 3-agent consensus gate (Claude/Whale/Quant), 2-of-3 vote.
- [server.py](server.py): Serves static files and `/proxy` endpoint with host allowlist.
- [app.py](app.py): Flask analytics/reporting endpoints.

## State and Data Files
- Runtime state is JSON-in-repo-root (no DB).
- Frequently touched files: [status.json](status.json), [alerts.json](alerts.json), [kelly_state.json](kelly_state.json), [updn_traded.json](updn_traded.json), [elite_wallets.json](elite_wallets.json).
- Treat state files as mutable runtime artifacts; avoid unnecessary formatting churn.

## Coding and Change Conventions
- Prefer small, surgical edits; avoid broad refactors unless requested.
- Preserve existing operational behavior in trading/risk code unless the task explicitly targets strategy logic.
- Keep network error handling tolerant (existing code often degrades gracefully instead of throwing).
- Maintain Windows compatibility for commands and scripts.

## Safety Guardrails (Important)
- Never expose or hardcode real secrets in commits, logs, docs, or responses.
- Do not execute fund-moving scripts unless explicitly requested and confirmed:
  - [transfer_usdc.py](transfer_usdc.py)
  - [withdraw_proxy.py](withdraw_proxy.py)
- If asked to modify private-key handling, prefer environment-variable based flow over file literals.

## Known Pitfalls
- [arb.py](arb.py) contains unresolved merge conflict markers; avoid relying on it without cleanup.
- [watchdog.py](watchdog.py) uses a lock file (`bot.lock`): stale lock files can block restart behavior after crashes.
- `requirements.txt` is minimal; imports in runtime modules require additional packages listed above.

## Validation Approach
- No canonical test command exists; validate by targeted smoke checks:
  - Import checks for edited modules.
  - Run only the relevant entrypoint script for the change.
  - For API-facing changes, verify with short runs and inspect JSON/log outputs.

## Documentation Linking Rule
- Do not duplicate long explanations that already exist; link to source files and [README.md](README.md).
- Keep agent guidance concise and action-oriented.

## Local Agent Customizations
- Skill: [.github/skills/safe-trading-edits/SKILL.md](.github/skills/safe-trading-edits/SKILL.md)
- Skill: [.github/skills/proxy-allowlist-updates/SKILL.md](.github/skills/proxy-allowlist-updates/SKILL.md)
- Skill: [.github/skills/state-json-hygiene/SKILL.md](.github/skills/state-json-hygiene/SKILL.md)
- Custom agent spec: [.github/agents/runtime-smoke-checker.md](.github/agents/runtime-smoke-checker.md)
- Custom agent spec: [.github/agents/trading-change-risk-reviewer.md](.github/agents/trading-change-risk-reviewer.md)
- Custom agent spec: [.github/agents/dependency-drift-checker.md](.github/agents/dependency-drift-checker.md)
- Instruction: [.github/instructions/commit-and-release-hygiene.md](.github/instructions/commit-and-release-hygiene.md)
- Instruction: [.github/instructions/customization-routing-guide.md](.github/instructions/customization-routing-guide.md)
- Prompt: [.github/prompts/incident-triage.prompt.md](.github/prompts/incident-triage.prompt.md)
- Prompt: [.github/prompts/dependency-drift-check.prompt.md](.github/prompts/dependency-drift-check.prompt.md)
- Prompt: [.github/prompts/watchdog-lock-recovery.prompt.md](.github/prompts/watchdog-lock-recovery.prompt.md)
- Prompt: [.github/prompts/first-run-bootstrap-check.prompt.md](.github/prompts/first-run-bootstrap-check.prompt.md)
- Prompt: [.github/prompts/post-change-validation-matrix.prompt.md](.github/prompts/post-change-validation-matrix.prompt.md)
