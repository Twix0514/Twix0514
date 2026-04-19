# Dependency Drift Check Prompt

Use this prompt to detect and resolve dependency drift between runtime imports and the declared environment.

## Prompt
You are auditing dependency drift in this Python workspace.

Inputs:
- Declared dependency file: requirements.txt
- Runtime modules likely imported by entrypoints: bot.py, agent_system.py, app.py, server.py, watchdog.py, wallet_scanner.py, ml_model.py
- Optional error logs from bot_stderr.log or server_output.log

Tasks:
1. Enumerate top-level third-party imports used by user files.
2. Compare imports against requirements.txt and identify likely missing packages.
3. Classify each mismatch as one of:
   - missing dependency
   - optional dependency
   - stale/unused declared dependency
4. Propose minimal remediation:
   - requirements.txt updates
   - one-time install commands for local smoke checks
5. Provide a short risk note for runtime-critical gaps that can stop startup.

Constraints:
- Do not run fund-moving scripts.
- Do not expose secrets.
- Keep recommendations Windows-compatible.

Output format:
- Findings (severity ordered)
- Suggested requirements.txt updates
- Local install and smoke-check commands
- Residual risks
