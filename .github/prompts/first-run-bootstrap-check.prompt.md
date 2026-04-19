# First Run Bootstrap Check Prompt

Use this prompt for first-time setup validation in this repository.

## Prompt
You are running a first-run bootstrap check for this Python Polymarket workspace.

Inputs:
- Current Python environment details.
- Declared dependencies from requirements.txt.
- Runtime entrypoint targets: bot.py, server.py, app.py, watchdog.py.
- Optional startup errors from terminal logs.

Tasks:
1. Verify interpreter/environment is active and suitable.
2. Verify dependencies needed by entrypoints are installed.
3. Verify required environment variables are set or intentionally stubbed:
   - POLY_PRIVATE_KEY
   - POLY_FUNDER
   - POLY_PROXY (optional)
   - ANTHROPIC_API_KEY (optional when Claude path used)
4. Run minimal startup checks in safe order:
   - import smoke for edited/runtime modules
   - app status check
   - server startup check
   - bot startup check only if explicitly requested
5. Produce a concise readiness summary and smallest next actions.

Constraints:
- Do not run fund-moving scripts.
- Do not print secrets.
- Keep commands Windows-compatible.

Output format:
- Findings (severity ordered)
- Ready/Not Ready status by entrypoint
- Required fixes
- Optional improvements
