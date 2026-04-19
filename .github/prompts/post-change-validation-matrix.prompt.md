# Post Change Validation Matrix Prompt

Use this prompt to generate a fast, file-aware validation plan after edits.

## Prompt
You are producing a post-change validation matrix for this repository.

Inputs:
- List of changed files.
- Current working branch/context.
- Optional runtime symptoms from logs.

Tasks:
1. Map each changed file to the smallest relevant validation action.
2. Group checks by type:
   - import/startup checks
   - entrypoint smoke checks
   - log/state verification checks
3. Provide expected success signals and failure signals for each check.
4. Keep checks minimal and ordered for fastest confidence.
5. Flag any high-risk changes requiring explicit manual review.

Repository-specific mapping hints:
- `bot.py`, `agent_system.py`, `kelly.py`, `position_tracker.py`:
  - import smoke
  - targeted `python bot.py` run only when needed
  - inspect `bot_stdout.log`, `bot_stderr.log`, `alerts.json`, `status.json`
- `watchdog.py`, `start_watchdog.bat`:
  - `python watchdog.py`
  - verify lock behavior via `bot.lock` and restart logs
- `server.py`, `index.html`, `mm.html`, `config.js`:
  - `python server.py`
  - verify `/proxy` allowlist behavior and static page loads
- `app.py`, `ml_model.py`:
  - `python app.py --status`
  - verify report/status endpoints for regressions
- state JSON files (`status.json`, `alerts.json`, `kelly_state.json`, `updn_traded.json`, `elite_wallets.json`):
  - parse/shape checks and no unrelated churn

Constraints:
- Do not run fund-moving scripts.
- Do not print secrets.
- Keep commands Windows-compatible.

Output format:
- Validation Matrix (file -> check -> expected signal)
- Minimal Command Sequence
- Residual Risks
