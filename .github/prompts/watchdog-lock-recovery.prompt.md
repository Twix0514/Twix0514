# Watchdog Lock Recovery Prompt

Use this prompt when bot restarts are blocked, watchdog behavior is inconsistent, or stale lock state is suspected.

## Prompt
You are triaging watchdog and lock recovery issues in this repository.

Inputs:
- Current process state from terminal.
- `bot.lock` contents (if present).
- Recent lines from `bot_stdout.log` and `bot_stderr.log`.
- Last run command and working directory.

Tasks:
1. Determine whether `bot.py` is actually running or only appears running due to stale lock state.
2. Classify the issue as one of:
   - stale lock file
   - crash loop with backoff growth
   - startup failure in `bot.py`
   - incorrect entry command or working directory
3. Produce minimal safe recovery steps in order:
   - quick restore first
   - then durable prevention
4. Provide explicit verification steps and expected signals in logs.

Constraints:
- Do not run fund-moving scripts.
- Do not expose secrets.
- Keep commands Windows-compatible.

Output format:
- Findings (severity ordered)
- Immediate Recovery Steps
- Durable Fixes
- Validation Signals
