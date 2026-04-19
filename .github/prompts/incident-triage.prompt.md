# Incident Triage Prompt

Use this prompt to quickly triage runtime issues in this repository.

## Prompt
You are triaging a runtime incident in this Polymarket workspace.

Inputs:
- Error symptom from user.
- Recent logs from `bot_stdout.log`, `bot_stderr.log`, and `server_output.log`.
- Current state snapshots from `alerts.json` and `status.json`.
- Last command and working directory context.

Tasks:
1. Identify the most likely failure domain:
   - trading runtime (`bot.py` / `agent_system.py` / `kelly.py`)
   - watchdog lifecycle (`watchdog.py` / `bot.lock`)
   - proxy/server (`server.py` / allowlist)
   - dependency/environment setup
2. Produce findings ordered by severity.
3. For each finding, include:
   - evidence line(s) from logs or state
   - likely root cause
   - minimal safe fix or next check
4. Distinguish confirmed findings from hypotheses.
5. Propose the smallest recovery sequence first (quick restore), then durable remediation.

Constraints:
- Do not run fund-moving scripts.
- Do not expose secrets.
- Prefer surgical edits and focused smoke checks.

Output format:
- **Findings** (severity ordered)
- **Immediate Recovery Steps**
- **Durable Fixes**
- **Validation Plan**
