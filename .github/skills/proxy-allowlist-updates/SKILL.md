# Proxy Allowlist Updates

Purpose: guide AI coding agents making safe, minimal changes to proxy/static serving behavior.

## Use This Skill When
- Editing `server.py` proxy behavior.
- Adding or removing hosts in `ALLOWED_HOSTS`.
- Changing request/response handling for `/proxy`.

## Guardrails
- Keep host allowlist explicit and minimal. Never add wildcard hosts.
- Preserve host validation before outbound requests.
- Keep timeout and error behavior fail-soft for user-facing tools.
- Do not expose secrets in responses, logs, or static files.

## Required Pre-Edit Checks
1. Read [AGENTS.md](../../../AGENTS.md) and [README.md](../../../README.md).
2. Confirm exactly which upstream API host is required.
3. Verify whether the host is truly needed for current app pages.

## Edit Pattern
1. Change only the smallest part needed in `server.py`.
2. Keep existing `ALLOWED_HOSTS` set style and structure.
3. Preserve CORS behavior and JSON error format unless explicitly requested.

## Validation Checklist
- Import/run check: `python server.py`
- Verify blocked host behavior still returns 403 JSON error.
- Verify allowed host pass-through still returns 200 and JSON payload.
- Confirm static file serving still works at `http://localhost:3000`.

## Common Pitfalls
- Accidentally allowing broad hosts increases SSRF risk.
- Changing proxy headers can break upstream API compatibility.
- Overly strict validation can break existing pages silently.

## Escalation Rule
If requested changes weaken allowlist constraints or bypass host validation, pause and request explicit confirmation.
