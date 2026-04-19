# Commit And Release Hygiene

Use this instruction when preparing commits, PR summaries, or release notes in this repository.

## Goals
- Keep change descriptions accurate, minimal, and operationally safe.
- Make runtime risk obvious for trading-related changes.
- Avoid leaking secrets or sensitive runtime data.

## Commit Message Pattern
Use short, imperative subjects and include scope when possible.

Examples:
- `bot: tighten LIVE entry guard for low-liquidity markets`
- `server: add host to proxy allowlist`
- `kelly: preserve backward-compatible state defaults`

## Required Content For Trading Changes
If files like `bot.py`, `agent_system.py`, `kelly.py`, or `position_tracker.py` changed, include:
1. Behavioral delta in plain language.
2. Risk impact (entry, sizing, exits, or execution).
3. Validation run (which entrypoint or import smoke check).

## Release Notes Style
- Start with user-visible behavior changes.
- Then list operational notes (state files, environment variables, lock file behavior).
- End with verification notes and known limitations.

## Safety Rules
- Never include private keys, API keys, wallet addresses, or raw secret-bearing logs.
- Do not claim tests passed unless they were actually run.
- If validation was partial, state exactly what was and was not checked.

## Suggested PR Checklist Snippet
- [ ] Scope is minimal and focused.
- [ ] No secret exposure in code, logs, or notes.
- [ ] Relevant smoke checks executed.
- [ ] Runtime risk called out for trading-path changes.
- [ ] JSON state artifact churn minimized.
