"""
POLY//AGENT SYSTEM — Three independent agents, 2/3 majority vote to trade.

Agent 1 — CLAUDE:   Anthropic API evaluates the market question and gives a
                     probability estimate. If gap vs market price < 7%, no edge.
Agent 2 — WHALE:    Checks if elite wallets (>70% WR) are actively buying.
                     Direction follows the money.
Agent 3 — QUANT:    ML model + momentum signals. Technical edge confirmation.

Entry gate:
  - Order book depth >= $500
  - Resolution 4–48 hours away
  - 2/3 agents agree on direction → FULL POSITION
  - Otherwise → NO TRADE

Exit rules (checked by position_tracker.py):
  - 80% of expected move hit → exit
  - Volume spikes 3× in 10 min → follow smart money out
  - 24h no movement (< 2% price change) → thesis stale → exit
"""

import os, json, time, logging, threading
from datetime import datetime, timezone
from typing import Optional

log = logging.getLogger("AGENTS")

ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")
MIN_CLAUDE_GAP    = 0.07    # 7% mispricing threshold — below this, skip
MIN_BOOK_DEPTH    = 500     # $500 minimum order book liquidity
MIN_HOURS_OUT     = 4       # resolution must be at least 4h away
MAX_HOURS_OUT     = 48      # and at most 48h away
VOTE_THRESHOLD    = 2       # votes needed out of 3

_claude_client = None
_claude_lock   = threading.Lock()


def _get_claude_client():
    global _claude_client
    if _claude_client is not None:
        return _claude_client
    with _claude_lock:
        if _claude_client is not None:
            return _claude_client
        if not ANTHROPIC_API_KEY:
            return None
        try:
            import anthropic
            _claude_client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
        except Exception as e:
            log.warning(f"[AGENT1] Claude unavailable: {e}")
    return _claude_client


# ── AGENT 1: CLAUDE AI ────────────────────────────────────────────────────────

def agent_claude(question: str, market_price: float, end_date: str) -> dict:
    """
    Ask Claude to estimate the true probability of YES resolution.
    Returns {"vote": "YES"/"NO"/"ABSTAIN", "estimate": float, "gap": float}
    """
    client = _get_claude_client()
    if client is None:
        return {"vote": "ABSTAIN", "estimate": market_price, "gap": 0.0, "reason": "Claude API unavailable"}

    today = datetime.now(timezone.utc).strftime("%B %d, %Y")
    prompt = f"""Today is {today}. A prediction market asks:

"{question}"

Market resolution date: {end_date}
Current market price (implied probability): {market_price:.1%}

Analyze this market and estimate the true probability this resolves YES.
Consider: current events, base rates, time remaining, and any available evidence.

Respond with ONLY a JSON object, no other text:
{{"probability": 0.XX, "reasoning": "one sentence"}}

The probability must be between 0.01 and 0.99."""

    try:
        resp = client.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=150,
            messages=[{"role": "user", "content": prompt}],
        )
        raw = resp.content[0].text.strip()
        # Extract JSON even if wrapped in markdown
        if "```" in raw:
            raw = raw.split("```")[1].replace("json", "").strip()
        data     = json.loads(raw)
        estimate = float(data["probability"])
        gap      = abs(estimate - market_price)

        if gap < MIN_CLAUDE_GAP:
            vote = "ABSTAIN"
            reason = f"gap {gap:.1%} < {MIN_CLAUDE_GAP:.0%} threshold"
        elif estimate > market_price:
            vote = "YES"
            reason = f"Claude {estimate:.1%} vs mkt {market_price:.1%} (+{gap:.1%}) — underpriced"
        else:
            vote = "NO"
            reason = f"Claude {estimate:.1%} vs mkt {market_price:.1%} (-{gap:.1%}) — overpriced"

        return {
            "vote":     vote,
            "estimate": estimate,
            "gap":      gap,
            "reason":   reason,
        }
    except Exception as e:
        log.debug(f"[AGENT1] Error: {e}")
        return {"vote": "ABSTAIN", "estimate": market_price, "gap": 0.0, "reason": str(e)}


# ── AGENT 2: WHALE SIGNAL ─────────────────────────────────────────────────────

def agent_whale(condition_id: str, elite_addresses: set, fetch_fn) -> dict:
    """
    Check if elite wallets (>70% WR) are trading this market in the last 4h.
    Direction = where they're putting money.
    Returns {"vote": "YES"/"NO"/"ABSTAIN", "count": int, "net_size": float}
    """
    if not elite_addresses:
        return {"vote": "ABSTAIN", "count": 0, "net_size": 0.0, "reason": "No elite wallets loaded"}

    try:
        trades = fetch_fn(
            f"https://data-api.polymarket.com/trades?conditionId={condition_id}&limit=200"
        )
        if not trades or not isinstance(trades, list):
            return {"vote": "ABSTAIN", "count": 0, "net_size": 0.0, "reason": "No trades found"}

        now = datetime.now(timezone.utc).timestamp()
        cutoff = now - 4 * 3600  # last 4 hours

        yes_size = 0.0
        no_size  = 0.0
        elite_traders = set()

        for t in trades:
            ts = t.get("timestamp") or 0
            if ts < cutoff:
                continue
            wallet = t.get("proxyWallet") or ""
            if wallet not in elite_addresses:
                continue
            elite_traders.add(wallet)
            outcome = str(t.get("outcome") or "").upper()
            size    = float(t.get("size") or t.get("amount") or 0)
            side    = str(t.get("side") or "BUY").upper()
            if side == "SELL":
                size = -size
            if outcome in ("YES", "UP"):
                yes_size += size
            elif outcome in ("NO", "DOWN"):
                no_size += size

        net = yes_size - no_size
        count = len(elite_traders)

        if count == 0:
            return {"vote": "ABSTAIN", "count": 0, "net_size": 0.0, "reason": "No elite activity in last 4h"}

        if abs(net) < 50:   # less than $50 net — no clear signal
            return {"vote": "ABSTAIN", "count": count, "net_size": net, "reason": f"Net size ${net:.0f} — no clear direction"}

        vote = "YES" if net > 0 else "NO"
        return {
            "vote":     vote,
            "count":    count,
            "net_size": net,
            "reason":   f"{count} elite wallets | net ${net:+.0f} (YES=${yes_size:.0f}, NO=${no_size:.0f})",
        }
    except Exception as e:
        log.debug(f"[AGENT2] Error: {e}")
        return {"vote": "ABSTAIN", "count": 0, "net_size": 0.0, "reason": str(e)}


# ── AGENT 3: QUANT ────────────────────────────────────────────────────────────

def agent_quant(yes_price: float, market: dict, get_ml_fn) -> dict:
    """
    ML model + momentum for technical confirmation.
    Returns {"vote": "YES"/"NO"/"ABSTAIN", "confidence": float}
    """
    try:
        predictor = get_ml_fn()
        ml_prob   = None
        if predictor and predictor.trained:
            try:
                result = predictor.predict(market, yes_price)
                ml_prob = result.get("p_yes")
            except Exception:
                pass

        # Momentum from outcomePrices history (if available)
        price_trend = None
        history = market.get("_price_history")
        if history and len(history) >= 5:
            recent = history[-3:]
            older  = history[-6:-3]
            if older:
                trend = sum(recent) / len(recent) - sum(older) / len(older)
                price_trend = trend

        # Combine
        if ml_prob is not None:
            gap = abs(ml_prob - yes_price)
            if gap < 0.05:
                return {"vote": "ABSTAIN", "confidence": gap, "reason": f"ML gap {gap:.1%} too small"}
            vote = "YES" if ml_prob > yes_price else "NO"
            conf = gap
        elif price_trend is not None:
            if abs(price_trend) < 0.02:
                return {"vote": "ABSTAIN", "confidence": 0.0, "reason": "No clear momentum"}
            vote = "YES" if price_trend > 0 else "NO"
            conf = abs(price_trend)
        else:
            return {"vote": "ABSTAIN", "confidence": 0.0, "reason": "No ML or momentum data"}

        return {
            "vote":       vote,
            "confidence": conf,
            "ml_prob":    ml_prob,
            "reason":     f"ML={ml_prob:.1%} vs mkt={yes_price:.1%}" if ml_prob else f"momentum {price_trend:+.3f}",
        }
    except Exception as e:
        log.debug(f"[AGENT3] Error: {e}")
        return {"vote": "ABSTAIN", "confidence": 0.0, "reason": str(e)}


# ── ENTRY GATE ────────────────────────────────────────────────────────────────

def check_entry_filters(market: dict, book_depth: float) -> tuple[bool, str]:
    """
    Hard filters before agents vote.
    Returns (pass: bool, reason: str)
    """
    # Filter 1: Order book depth
    if book_depth < MIN_BOOK_DEPTH:
        return False, f"Book depth ${book_depth:.0f} < ${MIN_BOOK_DEPTH}"

    # Filter 2: Resolution window 4–48h
    end_str = market.get("endDate") or ""
    if not end_str:
        return False, "No endDate"
    try:
        end_dt   = datetime.fromisoformat(end_str.replace("Z", "+00:00"))
        hours_left = (end_dt - datetime.now(timezone.utc)).total_seconds() / 3600
        if hours_left < MIN_HOURS_OUT:
            return False, f"Resolves in {hours_left:.1f}h — too soon (min {MIN_HOURS_OUT}h)"
        if hours_left > MAX_HOURS_OUT:
            return False, f"Resolves in {hours_left:.1f}h — too far (max {MAX_HOURS_OUT}h)"
    except Exception as e:
        return False, f"endDate parse error: {e}"

    return True, "OK"


def vote(
    market:          dict,
    yes_price:       float,
    condition_id:    str,
    book_depth:      float,
    elite_addresses: set,
    fetch_fn,
    get_ml_fn,
) -> dict:
    """
    Run all three agents in parallel, tally votes.
    Returns decision dict with full audit trail.
    """
    # Hard filters first
    ok, reason = check_entry_filters(market, book_depth)
    if not ok:
        return {"trade": False, "reason": reason, "agents": {}}

    question = market.get("question", "")
    end_date = market.get("endDate", "")

    # Run agents (in parallel via threads for speed)
    results = {}

    def _run(name, fn, *args):
        try:
            results[name] = fn(*args)
        except Exception as e:
            results[name] = {"vote": "ABSTAIN", "reason": str(e)}

    threads = [
        threading.Thread(target=_run, args=("claude", agent_claude, question, yes_price, end_date)),
        threading.Thread(target=_run, args=("whale",  agent_whale,  condition_id, elite_addresses, fetch_fn)),
        threading.Thread(target=_run, args=("quant",  agent_quant,  yes_price, market, get_ml_fn)),
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=15)

    claude_r = results.get("claude", {"vote": "ABSTAIN"})
    whale_r  = results.get("whale",  {"vote": "ABSTAIN"})
    quant_r  = results.get("quant",  {"vote": "ABSTAIN"})

    votes = [claude_r["vote"], whale_r["vote"], quant_r["vote"]]
    yes_votes = votes.count("YES")
    no_votes  = votes.count("NO")

    if yes_votes >= VOTE_THRESHOLD:
        direction = "YES"
        trade = True
    elif no_votes >= VOTE_THRESHOLD:
        direction = "NO"
        trade = True
    else:
        direction = None
        trade = False

    summary = (
        f"CLAUDE={claude_r['vote']} | WHALE={whale_r['vote']} | QUANT={quant_r['vote']}"
        f" → {'TRADE ' + direction if trade else 'NO TRADE'}"
    )

    return {
        "trade":     trade,
        "direction": direction,
        "reason":    summary,
        "agents": {
            "claude": claude_r,
            "whale":  whale_r,
            "quant":  quant_r,
        },
    }
