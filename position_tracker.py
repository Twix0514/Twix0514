"""
POLY//POSITION TRACKER
Monitors open positions and applies 3 exit rules:

  Rule 1: 80% of expected move hit -> exit (lock in gains)
  Rule 2: Volume spikes 3x in 10 minutes -> follow smart money out
  Rule 3: 24h no movement (< 2% change) -> thesis stale -> exit

Positions stored in positions.json. Updated by bot.py on every trade.
Monitored continuously in background thread.
"""

import json, time, threading, logging
from datetime import datetime, timezone
from pathlib import Path
from collections import deque

log = logging.getLogger("POS_TRACKER")

POSITIONS_FILE    = Path(__file__).parent / "positions.json"
MOVE_TARGET_PCT   = 0.80    # exit when 80% of expected move is captured
VOL_SPIKE_MULT    = 3.0     # exit if volume spikes 3x in 10 minutes
STALE_HOURS       = 24      # exit if no movement for 24 hours
STALE_MIN_MOVE    = 0.02    # "no movement" = < 2% price change from entry
POLL_SECS         = 30      # check positions every 30 seconds

_pos_lock   = threading.Lock()
_vol_history = {}           # token_id -> deque of (timestamp, volume) tuples


def load_positions() -> dict:
    if not POSITIONS_FILE.exists():
        return {}
    try:
        return json.loads(POSITIONS_FILE.read_text())
    except Exception:
        return {}


def save_positions(positions: dict):
    with _pos_lock:
        POSITIONS_FILE.write_text(json.dumps(positions, indent=2))


def open_position(token_id: str, market: str, entry_price: float,
                  size_shares: float, direction: str, condition_id: str = ""):
    """Record a new open position."""
    positions = load_positions()
    positions[token_id] = {
        "token_id":      token_id,
        "condition_id":  condition_id,
        "market":        market[:80],
        "direction":     direction,          # YES or NO
        "entry_price":   entry_price,
        "expected_exit": 1.0,               # tokens resolve to $1
        "size_shares":   size_shares,
        "opened_at":     datetime.now(timezone.utc).isoformat(),
        "last_price":    entry_price,
        "last_price_at": datetime.now(timezone.utc).isoformat(),
        "exit_reason":   None,
    }
    save_positions(positions)
    log.info(f"[TRACKER] Position opened: {direction} {size_shares:.1f}sh @ {entry_price:.3f} | {market[:50]}")


def close_position(token_id: str, reason: str, current_price: float = None):
    """Mark position as closed (removes from tracking, returns position dict)."""
    positions = load_positions()
    pos = positions.pop(token_id, None)
    if pos:
        pos["exit_reason"]  = reason
        pos["exit_price"]   = current_price or pos.get("last_price", pos["entry_price"])
        pos["closed_at"]    = datetime.now(timezone.utc).isoformat()
        entry  = pos["entry_price"]
        exit_p = pos["exit_price"]
        shares = pos["size_shares"]
        pnl    = (exit_p - entry) * shares
        log.info(f"[TRACKER] EXIT: {reason} | P&L ${pnl:+.2f} | {pos['market'][:50]}")
        save_positions(positions)
    return pos


def _check_move_target(pos: dict, current_price: float) -> bool:
    """
    Rule 1: 80% of expected move captured.
    Expected move = (1.0 - entry_price) for YES tokens (resolves to $1).
    """
    entry    = pos["entry_price"]
    expected = pos["expected_exit"] - entry   # e.g. 1.0 - 0.75 = 0.25
    if expected <= 0:
        return False
    actual   = current_price - entry
    captured = actual / expected
    if captured >= MOVE_TARGET_PCT:
        log.info(f"[TRACKER] Rule1 triggered: {captured:.0%} of expected move captured")
        return True
    return False


def _check_volume_spike(token_id: str, current_vol: float) -> bool:
    """
    Rule 2: Volume spikes 3x in 10 minutes.
    Tracks rolling 10-min volume window.
    """
    now = time.time()
    if token_id not in _vol_history:
        _vol_history[token_id] = deque()

    hist = _vol_history[token_id]
    hist.append((now, current_vol))

    # Keep only last 12 minutes
    while hist and now - hist[0][0] > 720:
        hist.popleft()

    if len(hist) < 3:
        return False

    # Compare last 10-min window volume to previous 10-min window
    cutoff = now - 600
    recent_vols = [v for ts, v in hist if ts >= cutoff]
    older_vols  = [v for ts, v in hist if ts < cutoff]

    if not older_vols or not recent_vols:
        return False

    recent_avg = sum(recent_vols) / len(recent_vols)
    older_avg  = sum(older_vols)  / len(older_vols)

    if older_avg <= 0:
        return False

    ratio = recent_avg / older_avg
    if ratio >= VOL_SPIKE_MULT:
        log.info(f"[TRACKER] Rule2 triggered: volume spike {ratio:.1f}x in 10min")
        return True
    return False


def _check_stale(pos: dict, current_price: float) -> bool:
    """
    Rule 3: 24h with less than 2% price movement from entry.
    """
    opened_at = datetime.fromisoformat(pos["opened_at"])
    age_hours = (datetime.now(timezone.utc) - opened_at).total_seconds() / 3600
    if age_hours < STALE_HOURS:
        return False
    move = abs(current_price - pos["entry_price"])
    if move < STALE_MIN_MOVE:
        log.info(f"[TRACKER] Rule3 triggered: {age_hours:.0f}h held, only {move:.1%} move")
        return True
    return False


def check_exits(positions: dict, get_price_fn, get_vol_fn, sell_fn, tg_fn):
    """
    Evaluate all open positions against exit rules.
    get_price_fn(token_id) -> float
    get_vol_fn(token_id)   -> float (current volume)
    sell_fn(token_id, size) -> bool
    tg_fn(msg, level)
    """
    for token_id, pos in list(positions.items()):
        try:
            current_price = get_price_fn(token_id)
            if current_price is None:
                continue

            # Update last known price
            pos["last_price"]    = current_price
            pos["last_price_at"] = datetime.now(timezone.utc).isoformat()

            current_vol = get_vol_fn(token_id) or 0.0
            exit_reason = None

            if _check_move_target(pos, current_price):
                exit_reason = f"80% target hit @ {current_price:.3f}"
            elif _check_volume_spike(token_id, current_vol):
                exit_reason = f"vol spike {VOL_SPIKE_MULT}x — smart money out"
            elif _check_stale(pos, current_price):
                exit_reason = f"thesis stale — 24h no movement"

            if exit_reason:
                size = pos.get("size_shares", 0)
                ok   = sell_fn(token_id, size)
                if ok:
                    close_position(token_id, exit_reason, current_price)
                    entry = pos["entry_price"]
                    pnl   = (current_price - entry) * size
                    tg_fn(f"EXIT [{exit_reason}] | P&L ${pnl:+.2f} | {pos['market'][:50]}", "EXIT")

        except Exception as e:
            log.error(f"[TRACKER] Error on {token_id[:12]}: {e}")


def start_tracker(get_price_fn, get_vol_fn, sell_fn, tg_fn):
    """Launch background position monitoring thread."""
    def _loop():
        log.info(f"[TRACKER] Position tracker started — checking every {POLL_SECS}s")
        log.info(f"[TRACKER] Exit rules: 80% target | {VOL_SPIKE_MULT}x vol spike | {STALE_HOURS}h stale")
        while True:
            try:
                positions = load_positions()
                if positions:
                    check_exits(positions, get_price_fn, get_vol_fn, sell_fn, tg_fn)
                    save_positions(positions)
            except Exception as e:
                log.error(f"[TRACKER] Loop error: {e}")
            time.sleep(POLL_SECS)

    t = threading.Thread(target=_loop, name="pos-tracker", daemon=True)
    t.start()
    return t
