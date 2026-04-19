import asyncio
from dataclasses import dataclass
from statistics import mean


def kelly_size(p_win, market_price, bankroll, max_fraction=0.25):
    """
    f* = (p * b - q) / b
    p = estimated probability
    b = payout ratio (1/price - 1)
    q = 1 - p
    """
    if market_price <= 0 or market_price >= 1:
        return 0.0
    b = (1 / market_price) - 1
    q = 1 - p_win
    f_star = (p_win * b - q) / b
    if f_star <= 0:
        return 0.0
    f_capped = min(f_star, max_fraction)
    return round(bankroll * f_capped, 2)


def score_market(market, claude_estimate):
    price = market["midpoint"]
    gap = abs(claude_estimate - price)
    depth = min(market["bids_depth"], market["asks_depth"])
    hours_left = market["hours_to_resolution"]

    if gap < 0.07:
        return None
    if depth < 500:
        return None
    if hours_left < 4:
        return None
    if hours_left > 168:
        return None

    return {
        "market": market.get("question", ""),
        "gap": round(gap, 3),
        "depth": depth,
        "hours": hours_left,
        "ev": round(gap * depth * 0.001, 2),
    }


def should_trade_from_checks(base_rate_ok, news_ok, whale_ok, disposition_ok, thesis_confidence):
    agrees = sum(1 for x in [base_rate_ok, news_ok, whale_ok, disposition_ok] if x)
    if agrees < 3:
        return False, "<3/4 checks agree"
    if thesis_confidence <= 0.75:
        return False, "thesis confidence <= 75%"
    return True, "3/4 checks agree and confidence > 75%"


def exit_reason(entry_price, current_price, expected_gap, volume_10min, avg_volume_10min, hours_since_entry):
    if current_price >= entry_price + (expected_gap * 0.85):
        return "TARGET_HIT"
    if avg_volume_10min > 0 and volume_10min > avg_volume_10min * 3:
        return "VOLUME_EXIT"
    price_change = (current_price - entry_price) / max(entry_price, 1e-9)
    if hours_since_entry > 24 and abs(price_change) < 0.02:
        return "STALE_THESIS"
    return None


@dataclass
class Wallet:
    balance: float


class ArbitrageAgent:
    name = "arbitrage"

    def evaluate(self, market):
        gap = float(market.get("arb_gap", 0) or 0)
        if gap >= 0.03:
            return {"agent": self.name, "action": "BUY", "confidence": min(0.9, 0.6 + gap), "reason": "related-market price gap"}
        return {"agent": self.name, "action": "HOLD", "confidence": 0.5, "reason": "no arb edge"}


class ConvergenceAgent:
    name = "convergence"

    def evaluate(self, market):
        estimate = float(market.get("estimate", market.get("claude_estimate", 0.5)) or 0.5)
        midpoint = float(market.get("midpoint", 0.5) or 0.5)
        gap = estimate - midpoint
        if gap >= 0.05:
            return {"agent": self.name, "action": "BUY", "confidence": min(0.95, 0.55 + abs(gap)), "reason": "price below estimate"}
        return {"agent": self.name, "action": "HOLD", "confidence": 0.5, "reason": "gap too small"}


class WhaleCopyAgent:
    name = "whale_copy"

    def __init__(self, delay_seconds=60):
        self.delay_seconds = delay_seconds

    def evaluate(self, market):
        whale_signal = bool(market.get("whale_signal", False))
        whale_conf = float(market.get("whale_confidence", 0.5) or 0.5)
        if whale_signal:
            return {"agent": self.name, "action": "BUY", "confidence": whale_conf, "reason": f"copy after {self.delay_seconds}s delay"}
        return {"agent": self.name, "action": "HOLD", "confidence": 0.5, "reason": "no whale buy signal"}


async def execute_consensus(agents, market, wallet, place_order):
    votes = [agent.evaluate(market) for agent in agents]
    buy_votes = [v for v in votes if v.get("action") == "BUY"]
    buy_count = len(buy_votes)

    if buy_count >= 2:
        p_win = mean(v.get("confidence", 0.5) for v in buy_votes)
        size = kelly_size(
            p_win=p_win,
            market_price=float(market.get("midpoint", 0.5) or 0.5),
            bankroll=float(wallet.balance),
        )
        if size > 0:
            await place_order(market, size=size, side="BUY")
        return {"action": "BUY", "size": size, "votes": votes, "mode": "full"}

    if buy_count == 1:
        p_win = buy_votes[0].get("confidence", 0.5)
        base_size = kelly_size(
            p_win=p_win,
            market_price=float(market.get("midpoint", 0.5) or 0.5),
            bankroll=float(wallet.balance),
        )
        size = round(base_size * 0.5, 2)
        if size > 0:
            await place_order(market, size=size, side="BUY")
        return {"action": "BUY", "size": size, "votes": votes, "mode": "half"}

    return {"action": "NO_TRADE", "size": 0.0, "votes": votes, "mode": "none"}


async def _demo_place_order(market, size, side):
    await asyncio.sleep(0)
    print(f"place_order side={side} size=${size:.2f} market={market.get('question', 'n/a')[:50]}")


if __name__ == "__main__":
    sample_market = {
        "question": "Will X happen?",
        "midpoint": 0.65,
        "estimate": 0.82,
        "arb_gap": 0.04,
        "whale_signal": True,
        "whale_confidence": 0.79,
    }
    agents = [ArbitrageAgent(), ConvergenceAgent(), WhaleCopyAgent(delay_seconds=60)]
    w = Wallet(balance=800)
    out = asyncio.run(execute_consensus(agents, sample_market, w, _demo_place_order))
    print(out)
