import argparse
import json
from datetime import datetime, timezone
from pathlib import Path


def parse_args():
    p = argparse.ArgumentParser(description="Build queue.json from markets JSON + estimates")
    p.add_argument("--markets", default="markets.json", help="Polymarket markets JSON file")
    p.add_argument("--estimates", default="estimates.json", help="JSON map: market_id -> estimated probability")
    p.add_argument("--books", default="", help="Optional JSON map: token_id -> order book payload")
    p.add_argument("--output", default="queue.json", help="Output queue JSON")
    p.add_argument("--min-gap", type=float, default=0.07)
    p.add_argument("--min-depth", type=float, default=500.0)
    p.add_argument("--min-hours", type=float, default=4.0)
    p.add_argument("--max-hours", type=float, default=168.0)
    return p.parse_args()


def load_json(path):
    return json.loads(Path(path).read_text(encoding="utf-8"))


def parse_prices(market):
    p = market.get("outcomePrices", [0.5, 0.5])
    if isinstance(p, str):
        try:
            p = json.loads(p)
        except Exception:
            p = [0.5, 0.5]
    try:
        return [float(x) for x in p]
    except Exception:
        return [0.5, 0.5]


def parse_tokens(market):
    t = market.get("clobTokenIds", [])
    if isinstance(t, str):
        try:
            t = json.loads(t)
        except Exception:
            t = []
    out = []
    for x in t:
        if isinstance(x, str):
            out.append(x)
        elif isinstance(x, dict):
            tid = x.get("token_id", "")
            if tid:
                out.append(tid)
    return out


def parse_hours_left(market):
    end_str = market.get("endDate") or market.get("resolutionDate") or ""
    if not end_str:
        return 9999.0
    try:
        end_dt = datetime.fromisoformat(end_str.replace("Z", "+00:00"))
        return max(0.0, (end_dt - datetime.now(timezone.utc)).total_seconds() / 3600)
    except Exception:
        return 9999.0


def depth_from_book(book):
    bids = sum(float(b.get("size", 0) or 0) * float(b.get("price", 0) or 0) for b in book.get("bids", []))
    asks = sum(float(a.get("size", 0) or 0) * float(a.get("price", 0) or 0) for a in book.get("asks", []))
    return round(bids, 2), round(asks, 2)


def market_midpoint(market):
    mid = market.get("midpoint")
    if mid is not None:
        try:
            return float(mid)
        except Exception:
            pass
    prices = parse_prices(market)
    return float(prices[0]) if prices else 0.5


def score_market(market, claude_estimate, min_gap, min_depth, min_hours, max_hours):
    price = market["midpoint"]
    gap = abs(claude_estimate - price)
    depth = min(market["bids_depth"], market["asks_depth"])
    hours_left = market["hours_to_resolution"]

    if gap < min_gap:
        return None
    if depth < min_depth:
        return None
    if hours_left < min_hours:
        return None
    if hours_left > max_hours:
        return None

    return {
        "market": market["question"],
        "gap": round(gap, 3),
        "depth": depth,
        "hours": round(hours_left, 2),
        "ev": round(gap * depth * 0.001, 2),
    }


def main():
    args = parse_args()
    markets_raw = load_json(args.markets)
    markets = markets_raw if isinstance(markets_raw, list) else markets_raw.get("markets", [])
    estimates = load_json(args.estimates)
    books = load_json(args.books) if args.books else {}

    survivors = []
    for m in markets:
        market_id = str(m.get("conditionId") or m.get("id") or "")
        if not market_id:
            continue
        if market_id not in estimates:
            continue

        tokens = parse_tokens(m)
        token_id = tokens[0] if tokens else ""
        bids_depth = float(m.get("bids_depth", 0) or 0)
        asks_depth = float(m.get("asks_depth", 0) or 0)

        if (bids_depth <= 0 or asks_depth <= 0) and token_id and token_id in books:
            bids_depth, asks_depth = depth_from_book(books[token_id])

        record = {
            "id": market_id,
            "question": str(m.get("question") or "")[:200],
            "token_id": token_id,
            "midpoint": market_midpoint(m),
            "bids_depth": bids_depth,
            "asks_depth": asks_depth,
            "hours_to_resolution": parse_hours_left(m),
        }

        est = float(estimates[market_id])
        scored = score_market(
            record,
            est,
            min_gap=args.min_gap,
            min_depth=args.min_depth,
            min_hours=args.min_hours,
            max_hours=args.max_hours,
        )
        if scored is None:
            continue

        record.update({
            "estimate": est,
            "gap": scored["gap"],
            "depth": scored["depth"],
            "ev": scored["ev"],
        })
        survivors.append(record)

    survivors.sort(key=lambda x: x.get("ev", 0), reverse=True)
    Path(args.output).write_text(json.dumps(survivors, indent=2), encoding="utf-8")
    print(f"Wrote {len(survivors)} survivors to {args.output}")


if __name__ == "__main__":
    main()
