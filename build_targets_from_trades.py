import argparse
import csv
import json
from collections import defaultdict
from pathlib import Path


def parse_args():
    p = argparse.ArgumentParser(description="Build targets.json from trades CSV")
    p.add_argument("--input", default="poly_data/processed/trades.csv", help="Path to trades CSV")
    p.add_argument("--output", default="targets.json", help="Output JSON path")
    p.add_argument("--wallet-col", default="maker", help="CSV column containing wallet")
    p.add_argument("--profit-col", default="profit", help="CSV column containing trade PnL")
    p.add_argument("--min-trades", type=int, default=100)
    p.add_argument("--min-win-rate", type=float, default=0.70)
    p.add_argument("--top", type=int, default=50)
    return p.parse_args()


def to_float(v):
    try:
        return float(v)
    except Exception:
        return 0.0


def main():
    args = parse_args()
    in_path = Path(args.input)
    if not in_path.exists():
        raise FileNotFoundError(f"Input CSV not found: {in_path}")

    stats = defaultdict(lambda: {"trades": 0, "wins": 0, "total_pnl": 0.0})

    with in_path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            wallet = str(row.get(args.wallet_col, "") or "").strip().lower()
            if not wallet:
                continue
            pnl = to_float(row.get(args.profit_col, 0))
            stats[wallet]["trades"] += 1
            if pnl > 0:
                stats[wallet]["wins"] += 1
            stats[wallet]["total_pnl"] += pnl

    ranked = []
    for wallet, s in stats.items():
        trades = s["trades"]
        if trades < args.min_trades:
            continue
        win_rate = s["wins"] / trades if trades else 0.0
        if win_rate <= args.min_win_rate:
            continue
        ranked.append(
            {
                "wallet": wallet,
                "trades": trades,
                "win_rate": round(win_rate, 4),
                "total_pnl": round(s["total_pnl"], 2),
            }
        )

    ranked.sort(key=lambda x: x["total_pnl"], reverse=True)
    top = ranked[: args.top]

    out = []
    for i, row in enumerate(top, start=1):
        out.append(
            {
                "rank": i,
                "wallet": row["wallet"],
                "tier": "poly_data-70w-100t",
                "trades": row["trades"],
                "win_rate": row["win_rate"],
                "pnl_all": row["total_pnl"],
            }
        )

    out_path = Path(args.output)
    out_path.write_text(json.dumps(out, indent=2), encoding="utf-8")
    print(f"Wrote {len(out)} targets to {out_path}")


if __name__ == "__main__":
    main()
