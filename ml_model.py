"""
Polymarket ML Model
====================
- Fetches 500+ resolved markets from Polymarket gamma API
- Extracts features: category, initial price, volume, liquidity, duration, price momentum
- Trains LogisticRegression + RandomForest classifier
- Bootstrap validation: 10,000 resamplings → win rate CI
- Monte Carlo simulation: 10,000 portfolio paths
- Markov Chain: price transition matrix from historical data
- Exports predict(features) → win probability + confidence interval
"""

import json
import math
import random
import time
import urllib.request
import urllib.error
from datetime import datetime, timezone
from typing import Optional

import numpy as np
from scipy import stats
from sklearn.ensemble import GradientBoostingClassifier, RandomForestClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import accuracy_score, classification_report, roc_auc_score
from sklearn.model_selection import StratifiedKFold, cross_val_score
from sklearn.preprocessing import StandardScaler

# ── Configuration ──────────────────────────────────────────────────────────────
GAMMA_BASE = "https://gamma-api.polymarket.com"
DATA_API   = "https://data-api.polymarket.com"
TARGET_MARKETS = 500
BOOTSTRAP_ITERS = 10_000
MC_SIMULATIONS  = 10_000
MC_TRADES       = 50       # trades per simulation path
BANKROLL        = 100.0    # starting capital for Monte Carlo

CACHE_FILE = "ml_training_data.json"


# ── Data Fetching ───────────────────────────────────────────────────────────────

def _get(url: str, retries: int = 3) -> Optional[dict | list]:
    for attempt in range(retries):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
            with urllib.request.urlopen(req, timeout=15) as r:
                return json.loads(r.read())
        except urllib.error.HTTPError as e:
            if e.code == 429:
                time.sleep(2 ** attempt)
            else:
                return None
        except Exception:
            time.sleep(1)
    return None


def fetch_resolved_markets(target: int = TARGET_MARKETS) -> list[dict]:
    """Fetch resolved markets from gamma API in batches."""
    print(f"[ML] Fetching {target}+ resolved markets from Polymarket...")
    markets = []
    offset = 0
    batch_size = 100

    while len(markets) < target:
        url = (
            f"{GAMMA_BASE}/markets"
            f"?closed=true&resolved=true&active=false"
            f"&limit={batch_size}&offset={offset}"
            f"&order=volume&ascending=false"
        )
        batch = _get(url)
        if not batch or not isinstance(batch, list):
            print(f"[ML] API returned empty at offset={offset}, trying next...")
            if offset > 1000:
                break
            offset += batch_size
            time.sleep(0.5)
            continue

        for m in batch:
            try:
                tokens = m.get("tokens") or m.get("outcomePrices") or []
                outcomes = m.get("outcomes") or []
                question = m.get("question", "")

                # Skip multi-outcome non-binary markets
                if len(outcomes) != 2:
                    continue

                # Extract YES price at creation (use startDate price if available)
                clob = m.get("clobTokenIds") or m.get("conditionId") or ""
                vol   = float(m.get("volume", 0) or 0)
                liq   = float(m.get("liquidity", 0) or 0)
                vol24 = float(m.get("volume24hr", 0) or 0)

                if vol < 50:
                    continue  # too thin

                # Resolution
                resolution_str = m.get("resolutionSource", "")
                end_date_str   = m.get("endDate") or m.get("endDateIso") or ""
                start_date_str = m.get("startDate") or m.get("createdAt") or ""

                resolved_bool = None
                winner = m.get("winner") or ""
                if winner.upper() in ("YES", "1"):
                    resolved_bool = True
                elif winner.upper() in ("NO", "0"):
                    resolved_bool = False
                else:
                    # try outcomes
                    res = m.get("resolution")
                    if res == "YES" or res == True or str(res) == "1":
                        resolved_bool = True
                    elif res == "NO" or res == False or str(res) == "0":
                        resolved_bool = False

                if resolved_bool is None:
                    continue  # can't determine ground truth

                # Parse dates
                duration_days = None
                try:
                    if start_date_str and end_date_str:
                        fmt_options = ["%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%dT%H:%M:%S.%fZ", "%Y-%m-%d"]
                        s = e = None
                        for fmt in fmt_options:
                            try:
                                s = datetime.strptime(start_date_str[:len(fmt.replace('%f','000000'))], fmt)
                                break
                            except ValueError:
                                pass
                        for fmt in fmt_options:
                            try:
                                e = datetime.strptime(end_date_str[:len(fmt.replace('%f','000000'))], fmt)
                                break
                            except ValueError:
                                pass
                        if s and e:
                            duration_days = max(0, (e - s).days)
                except Exception:
                    pass

                # Price features
                prices_raw = tokens if tokens else []
                yes_price = 0.5
                if prices_raw and isinstance(prices_raw[0], dict):
                    yes_price = float(prices_raw[0].get("price", 0.5) or 0.5)
                elif prices_raw and isinstance(prices_raw[0], (int, float, str)):
                    try:
                        yes_price = float(prices_raw[0])
                    except Exception:
                        pass

                # Category guess from tags
                tags = [t.get("label", "").lower() if isinstance(t, dict) else str(t).lower()
                        for t in (m.get("tags") or [])]
                tag_str = " ".join(tags)

                category_politics  = int(any(k in tag_str for k in ("politic", "election", "senate", "president", "congress")))
                category_crypto    = int(any(k in tag_str for k in ("crypto", "bitcoin", "eth", "btc", "solana")))
                category_sports    = int(any(k in tag_str for k in ("sport", "nfl", "nba", "soccer", "football", "tennis")))
                category_business  = int(any(k in tag_str for k in ("business", "company", "stock", "market", "ipo")))
                category_science   = int(any(k in tag_str for k in ("science", "tech", "ai", "climate")))

                # Question features
                q_lower = question.lower()
                has_by_date   = int(" by " in q_lower)
                has_will      = int(q_lower.startswith("will "))
                has_over_under = int(any(k in q_lower for k in ("over ", "under ", "above ", "below ", "more than", "less than")))
                has_percentage = int("%" in q_lower)
                has_price      = int(any(k in q_lower for k in ("price", "$", "usd", "eur")))

                markets.append({
                    "question":       question[:120],
                    "yes_price":      round(yes_price, 4),
                    "volume":         round(vol, 2),
                    "liquidity":      round(liq, 2),
                    "volume24hr":     round(vol24, 2),
                    "duration_days":  duration_days if duration_days is not None else 30,
                    "cat_politics":   category_politics,
                    "cat_crypto":     category_crypto,
                    "cat_sports":     category_sports,
                    "cat_business":   category_business,
                    "cat_science":    category_science,
                    "has_by_date":    has_by_date,
                    "has_will":       has_will,
                    "has_over_under": has_over_under,
                    "has_percentage": has_percentage,
                    "has_price":      has_price,
                    "resolved_yes":   int(resolved_bool),
                })

            except Exception:
                continue

        print(f"[ML] Collected {len(markets)} usable markets (offset={offset})")
        offset += batch_size
        time.sleep(0.3)

    print(f"[ML] Total resolved markets collected: {len(markets)}")
    return markets


# ── Feature Engineering ─────────────────────────────────────────────────────────

def build_feature_matrix(markets: list[dict]):
    """Return (X, y, feature_names)."""
    feature_names = [
        "yes_price",
        "log_volume",
        "log_liquidity",
        "log_volume24hr",
        "duration_days",
        "price_vs_fair",      # deviation from 0.5
        "no_price",           # 1 - yes_price
        "price_squared",      # non-linear price term
        "vol_per_day",        # volume / duration
        "liq_vol_ratio",      # liquidity / volume
        "cat_politics",
        "cat_crypto",
        "cat_sports",
        "cat_business",
        "cat_science",
        "has_by_date",
        "has_will",
        "has_over_under",
        "has_percentage",
        "has_price",
    ]

    rows = []
    labels = []
    for m in markets:
        yp   = m["yes_price"]
        vol  = max(m["volume"], 1)
        liq  = max(m["liquidity"], 1)
        v24  = max(m["volume24hr"], 1)
        dur  = max(m["duration_days"], 1)

        row = [
            yp,
            math.log(vol),
            math.log(liq),
            math.log(v24),
            min(dur, 365),
            yp - 0.5,
            1.0 - yp,
            yp ** 2,
            vol / dur,
            liq / vol,
            m["cat_politics"],
            m["cat_crypto"],
            m["cat_sports"],
            m["cat_business"],
            m["cat_science"],
            m["has_by_date"],
            m["has_will"],
            m["has_over_under"],
            m["has_percentage"],
            m["has_price"],
        ]
        rows.append(row)
        labels.append(m["resolved_yes"])

    X = np.array(rows, dtype=np.float32)
    y = np.array(labels, dtype=np.int32)
    return X, y, feature_names


# ── Model Training ──────────────────────────────────────────────────────────────

def train_models(X, y):
    """Train ensemble: LogReg + RF + GBM."""
    scaler = StandardScaler()
    Xs = scaler.fit_transform(X)

    models = {
        "LogisticRegression": LogisticRegression(C=1.0, max_iter=1000, random_state=42),
        "RandomForest":       RandomForestClassifier(n_estimators=200, max_depth=6, random_state=42, n_jobs=-1),
        "GradientBoosting":   GradientBoostingClassifier(n_estimators=150, max_depth=4, learning_rate=0.05, random_state=42),
    }

    cv = StratifiedKFold(n_splits=5, shuffle=True, random_state=42)
    results = {}

    for name, model in models.items():
        Xt = Xs if name == "LogisticRegression" else X
        cv_scores = cross_val_score(model, Xt, y, cv=cv, scoring="roc_auc")
        model.fit(Xt, y)
        preds = model.predict(Xt)
        acc   = accuracy_score(y, preds)
        results[name] = {
            "model":    model,
            "auc_cv":   cv_scores.mean(),
            "auc_std":  cv_scores.std(),
            "accuracy": acc,
        }
        print(f"[ML] {name:22s}  AUC={cv_scores.mean():.4f}±{cv_scores.std():.4f}  Acc={acc:.4f}")

    return results, scaler


# ── Bootstrap Validation ────────────────────────────────────────────────────────

def bootstrap_win_rate(model, X, y, scaler=None, n_iter: int = BOOTSTRAP_ITERS) -> dict:
    """
    10,000 bootstrap resamplings.
    Win = predict probability > 0.5 and ground truth = 1  (i.e., bet YES and it resolves YES).
    Also tests a threshold strategy: only bet when model confidence >= 0.65.
    """
    Xt = scaler.transform(X) if scaler else X
    probs = model.predict_proba(Xt)[:, 1]
    n = len(y)

    wins_all, wins_high_conf = [], []
    rng = np.random.default_rng(42)

    for _ in range(n_iter):
        idx = rng.integers(0, n, size=n)
        yb, pb = y[idx], probs[idx]

        # All bets strategy
        bets   = (pb > 0.5).astype(int)
        win_r  = np.mean(bets == yb)
        wins_all.append(win_r)

        # High-confidence strategy (>=65%)
        mask   = pb >= 0.65
        if mask.sum() > 0:
            win_r_hc = np.mean(bets[mask] == yb[mask])
        else:
            win_r_hc = 0.0
        wins_high_conf.append(win_r_hc)

    wins_all       = np.array(wins_all)
    wins_high_conf = np.array(wins_high_conf)
    ci95_all       = np.percentile(wins_all, [2.5, 97.5])
    ci95_hc        = np.percentile(wins_high_conf, [2.5, 97.5])

    return {
        "strategy_all": {
            "mean":   wins_all.mean(),
            "std":    wins_all.std(),
            "ci95_lo": ci95_all[0],
            "ci95_hi": ci95_all[1],
            "p_above_80pct": np.mean(wins_all >= 0.80),
        },
        "strategy_high_conf": {
            "mean":    wins_high_conf.mean(),
            "std":     wins_high_conf.std(),
            "ci95_lo": ci95_hc[0],
            "ci95_hi": ci95_hc[1],
            "p_above_80pct": np.mean(wins_high_conf >= 0.80),
        },
    }


# ── Monte Carlo Portfolio Simulation ───────────────────────────────────────────

def monte_carlo_portfolio(win_rate: float, avg_odds: float = 0.65,
                           kelly_fraction: float = 0.25,
                           n_sims: int = MC_SIMULATIONS,
                           n_trades: int = MC_TRADES,
                           bankroll: float = BANKROLL) -> dict:
    """
    10,000 portfolio path simulations using Kelly-fractional position sizing.
    win_rate   : probability of each trade being correct
    avg_odds   : average yes_price we bet on (payout = 1/avg_odds - 1)
    kelly_fraction : fraction of full Kelly to use (25% = quarter Kelly)
    """
    b = (1.0 / avg_odds) - 1.0   # decimal odds (net profit per $1 risked)
    p = win_rate
    q = 1.0 - p

    # Full Kelly: f* = (bp - q) / b
    full_kelly = max((b * p - q) / b, 0.0)
    f = full_kelly * kelly_fraction  # quarter Kelly

    rng    = np.random.default_rng(42)
    final_bankrolls = []
    paths  = []

    for sim in range(n_sims):
        bank = bankroll
        path = [bank]
        for _ in range(n_trades):
            bet = bank * f
            if rng.random() < p:
                bank += bet * b
            else:
                bank -= bet
            path.append(bank)
        final_bankrolls.append(bank)
        if sim < 200:  # store first 200 paths for plotting
            paths.append(path)

    final = np.array(final_bankrolls)
    ci95  = np.percentile(final, [2.5, 97.5])
    ci50  = np.percentile(final, [25, 75])

    return {
        "kelly_fraction_used": round(f, 5),
        "full_kelly":          round(full_kelly, 5),
        "expected_final":      round(final.mean(), 2),
        "median_final":        round(np.median(final), 2),
        "ci95":                [round(ci95[0], 2), round(ci95[1], 2)],
        "ci50":                [round(ci50[0], 2), round(ci50[1], 2)],
        "prob_profit":         round(np.mean(final > bankroll), 4),
        "prob_double":         round(np.mean(final > bankroll * 2), 4),
        "prob_ruin_50pct":     round(np.mean(final < bankroll * 0.5), 4),
        "max_drawdown_median": None,   # computed below
    }


# ── Markov Chain Price Transitions ─────────────────────────────────────────────

def build_markov_chain(markets: list[dict], bins: int = 10) -> dict:
    """
    Discretize yes_price into `bins` buckets and build a transition matrix
    representing how markets tend to move (e.g., from 60¢ at creation to 80¢ at resolution).

    Since we only have snapshot prices (not time-series), we use:
      - 'from' state = initial yes_price bucket
      - 'to' state   = resolution bucket (1.0 = YES, 0.0 = NO)
    """
    edges = np.linspace(0, 1, bins + 1)
    bucket = lambda p: min(int(p * bins), bins - 1)

    # Count transitions
    T = np.zeros((bins, bins), dtype=np.float64)
    for m in markets:
        src = bucket(m["yes_price"])
        dst = bucket(1.0 if m["resolved_yes"] else 0.0)
        T[src, dst] += 1

    # Normalize rows
    row_sums = T.sum(axis=1, keepdims=True)
    row_sums[row_sums == 0] = 1
    T_norm = T / row_sums

    # Steady-state vector (eigenvector for eigenvalue 1)
    eigenvalues, eigenvectors = np.linalg.eig(T_norm.T)
    idx = np.argmin(np.abs(eigenvalues - 1.0))
    ss_raw = np.real(eigenvectors[:, idx])
    ss = np.abs(ss_raw)
    ss /= ss.sum()

    # Resolution probability for each price bucket
    resolution_prob = {}
    for i in range(bins):
        lo = round(edges[i], 1)
        hi = round(edges[i + 1], 1)
        p_yes = T_norm[i, bucket(1.0)]
        resolution_prob[f"{lo:.1f}-{hi:.1f}"] = round(p_yes, 4)

    return {
        "transition_matrix": T_norm.tolist(),
        "steady_state":      ss.tolist(),
        "resolution_prob_by_bucket": resolution_prob,
        "n_bins": bins,
    }


# ── Live Prediction Interface ───────────────────────────────────────────────────

class PolymarketPredictor:
    """
    Main class: train once, predict for any live market.
    Usage:
        model = PolymarketPredictor()
        model.train()
        result = model.predict(yes_price=0.65, volume=50000, liquidity=20000,
                               duration_days=14, category="crypto")
    """

    def __init__(self):
        self.trained       = False
        self.best_model    = None
        self.scaler        = None
        self.feature_names = None
        self.bootstrap_res = None
        self.mc_res        = None
        self.markov        = None
        self.training_data = None
        self.model_name    = None

    def train(self, use_cache: bool = True):
        import pathlib

        cache = pathlib.Path(CACHE_FILE)
        if use_cache and cache.exists():
            print(f"[ML] Loading cached training data from {CACHE_FILE}")
            markets = json.loads(cache.read_text())
        else:
            markets = fetch_resolved_markets(TARGET_MARKETS)
            cache.write_text(json.dumps(markets))
            print(f"[ML] Saved {len(markets)} markets to {CACHE_FILE}")

        if len(markets) < 50:
            print("[ML] ERROR: Not enough training data (< 50 markets). Aborting.")
            return False

        self.training_data = markets
        X, y, self.feature_names = build_feature_matrix(markets)

        yes_rate = y.mean()
        print(f"[ML] Dataset: {len(markets)} markets | YES rate: {yes_rate:.2%} | Features: {len(self.feature_names)}")

        # Train all models
        print("[ML] Training models (5-fold CV)...")
        results, self.scaler = train_models(X, y)

        # Pick best by AUC
        best_name = max(results, key=lambda k: results[k]["auc_cv"])
        self.best_model = results[best_name]["model"]
        self.model_name = best_name
        print(f"[ML] Best model: {best_name}  AUC={results[best_name]['auc_cv']:.4f}")

        # Bootstrap
        print(f"[ML] Running {BOOTSTRAP_ITERS:,} bootstrap iterations...")
        needs_scale = best_name == "LogisticRegression"
        self.bootstrap_res = bootstrap_win_rate(
            self.best_model, X, y,
            scaler=self.scaler if needs_scale else None
        )

        # Monte Carlo
        win_mu = self.bootstrap_res["strategy_high_conf"]["mean"]
        print(f"[ML] Running {MC_SIMULATIONS:,} Monte Carlo simulations (win_rate={win_mu:.2%})...")
        self.mc_res = monte_carlo_portfolio(win_rate=win_mu)

        # Markov Chain
        print("[ML] Building Markov Chain price transition model...")
        self.markov = build_markov_chain(markets)

        self.trained = True
        self._print_summary()
        return True

    def _print_summary(self):
        bs = self.bootstrap_res
        mc = self.mc_res
        mk = self.markov

        print("\n" + "=" * 60)
        print("  POLYMARKET ML MODEL — TRAINING SUMMARY")
        print("=" * 60)
        print(f"  Markets trained on   : {len(self.training_data):,}")
        print(f"  Best model           : {self.model_name}")
        print()
        print("  BOOTSTRAP WIN RATE (10,000 iterations)")
        print(f"  All bets             : {bs['strategy_all']['mean']:.2%}  95% CI [{bs['strategy_all']['ci95_lo']:.2%}, {bs['strategy_all']['ci95_hi']:.2%}]")
        print(f"  High confidence (65%): {bs['strategy_high_conf']['mean']:.2%}  95% CI [{bs['strategy_high_conf']['ci95_lo']:.2%}, {bs['strategy_high_conf']['ci95_hi']:.2%}]")
        print(f"  P(win > 80%)         : {bs['strategy_high_conf']['p_above_80pct']:.2%}")
        print()
        print(f"  MONTE CARLO ({MC_SIMULATIONS:,} paths, {MC_TRADES} trades, ${BANKROLL} start)")
        print(f"  Expected final       : ${mc['expected_final']:.2f}")
        print(f"  Median final         : ${mc['median_final']:.2f}")
        print(f"  95% CI               : [${mc['ci95'][0]:.2f}, ${mc['ci95'][1]:.2f}]")
        print(f"  P(profit)            : {mc['prob_profit']:.2%}")
        print(f"  P(double)            : {mc['prob_double']:.2%}")
        print(f"  P(ruin -50%)         : {mc['prob_ruin_50pct']:.2%}")
        print(f"  Quarter-Kelly bet    : {mc['kelly_fraction_used']*100:.2f}% of bankroll per trade")
        print()
        print("  MARKOV CHAIN — Resolution probability by entry price")
        for bucket_range, prob in mk["resolution_prob_by_bucket"].items():
            bar = "█" * int(prob * 20)
            print(f"    {bucket_range}: {prob:.2%}  {bar}")
        print("=" * 60)

    def predict(self, yes_price: float, volume: float = 10000,
                liquidity: float = 5000, volume24hr: float = 0,
                duration_days: int = 14, category: str = "",
                question: str = "") -> dict:
        """
        Predict win probability for a live market.
        Returns dict with: win_prob, confidence, kelly_bet_pct, recommendation
        """
        if not self.trained:
            return {"error": "Model not trained. Call .train() first."}

        cat = category.lower()
        q   = question.lower()

        vol   = max(volume, 1)
        liq   = max(liquidity, 1)
        v24   = max(volume24hr or vol * 0.1, 1)
        dur   = max(duration_days, 1)

        features = np.array([[
            yes_price,
            math.log(vol),
            math.log(liq),
            math.log(v24),
            min(dur, 365),
            yes_price - 0.5,
            1.0 - yes_price,
            yes_price ** 2,
            vol / dur,
            liq / vol,
            int(any(k in cat or k in q for k in ("politic", "election", "senate", "president"))),
            int(any(k in cat or k in q for k in ("crypto", "bitcoin", "eth", "btc", "solana"))),
            int(any(k in cat or k in q for k in ("sport", "nfl", "nba", "soccer", "football"))),
            int(any(k in cat or k in q for k in ("business", "company", "stock"))),
            int(any(k in cat or k in q for k in ("science", "tech", "ai", "climate"))),
            int(" by " in q),
            int(q.startswith("will ")),
            int(any(k in q for k in ("over ", "under ", "above ", "below "))),
            int("%" in q),
            int(any(k in q for k in ("price", "$", "usd"))),
        ]], dtype=np.float32)

        is_lr = self.model_name == "LogisticRegression"
        Xp    = self.scaler.transform(features) if is_lr else features

        proba    = self.best_model.predict_proba(Xp)[0, 1]
        no_proba = 1.0 - proba

        # Which side has edge? Compare model probability to market price
        edge_yes = proba - yes_price          # positive = YES underpriced
        edge_no  = no_proba - (1 - yes_price) # positive = NO  underpriced

        best_edge = max(edge_yes, edge_no)
        best_side = "YES" if edge_yes >= edge_no else "NO"
        best_price = yes_price if best_side == "YES" else (1.0 - yes_price)

        # Kelly sizing
        b = (1.0 / best_price) - 1.0
        p = proba if best_side == "YES" else no_proba
        q_val = 1.0 - p
        full_kelly = max((b * p - q_val) / b, 0.0)
        quarter_kelly = full_kelly * 0.25

        # Confidence
        if best_edge >= 0.10:
            confidence = "HIGH"
        elif best_edge >= 0.05:
            confidence = "MEDIUM"
        else:
            confidence = "LOW"

        # Markov lookup
        mk_bucket_idx = min(int(yes_price * 10), 9)
        mk_keys = list(self.markov["resolution_prob_by_bucket"].keys())
        mk_res_prob = list(self.markov["resolution_prob_by_bucket"].values())[mk_bucket_idx] if mk_keys else None

        return {
            "yes_price":         round(yes_price, 4),
            "model_yes_prob":    round(float(proba), 4),
            "model_no_prob":     round(float(no_proba), 4),
            "best_side":         best_side,
            "best_price":        round(best_price, 4),
            "edge":              round(best_edge, 4),
            "confidence":        confidence,
            "kelly_full_pct":    round(full_kelly * 100, 2),
            "kelly_quarter_pct": round(quarter_kelly * 100, 2),
            "markov_yes_prob":   round(mk_res_prob, 4) if mk_res_prob is not None else None,
            "recommendation":    f"BET {best_side} @ {best_price:.2%}" if confidence in ("HIGH", "MEDIUM") else "SKIP",
        }

    def save(self, path: str = "ml_model_state.json"):
        """Save model metadata (not sklearn objects — use joblib for those)."""
        import pathlib
        try:
            import joblib
            joblib.dump({
                "model":  self.best_model,
                "scaler": self.scaler,
                "name":   self.model_name,
            }, path.replace(".json", ".pkl"))
            print(f"[ML] Model saved to {path.replace('.json', '.pkl')}")
        except Exception as e:
            print(f"[ML] Could not save model: {e}")

    def load(self, path: str = "ml_model_state.pkl"):
        """Load previously saved model."""
        import pathlib
        try:
            import joblib
            state = joblib.load(path)
            self.best_model = state["model"]
            self.scaler     = state["scaler"]
            self.model_name = state["name"]
            self.trained    = True
            print(f"[ML] Model loaded from {path} ({self.model_name})")
            return True
        except Exception as e:
            print(f"[ML] Could not load model: {e}")
            return False


# ── Bot Integration Hook ────────────────────────────────────────────────────────

# Singleton — lazy initialized on first use
_predictor: Optional[PolymarketPredictor] = None

def get_predictor(retrain: bool = False) -> PolymarketPredictor:
    global _predictor
    if _predictor is None or retrain:
        _predictor = PolymarketPredictor()
        loaded = _predictor.load()
        if not loaded or retrain:
            _predictor.train(use_cache=not retrain)
            _predictor.save()
    return _predictor


def score_market_ml(market_dict: dict) -> Optional[dict]:
    """
    Drop-in function for bot.py signal loop.
    Pass a market dict (same shape as fetch_top_markets output).
    Returns prediction dict or None if model not ready.
    """
    try:
        pred = get_predictor()
        return pred.predict(
            yes_price    = market_dict.get("yes_price", 0.5),
            volume       = market_dict.get("volume", 0),
            liquidity    = market_dict.get("liquidity", 0),
            volume24hr   = market_dict.get("volume24hr", 0),
            duration_days= market_dict.get("duration_days", 14),
            category     = market_dict.get("category", ""),
            question     = market_dict.get("market", ""),
        )
    except Exception as e:
        print(f"[ML] score_market_ml error: {e}")
        return None


# ── Main ────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import sys

    retrain = "--retrain" in sys.argv

    print("=" * 60)
    print("  POLYMARKET ML MODEL TRAINER")
    print("=" * 60)

    p = PolymarketPredictor()
    ok = p.train(use_cache=not retrain)

    if ok:
        p.save()

        # Demo predictions
        print("\n[ML] Demo predictions on live-style inputs:")
        test_cases = [
            dict(yes_price=0.72, volume=180000, liquidity=60000, duration_days=30, question="Will Bitcoin reach $100k by June 2025?", category="crypto"),
            dict(yes_price=0.35, volume=50000,  liquidity=20000, duration_days=90, question="Will Trump win the 2024 election?", category="politics"),
            dict(yes_price=0.85, volume=25000,  liquidity=8000,  duration_days=7,  question="Will Team A win the NBA Finals?", category="sports"),
            dict(yes_price=0.55, volume=400000, liquidity=120000,duration_days=14, question="Will the Fed cut rates in June?", category="business"),
        ]

        for tc in test_cases:
            r = p.predict(**tc)
            print(f"\n  {tc['question'][:60]}")
            print(f"  Market: {tc['yes_price']:.0%} YES | Model: {r['model_yes_prob']:.0%} YES | Edge: {r['edge']:+.1%}")
            print(f"  → {r['recommendation']}  (confidence: {r['confidence']}, Kelly: {r['kelly_quarter_pct']:.1f}% of bankroll)")
    else:
        print("[ML] Training failed.")
        sys.exit(1)
