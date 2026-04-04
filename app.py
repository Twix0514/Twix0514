import argparse
import os
from pathlib import Path
from flask import Flask, jsonify, request
import requests
from datetime import datetime
from collections import defaultdict
import statistics


app = Flask(__name__)

# Polymarket API endpoints
GAMMA_API_BASE = "https://gamma-api.polymarket.com"
CLOB_API_BASE = "https://clob.polymarket.com"
DATA_API_BASE = "https://data.polymarket.com"

# Price history cache
price_history = defaultdict(list)


def fetch_markets(limit=10):
    """Fetch markets from Polymarket Gamma API"""
    try:
        response = requests.get(f"{GAMMA_API_BASE}/markets", params={"limit": limit}, timeout=5)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        return {"error": str(e)}


def fetch_events(limit=5):
    """Fetch events from Polymarket Gamma API"""
    try:
        response = requests.get(f"{GAMMA_API_BASE}/events", params={"limit": limit}, timeout=5)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        return {"error": str(e)}


def calculate_arbitrage(market):
    """Detect arbitrage opportunities"""
    try:
        prices = [float(p) for p in market.get("outcomePrices", [])]
        if not prices or len(prices) < 2:
            return None
        
        total_prob = sum(prices)
        if total_prob == 0:
            return None
            
        arbitrage_spread = total_prob - 1.0
        return {
            "market_id": market.get("id"),
            "arbitrage_opportunity": arbitrage_spread,
            "type": "overpriced" if arbitrage_spread > 0 else "underpriced",
            "magnitude": abs(arbitrage_spread) * 100  # in percentage points
        }
    except:
        return None


def calculate_volatility(prices):
    """Calculate price volatility"""
    if len(prices) < 2:
        return 0
    try:
        return statistics.stdev(prices)
    except:
        return 0


def track_price_history(market):
    """Track price history for volatility analysis"""
    market_id = market.get("id")
    if market_id:
        prices = [float(p) for p in market.get("outcomePrices", [0, 0])]
        if prices:
            price_history[market_id].append({
                "timestamp": datetime.now().isoformat(),
                "prices": prices
            })
            # Keep only last 100 records
            if len(price_history[market_id]) > 100:
                price_history[market_id] = price_history[market_id][-100:]


def calculate_portfolio_risk(markets):
    """Estimate portfolio risk based on market diversification"""
    if not markets:
        return {"risk_score": 0, "analysis": "No markets"}
    
    total_markets = len(markets)
    concentrated = sum(1 for m in markets if len(m.get("outcomePrices", [])) > 0)
    
    risk_score = (concentrated / total_markets * 100) if total_markets > 0 else 0
    
    return {
        "total_markets": total_markets,
        "concentrated_markets": concentrated,
        "risk_score": round(risk_score, 2),
        "risk_level": "high" if risk_score > 70 else "medium" if risk_score > 40 else "low"
    }


@app.route("/")
def home():
    admin_path = os.getenv("ADMIN_PATH", "not set")
    return f"""
    <html>
    <head><title>Twix0514 - Polymarket Trade Tracker</title>
    <style>
        body {{ font-family: Arial, sans-serif; margin: 20px; background: #f5f5f5; }}
        h1 {{ color: #333; }}
        .info-box {{ background: white; padding: 15px; border-radius: 5px; margin: 10px 0; }}
        a {{ color: #007bff; text-decoration: none; margin-right: 15px; }}
        a:hover {{ text-decoration: underline; }}
        .code {{ font-family: monospace; background: #f0f0f0; padding: 2px 5px; border-radius: 3px; }}
    </style>
    </head>
    <body>
        <h1>Polymarket Trade Tracker</h1>
        <div class="info-box">
            <p><strong>ADMIN_PATH:</strong> {admin_path}</p>
            <p><strong>Working Directory:</strong> {Path.cwd()}</p>
        </div>
        <div class="info-box">
            <h2>Market Data Endpoints</h2>
            <a href="/api/markets">Markets</a>
            <a href="/api/events">Events</a>
        </div>
        <div class="info-box">
            <h2>Advanced Analysis Endpoints</h2>
            <a href="/api/analysis/arbitrage">Arbitrage Detection</a>
            <a href="/api/analysis/volatility">Volatility Analysis</a>
            <a href="/api/analysis/portfolio-risk">Portfolio Risk</a>
            <a href="/api/analysis/market-prices">Price Tracking</a>
        </div>
        <div class="info-box">
            <h2>User Data Endpoints</h2>
            <p>Use query parameter: <span class="code">?user=0x...</span></p>
            <a href="/api/user/positions?user=0x1a4197EdA8Ea1d684C0B8924ce672cc3e45AD7B5">User Positions</a>
            <a href="/api/user/trades?user=0x1a4197EdA8Ea1d684C0B8924ce672cc3e45AD7B5">User Trades</a>
        </div>
        <div class="info-box">
            <h2>System Endpoints</h2>
            <a href="/status">Status</a>
        </div>
    </body>
    </html>
    """


@app.route("/api/markets")
def api_markets():
    """Fetch and return Polymarket markets"""
    limit = request.args.get("limit", 10, type=int)
    markets = fetch_markets(limit)
    if isinstance(markets, list):
        for market in markets:
            track_price_history(market)
    return jsonify(markets)


@app.route("/api/events")
def api_events():
    """Fetch and return Polymarket events"""
    limit = request.args.get("limit", 5, type=int)
    events = fetch_events(limit)
    return jsonify(events)


@app.route("/api/analysis/arbitrage")
def api_arbitrage():
    """Detect arbitrage opportunities"""
    limit = request.args.get("limit", 20, type=int)
    markets = fetch_markets(limit)
    
    if isinstance(markets, dict) and "error" in markets:
        return jsonify(markets)
    
    arbitrage_opportunities = []
    for market in markets:
        arb = calculate_arbitrage(market)
        if arb and arb["magnitude"] > 0.01:  # Only report significant spreads
            arbitrage_opportunities.append(arb)
    
    return jsonify({
        "total_markets_analyzed": len(markets),
        "opportunities_found": len(arbitrage_opportunities),
        "opportunities": sorted(arbitrage_opportunities, key=lambda x: x["magnitude"], reverse=True)
    })


@app.route("/api/analysis/volatility")
def api_volatility():
    """Analyze market volatility"""
    volatility_data = []
    for market_id, history in price_history.items():
        if len(history) > 1:
            prices = [h["prices"][0] for h in history]
            vol = calculate_volatility([float(p) for p in prices])
            volatility_data.append({
                "market_id": market_id,
                "volatility": round(vol, 6),
                "samples": len(history),
                "last_update": history[-1]["timestamp"]
            })
    
    return jsonify({
        "total_markets_tracked": len(volatility_data),
        "markets": sorted(volatility_data, key=lambda x: x["volatility"], reverse=True)[:10]
    })


@app.route("/api/analysis/portfolio-risk")
def api_portfolio_risk():
    """Analyze portfolio risk"""
    limit = request.args.get("limit", 50, type=int)
    markets = fetch_markets(limit)
    
    if isinstance(markets, dict) and "error" in markets:
        return jsonify(markets)
    
    risk = calculate_portfolio_risk(markets)
    return jsonify(risk)


@app.route("/api/analysis/market-prices")
def api_market_prices():
    """Get current price tracking data"""
    market_id = request.args.get("market_id", type=int)
    
    if market_id:
        if market_id in price_history:
            return jsonify({
                "market_id": market_id,
                "history_count": len(price_history[market_id]),
                "latest": price_history[market_id][-1] if price_history[market_id] else None
            })
        else:
            return jsonify({"error": "Market not tracked yet"}), 404
    
    return jsonify({
        "tracked_markets": len(price_history),
        "market_ids": list(price_history.keys())
    })


@app.route("/api/user/positions")
def api_user_positions():
    """Fetch and return user positions"""
    user_address = request.args.get("user")
    if not user_address:
        return jsonify({"error": "user parameter is required"}), 400
    
    try:
        response = requests.get(f"{DATA_API_BASE}/positions", params={"user": user_address}, timeout=5)
        response.raise_for_status()
        return jsonify(response.json())
    except Exception as e:
        return jsonify({"error": str(e)}), 503


@app.route("/api/user/trades")
def api_user_trades():
    """Fetch and return user trades"""
    user_address = request.args.get("user")
    if not user_address:
        return jsonify({"error": "user parameter is required"}), 400
    
    try:
        response = requests.get(f"{DATA_API_BASE}/trades", params={"user": user_address}, timeout=5)
        response.raise_for_status()
        return jsonify(response.json())
    except Exception as e:
        return jsonify({"error": str(e)}), 503


@app.route("/status")
def status():
    return {
        "status": "ok",
        "admin_path": os.getenv("ADMIN_PATH", "not set"),
        "working_directory": str(Path.cwd()),
        "polymarket_integration": "enabled",
        "endpoints": {
            "markets": "/api/markets",
            "events": "/api/events",
            "arbitrage": "/api/analysis/arbitrage",
            "volatility": "/api/analysis/volatility",
            "portfolio_risk": "/api/analysis/portfolio-risk",
            "price_tracking": "/api/analysis/market-prices",
            "user_positions": "/api/user/positions?user=0x...",
            "user_trades": "/api/user/trades?user=0x..."
        }
    }


def parse_args():
    parser = argparse.ArgumentParser(description="Polymarket Trade Tracker with Advanced Analysis")
    parser.add_argument("--host", default="127.0.0.1", help="Host to bind to")
    parser.add_argument("--port", type=int, default=5000, help="Port to bind to")
    parser.add_argument("--debug", action="store_true", help="Enable debug mode")
    return parser.parse_args()


def main():
    args = parse_args()
    print(f"Starting Polymarket Trade Tracker with Advanced Analysis on {args.host}:{args.port}")
    app.run(host=args.host, port=args.port, debug=args.debug)


if __name__ == "__main__":
    main()
