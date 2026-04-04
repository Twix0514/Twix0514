import argparse
import os
from pathlib import Path
from flask import Flask, jsonify, request
import requests


app = Flask(__name__)

# Polymarket API endpoints
GAMMA_API_BASE = "https://gamma-api.polymarket.com"
CLOB_API_BASE = "https://clob.polymarket.com"
DATA_API_BASE = "https://data.polymarket.com"


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


def fetch_user_positions(user_address):
    """Fetch user positions from Polymarket Data API"""
    try:
        response = requests.get(f"{DATA_API_BASE}/positions", params={"user": user_address}, timeout=5)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        return {"error": str(e)}


def fetch_user_trades(user_address):
    """Fetch user trades from Polymarket Data API"""
    try:
        response = requests.get(f"{DATA_API_BASE}/trades", params={"user": user_address}, timeout=5)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        return {"error": str(e)}


def fetch_user_activity(user_address):
    """Fetch user activity from Polymarket Data API"""
    try:
        response = requests.get(f"{DATA_API_BASE}/activity", params={"user": user_address}, timeout=5)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        return {"error": str(e)}


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
            <h2>User Data Endpoints</h2>
            <p>Use query parameter: <span class="code">?user=0x...</span></p>
            <a href="/api/user/positions?user=0x1a4197EdA8Ea1d684C0B8924ce672cc3e45AD7B5">User Positions</a>
            <a href="/api/user/trades?user=0x1a4197EdA8Ea1d684C0B8924ce672cc3e45AD7B5">User Trades</a>
            <a href="/api/user/activity?user=0x1a4197EdA8Ea1d684C0B8924ce672cc3e45AD7B5">User Activity</a>
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
    return jsonify(markets)


@app.route("/api/events")
def api_events():
    """Fetch and return Polymarket events"""
    limit = request.args.get("limit", 5, type=int)
    events = fetch_events(limit)
    return jsonify(events)


@app.route("/api/user/positions")
def api_user_positions():
    """Fetch and return user positions"""
    user_address = request.args.get("user")
    if not user_address:
        return jsonify({"error": "user parameter is required"}), 400
    positions = fetch_user_positions(user_address)
    return jsonify(positions)


@app.route("/api/user/trades")
def api_user_trades():
    """Fetch and return user trades"""
    user_address = request.args.get("user")
    if not user_address:
        return jsonify({"error": "user parameter is required"}), 400
    trades = fetch_user_trades(user_address)
    return jsonify(trades)


@app.route("/api/user/activity")
def api_user_activity():
    """Fetch and return user activity"""
    user_address = request.args.get("user")
    if not user_address:
        return jsonify({"error": "user parameter is required"}), 400
    activity = fetch_user_activity(user_address)
    return jsonify(activity)


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
            "user_positions": "/api/user/positions?user=0x...",
            "user_trades": "/api/user/trades?user=0x...",
            "user_activity": "/api/user/activity?user=0x..."
        }
    }


def parse_args():
    parser = argparse.ArgumentParser(description="Polymarket Trade Tracker")
    parser.add_argument("--host", default="127.0.0.1", help="Host to bind to")
    parser.add_argument("--port", type=int, default=5000, help="Port to bind to")
    parser.add_argument("--debug", action="store_true", help="Enable debug mode")
    return parser.parse_args()


def main():
    args = parse_args()
    print(f"Starting Polymarket Trade Tracker on {args.host}:{args.port}")
    app.run(host=args.host, port=args.port, debug=args.debug)


if __name__ == "__main__":
    main()
