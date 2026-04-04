import argparse
import os
from pathlib import Path
from flask import Flask, jsonify, request
import requests
from datetime import datetime
from collections import defaultdict
import statistics
import plotly.graph_objects as go


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
            "magnitude": abs(arbitrage_spread) * 100
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


@app.route("/report")
def generate_report():
    """Generate comprehensive market analysis report with charts"""
    limit = request.args.get("limit", 50, type=int)
    markets = fetch_markets(limit)
    
    if isinstance(markets, dict) and "error" in markets:
        return "<h1>Error</h1><p>Failed to fetch markets</p>", 500
    
    # Arbitrage data
    arbitrage_data = []
    for m in markets:
        arb = calculate_arbitrage(m)
        if arb and arb["magnitude"] > 0.01:
            arbitrage_data.append(arb)
    
    # Price spread data
    price_spreads = []
    price_labels = []
    for m in markets:
        prices = [float(p) for p in m.get("outcomePrices", [0, 0])]
        if prices and len(prices) >= 2:
            spread = abs(prices[0] - prices[1])
            price_spreads.append(spread)
            price_labels.append(str(m.get("id", "?")))
    
    # Risk assessment
    risk_data = calculate_portfolio_risk(markets)
    
    # Create bar chart for price spreads
    fig1 = go.Figure()
    fig1.add_trace(go.Bar(
        x=price_labels[:20],
        y=price_spreads[:20],
        name="Price Spread",
        marker_color="lightblue"
    ))
    fig1.update_layout(
        title="Market Price Spreads (Top 20)",
        xaxis_title="Market ID",
        yaxis_title="Spread (Yes - No)",
        height=400
    )
    
    # Arbitrage opportunities chart
    fig2_html = ""
    if arbitrage_data:
        arb_ids = [str(a["market_id"]) for a in arbitrage_data[:10]]
        arb_mags = [a["magnitude"] for a in arbitrage_data[:10]]
        
        fig2 = go.Figure()
        fig2.add_trace(go.Bar(
            x=arb_ids,
            y=arb_mags,
            name="Arbitrage Spread (%)",
            marker_color="coral"
        ))
        fig2.update_layout(
            title="Top 10 Arbitrage Opportunities",
            xaxis_title="Market ID",
            yaxis_title="Spread (%)",
            height=400
        )
        fig2_html = f'<div class="chart-container"><div class="chart-title">⚡ Arbitrage Opportunities</div>{fig2.to_html(include_plotlyjs=False, div_id="chart2")}</div>'
    
    # Risk gauge
    fig3 = go.Figure(go.Indicator(
        mode="gauge+number",
        value=risk_data.get("risk_score", 0),
        domain={"x": [0, 1], "y": [0, 1]},
        title={"text": "Portfolio Risk Score"},
        gauge={
            "axis": {"range": [None, 100]},
            "bar": {"color": "darkblue"},
            "steps": [
                {"range": [0, 30], "color": "lightgreen"},
                {"range": [30, 70], "color": "lightyellow"},
                {"range": [70, 100], "color": "lightcoral"}
            ]
        }
    ))
    fig3.update_layout(height=400)
    
    # Generate HTML report
    chart1_html = fig1.to_html(include_plotlyjs="cdn", div_id="chart1")
    chart3_html = fig3.to_html(include_plotlyjs=False, div_id="chart3")
    
    arb_rows = ""
    for opp in sorted(arbitrage_data, key=lambda x: x["magnitude"], reverse=True)[:10]:
        arb_rows += f"<tr><td>{opp['market_id']}</td><td>{opp['type'].upper()}</td><td>{opp['magnitude']:.4f}%</td></tr>"
    
    risk_rows = ""
    for key, value in risk_data.items():
        risk_rows += f"<tr><td>{key.replace('_', ' ').title()}</td><td>{value}</td></tr>"
    
    report_html = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <title>Polymarket Trade Tracker - Analysis Report</title>
        <script src="https://cdn.plot.ly/plotly-latest.min.js"></script>
        <style>
            body {{ font-family: Arial, sans-serif; margin: 20px; background: #f5f5f5; }}
            h1 {{ color: #333; text-align: center; }}
            .summary {{ background: white; padding: 20px; border-radius: 5px; margin: 20px 0; }}
            .stat {{ display: inline-block; margin: 10px 20px; }}
            .stat-value {{ font-size: 24px; font-weight: bold; color: #007bff; }}
            .stat-label {{ font-size: 14px; color: #666; }}
            .chart-container {{ background: white; padding: 20px; border-radius: 5px; margin: 20px 0; box-shadow: 0 2px 4px rgba(0,0,0,0.1); }}
            .chart-title {{ font-size: 18px; font-weight: bold; margin-bottom: 10px; }}
            table {{ width: 100%; border-collapse: collapse; background: white; margin: 20px 0; }}
            th, td {{ padding: 10px; text-align: left; border-bottom: 1px solid #ddd; }}
            th {{ background-color: #007bff; color: white; }}
            tr:hover {{ background-color: #f5f5f5; }}
        </style>
    </head>
    <body>
        <h1>📊 Polymarket Trade Tracker - Analysis Report</h1>
        <p style="text-align: center; color: #666;">Generated: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}</p>
        
        <div class="summary">
            <h2>📈 Summary Statistics</h2>
            <div class="stat">
                <div class="stat-value">{len(markets)}</div>
                <div class="stat-label">Total Markets</div>
            </div>
            <div class="stat">
                <div class="stat-value">{len(arbitrage_data)}</div>
                <div class="stat-label">Arbitrage Opportunities</div>
            </div>
            <div class="stat">
                <div class="stat-value">{risk_data.get("risk_level", "N/A").upper()}</div>
                <div class="stat-label">Risk Level</div>
            </div>
            <div class="stat">
                <div class="stat-value">{risk_data.get("risk_score", 0):.1f}%</div>
                <div class="stat-label">Risk Score</div>
            </div>
        </div>
        
        <div class="chart-container">
            <div class="chart-title">💹 Market Price Spreads</div>
            {chart1_html}
        </div>
        
        {fig2_html}
        
        <div class="chart-container">
            <div class="chart-title">⚠️ Portfolio Risk Assessment</div>
            {chart3_html}
        </div>
        
        <div class="summary">
            <h2>🎯 Top Arbitrage Opportunities</h2>
            <table>
                <thead>
                    <tr><th>Market ID</th><th>Type</th><th>Spread (%)</th></tr>
                </thead>
                <tbody>
                    {arb_rows}
                </tbody>
            </table>
        </div>
        
        <div class="summary">
            <h2>📋 Risk Assessment Details</h2>
            <table>
                <tr><th>Metric</th><th>Value</th></tr>
                {risk_rows}
            </table>
        </div>
        
        <div style="text-align: center; margin: 40px 0; color: #666;">
            <p>Polymarket Trade Tracker | Powered by Flask & Plotly</p>
            <a href="/">← Back to Home</a> | <a href="/status">API Status</a>
        </div>
    </body>
    </html>
    """
    return report_html, 200, {"Content-Type": "text/html"}


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
    </style>
    </head>
    <body>
        <h1>Polymarket Trade Tracker</h1>
        <div class="info-box">
            <h2>📊 Reports & Analysis</h2>
            <a href="/report">📈 View Full Report</a>
            <a href="/api/analysis/arbitrage">⚡ Arbitrage Detection</a>
            <a href="/api/analysis/portfolio-risk">⚠️ Portfolio Risk</a>
        </div>
        <div class="info-box">
            <h2>API Endpoints</h2>
            <a href="/api/markets">Markets</a>
            <a href="/api/events">Events</a>
            <a href="/status">Status</a>
        </div>
    </body>
    </html>
    """


@app.route("/api/markets")
def api_markets():
    limit = request.args.get("limit", 10, type=int)
    markets = fetch_markets(limit)
    if isinstance(markets, list):
        for market in markets:
            track_price_history(market)
    return jsonify(markets)


@app.route("/api/events")
def api_events():
    limit = request.args.get("limit", 5, type=int)
    events = fetch_events(limit)
    return jsonify(events)


@app.route("/api/analysis/arbitrage")
def api_arbitrage():
    limit = request.args.get("limit", 20, type=int)
    markets = fetch_markets(limit)
    if isinstance(markets, dict) and "error" in markets:
        return jsonify(markets)
    arbitrage_opportunities = []
    for market in markets:
        arb = calculate_arbitrage(market)
        if arb and arb["magnitude"] > 0.01:
            arbitrage_opportunities.append(arb)
    return jsonify({
        "total_markets_analyzed": len(markets),
        "opportunities_found": len(arbitrage_opportunities),
        "opportunities": sorted(arbitrage_opportunities, key=lambda x: x["magnitude"], reverse=True)
    })


@app.route("/api/analysis/portfolio-risk")
def api_portfolio_risk():
    limit = request.args.get("limit", 50, type=int)
    markets = fetch_markets(limit)
    if isinstance(markets, dict) and "error" in markets:
        return jsonify(markets)
    risk = calculate_portfolio_risk(markets)
    return jsonify(risk)


@app.route("/status")
def status():
    return {
        "status": "ok",
        "admin_path": os.getenv("ADMIN_PATH", "not set"),
        "polymarket_integration": "enabled",
        "endpoints": {
            "report": "/report",
            "markets": "/api/markets",
            "events": "/api/events",
            "arbitrage": "/api/analysis/arbitrage",
            "portfolio_risk": "/api/analysis/portfolio-risk"
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
