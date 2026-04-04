import argparse
import os
from pathlib import Path
from flask import Flask


app = Flask(__name__)


@app.route("/")
def home():
    admin_path = os.getenv("ADMIN_PATH", "not set")
    return f"""
    <html>
    <head><title>Twix0514</title></head>
    <body style="font-family: Arial, sans-serif; margin: 20px;">
        <h1>Hello from Twix0514 App</h1>
        <p><strong>ADMIN_PATH:</strong> {admin_path}</p>
        <p><strong>Working Directory:</strong> {Path.cwd()}</p>
        <p><a href="/status">Check Status</a></p>
    </body>
    </html>
    """


@app.route("/status")
def status():
    admin_path = os.getenv("ADMIN_PATH", "not set")
    return {
        "status": "ok",
        "admin_path": admin_path,
        "working_directory": str(Path.cwd()),
    }


def parse_args():
    parser = argparse.ArgumentParser(description="Flask app for Twix0514 repository.")
    parser.add_argument("--host", default="127.0.0.1", help="Host to bind to")
    parser.add_argument("--port", type=int, default=5000, help="Port to bind to")
    parser.add_argument("--debug", action="store_true", help="Enable debug mode")
    return parser.parse_args()


def main():
    args = parse_args()
    print(f"Starting Flask app on {args.host}:{args.port}")
    app.run(host=args.host, port=args.port, debug=args.debug)


if __name__ == "__main__":
    main()
