"""
POLY//SERVER — static files + API proxy on http://localhost:3000
"""
import http.server, urllib.request, urllib.parse, os, sys, json

PORT = 3000
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

PROXY_HEADERS = {
    "User-Agent":  "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/124.0 Safari/537.36",
    "Accept":      "application/json",
    "Origin":      "https://polymarket.com",
    "Referer":     "https://polymarket.com/",
}
ALLOWED_HOSTS = {
    "gamma-api.polymarket.com",
    "data-api.polymarket.com",
    "clob.polymarket.com",
    "api.coingecko.com",
}

class Handler(http.server.SimpleHTTPRequestHandler):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, directory=BASE_DIR, **kwargs)

    def do_GET(self):
        parsed = urllib.parse.urlparse(self.path)
        if parsed.path == "/proxy":
            self._handle_proxy(parsed)
        else:
            super().do_GET()

    def _handle_proxy(self, parsed):
        qs = urllib.parse.parse_qs(parsed.query)
        target = qs.get("url", [""])[0]
        if not target:
            self._send_json_error(400, "Missing url param"); return

        target_host = urllib.parse.urlparse(target).hostname
        if target_host not in ALLOWED_HOSTS:
            self._send_json_error(403, f"Host not allowed: {target_host}"); return

        try:
            req = urllib.request.Request(target, headers=PROXY_HEADERS)
            with urllib.request.urlopen(req, timeout=12) as r:
                body = r.read()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
        except Exception as e:
            self._send_json_error(502, str(e))

    def _send_json_error(self, code, msg):
        body = json.dumps({"error": msg}).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, fmt, *args):
        if args and "/proxy" not in str(args[0]):
            super().log_message(fmt, *args)

if __name__ == "__main__":
    os.chdir(BASE_DIR)
    print(f"POLY//SERVER  http://localhost:{PORT}")
    srv = http.server.ThreadingHTTPServer(("", PORT), Handler)
    srv.serve_forever()
