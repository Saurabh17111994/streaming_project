#!/usr/bin/env python3
"""Dev-only alert webhook receiver for OpenObserve.

Runs in OpenObserve's network namespace (compose: network_mode:
service:openobserve) so the alert destination URL http://localhost:9999/noop
is reachable from O2 and passes the loopback allowance of the SSRF guard
(ZO_SSRF_ALLOW_LOOPBACK=true, dev only).

Logs every request (method, path, headers, body) to stdout; replies 200.
Stdlib only. Never deploy in production.
"""
import json
from http.server import BaseHTTPRequestHandler, HTTPServer


class Handler(BaseHTTPRequestHandler):
    def _handle(self):
        length = int(self.headers.get("Content-Length") or 0)
        body = self.rfile.read(length).decode("utf-8", "replace") if length else ""
        print(
            f"{self.command} {self.path} "
            f"content-type={self.headers.get('Content-Type')} body={body[:2000]}",
            flush=True,
        )
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(json.dumps({"ok": True}).encode())

    do_POST = _handle
    do_PUT = _handle
    do_GET = _handle


if __name__ == "__main__":
    HTTPServer(("127.0.0.1", 9999), Handler).serve_forever()
