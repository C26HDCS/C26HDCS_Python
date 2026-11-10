"""Local-only HTTP endpoint for manual delivery trials."""

import json
from collections.abc import Callable
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer


MAX_BODY_BYTES = 1024 * 1024


def make_test_sink(
    port: int = 8765,
    on_receive: Callable[[dict], None] | None = None,
) -> ThreadingHTTPServer:
    class Handler(BaseHTTPRequestHandler):
        def do_POST(self) -> None:
            if self.path != "/dcs":
                self.send_error(404)
                return
            try:
                length = int(self.headers.get("Content-Length", ""))
            except ValueError:
                self.send_error(411, "Content-Length required")
                return
            if length < 1 or length > MAX_BODY_BYTES:
                self.send_error(413, "Invalid body length")
                return
            try:
                payload = json.loads(self.rfile.read(length))
            except (UnicodeDecodeError, json.JSONDecodeError):
                self.send_error(400, "Invalid JSON")
                return
            if not isinstance(payload, dict):
                self.send_error(400, "JSON object required")
                return

            if on_receive is None:
                print(json.dumps(payload, ensure_ascii=False), flush=True)
            else:
                on_receive(payload)
            body = b'{"received":true}'
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *_: object) -> None:
            pass

    class Server(ThreadingHTTPServer):
        allow_reuse_address = True
        daemon_threads = True

    return Server(("127.0.0.1", port), Handler)


def run_test_sink(port: int = 8765) -> None:
    with make_test_sink(port) as server:
        print(f"Test sink listening on http://127.0.0.1:{server.server_port}/dcs", flush=True)
        try:
            server.serve_forever()
        except KeyboardInterrupt:
            pass
