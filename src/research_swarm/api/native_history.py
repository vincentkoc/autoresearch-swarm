"""Read-only native timeline server. Never initializes a controller or opens a database."""

from __future__ import annotations

import argparse
import json
from datetime import UTC, datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlsplit

from research_swarm.control.native_history import NativeHistory
from research_swarm.control.report_view import render_native_history_html

MAX_DOCUMENT_BYTES = 8 * 1024 * 1024


def read_json(path: Path) -> dict[str, object]:
    with path.open("rb") as stream:
        raw = stream.read(MAX_DOCUMENT_BYTES + 1)
    if len(raw) > MAX_DOCUMENT_BYTES:
        raise ValueError("Document exceeds native history limit")
    value = json.loads(raw)
    if not isinstance(value, dict):
        raise ValueError("Object document required")
    return value


def server(history_path: Path, status_path: Path, host: str, port: int) -> ThreadingHTTPServer:
    """The two local input paths are fixed at startup and never selected by HTTP clients."""
    NativeHistory.model_validate(read_json(history_path))

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:
            route = urlsplit(self.path)
            if route.path not in {"/", "/healthz", "/v1/native-history"}:
                self.send_error(404)
                return
            try:
                query = parse_qs(route.query, strict_parsing=True) if route.query else {}
            except ValueError:
                self.send_error(400)
                return
            if set(query) - {"offset", "limit"} or (route.path != "/v1/native-history" and query):
                self.send_error(400)
                return
            if any(len(values) != 1 for values in query.values()):
                self.send_error(400)
                return
            try:
                history = NativeHistory.model_validate(read_json(history_path))
                if route.path == "/":
                    status = read_json(status_path)
                    body = render_native_history_html(history, status).encode()
                    content_type = "text/html; charset=utf-8"
                else:
                    now = datetime.now(UTC)
                    captured = datetime.fromisoformat(history.captured_at.replace("Z", "+00:00"))
                    value: dict[str, object] = {
                        "readonly": True,
                        "captured_at": history.captured_at,
                        "age_seconds": max(0, (now - captured).total_seconds()),
                        "inventory": history.inventory,
                        "total_observations": len(history.observations),
                    }
                    if route.path == "/v1/native-history":
                        offset = int(query.get("offset", ["0"])[0])
                        limit = int(query.get("limit", ["100"])[0])
                        if not 0 <= offset <= len(history.observations) or not 1 <= limit <= 1000:
                            raise ValueError("Invalid page")
                        value["observations"] = [
                            row.model_dump()
                            for row in history.observations[offset : offset + limit]
                        ]
                        value["next_offset"] = (
                            offset + limit if offset + limit < len(history.observations) else None
                        )
                    body = json.dumps(value, allow_nan=False).encode()
                    content_type = "application/json"
            except (OSError, ValueError, TypeError):
                self.send_error(503, "Native history input unavailable")
                return
            self.send_response(200)
            self.send_header("Content-Type", content_type)
            self.send_header("Cache-Control", "no-store")
            self.send_header("X-Content-Type-Options", "nosniff")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, format: str, *args: object) -> None:
            pass

    return ThreadingHTTPServer((host, port), Handler)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--history", type=Path, required=True)
    parser.add_argument("--status", type=Path, required=True)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8765)
    args = parser.parse_args()
    with server(args.history, args.status, args.host, args.port) as listener:
        listener.serve_forever()


if __name__ == "__main__":
    main()
