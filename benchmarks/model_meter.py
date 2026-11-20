"""Meter the local Ollama calls made by one serial benchmark campaign."""

import json
import threading
import time
from contextlib import contextmanager, suppress
from http.server import BaseHTTPRequestHandler, HTTPServer

import httpx


def verify_model(port, name, digest):
    with httpx.Client(base_url=f"http://127.0.0.1:{port}", trust_env=False, timeout=5) as client:
        tags = client.get("/api/tags")
        tags.raise_for_status()
        if not any(m["name"] == name and m["digest"] == digest for m in tags.json()["models"]):
            raise ValueError("Registered local Ollama model is unavailable")


@contextmanager
def measure_requests(port, ledger):
    """Forward generated prompts on loopback, force CPU and retain response counters."""
    with httpx.Client(base_url=f"http://127.0.0.1:{port}", trust_env=False, timeout=30) as client:

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, fmt, *args):
                pass

            def do_POST(self):
                if self.path != "/api/generate":
                    self.send_error(404)
                    return
                payload = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
                payload.setdefault("options", {})["num_gpu"] = 0
                start = time.monotonic()
                status = 502
                body = {"error": "local generation failed"}
                try:
                    response = client.post("/api/generate", json=payload)
                    status = response.status_code
                    body = response.json()
                except (httpx.HTTPError, ValueError) as exc:
                    body = {"error": str(exc)}
                resident_cpu = None
                if status == 200:
                    try:
                        models = client.get("/api/ps").json()["models"]
                        resident_cpu = any(
                            m["name"] == payload["model"] and m["size_vram"] == 0 for m in models
                        )
                    except (httpx.HTTPError, KeyError, ValueError):
                        resident_cpu = False
                record = {
                    "resident_cpu": resident_cpu,
                    "model": payload["model"],
                    "status": status,
                    "seconds": time.monotonic() - start,
                    "input_tokens": body.get("prompt_eval_count"),
                    "output_tokens": body.get("eval_count"),
                }
                with ledger.open("a", encoding="utf-8") as output:
                    output.write(json.dumps(record, allow_nan=False) + "\n")
                data = json.dumps(body).encode()
                self.send_response(status)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(data)))
                self.end_headers()
                with suppress(BrokenPipeError, ConnectionResetError):
                    self.wfile.write(data)

        with HTTPServer(("127.0.0.1", 0), Handler) as server:
            thread = threading.Thread(target=server.serve_forever, daemon=True)
            thread.start()
            try:
                yield server.server_port
            finally:
                server.shutdown()
                thread.join()
