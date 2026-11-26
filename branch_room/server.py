"""JSON HTTP adapter for room, program-event and simulator callers."""

from __future__ import annotations

import argparse
import json
import os
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any
from urllib.parse import parse_qs, urlparse

from .service import RoomEventService, ServiceError


def create_handler(service: RoomEventService):
    class Handler(BaseHTTPRequestHandler):
        server_version = "BranchRoom/1.0"

        def _send(self, status: int, payload: Any) -> None:
            body = json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def _body(self) -> dict[str, Any]:
            length = int(self.headers.get("Content-Length", "0"))
            if not length:
                return {}
            raw = self.rfile.read(length)
            value = json.loads(raw.decode("utf-8"))
            if not isinstance(value, dict):
                raise ValueError("JSON body must be an object")
            return value

        def log_message(self, fmt: str, *args: Any) -> None:
            return

        def do_GET(self) -> None:  # noqa: N802
            parsed = urlparse(self.path)
            parts = [p for p in parsed.path.split("/") if p]
            query = parse_qs(parsed.query)
            try:
                if parsed.path == "/health":
                    self._send(200, {"ok": True})
                elif len(parts) >= 2 and parts[0] == "v1" and parts[1] == "rooms" and len(parts) == 3:
                    self._send(200, service.inspect_room(parts[2]))
                elif len(parts) >= 2 and parts[0] == "v1" and parts[1] == "rounds" and len(parts) == 3:
                    self._send(200, service.inspect_round(parts[2]))
                elif len(parts) == 4 and parts[0] == "v1" and parts[1] == "accounts" and parts[3] == "progress":
                    room = query.get("room", [None])[0]
                    self._send(200, service.account_progress(parts[2], room))
                else:
                    self._send(404, {"ok": False, "error": "not_found"})
            except ServiceError as error:
                status = 404 if error.code in {"unknown_room", "unknown_round"} else 400
                self._send(status, {"ok": False, "error": {"code": error.code, "message": str(error)}})

        def do_POST(self) -> None:  # noqa: N802
            parsed = urlparse(self.path)
            parts = [p for p in parsed.path.split("/") if p]
            try:
                body = self._body()
                if parsed.path in {"/v1/events", "/event"}:
                    message = body
                elif len(parts) == 2 and parts[0] == "v1":
                    kind = parts[1]
                    at = int(body.get("at", 0))
                    record = body.get("record", {k: v for k, v in body.items() if k not in {"at", "command_id", "id"}})
                    message = {"at": at, "kind": kind, "record": record}
                else:
                    self._send(404, {"ok": False, "error": "not_found"})
                    return
                response = service.handle(message, command_id=body.get("command_id") or body.get("id"))
                self._send(200 if response.get("ok") else 422, response)
            except (ValueError, TypeError) as error:
                self._send(400, {"ok": False, "error": {"code": "bad_request", "message": str(error)}})
            except ServiceError as error:
                self._send(409, {"ok": False, "error": {"code": error.code, "message": str(error)}})

    return Handler


def serve(db_path: str = "branch_room.sqlite3", host: str = "127.0.0.1", port: int | None = None) -> None:
    if port is None:
        port = int(os.environ.get("PORT", "8000"))
    service = RoomEventService(db_path)
    server = ThreadingHTTPServer((host, port), create_handler(service))
    print(f"branch-room service on http://{host}:{port} using {db_path}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
        service.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Run branch-room HTTP service")
    parser.add_argument("--db", default="branch_room.sqlite3")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=int(os.environ.get("PORT", "8000")))
    args = parser.parse_args()
    serve(args.db, args.host, args.port)


if __name__ == "__main__":
    main()
