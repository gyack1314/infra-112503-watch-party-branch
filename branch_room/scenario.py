"""Run simulation JSONL messages through the room service.

Example::

    python3 -m branch_room.scenario examples/fallback-and-late-join.jsonl \
        --db /tmp/branch-room.sqlite3
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any, Iterable

from .service import RoomEventService

ROOT = Path(__file__).resolve().parent.parent


def read_messages(path: Path, supported: set[str] | None = None) -> Iterable[dict[str, Any]]:
    supported = supported or set(json.loads((ROOT / "examples" / "message_types.json").read_text("utf-8")))
    previous = -1
    with path.open(encoding="utf-8") as handle:
        for line_number, raw in enumerate(handle, 1):
            if not raw.strip():
                continue
            row = json.loads(raw)
            if not isinstance(row, dict) or set(row) != {"at", "kind", "record"}:
                raise ValueError(f"{path.name} line {line_number}: expected at/kind/record")
            if type(row["at"]) is not int or row["at"] < 0 or row["at"] < previous:
                raise ValueError(f"{path.name} line {line_number}: invalid/non-monotonic at")
            if row["kind"] not in supported:
                raise ValueError(f"{path.name} line {line_number}: unknown kind {row['kind']}")
            if not isinstance(row["record"], dict):
                raise ValueError(f"{path.name} line {line_number}: record must be object")
            previous = row["at"]
            yield row


def run_file(
    service: RoomEventService,
    path: str | Path,
    source: str | None = None,
    print_all: bool = False,
) -> list[dict[str, Any]]:
    path = Path(path)
    source = source or path.name
    responses: list[dict[str, Any]] = []
    for line_number, message in enumerate(read_messages(path), 1):
        command_id = f"{source}:{line_number}"
        response = service.handle(message, command_id=command_id)
        responses.append(response)
    return responses


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run interactive-drama simulation records")
    parser.add_argument("paths", nargs="+", type=Path, help="JSONL scenario files")
    parser.add_argument("--db", default=":memory:", help="SQLite path; use a file for restartable service")
    parser.add_argument("--reset", action="store_true", help="delete an existing file database first")
    parser.add_argument("--all", action="store_true", help="print every command response")
    args = parser.parse_args(argv)

    if args.reset and args.db != ":memory:":
        for suffix in ("", "-wal", "-shm"):
            Path(args.db + suffix).unlink(missing_ok=True)

    service = RoomEventService(args.db)
    try:
        for path in args.paths:
            responses = run_file(service, path, print_all=args.all)
            if args.all:
                for response in responses:
                    print(json.dumps(response, ensure_ascii=False, indent=2))
            else:
                for response in responses:
                    if response.get("kind") == "inspect":
                        print(json.dumps(response["result"]["snapshot"], ensure_ascii=False, indent=2))
    finally:
        service.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
