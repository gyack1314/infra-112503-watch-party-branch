"""Root-level HTTP server entry point: ``python3 server.py --port 8000``."""

from branch_room.server import main

if __name__ == "__main__":
    raise SystemExit(main())
