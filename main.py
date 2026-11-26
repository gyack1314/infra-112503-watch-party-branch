"""Unified entry point.

- Process simulations: ``python3 main.py scenario examples/...``
- Run HTTP service:    ``python3 main.py serve --db room.sqlite3``
"""

import sys

from branch_room.scenario import main as scenario_main
from branch_room.server import main as server_main


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "serve":
        sys.argv.pop(1)
        raise SystemExit(server_main())
    if len(sys.argv) > 1 and sys.argv[1] == "scenario":
        sys.argv.pop(1)
    raise SystemExit(scenario_main())
