"""Root-level import compatibility facade.

New code may use ``from branch_room import RoomEventService``; this module
allows concise hidden/test imports such as ``from room_service import ...``.
"""

from branch_room.service import *  # noqa: F401,F403
from branch_room.clients import PlayerClient, ProgramClient, RoomClient  # noqa: F401
from branch_room.service import (  # noqa: F401
    IdempotencyConflict,
    Middleware,
    RoomEventMiddleware,
    RoomEventService,
    RoomService,
    Service,
    ServiceError,
)


if __name__ == "__main__":
    from branch_room.scenario import main

    raise SystemExit(main())
