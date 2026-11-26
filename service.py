"""Compatibility alias for ``room_service`` / ``branch_room.service``."""

from branch_room.service import *  # noqa: F401,F403
from branch_room.service import (  # noqa: F401
    IdempotencyConflict,
    RoomEventMiddleware,
    RoomEventService,
    ServiceError,
)

if __name__ == "__main__":
    from branch_room.scenario import main

    raise SystemExit(main())
