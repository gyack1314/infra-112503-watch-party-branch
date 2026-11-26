"""Compatibility import for environments expecting ``middleware.py``."""

from branch_room.service import *  # noqa: F401,F403
from branch_room.service import (  # noqa: F401
    RoomEventMiddleware,
    RoomEventService,
    ServiceError,
)
