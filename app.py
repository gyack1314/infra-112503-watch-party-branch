"""Convenience application factory for HTTP/JSON integrations."""

import os

from branch_room.server import create_handler
from branch_room.service import RoomEventService


def create_app(db_path: str | None = None):
    """Return ``(service, request_handler_class)``.

    The standard-library HTTP server is created by ``branch_room.server.serve``;
    this factory is useful for tests or embedding.
    """
    service = RoomEventService(db_path or os.environ.get("BRANCH_ROOM_DB", ":memory:"))
    return service, create_handler(service)


application = None  # Created by an explicit WSGI/ASGI adapter if embedded.
