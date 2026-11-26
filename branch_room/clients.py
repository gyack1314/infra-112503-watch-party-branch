"""Small role-specific facades over :class:`RoomEventService`.

These are optional.  Every caller can use ``service.handle(message)`` or the
HTTP/JSON adapter; these classes simply make the room, program-operator and
simulated-player interactions explicit.
"""

from __future__ import annotations

from typing import Any

from .service import RoomEventService


class _Client:
    def __init__(self, service: RoomEventService) -> None:
        self.service = service

    def _send(self, at: int, kind: str, **record: Any) -> dict[str, Any]:
        return self.service.handle({"at": at, "kind": kind, "record": record})


class ProgramClient(_Client):
    def announce_branch(self, at: int = 0, **record: Any) -> dict[str, Any]:
        return self._send(at, "branch", **record)

    update_branch = announce_branch

    def report_ready(self, segment: str, available: bool, at: int = 0) -> dict[str, Any]:
        return self._send(at, "ready", segment=segment, available=available)


class RoomClient(_Client):
    def __init__(self, service: RoomEventService, room: str) -> None:
        super().__init__(service)
        self.room = room

    def set_member(self, account: str, active: bool, at: int) -> dict[str, Any]:
        return self._send(at, "member", room=self.room, account=account, active=active)

    def start_round(
        self, node: str, round_id: str, close_at: int, at: int, **extra: Any
    ) -> dict[str, Any]:
        return self._send(
            at,
            "start",
            room=self.room,
            node=node,
            round=round_id,
            close_at=close_at,
            **extra,
        )

    def close_round(self, round_id: str, at: int) -> dict[str, Any]:
        return self._send(at, "close", round=round_id)

    def inspect(self) -> dict[str, Any]:
        return self.service.inspect_room(self.room)


class PlayerClient(_Client):
    def __init__(self, service: RoomEventService, room: str, account: str, device: str) -> None:
        super().__init__(service)
        self.room = room
        self.account = account
        self.device = device

    def ballot(self, round_id: str, option: str, at: int) -> dict[str, Any]:
        return self._send(
            at,
            "ballot",
            round=round_id,
            account=self.account,
            device=self.device,
            option=option,
        )

    def position(self, segment: str, playhead: int, at: int) -> dict[str, Any]:
        return self._send(
            at,
            "position",
            room=self.room,
            account=self.account,
            device=self.device,
            segment=segment,
            playhead=playhead,
        )

    def resume(self, segment: str, playhead: int, at: int) -> dict[str, Any]:
        return self._send(
            at,
            "resume",
            room=self.room,
            account=self.account,
            device=self.device,
            segment=segment,
            playhead=playhead,
        )

    def switched(self, round_id: str, segment: str, at: int, **extra: Any) -> dict[str, Any]:
        return self._send(
            at,
            "switched",
            round=round_id,
            account=self.account,
            device=self.device,
            segment=segment,
            **extra,
        )
