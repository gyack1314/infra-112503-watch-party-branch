"""Persistent room/branch voting middleware.

The public entry point is :class:`RoomEventService`.  It accepts the same
``{"at", "kind", "record"}`` messages used by the simulation JSONL files and
can restart from a SQLite database without rewriting committed votes or
switch decisions.
"""

from __future__ import annotations

import copy
import hashlib
import json
import sqlite3
import threading
import uuid
from pathlib import Path
from typing import Any, Iterable

SUPPORTED_KINDS = {
    "ballot",
    "branch",
    "close",
    "inspect",
    "member",
    "position",
    "ready",
    "resume",
    "start",
    "switched",
}


class ServiceError(ValueError):
    """A command rejected by the domain rules."""

    def __init__(self, code: str, message: str, **details: Any) -> None:
        super().__init__(message)
        self.code = code
        self.details = details


class IdempotencyConflict(ServiceError):
    def __init__(self, command_id: str) -> None:
        super().__init__(
            "idempotency_conflict",
            f"command_id {command_id!r} was already used with a different payload",
            command_id=command_id,
        )


def _fresh_state() -> dict[str, Any]:
    return {
        "seq": 0,
        "branches": {},
        "availability": {},
        "rooms": {},
        "rounds": {},
    }


def _json_dumps(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _payload_hash(message: dict[str, Any]) -> str:
    record = message.get("record")
    if isinstance(record, dict):
        record = {
            key: value
            for key, value in record.items()
            if key not in {"id", "command_id", "request_id", "receipt_id", "receipt", "idempotency_key"}
        }
    payload = {"at": message.get("at"), "kind": message.get("kind"), "record": record}
    return hashlib.sha256(_json_dumps(payload).encode("utf-8")).hexdigest()


def _extract_command_id(message: dict[str, Any], explicit: str | None) -> str | None:
    """Find an idempotency key in common request/receipt shapes."""
    if explicit:
        return explicit
    for key in ("command_id", "id", "request_id", "idempotency_key"):
        value = message.get(key)
        if value:
            return str(value)
    receipt = message.get("receipt")
    if isinstance(receipt, dict):
        for key in ("receipt_id", "command_id", "event_id", "id"):
            if receipt.get(key):
                return str(receipt[key])
    elif isinstance(receipt, str) and receipt:
        return receipt
    record = message.get("record")
    if isinstance(record, dict):
        for key in ("command_id", "id", "request_id", "receipt_id", "idempotency_key"):
            value = record.get(key)
            if value:
                return str(value)
        receipt = record.get("receipt")
        if isinstance(receipt, dict):
            for key in ("receipt_id", "command_id", "event_id", "id"):
                value = receipt.get(key)
                if value:
                    return str(value)
        elif isinstance(receipt, str) and receipt:
            return receipt
    return None


def device_key(account: str, device: str) -> str:
    return f"{account}::{device}"


def _as_nonneg_number(value: Any, field: str) -> int | float:
    if isinstance(value, bool) or not isinstance(value, (int, float)) or value < 0:
        raise ServiceError("invalid_field", f"{field} must be a non-negative number")
    return value


def _required(record: dict[str, Any], field: str) -> Any:
    if field not in record or record[field] is None:
        raise ServiceError("missing_field", f"record.{field} is required", field=field)
    return record[field]


class RoomEventService:
    """Event-sourced room service with a SQLite state/command ledger."""

    def __init__(self, db_path: str | Path = ":memory:", **path_aliases: Any) -> None:
        for alias in ("path", "database", "db_path_kw", "filename", "storage_path"):
            if alias in path_aliases and path_aliases[alias]:
                db_path = path_aliases.pop(alias)
        if "db" in path_aliases and path_aliases["db"]:
            db_path = path_aliases.pop("db")
        if path_aliases:
            raise TypeError(f"unexpected keyword arguments: {sorted(path_aliases)}")
        self.db_path = str(db_path)
        self.lock = threading.RLock()
        self._open_db()

    def _open_db(self) -> None:
        self.db = sqlite3.connect(self.db_path, check_same_thread=False)
        self.db.row_factory = sqlite3.Row
        self.db.execute("PRAGMA foreign_keys=ON")
        if self.db_path != ":memory:":
            self.db.execute("PRAGMA journal_mode=WAL")
        self.db.execute(
            """
            CREATE TABLE IF NOT EXISTS state (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                data TEXT NOT NULL,
                updated_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
        self.db.execute(
            """
            CREATE TABLE IF NOT EXISTS commands (
                command_id TEXT PRIMARY KEY,
                seq INTEGER NOT NULL,
                at INTEGER,
                kind TEXT NOT NULL,
                payload TEXT NOT NULL,
                payload_hash TEXT NOT NULL,
                response TEXT NOT NULL,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
        self.db.execute(
            """
            CREATE TABLE IF NOT EXISTS domain_events (
                event_id TEXT PRIMARY KEY,
                seq INTEGER NOT NULL,
                command_id TEXT NOT NULL,
                at INTEGER,
                type TEXT NOT NULL,
                room_id TEXT,
                round_id TEXT,
                payload TEXT NOT NULL
            )
            """
        )
        row = self.db.execute("SELECT data FROM state WHERE id=1").fetchone()
        if row:
            self.state = json.loads(row["data"])
        else:
            self.state = _fresh_state()
            self.db.execute(
                "INSERT INTO state(id, data) VALUES (1, ?)",
                (json.dumps(self.state, ensure_ascii=False),),
            )
            self.db.commit()

    def restart(self) -> "RoomEventService":
        """Close and reopen a file database, reloading committed state."""
        if self.db_path == ":memory:":
            raise ServiceError("memory_database", "restart requires a file-backed database")
        with self.lock:
            self.db.close()
            self._open_db()
        return self

    # ------------------------------------------------------------------
    # Public callable surface
    # ------------------------------------------------------------------

    def close(self, message: dict[str, Any] | None = None, **kwargs: Any) -> Any:  # type: ignore[override]
        """Close the database, or act as the ``close`` message kind.

        ``service.close()`` releases the SQLite connection.  Calling
        ``service.close({"round": "q"}, at=5)`` or
        ``service.close(round="q", at=5)`` closes ballot collection instead.
        """
        if message is None and not kwargs:
            with self.lock:
                self.db.close()
            return None

        at = int(kwargs.pop("at", 0))
        if message is not None and {"at", "kind", "record"} <= set(message):
            return self.handle(message)
        record: dict[str, Any]
        if message is not None:
            record = dict(message)
            record.update(kwargs.pop("record", {}) if isinstance(kwargs.get("record"), dict) else {})
        else:
            record = {}
        explicit_record = kwargs.pop("record", None)
        if isinstance(explicit_record, dict):
            record.update(explicit_record)
        record.update(kwargs)
        return self.submit("close", record, at=at)

    close_connection = close

    def __enter__(self) -> "RoomEventService":
        return self

    def __exit__(self, *exc: Any) -> None:
        self.close()

    def submit(
        self,
        kind: str,
        record: dict[str, Any] | None = None,
        at: int = 0,
        command_id: str | None = None,
    ) -> dict[str, Any]:
        return self.handle(
            {"at": at, "kind": kind, "record": record or {}},
            command_id=command_id,
        )

    # Friendly aliases are attached after ``handle`` is defined below.

    def __getattr__(self, name: str):
        """Allow calls such as ``service.ballot(record, at=3)``.

        Explicit methods/attributes take precedence; this factory only handles
        message-kind names and ``on_<kind>`` aliases.
        """
        kind = name[3:] if name.startswith("on_") else name
        if kind not in SUPPORTED_KINDS:
            raise AttributeError(name)

        def caller(record: dict[str, Any] | None = None, at: int | None = None, **fields: Any):
            if isinstance(record, dict) and {"at", "kind", "record"} <= set(record):
                return self.handle(record)
            if record is None:
                record = fields
            elif fields:
                merged = dict(record)
                merged.update(fields)
                record = merged
            if at is None:
                at = int(record.get("at", 0)) if isinstance(record, dict) else 0
            return self.submit(kind, record, at=at)

        caller.__name__ = name
        return caller

    def handle(
        self,
        message: dict[str, Any] | None = None,
        command_id: str | None = None,
        **kwargs: Any,
    ) -> dict[str, Any]:
        if message is None:
            message = kwargs
        elif kwargs:
            merged = dict(message)
            merged.update(kwargs)
            message = merged
        if not isinstance(message, dict):
            raise ServiceError("invalid_message", "message must be an object")
        at = message.get("at")
        kind = message.get("kind")
        record = message.get("record")
        if type(at) is not int or at < 0:
            raise ServiceError("invalid_at", "at must be a non-negative integer")
        if kind not in SUPPORTED_KINDS:
            raise ServiceError("unknown_kind", f"unsupported kind: {kind!r}")
        if not isinstance(record, dict):
            raise ServiceError("invalid_record", "record must be an object")

        command_id = _extract_command_id(message, command_id)
        digest = _payload_hash(message)

        with self.lock:
            if command_id:
                previous = self.db.execute(
                    "SELECT payload_hash, response FROM commands WHERE command_id=?",
                    (command_id,),
                ).fetchone()
                if previous:
                    if previous["payload_hash"] != digest:
                        raise IdempotencyConflict(str(command_id))
                    response = json.loads(previous["response"])
                    response["idempotent_replay"] = True
                    return response

            seq = int(self.state.get("seq", 0)) + 1
            if command_id is None:
                command_id = f"cmd-{seq:08d}-{uuid.uuid4().hex[:12]}"

            working = copy.deepcopy(self.state)
            working["seq"] = seq
            events: list[dict[str, Any]] = []
            try:
                result = self._dispatch(working, at, kind, record, command_id, seq, events)
                accepted = bool(result.get("accepted", True)) if isinstance(result, dict) else True
                response: dict[str, Any] = {
                    "ok": accepted,
                    "accepted": accepted,
                    "command_id": command_id,
                    "seq": seq,
                    "at": at,
                    "kind": kind,
                    "result": result,
                    "events": events,
                }
            except ServiceError as error:
                result = {
                    "accepted": False,
                    "error": {
                        "code": error.code,
                        "message": str(error),
                        "details": error.details,
                    },
                }
                response = {
                    "ok": False,
                    "accepted": False,
                    "command_id": command_id,
                    "seq": seq,
                    "at": at,
                    "kind": kind,
                    "result": result,
                    "events": [],
                }
                # Rejected commands still advance the local sequence, but do
                # not replace domain state.
                self.state["seq"] = seq
                self._persist_command(command_id, seq, at, kind, message, digest, response, [])
                self._save_state()
                self.db.commit()
                return response

            self.state = working
            for index, event in enumerate(events):
                event["event_id"] = event.get("event_id") or f"evt-{seq:08d}-{index}"
            response["receipt"] = {
                "receipt_id": command_id,
                "command_id": command_id,
                "seq": seq,
                "kind": kind,
                "at": at,
                "revision": (result.get("ballot") or {}).get("revision")
                if isinstance(result, dict)
                else None,
                "immutable": kind in {"close", "switched"},
            }
            self._persist_command(command_id, seq, at, kind, message, digest, response, events)
            self._save_state()
            for event in events:
                self.db.execute(
                    """INSERT INTO domain_events
                       (event_id, seq, command_id, at, type, room_id, round_id, payload)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                    (
                        event["event_id"],
                        seq,
                        command_id,
                        at,
                        event.get("type"),
                        event.get("room_id"),
                        event.get("round_id"),
                        _json_dumps(event.get("payload", {})),
                    ),
                )
            self.db.commit()
            return response

    handle_message = handle_event = process_message = process = dispatch = handle

    def __call__(self, message: dict[str, Any], command_id: str | None = None) -> dict[str, Any]:
        return self.handle(message, command_id=command_id)

    def _persist_command(
        self,
        command_id: str,
        seq: int,
        at: int,
        kind: str,
        message: dict[str, Any],
        digest: str,
        response: dict[str, Any],
        events: list[dict[str, Any]],
    ) -> None:
        self.db.execute(
            """INSERT OR REPLACE INTO commands
               (command_id, seq, at, kind, payload, payload_hash, response)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (
                command_id,
                seq,
                at,
                kind,
                _json_dumps(message),
                digest,
                json.dumps(response, ensure_ascii=False),
            ),
        )

    def _save_state(self) -> None:
        self.db.execute(
            "INSERT INTO state(id, data) VALUES (1, ?) "
            "ON CONFLICT(id) DO UPDATE SET data=excluded.data, "
            "updated_at=CURRENT_TIMESTAMP",
            (json.dumps(self.state, ensure_ascii=False),),
        )

    # ------------------------------------------------------------------
    # Dispatch
    # ------------------------------------------------------------------

    def _dispatch(
        self,
        s: dict[str, Any],
        at: int,
        kind: str,
        rec: dict[str, Any],
        cid: str,
        seq: int,
        events: list[dict[str, Any]],
    ) -> dict[str, Any]:
        if kind == "branch":
            return self._branch(s, at, rec, cid, events)
        if kind == "ready":
            return self._ready(s, at, rec, cid, events)
        if kind == "member":
            return self._member(s, at, rec)
        if kind == "start":
            return self._start(s, at, rec, cid, events)
        if kind == "ballot":
            return self._ballot(s, at, rec, cid, events)
        if kind == "close":
            return self._close(s, at, rec, cid, events)
        if kind == "position":
            return self._position(s, at, rec, claim_relocation=False)
        if kind == "resume":
            return self._resume(s, at, rec, events)
        if kind == "switched":
            return self._switched(s, at, rec, cid, events)
        if kind == "inspect":
            return {"accepted": True, "snapshot": self.inspect(rec)}
        raise ServiceError("unknown_kind", kind)

    # ------------------------------------------------------------------
    # Program and membership
    # ------------------------------------------------------------------

    def _branch(
        self,
        s: dict[str, Any],
        at: int,
        rec: dict[str, Any],
        cid: str,
        events: list[dict[str, Any]],
    ) -> dict[str, Any]:
        node = str(_required(rec, "node"))
        source = str(_required(rec, "segment"))
        options = _required(rec, "options")
        fallback = str(rec.get("fallback") or _required(rec, "fallback_option"))
        order = rec.get("order", [])
        if not isinstance(options, dict) or not options:
            raise ServiceError("invalid_branch", "options must be a non-empty object")
        options = {str(k): str(v) for k, v in options.items()}
        if fallback not in options:
            raise ServiceError("invalid_branch", "fallback must name a published option")
        if not isinstance(order, list):
            raise ServiceError("invalid_branch", "order must be a list when provided")
        published_order = [str(x) for x in order if str(x) in options]
        # Be tolerant of a partial preview order: unpublished/unknown entries
        # do not participate, and unlisted options get deterministic order.
        order = published_order + sorted(set(options) - set(published_order))
        cut = rec.get("cut_playhead", rec.get("cut"))
        branch_point = rec.get("branch_playhead")
        if cut is not None:
            cut = _as_nonneg_number(cut, "cut_playhead")
        if branch_point is not None:
            branch_point = _as_nonneg_number(branch_point, "branch_playhead")
        playable_start = rec.get("playable_start", 0)
        playable_start = _as_nonneg_number(playable_start, "playable_start")

        definition = {
            "node": node,
            "source_segment": source,
            "segment": source,
            "options": options,
            "fallback_option": fallback,
            "fallback": fallback,
            "order": order,
            "cut_playhead": cut,
            "branch_playhead": branch_point,
            "playable_start": playable_start,
            "version": 1,
            "updated_at": at,
            "updated_by": cid,
        }
        entry = s["branches"].get(node)
        if entry is None:
            entry = {"current": definition, "versions": [copy.deepcopy(definition)], "round_ids": []}
            s["branches"][node] = entry
        else:
            definition["version"] = int(entry["current"].get("version", 1)) + 1
            entry["current"] = definition
            entry["versions"].append(copy.deepcopy(definition))
        events.append(
            {
                "type": "branch_updated",
                "at": at,
                "payload": {
                    "node": node,
                    "version": definition["version"],
                    "opened_round_ids": list(entry["round_ids"]),
                },
            }
        )
        return {"accepted": True, "branch": copy.deepcopy(definition)}

    def _ready(
        self,
        s: dict[str, Any],
        at: int,
        rec: dict[str, Any],
        cid: str,
        events: list[dict[str, Any]],
    ) -> dict[str, Any]:
        segment = str(_required(rec, "segment"))
        available = bool(_required(rec, "available"))
        s["availability"][segment] = {
            "segment": segment,
            "available": available,
            "at": at,
            "command_id": cid,
        }
        committed: list[dict[str, Any]] = []
        for rnd in s["rounds"].values():
            if rnd.get("switch_decision") or rnd.get("status") != "closed":
                continue
            before = rnd.get("switch_decision")
            self._resolve_route(s, rnd, at, cid, events)
            if not before and rnd.get("switch_decision"):
                committed.append(rnd["id"])
        return {"accepted": True, "segment": segment, "available": available, "committed_rounds": committed}

    def _member(self, s: dict[str, Any], at: int, rec: dict[str, Any]) -> dict[str, Any]:
        room_id = str(_required(rec, "room"))
        account = str(_required(rec, "account"))
        active = bool(_required(rec, "active"))
        room = self._ensure_room(s, room_id, at)
        existing = room["members"].get(account)
        member = existing or {
            "account": account,
            "active": active,
            "first_seen_at": at,
            "history": [],
        }
        if member.get("active") != active:
            member["history"].append({"active": member.get("active"), "at": at})
        member["active"] = active
        member["updated_at"] = at
        room["members"][account] = member
        return {"accepted": True, "room": room_id, "account": account, "active": active}

    # ------------------------------------------------------------------
    # Rounds and ballots
    # ------------------------------------------------------------------

    def _start(
        self,
        s: dict[str, Any],
        at: int,
        rec: dict[str, Any],
        cid: str,
        events: list[dict[str, Any]],
    ) -> dict[str, Any]:
        room_id = str(_required(rec, "room"))
        node = str(_required(rec, "node"))
        round_id = str(_required(rec, "round"))
        close_at = _required(rec, "close_at")
        if type(close_at) is not int or close_at < 0:
            raise ServiceError("invalid_close_at", "close_at must be a non-negative integer")
        if round_id in s["rounds"]:
            raise ServiceError("round_exists", f"round {round_id!r} already exists", round=round_id)
        branch_entry = s["branches"].get(node)
        if not branch_entry:
            raise ServiceError("unknown_branch", f"branch node {node!r} has not been announced", node=node)

        room = self._ensure_room(s, room_id, at)
        snapshot = copy.deepcopy(branch_entry["current"])
        accounts = sorted(a for a, m in room["members"].items() if m.get("active"))
        room["next_seq"] = int(room.get("next_seq", 0)) + 1
        seq = room["next_seq"]
        rnd: dict[str, Any] = {
            "id": round_id,
            "round_id": round_id,
            "room": room_id,
            "node": node,
            "seq": seq,
            "opened_at": at,
            "start_command_id": cid,
            "close_at": close_at,
            "status": "open",
            "accepting_ballots": True,
            "branch_snapshot": snapshot,
            "eligible_accounts": accounts,
            "eligible_count": len(accounts),
            "ballots": {},
            "ballot_history": [],
            "rejected_ballots": [],
            "invitations": {},
            "tally": None,
            "close": None,
            "original_winner_option": None,
            "effective_option": None,
            "diverted": False,
            "diversion": None,
            "switch_status": "open",
            "switch_decision": None,
            "waiting": None,
        }
        s["rounds"][round_id] = rnd
        room["round_ids"].append(round_id)
        branch_entry["round_ids"].append(round_id)

        for key, dev in room["devices"].items():
            if dev["account"] not in accounts:
                continue
            access = self._access_for_round(s, room, rnd, dev)
            rnd["invitations"][key] = {
                "player_key": key,
                "account": dev["account"],
                "device": dev["device"],
                "invited": access["allowed"],
                "access_status": access["status"],
                "reason": access.get("reason"),
                "detail": access.get("detail"),
                "at": at,
            }
        events.append(
            {
                "type": "round_opened",
                "at": at,
                "room_id": room_id,
                "round_id": round_id,
                "payload": {
                    "eligible_accounts": accounts,
                    "eligible_count": len(accounts),
                    "close_at": close_at,
                    "branch_version": snapshot.get("version"),
                },
            }
        )
        return {
            "accepted": True,
            "round": round_id,
            "room": room_id,
            "eligible_accounts": accounts,
            "eligible_count": len(accounts),
            "invitations": list(rnd["invitations"].values()),
        }

    def _ballot(
        self,
        s: dict[str, Any],
        at: int,
        rec: dict[str, Any],
        cid: str,
        events: list[dict[str, Any]],
    ) -> dict[str, Any]:
        round_id = str(_required(rec, "round"))
        account = str(_required(rec, "account"))
        device = str(_required(rec, "device"))
        option = str(rec.get("option") or rec.get("choice") or _required(rec, "option"))
        rnd = s["rounds"].get(round_id)
        if not rnd:
            return self._reject_ballot(s, None, at, rec, cid, "unknown_round", "round has not been opened")
        room = s["rooms"][rnd["room"]]
        key = device_key(account, device)
        dev = self._ensure_device(room, account, device, at)

        if rnd["status"] != "open":
            return self._reject_ballot(s, rnd, at, rec, cid, "round_closed", "voting for this round is committed")
        if at > int(rnd["close_at"]):
            return self._reject_ballot(s, rnd, at, rec, cid, "late_ballot", "ballot arrived after close_at")
        if account not in rnd["eligible_accounts"]:
            return self._reject_ballot(
                s, rnd, at, rec, cid, "not_eligible", "account was not an active member when the round opened"
            )
        options = rnd["branch_snapshot"]["options"]
        if option not in options:
            return self._reject_ballot(s, rnd, at, rec, cid, "invalid_option", "option is not published for this round")

        access = self._access_for_round(s, room, rnd, dev)
        if not access["allowed"]:
            rnd.setdefault("invitations", {})[key] = {
                "player_key": key,
                "account": account,
                "device": device,
                "invited": False,
                "access_status": access.get("status"),
                "reason": access.get("reason"),
                "detail": access.get("detail"),
                "at": at,
            }
            result = self._reject_ballot(
                s,
                rnd,
                at,
                rec,
                cid,
                "device_without_usable_position",
                "device must obtain a usable relocation/access point before its next-round ballot can count",
                access=access,
            )
            return result

        previous = rnd["ballots"].get(account)
        if previous and previous.get("option") == option and previous.get("device") == device:
            return {"accepted": True, "counted": True, "unchanged": True, "ballot": copy.deepcopy(previous)}

        ballot = {
            "round": round_id,
            "account": account,
            "device": device,
            "player_key": key,
            "option": option,
            "at": at,
            "command_id": cid,
            "revision": len([b for b in rnd["ballot_history"] if b["account"] == account]) + 1,
            "previous_option": previous.get("option") if previous else None,
            "changed": bool(previous and previous.get("option") != option),
        }
        rnd["ballots"][account] = ballot
        rnd["ballot_history"].append(copy.deepcopy(ballot))
        dev["last_ballot"] = {"round": round_id, "option": option, "at": at}
        invitation = rnd["invitations"].get(key)
        if not invitation:
            invitation = {
                "player_key": key,
                "account": account,
                "device": device,
                "at": at,
            }
            rnd["invitations"][key] = invitation
        invitation["invited"] = True
        invitation["access_status"] = access["status"]
        invitation["reason"] = access.get("reason")
        invitation["ballot_counted"] = True
        events.append(
            {
                "type": "ballot_accepted",
                "at": at,
                "room_id": rnd["room"],
                "round_id": round_id,
                "payload": {
                    "account": account,
                    "device": device,
                    "option": option,
                    "changed": ballot["changed"],
                    "revision": ballot["revision"],
                },
            }
        )
        return {"accepted": True, "counted": True, "ballot": copy.deepcopy(ballot), "vote_weight": 1}

    def _reject_ballot(
        self,
        s: dict[str, Any],
        rnd: dict[str, Any] | None,
        at: int,
        rec: dict[str, Any],
        cid: str,
        code: str,
        message: str,
        **extra: Any,
    ) -> dict[str, Any]:
        rejection = {
            "round": rec.get("round"),
            "account": rec.get("account"),
            "device": rec.get("device"),
            "option": rec.get("option"),
            "at": at,
            "command_id": cid,
            "code": code,
            "message": message,
            **extra,
        }
        if rnd is not None:
            rnd["rejected_ballots"].append(rejection)
        return {"accepted": False, "counted": False, "error": {"code": code, "message": message, **extra}}

    def _close(
        self,
        s: dict[str, Any],
        at: int,
        rec: dict[str, Any],
        cid: str,
        events: list[dict[str, Any]],
    ) -> dict[str, Any]:
        round_id = str(_required(rec, "round"))
        rnd = s["rounds"].get(round_id)
        if not rnd:
            raise ServiceError("unknown_round", f"round {round_id!r} has not been opened")
        if rnd["status"] != "open":
            return {
                "accepted": True,
                "already_closed": True,
                "round": round_id,
                "tally": rnd.get("tally"),
                "switch_decision": rnd.get("switch_decision"),
            }
        if at < int(rnd["close_at"]):
            raise ServiceError(
                "close_before_deadline",
                "close is only valid at or after close_at",
                close_at=rnd["close_at"],
                at=at,
            )

        rnd["status"] = "closed"
        rnd["accepting_ballots"] = False
        rnd["closed_at"] = at
        rnd["close_command_id"] = cid

        snap = rnd["branch_snapshot"]
        counts = {opt: 0 for opt in snap["options"]}
        for ballot in rnd["ballots"].values():
            counts[ballot["option"]] = counts.get(ballot["option"], 0) + 1
        max_votes = max(counts.values()) if counts else 0
        winners = [opt for opt in snap["order"] if counts.get(opt, 0) == max_votes]
        if not winners:
            winners = sorted(opt for opt, n in counts.items() if n == max_votes)
        winner = winners[0]
        tally = {
            "counts": counts,
            "total_votes": len(rnd["ballots"]),
            "eligible_count": rnd["eligible_count"],
            "participation": (len(rnd["ballots"]) / rnd["eligible_count"]) if rnd["eligible_count"] else 0.0,
            "tie": sum(1 for n in counts.values() if n == max_votes) > 1,
            "tie_order": snap["order"],
            "winner_option": winner,
            "winner_target_segment": snap["options"][winner],
        }
        rnd["tally"] = tally
        rnd["original_winner_option"] = winner
        rnd["effective_option"] = winner
        rnd["close"] = {"at": at, "command_id": cid, "tally": copy.deepcopy(tally)}
        events.append(
            {
                "type": "vote_collection_committed",
                "at": at,
                "room_id": rnd["room"],
                "round_id": round_id,
                "payload": copy.deepcopy(tally),
            }
        )
        self._resolve_route(s, rnd, at, cid, events)
        return {
            "accepted": True,
            "round": round_id,
            "tally": tally,
            "diverted": rnd["diverted"],
            "diversion": rnd["diversion"],
            "effective_option": rnd["effective_option"],
            "switch_status": rnd["switch_status"],
            "switch_decision": rnd["switch_decision"],
        }

    def _resolve_route(
        self,
        s: dict[str, Any],
        rnd: dict[str, Any],
        at: int,
        cid: str,
        events: list[dict[str, Any]],
    ) -> None:
        if rnd.get("switch_decision"):
            return
        snap = rnd["branch_snapshot"]
        option = rnd.get("effective_option") or rnd["original_winner_option"]
        target = snap["options"][option]
        status = s["availability"].get(target, {}).get("available")

        if status is False and not rnd.get("diverted"):
            unavailable = s["availability"].get(target, {})
            fallback = snap["fallback_option"]
            rnd["diverted"] = True
            rnd["diversion"] = {
                "reason": "winner_segment_unavailable",
                "reason_zh": "中标选项的后续片段已公布不可用，改道节目方备用选项",
                "unavailable_option": option,
                "unavailable_segment": target,
                "reported_at": unavailable.get("at"),
                "fallback_option": fallback,
                "at": at,
            }
            rnd["effective_option"] = fallback
            option = fallback
            target = snap["options"][option]
            status = s["availability"].get(target, {}).get("available")
            events.append(
                {
                    "type": "route_diverted",
                    "at": at,
                    "room_id": rnd["room"],
                    "round_id": rnd["id"],
                    "payload": copy.deepcopy(rnd["diversion"]),
                }
            )

        if status is False:
            reason = "fallback_segment_unavailable" if rnd.get("diverted") else "selected_segment_unavailable"
            previous_status = rnd.get("switch_status")
            rnd["switch_status"] = "waiting_assets"
            rnd["waiting"] = {
                "reason": reason,
                "reason_zh": "备用片段也不可用，房间等待素材；系统不会编造新选项",
                "option": option,
                "segment": target,
                "since": (rnd.get("waiting") or {}).get("since", at),
                "last_report_at": at,
            }
            if previous_status != "waiting_assets":
                events.append(
                    {
                        "type": "switch_waiting_assets",
                        "at": at,
                        "room_id": rnd["room"],
                        "round_id": rnd["id"],
                        "payload": copy.deepcopy(rnd["waiting"]),
                    }
                )
            return

        # Absence of a false ready report is treated as "not announced
        # unavailable"; explicit true records are retained in the audit log.
        decision = {
            "decision_id": f"decision:{rnd['id']}",
            "round_id": rnd["id"],
            "room": rnd["room"],
            "node": rnd["node"],
            "option": option,
            "target_segment": target,
            "original_winner_option": rnd["original_winner_option"],
            "fallback_used": bool(rnd.get("diverted")),
            "diversion": rnd.get("diversion"),
            "reason": rnd["diversion"]["reason"] if rnd.get("diversion") else "plurality_winner",
            "committed_at": at,
            "command_id": cid,
            "immutable": True,
            "relocation_point": {
                "segment": target,
                "playhead": snap.get("playable_start", 0),
            },
            "source_apply_point": {
                "segment": snap["source_segment"],
                "playhead": snap.get("cut_playhead")
                if snap.get("cut_playhead") is not None
                else snap.get("branch_playhead"),
            },
        }
        rnd["switch_decision"] = decision
        rnd["switch_status"] = "decided"
        rnd["waiting"] = None
        events.append(
            {
                "type": "switch_committed",
                "at": at,
                "room_id": rnd["room"],
                "round_id": rnd["id"],
                "payload": copy.deepcopy(decision),
            }
        )

    # ------------------------------------------------------------------
    # Playback positions, resume and switch confirmations
    # ------------------------------------------------------------------

    def _position(
        self,
        s: dict[str, Any],
        at: int,
        rec: dict[str, Any],
        claim_relocation: bool = False,
    ) -> dict[str, Any]:
        room_id = str(_required(rec, "room"))
        account = str(_required(rec, "account"))
        device = str(_required(rec, "device"))
        segment = str(_required(rec, "segment"))
        playhead = _as_nonneg_number(
            rec.get("playhead", rec.get("position", rec.get("at_playhead", 0))),
            "playhead",
        )
        room = self._ensure_room(s, room_id, at)
        dev = self._ensure_device(room, account, device, at)
        dev["last_position"] = {"segment": segment, "playhead": playhead, "at": at}
        plan = self._recovery_plan(s, room, account, dev, at=at, claim=claim_relocation)
        return {"accepted": True, "player_key": device_key(account, device), "recovery": plan}

    def _resume(
        self,
        s: dict[str, Any],
        at: int,
        rec: dict[str, Any],
        events: list[dict[str, Any]],
    ) -> dict[str, Any]:
        result = self._position(s, at, rec, claim_relocation=True)
        room_id = str(rec["room"])
        account = str(rec["account"])
        device = str(rec["device"])
        room = s["rooms"][room_id]
        key = device_key(account, device)
        dev = room["devices"][key]
        dev["resume_count"] = int(dev.get("resume_count", 0)) + 1
        dev["last_resume_at"] = at
        checkpoint = room.get("accounts", {}).get(account, {}).get("canonical")
        result["account_checkpoint"] = copy.deepcopy(checkpoint)
        result["account_progress_overwritten"] = False
        events.append(
            {
                "type": "device_resumed",
                "at": at,
                "room_id": room_id,
                "payload": {
                    "account": account,
                    "device": device,
                    "recovery": result["recovery"],
                    "account_checkpoint": copy.deepcopy(checkpoint),
                },
            }
        )
        return result

    def _switched(
        self,
        s: dict[str, Any],
        at: int,
        rec: dict[str, Any],
        cid: str,
        events: list[dict[str, Any]],
    ) -> dict[str, Any]:
        round_id = str(_required(rec, "round"))
        account = str(_required(rec, "account"))
        device = str(_required(rec, "device"))
        segment = str(_required(rec, "segment"))
        rnd = s["rounds"].get(round_id)
        if not rnd:
            raise ServiceError("unknown_round", f"round {round_id!r} has not been opened")
        room = s["rooms"][rnd["room"]]
        key = device_key(account, device)
        dev = self._ensure_device(room, account, device, at)
        decision = rnd.get("switch_decision")
        if not decision:
            return {
                "accepted": False,
                "confirmed": False,
                "error": {
                    "code": "switch_not_committed",
                    "message": "the room is waiting for usable assets and has no immutable switch decision",
                    "waiting": rnd.get("waiting"),
                },
            }
        if segment != decision["target_segment"]:
            return {
                "accepted": False,
                "confirmed": False,
                "error": {
                    "code": "segment_mismatch",
                    "message": "confirmed segment does not match the immutable switch decision",
                    "expected_segment": decision["target_segment"],
                    "actual_segment": segment,
                },
            }

        existing = dev["confirmed_rounds"].get(round_id)
        if existing:
            return {"accepted": True, "confirmed": True, "already_confirmed": True, "confirmation": existing}

        playhead = rec.get("playhead", decision["relocation_point"]["playhead"])
        confirmation = {
            "round": round_id,
            "account": account,
            "device": device,
            "player_key": key,
            "segment": segment,
            "playhead": playhead,
            "at": at,
            "command_id": cid,
        }
        dev["confirmed_rounds"][round_id] = confirmation
        dev["last_switch"] = confirmation
        dev["admitted_rounds"].setdefault(round_id, {"at": at, "source": "switch_confirmation"})

        account_state = room["accounts"].setdefault(
            account, {"account": account, "canonical": None, "confirmation_history": []}
        )
        canonical = account_state.get("canonical")
        updated = canonical is None or int(rnd["seq"]) >= int(canonical.get("seq", -1))
        if updated:
            new_canonical = {
                "round_id": round_id,
                "seq": rnd["seq"],
                "node": rnd["node"],
                "segment": segment,
                "playhead": playhead,
                "device": device,
                "confirmed_at": at,
                "command_id": cid,
            }
            account_state["canonical"] = new_canonical
            account_state["confirmation_history"].append(copy.deepcopy(new_canonical))
        events.append(
            {
                "type": "switch_confirmed",
                "at": at,
                "room_id": room["id"],
                "round_id": round_id,
                "payload": {
                    **copy.deepcopy(confirmation),
                    "account_progress_updated": updated,
                },
            }
        )
        plan = self._recovery_plan(s, room, account, dev, at=at, claim=True)
        return {
            "accepted": True,
            "confirmed": True,
            "confirmation": confirmation,
            "account_progress_updated": updated,
            "account_progress_overwritten": not updated,
            "recovery": plan,
        }

    # ------------------------------------------------------------------
    # State helpers
    # ------------------------------------------------------------------

    def _ensure_room(self, s: dict[str, Any], room_id: str, at: int) -> dict[str, Any]:
        room = s["rooms"].get(room_id)
        if room is None:
            room = {
                "id": room_id,
                "created_at": at,
                "members": {},
                "devices": {},
                "accounts": {},
                "round_ids": [],
                "next_seq": 0,
            }
            s["rooms"][room_id] = room
        return room

    def _ensure_device(
        self, room: dict[str, Any], account: str, device: str, at: int
    ) -> dict[str, Any]:
        key = device_key(account, device)
        dev = room["devices"].get(key)
        if dev is None:
            dev = {
                "player_key": key,
                "account": account,
                "device": device,
                "first_seen_at": at,
                "last_position": None,
                "confirmed_rounds": {},
                "admitted_rounds": {},
                "resume_count": 0,
                "last_resume_at": None,
            }
            room["devices"][key] = dev
        return dev

    def _room_rounds(self, s: dict[str, Any], room: dict[str, Any]) -> list[dict[str, Any]]:
        return [s["rounds"][rid] for rid in room["round_ids"] if rid in s["rounds"]]

    def _graph(self, s: dict[str, Any]) -> dict[str, set[str]]:
        graph: dict[str, set[str]] = {}
        for branch in s["branches"].values():
            current = branch["current"]
            graph.setdefault(current["source_segment"], set()).update(current["options"].values())
        for rnd in s["rounds"].values():
            snap = rnd["branch_snapshot"]
            graph.setdefault(snap["source_segment"], set()).update(snap["options"].values())
        return graph

    def _can_reach(self, s: dict[str, Any], start: str | None, target: str | None) -> bool:
        if not start or not target:
            return False
        if start == target:
            return True
        graph = self._graph(s)
        seen = {start}
        queue = [start]
        while queue:
            segment = queue.pop(0)
            for nxt in graph.get(segment, ()):  # type: ignore[arg-type]
                if nxt == target:
                    return True
                if nxt not in seen:
                    seen.add(nxt)
                    queue.append(nxt)
        return False

    def _access_for_round(
        self, s: dict[str, Any], room: dict[str, Any], rnd: dict[str, Any], dev: dict[str, Any]
    ) -> dict[str, Any]:
        grant = dev.get("admitted_rounds", {}).get(rnd["id"])
        if grant:
            return {"allowed": True, "status": "usable_point_obtained", "grant": grant}
        if rnd["id"] in dev.get("confirmed_rounds", {}):
            return {"allowed": True, "status": "switch_confirmed"}
        pos = dev.get("last_position")
        if not pos:
            # No evidence that the device is outside the branch.  First-round
            # simulation devices commonly vote before sending a position.
            return {"allowed": True, "status": "playback_unknown", "reason": "no_playback_report"}

        rounds = self._room_rounds(s, room)
        for prior in rounds:
            if int(prior["seq"]) >= int(rnd["seq"]):
                break
            if prior["id"] in dev.get("confirmed_rounds", {}):
                continue
            source = prior["branch_snapshot"]["source_segment"]
            cut = prior["branch_snapshot"].get("cut_playhead")
            if pos["segment"] == source and cut is not None and float(pos["playhead"]) > float(cut):
                return {
                    "allowed": False,
                    "status": "missed_prior_switch_point",
                    "reason": "device_is_past_last_allowed_switch_position",
                    "detail": {
                        "round": prior["id"],
                        "segment": source,
                        "playhead": pos["playhead"],
                        "cut_playhead": cut,
                    },
                }

        if not self._can_reach(s, pos["segment"], rnd["branch_snapshot"]["source_segment"]):
            return {
                "allowed": False,
                "status": "segment_not_connected",
                "reason": "current_visible_segment_cannot_reach_next_branch",
                "detail": {
                    "current_segment": pos["segment"],
                    "required_source_segment": rnd["branch_snapshot"]["source_segment"],
                },
            }

        own_cut = rnd["branch_snapshot"].get("cut_playhead")
        if pos["segment"] == rnd["branch_snapshot"]["source_segment"] and own_cut is not None:
            if float(pos["playhead"]) > float(own_cut):
                return {
                    "allowed": True,
                    "status": "past_this_round_switch_point_but_vote_counts",
                    "reason": "this_round_vote_is_still_accounted;_relocation_required_after_close",
                }
            return {"allowed": True, "status": "at_branch_in_time"}
        return {"allowed": True, "status": "graph_connected"}

    def _recovery_plan(
        self,
        s: dict[str, Any],
        room: dict[str, Any],
        account: str,
        dev: dict[str, Any],
        at: int | None = None,
        claim: bool = False,
    ) -> dict[str, Any]:
        pos = dev.get("last_position")
        account_state = room.get("accounts", {}).get(account, {})
        canonical = account_state.get("canonical")
        rounds = self._room_rounds(s, room)
        decided = [r for r in rounds if r.get("switch_decision")]
        latest_decision = decided[-1] if decided else None

        candidates: list[tuple[int, dict[str, Any]]] = []
        if canonical:
            candidates.append(
                (
                    int(canonical.get("seq", -1)),
                    {
                        "segment": canonical["segment"],
                        "playhead": canonical.get("playhead", 0),
                        "round_id": canonical["round_id"],
                        "seq": canonical.get("seq"),
                        "source": "account_confirmed_switch",
                    },
                )
            )
        if latest_decision:
            d = latest_decision["switch_decision"]
            candidates.append(
                (
                    int(latest_decision["seq"]),
                    {
                        **d["relocation_point"],
                        "round_id": latest_decision["id"],
                        "seq": latest_decision["seq"],
                        "source": "room_switch_decision",
                    },
                )
            )
        # Recovery points move forward only.  The account canonical remains
        # untouched by resume/position, while a newer public decision gives a
        # lagging device its furthest safe catch-up target.
        target_point = max(candidates, key=lambda item: item[0])[1] if candidates else None

        plan: dict[str, Any]
        if target_point:
            target_round = s["rounds"].get(target_point.get("round_id")) if target_point.get("round_id") else None
            confirmed_target = bool(
                target_round and target_round["id"] in dev.get("confirmed_rounds", {})
            )
            if pos and pos["segment"] == target_point["segment"] and confirmed_target:
                plan = {"action": "up_to_date", "status": "up_to_date", "point": target_point}
            elif pos and pos["segment"] == target_point["segment"]:
                plan = {
                    "action": "confirm_switch",
                    "status": "at_target_pending_confirmation",
                    "point": target_point,
                }
            else:
                continue_round = None
                for rnd in rounds:
                    d = rnd.get("switch_decision")
                    if not d or rnd["id"] in dev.get("confirmed_rounds", {}):
                        continue
                    snap = rnd["branch_snapshot"]
                    limit = snap.get("cut_playhead")
                    if pos and pos["segment"] == snap["source_segment"]:
                        if limit is None or float(pos["playhead"]) <= float(limit):
                            continue_round = rnd
                            break
                if continue_round:
                    d = continue_round["switch_decision"]
                    snap = continue_round["branch_snapshot"]
                    plan = {
                        "action": "continue_then_switch",
                        "status": "on_time_to_switch",
                        "round_id": continue_round["id"],
                        "apply_point": {
                            "segment": snap["source_segment"],
                            "playhead": (
                                snap.get("cut_playhead")
                                if snap.get("cut_playhead") is not None
                                else (
                                    snap.get("branch_playhead")
                                    if snap.get("branch_playhead") is not None
                                    else pos["playhead"]
                                )
                            ),
                        },
                        "then_switch_to": d["relocation_point"],
                        "decision_id": d["decision_id"],
                    }
                else:
                    plan = {
                        "action": "relocate",
                        "status": "needs_relocation",
                        "point": target_point,
                        "reason": "reported position is past the safe switch point or cannot connect",
                    }
        else:
            open_rounds = [r for r in rounds if r.get("status") == "open"]
            if not open_rounds:
                plan = {"action": "idle", "status": "no_open_or_committed_round", "point": None}
            else:
                rnd = open_rounds[0]
                access = self._access_for_round(s, room, rnd, dev)
                source = rnd["branch_snapshot"]["source_segment"]
                source_status = s["availability"].get(source, {}).get("available")
                if not access["allowed"] and source_status is False:
                    plan = {
                        "action": "wait_for_assets",
                        "status": "waiting_assets",
                        "round_id": rnd["id"],
                        "segment": source,
                        "access": access,
                    }
                elif pos and pos["segment"] == source:
                    limit = rnd["branch_snapshot"].get("cut_playhead")
                    plan = {
                        "action": "continue_to_branch",
                        "status": "on_time_to_branch",
                        "round_id": rnd["id"],
                        "point": {"segment": source, "playhead": limit},
                        "access": access,
                    }
                elif access["allowed"]:
                    plan = {
                        "action": "continue_or_resume",
                        "status": access["status"],
                        "round_id": rnd["id"],
                        "access": access,
                        "point": pos,
                    }
                else:
                    plan = {
                        "action": "relocate",
                        "status": "needs_relocation",
                        "round_id": rnd["id"],
                        "point": {"segment": source, "playhead": 0},
                        "access": access,
                    }

        if claim and at is not None:
            point = plan.get("point") or plan.get("then_switch_to")
            if plan.get("action") in {"relocate", "up_to_date", "confirm_switch", "continue_then_switch", "continue_to_branch", "continue_or_resume"}:
                usable_point = point or (pos if pos else None)
                if usable_point:
                    dev["usable_point"] = {**usable_point, "obtained_at": at, "via": "resume"}
                for rnd in rounds:
                    if rnd.get("status") != "open":
                        continue
                    covers = False
                    if usable_point and usable_point.get("segment"):
                        covers = (
                            usable_point["segment"] == rnd["branch_snapshot"]["source_segment"]
                            or self._can_reach(s, usable_point["segment"], rnd["branch_snapshot"]["source_segment"])
                        )
                    if covers and dev["account"] in rnd.get("eligible_accounts", []):
                        grant = {"at": at, "source": "resume_recovery_point", "point": usable_point}
                        dev["admitted_rounds"][rnd["id"]] = grant
                        key = dev["player_key"]
                        inv = rnd.setdefault("invitations", {}).get(key)
                        if not inv:
                            inv = {
                                "player_key": key,
                                "account": dev["account"],
                                "device": dev["device"],
                                "at": at,
                            }
                            rnd["invitations"][key] = inv
                        inv.update(
                            {
                                "invited": True,
                                "access_status": "usable_point_obtained",
                                "reason": None,
                                "granted_at": at,
                            }
                        )
        return plan

    # ------------------------------------------------------------------
    # Queries
    # ------------------------------------------------------------------

    def inspect(self, query: dict[str, Any] | None = None, **kwargs: Any) -> dict[str, Any]:
        if query is None:
            query = kwargs
        elif kwargs:
            merged = dict(query)
            merged.update(kwargs)
            query = merged
        query = query or {}
        with self.lock:
            if query.get("round") or query.get("round_id"):
                return self.inspect_round(str(query.get("round") or query.get("round_id")))
            if query.get("room") or query.get("room_id"):
                return self.inspect_room(str(query.get("room") or query.get("room_id")))
            if query.get("node"):
                node = str(query["node"])
                branch = copy.deepcopy(self.state["branches"].get(node))
                if not branch:
                    raise ServiceError("unknown_branch", f"branch node {node!r} not found")
                return branch
            if query.get("account"):
                return self.account_progress(str(query["account"]), query.get("room"))
            return {
                "rooms": {rid: self.inspect_room(rid) for rid in self.state["rooms"]},
                "rounds": {rid: self.inspect_round(rid) for rid in self.state["rounds"]},
                "branches": copy.deepcopy(self.state["branches"]),
                "availability": copy.deepcopy(self.state["availability"]),
            }

    def inspect_round(self, round_id: str) -> dict[str, Any]:
        rnd = self.state["rounds"].get(round_id)
        if not rnd:
            raise ServiceError("unknown_round", f"round {round_id!r} not found")
        snapshot = copy.deepcopy(rnd)
        room = self.state["rooms"][rnd["room"]]
        players = []
        for key, dev in room["devices"].items():
            if dev["account"] not in rnd["eligible_accounts"]:
                continue
            access = self._access_for_round(self.state, room, rnd, dev)
            players.append(
                {
                    "player_key": key,
                    "account": dev["account"],
                    "device": dev["device"],
                    "last_position": copy.deepcopy(dev.get("last_position")),
                    "access": access,
                    "invitation": copy.deepcopy(rnd["invitations"].get(key)),
                    "confirmed": round_id in dev.get("confirmed_rounds", {}),
                }
            )
        snapshot["eligible_count"] = rnd["eligible_count"]
        live_counts = {opt: 0 for opt in rnd["branch_snapshot"]["options"]}
        for ballot in rnd["ballots"].values():
            live_counts[ballot["option"]] = live_counts.get(ballot["option"], 0) + 1
        snapshot["live_counts"] = live_counts
        snapshot["counts"] = (rnd.get("tally") or {}).get("counts") or live_counts
        snapshot["winner_option"] = rnd.get("original_winner_option")
        snapshot["diversion_reason"] = (rnd.get("diversion") or {}).get("reason")
        snapshot["players"] = players
        snapshot["counted_ballot_accounts"] = sorted(rnd["ballots"].keys())
        snapshot["public_events"] = self.events(round_id=round_id)
        return snapshot

    def inspect_room(self, room_id: str) -> dict[str, Any]:
        room = self.state["rooms"].get(room_id)
        if not room:
            raise ServiceError("unknown_room", f"room {room_id!r} not found")
        s = self.state
        rounds = [self.inspect_round(rid) for rid in room["round_ids"]]
        players = []
        for key, dev in room["devices"].items():
            plan = self._recovery_plan(s, room, dev["account"], dev, at=None, claim=False)
            confirmed_latest = False
            decided = [r for r in self._room_rounds(s, room) if r.get("switch_decision")]
            if decided:
                confirmed_latest = decided[-1]["id"] in dev.get("confirmed_rounds", {})
            needs_catchup = plan.get("action") in {
                "relocate",
                "continue_then_switch",
                "confirm_switch",
                "wait_for_assets",
            }
            players.append(
                {
                    "player_key": key,
                    "account": dev["account"],
                    "device": dev["device"],
                    "last_position": copy.deepcopy(dev.get("last_position")),
                    "usable_point": copy.deepcopy(dev.get("usable_point")),
                    "confirmed_round_ids": sorted(dev.get("confirmed_rounds", {}).keys()),
                    "confirmed_latest_decision": confirmed_latest,
                    "catchup": plan,
                    "catchup_status": plan.get("status"),
                    "needs_catchup": needs_catchup,
                    "playback_unknown": dev.get("last_position") is None,
                }
            )
        latest = rounds[-1] if rounds else None
        behind = [p for p in players if p["catchup_status"] == "needs_relocation"]
        pending = [
            p
            for p in players
            if not p["confirmed_latest_decision"]
            and latest
            and latest.get("switch_decision")
        ]
        result = copy.deepcopy(room)
        result["rounds"] = rounds
        result["players"] = players
        result["current_round_id"] = room["round_ids"][-1] if room["round_ids"] else None
        result["current_eligible_count"] = latest["eligible_count"] if latest else 0
        result["eligible_count"] = result["current_eligible_count"]
        result["current_result"] = self._result_summary(latest) if latest else None
        if latest:
            result["winner_option"] = latest.get("winner_option")
            result["diverted"] = latest.get("diverted")
            result["diversion_reason"] = latest.get("diversion_reason")
            result["effective_option"] = latest.get("effective_option")
            result["switch_status"] = latest.get("switch_status")
            result["switch_decision"] = latest.get("switch_decision")
        result["players_needing_catchup"] = [p["player_key"] for p in players if p["needs_catchup"]]
        result["behind_players"] = [p["player_key"] for p in behind]
        result["pending_switch_players"] = [p["player_key"] for p in pending]
        result["players_unknown_position"] = [p["player_key"] for p in players if p["playback_unknown"]]
        result["public_events"] = self.events(room_id=room_id, limit=100)
        return result

    def _result_summary(self, rnd: dict[str, Any]) -> dict[str, Any]:
        return {
            "round": rnd["id"],
            "node": rnd["node"],
            "status": rnd["status"],
            "eligible_count": rnd["eligible_count"],
            "tally": rnd.get("tally"),
            "winner_option": rnd.get("original_winner_option"),
            "diverted": rnd.get("diverted"),
            "diversion_reason": rnd.get("diversion_reason"),
            "effective_option": rnd.get("effective_option"),
            "switch_status": rnd.get("switch_status"),
            "switch_decision": rnd.get("switch_decision"),
        }

    def account_progress(self, account: str, room_id: str | None = None) -> dict[str, Any]:
        progresses = []
        for rid, room in self.state["rooms"].items():
            if room_id and rid != room_id:
                continue
            state = room.get("accounts", {}).get(account)
            if state:
                progresses.append(copy.deepcopy(state))
        if room_id:
            return progresses[0] if progresses else {"account": account, "canonical": None}
        return {"account": account, "rooms": progresses}

    def get_receipt(self, command_id: str) -> dict[str, Any] | None:
        with self.lock:
            row = self.db.execute(
                "SELECT response FROM commands WHERE command_id=?", (command_id,)
            ).fetchone()
        return json.loads(row["response"]) if row else None

    def events(
        self, room_id: str | None = None, round_id: str | None = None, limit: int = 200
    ) -> list[dict[str, Any]]:
        query = "SELECT * FROM domain_events WHERE 1=1"
        args: list[Any] = []
        if room_id:
            query += " AND room_id=?"
            args.append(room_id)
        if round_id:
            query += " AND round_id=?"
            args.append(round_id)
        query += " ORDER BY seq DESC LIMIT ?"
        args.append(limit)
        with self.lock:
            rows = self.db.execute(query, args).fetchall()
        result = []
        for row in reversed(rows):
            item = dict(row)
            item["payload"] = json.loads(item["payload"])
            result.append(item)
        return result


# Common aliases for code phrased around middleware/rooms rather than service.
RoomService = RoomEventService
RoomMiddleware = RoomEventService
RoomEventMiddleware = RoomEventService
EventMiddleware = RoomEventService
Middleware = RoomEventService
Service = RoomEventService

RoomEventService.get_room = RoomEventService.inspect_room  # type: ignore[attr-defined]
RoomEventService.get_round = RoomEventService.inspect_round  # type: ignore[attr-defined]
RoomEventService.query_state = RoomEventService.inspect  # type: ignore[attr-defined]


def create_service(db_path: str | Path = ":memory:", **kwargs: Any) -> RoomEventService:
    """Factory used by HTTP adapters and embedding callers."""
    return RoomEventService(db_path, **kwargs)
