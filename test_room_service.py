"""Behavior tests for persistent branch-room middleware."""

from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from branch_room import RoomEventService
from branch_room.scenario import run_file
from room_service import RoomEventService as RootFacadeService

ROOT = Path(__file__).parent


def msg(at: int, kind: str, **record):
    return {"at": at, "kind": kind, "record": record}


class ExampleScenarioTests(unittest.TestCase):
    def test_device_outside_branch_tally_eligibility_and_fallback(self):
        with tempfile.TemporaryDirectory() as td:
            svc = RoomEventService(Path(td) / "room.sqlite3")
            responses = run_file(
                svc, ROOT / "examples" / "fallback-and-late-join.jsonl", source="fallback"
            )
            inspect_response = [r for r in responses if r["kind"] == "inspect"][0]
            snap = inspect_response["result"]["snapshot"]
            self.assertEqual(snap["eligible_count"], 2)
            self.assertEqual(snap["counts"], {"river": 2, "town": 0})
            self.assertEqual(snap["winner_option"], "river")
            self.assertTrue(snap["diverted"])
            self.assertEqual(snap["diversion_reason"], "winner_segment_unavailable")
            self.assertEqual(snap["effective_option"], "town")
            self.assertEqual(snap["switch_decision"]["target_segment"], "t")
            self.assertEqual([x["account"] for x in snap["rejected_ballots"]], ["c"])
            svc.close()

    def test_late_device_resume_grants_next_round_ballot(self):
        with tempfile.TemporaryDirectory() as td:
            svc = RoomEventService(Path(td) / "room.sqlite3")
            responses = run_file(
                svc, ROOT / "examples" / "device-outside-the-branch.jsonl", source="device"
            )
            room_snap = [r for r in responses if r["kind"] == "inspect"][0]["result"]["snapshot"]
            q2 = next(r for r in room_snap["rounds"] if r["id"] == "q2")
            self.assertEqual(q2["eligible_count"], 1)
            self.assertIn("a", q2["ballots"])
            self.assertEqual(q2["ballots"]["a"]["option"], "hill")
            pad = next(p for p in room_snap["players"] if p["device"] == "pad")
            self.assertEqual(pad["catchup_status"], "needs_relocation")
            self.assertEqual(pad["catchup"]["point"]["segment"], "r")
            self.assertEqual(pad["catchup"]["point"]["playhead"], 0)
            self.assertIn("a::pad", room_snap["players_needing_catchup"])
            svc.close()

    def test_root_facade_and_package_are_same_behavior(self):
        self.assertIs(RootFacadeService, RoomEventService)


class PersistenceAndIdempotencyTests(unittest.TestCase):
    def test_restart_keeps_vote_and_decision_and_receipt_replay_cannot_rewrite(self):
        with tempfile.TemporaryDirectory() as td:
            db = Path(td) / "persistent.sqlite3"
            svc = RoomEventService(db)
            commands = [
                msg(0, "branch", node="c", segment="intro",
                    options={"yes": "y", "no": "n"}, fallback="no",
                    order=["yes", "no"], cut_playhead=10),
                msg(1, "member", room="rm", account="a", active=True),
                msg(2, "start", room="rm", node="c", round="q", close_at=5),
                msg(3, "ballot", round="q", account="a", device="p", option="yes"),
                msg(5, "close", round="q"),
            ]
            for i, command in enumerate(commands):
                svc.handle(command, command_id=f"cmd-{i}")
            before = svc.inspect_round("q")
            self.assertEqual(before["counts"]["yes"], 1)
            self.assertEqual(before["switch_decision"]["target_segment"], "y")
            svc.close()

            restarted = RoomEventService(db)
            after = restarted.inspect_round("q")
            self.assertEqual(after["counts"], before["counts"])
            self.assertEqual(after["switch_decision"], before["switch_decision"])

            replay_ballot = restarted.handle(commands[3], command_id="cmd-3")
            self.assertTrue(replay_ballot["idempotent_replay"])
            replay_close = restarted.handle(commands[4], command_id="cmd-4")
            self.assertTrue(replay_close["idempotent_replay"])

            # A newly delivered Republican ballot after commitment is rejected.
            late_rewrite = restarted.handle(
                msg(6, "ballot", round="q", account="a", device="p", option="no"),
                command_id="new-rewrite",
            )
            self.assertFalse(late_rewrite["accepted"])
            self.assertEqual(late_rewrite["result"]["error"]["code"], "round_closed")
            final = restarted.inspect_round("q")
            self.assertEqual(final["ballots"]["a"]["option"], "yes")
            self.assertEqual(final["switch_decision"]["target_segment"], "y")
            restarted.close()

    def test_same_command_id_with_different_payload_conflicts(self):
        svc = RoomEventService(":memory:")
        command = msg(1, "member", room="r", account="a", active=True)
        svc.handle(command, command_id="same")
        command["record"]["active"] = False
        with self.assertRaises(Exception):
            svc.handle(command, command_id="same")
        svc.close()


class RoutingAndProgramUpdateTests(unittest.TestCase):
    def test_unavailable_fallback_waits_then_ready_commits_without_new_option(self):
        svc = RoomEventService(":memory:")
        svc.submit("branch", {
            "node": "c", "segment": "s",
            "options": {"win": "w", "fb": "f"},
            "fallback": "fb", "order": ["win", "fb"],
        })
        svc.submit("ready", {"segment": "w", "available": False})
        svc.submit("ready", {"segment": "f", "available": False})
        svc.submit("member", {"room": "r", "account": "a", "active": True}, at=1)
        svc.submit("start", {"room": "r", "node": "c", "round": "q", "close_at": 5}, at=2)
        svc.submit("ballot", {"round": "q", "account": "a", "device": "d", "option": "win"}, at=3)
        closed = svc.submit("close", {"round": "q"}, at=5)
        self.assertFalse(closed["accepted"] is False)
        result = closed["result"]
        self.assertEqual(result["effective_option"], "fb")
        self.assertEqual(result["switch_status"], "waiting_assets")
        self.assertIsNone(result["switch_decision"])
        snap = svc.inspect_round("q")
        self.assertEqual(snap["waiting"]["segment"], "f")

        ready = svc.submit("ready", {"segment": "f", "available": True}, at=8)
        self.assertEqual(ready["result"]["committed_rounds"], ["q"])
        snap = svc.inspect_round("q")
        self.assertEqual(snap["switch_status"], "decided")
        self.assertEqual(snap["switch_decision"]["target_segment"], "f")
        self.assertTrue(snap["switch_decision"]["fallback_used"])
        svc.close()

    def test_branch_update_changes_only_unopened_round(self):
        svc = RoomEventService(":memory:")
        first = {
            "node": "c", "segment": "s",
            "options": {"x": "xseg", "y": "yseg"},
            "fallback": "y", "order": ["x", "y"],
        }
        svc.submit("branch", first)
        svc.submit("member", {"room": "r", "account": "a", "active": True}, at=1)
        svc.submit("start", {"room": "r", "node": "c", "round": "old", "close_at": 5}, at=2)

        updated = dict(first)
        updated["options"] = {"x": "xseg", "z": "zseg"}
        updated["fallback"] = "z"
        updated["order"] = ["x", "z"]
        svc.submit("branch", updated, at=3)
        svc.submit("start", {"room": "r", "node": "c", "round": "new", "close_at": 9}, at=4)

        old = svc.inspect_round("old")
        new = svc.inspect_round("new")
        self.assertEqual(set(old["branch_snapshot"]["options"]), {"x", "y"})
        self.assertEqual(set(new["branch_snapshot"]["options"]), {"x", "z"})
        svc.close()


class DeviceAccessAndAccountTests(unittest.TestCase):
    def _graph_service(self):
        svc = RoomEventService(":memory:")
        svc.submit("branch", {
            "node": "c1", "segment": "main",
            "options": {"a": "a", "b": "b"},
            "fallback": "b", "order": ["a", "b"], "cut_playhead": 10,
        })
        svc.submit("branch", {
            "node": "c2", "segment": "a",
            "options": {"x": "x", "y": "y"},
            "fallback": "y", "order": ["x", "y"], "cut_playhead": 20,
        })
        svc.submit("member", {"room": "r", "account": "a", "active": True}, at=1)
        svc.submit("start", {"room": "r", "node": "c1", "round": "q1", "close_at": 5}, at=2)
        svc.submit("ballot", {"round": "q1", "account": "a", "device": "tv", "option": "a"}, at=3)
        svc.submit("close", {"round": "q1"}, at=5)
        return svc

    def test_unconnected_device_is_not_invited_until_resume_grants_point(self):
        svc = self._graph_service()
        svc.submit("position", {
            "room": "r", "account": "a", "device": "screen",
            "segment": "side_story", "playhead": 4,
        }, at=6)
        opened = svc.submit("start", {
            "room": "r", "node": "c2", "round": "q2", "close_at": 12,
        }, at=7)
        invitation = next(i for i in opened["result"]["invitations"] if i["device"] == "screen")
        self.assertFalse(invitation["invited"])
        self.assertEqual(invitation["access_status"], "segment_not_connected")

        blocked_ballot = svc.submit("ballot", {
            "round": "q2", "account": "a", "device": "screen", "option": "x",
        }, at=8)
        self.assertFalse(blocked_ballot["accepted"])
        self.assertEqual(blocked_ballot["result"]["error"]["code"], "device_without_usable_position")

        resumed = svc.submit("resume", {
            "room": "r", "account": "a", "device": "screen",
            "segment": "side_story", "playhead": 4,
        }, at=9)
        self.assertEqual(resumed["result"]["recovery"]["action"], "relocate")
        self.assertEqual(resumed["result"]["recovery"]["point"]["segment"], "a")

        allowed_ballot = svc.submit("ballot", {
            "round": "q2", "account": "a", "device": "screen", "option": "x",
        }, at=10)
        self.assertTrue(allowed_ballot["accepted"])
        q2 = svc.inspect_round("q2")
        self.assertEqual(q2["counts"]["x"], 1)
        svc.close()

    def test_stale_device_cannot_overwrite_account_progress(self):
        svc = self._graph_service()
        svc.submit("switched", {
            "round": "q1", "account": "a", "device": "tv", "segment": "a",
        }, at=6)
        svc.submit("start", {
            "room": "r", "node": "c2", "round": "q2", "close_at": 12,
        }, at=7)
        svc.submit("ballot", {"round": "q2", "account": "a", "device": "phone", "option": "x"}, at=8)
        svc.submit("close", {"round": "q2"}, at=12)
        svc.submit("switched", {
            "round": "q2", "account": "a", "device": "phone", "segment": "x",
        }, at=13)

        stale_resume = svc.submit("resume", {
            "room": "r", "account": "a", "device": "oldpad",
            "segment": "main", "playhead": 18,
        }, at=14)
        self.assertFalse(stale_resume["result"]["account_progress_overwritten"])
        self.assertEqual(stale_resume["result"]["account_checkpoint"]["segment"], "x")
        self.assertEqual(stale_resume["result"]["recovery"]["point"]["segment"], "x")

        old_confirmation = svc.submit("switched", {
            "round": "q1", "account": "a", "device": "oldpad", "segment": "a",
        }, at=15)
        self.assertTrue(old_confirmation["accepted"])
        self.assertFalse(old_confirmation["result"]["account_progress_updated"])
        progress = svc.account_progress("a", "r")
        self.assertEqual(progress["canonical"]["round_id"], "q2")
        self.assertEqual(progress["canonical"]["segment"], "x")
        svc.close()

    def test_late_member_is_not_eligible_but_can_be_eligible_next_round(self):
        svc = RoomEventService(":memory:")
        svc.submit("branch", {
            "node": "c", "segment": "s", "options": {"a": "a", "b": "b"},
            "fallback": "b", "order": ["a", "b"],
        })
        svc.submit("member", {"room": "r", "account": "old", "active": True}, at=1)
        svc.submit("start", {"room": "r", "node": "c", "round": "q1", "close_at": 5}, at=2)
        svc.submit("member", {"room": "r", "account": "new", "active": True}, at=3)
        rejected = svc.submit("ballot", {
            "round": "q1", "account": "new", "device": "d", "option": "a",
        }, at=4)
        self.assertEqual(rejected["result"]["error"]["code"], "not_eligible")
        svc.submit("close", {"round": "q1"}, at=5)
        svc.submit("start", {"room": "r", "node": "c", "round": "q2", "close_at": 9}, at=6)
        q2 = svc.inspect_round("q2")
        self.assertEqual(q2["eligible_count"], 2)
        accepted = svc.submit("ballot", {
            "round": "q2", "account": "new", "device": "d", "option": "a",
        }, at=7)
        self.assertTrue(accepted["accepted"])
        svc.close()


class TimingAndTieTests(unittest.TestCase):
    def test_same_second_line_order_decides_whether_close_wins(self):
        svc = RoomEventService(":memory:")
        svc.submit("branch", {
            "node": "c", "segment": "s",
            "options": {"a": "a", "b": "b"}, "fallback": "a", "order": ["a", "b"],
        })
        svc.submit("member", {"room": "r", "account": "u", "active": True}, at=1)

        svc.submit("start", {"room": "r", "node": "c", "round": "closed-first", "close_at": 10}, at=2)
        close_first = svc.submit("close", {"round": "closed-first"}, at=10)
        after_close = svc.submit("ballot", {
            "round": "closed-first", "account": "u", "device": "d", "option": "a",
        }, at=10)
        self.assertTrue(close_first["accepted"])
        self.assertFalse(after_close["accepted"])
        self.assertEqual(after_close["result"]["error"]["code"], "round_closed")

        svc.submit("start", {"room": "r", "node": "c", "round": "ballot-first", "close_at": 10}, at=2)
        before_close = svc.submit("ballot", {
            "round": "ballot-first", "account": "u", "device": "d", "option": "a",
        }, at=10)
        close_after = svc.submit("close", {"round": "ballot-first"}, at=10)
        self.assertTrue(before_close["accepted"])
        self.assertTrue(close_after["accepted"])
        self.assertEqual(svc.inspect_round("ballot-first")["counts"]["a"], 1)
        svc.close()

    def test_tie_uses_published_order_not_lexicographic_accident(self):
        svc = RoomEventService(":memory:")
        svc.submit("branch", {
            "node": "c", "segment": "s",
            "options": {"x": "x", "y": "y"}, "fallback": "x", "order": ["y", "x"],
        })
        for account in ("a", "b"):
            svc.submit("member", {"room": "r", "account": account, "active": True}, at=1)
        svc.submit("start", {"room": "r", "node": "c", "round": "q", "close_at": 5}, at=2)
        svc.submit("ballot", {"round": "q", "account": "a", "device": "d1", "option": "x"}, at=3)
        svc.submit("ballot", {"round": "q", "account": "b", "device": "d2", "option": "y"}, at=4)
        svc.submit("close", {"round": "q"}, at=5)
        snap = svc.inspect_round("q")
        self.assertTrue(snap["tally"]["tie"])
        self.assertEqual(snap["winner_option"], "y")
        svc.close()


if __name__ == "__main__":
    unittest.main()
