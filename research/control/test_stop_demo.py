#!/usr/bin/env python3
"""Unit tests for /stop-demo state machine."""

from __future__ import annotations

import sys
import time
import threading
import unittest
from pathlib import Path
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parent))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "analysis"))

import server  # noqa: E402


def _idle_state():
    return {
        "running": False,
        "started_at": None,
        "finished_at": None,
        "last_error": None,
        "latest_run": None,
        "demo_session_start": None,
        "demo_progress": server._demo_progress_idle(),
        "traffic_stop": False,
        "stop_requested": False,
        "stopping": False,
        "active_demo_id": None,
        "extend_batches": 0,
        "traffic_deadline": None,
        "batches_completed": 0,
        "demo_total_changes": None,
        "demo_gate_failures": None,
        "demo_expected_failures": server.DEMO_EXPECTED_FAILURES,
        "demo_fail_stamped": 0,
    }


class StopDemoTests(unittest.TestCase):
    def setUp(self):
        with server.LOCK:
            server.STATE.update(_idle_state())
        self.client = server.APP.test_client()

    def tearDown(self):
        with server.LOCK:
            server.STATE.update(_idle_state())

    def _mark_running(self, demo_id: str = "test-demo"):
        with server.LOCK:
            server.STATE["running"] = True
            server.STATE["active_demo_id"] = demo_id
            server.STATE["stop_requested"] = False
            server.STATE["stopping"] = False
            server.STATE["traffic_stop"] = False
            server.STATE["last_error"] = None
            server.STATE["extend_batches"] = 0
            prog = server._demo_progress_idle()
            prog.update({
                "demo_id": demo_id,
                "phase": "submitting_traffic",
                "traffic_active": True,
                "extend_available": True,
            })
            server.STATE["demo_progress"] = prog

    def _wait_until(self, pred, timeout: float = 2.0):
        deadline = time.time() + timeout
        while time.time() < deadline:
            if pred():
                return
            time.sleep(0.02)
        self.fail("timeout waiting for stop-demo state")

    def test_stop_idle_is_idempotent(self):
        resp = self.client.post("/stop-demo")
        self.assertEqual(resp.status_code, 200)
        data = resp.get_json()
        self.assertTrue(data["ok"])
        self.assertFalse(data["stopped"])
        self.assertTrue(data["already_stopped"])
        self.assertFalse(server.STATE["running"])

    def test_close_demo_alias_idle(self):
        resp = self.client.post("/close-demo")
        self.assertEqual(resp.status_code, 200)
        self.assertTrue(resp.get_json()["ok"])

    def test_progress_stop_available_while_running(self):
        self._mark_running()
        prog = self.client.get("/demo-progress").get_json()
        self.assertTrue(prog["running"])
        self.assertTrue(prog["stop_available"])
        self.assertFalse(prog["stopping"])

    def test_progress_stopping_keeps_run_blocked(self):
        self._mark_running()
        with server.LOCK:
            server.STATE["stopping"] = True
            server.STATE["stop_requested"] = True
        prog = self.client.get("/demo-progress").get_json()
        self.assertTrue(prog["running"])
        self.assertTrue(prog["stopping"])
        self.assertFalse(prog["stop_available"])

    def test_extend_rejected_while_stopping(self):
        self._mark_running()
        with server.LOCK:
            server.STATE["stopping"] = True
            server.STATE["stop_requested"] = True
        resp = self.client.post("/extend-demo", json={"batches": 1})
        self.assertEqual(resp.status_code, 409)
        self.assertIn("stopping", resp.get_json()["message"])

    def test_extend_still_works_while_running(self):
        self._mark_running()
        resp = self.client.post("/extend-demo", json={"batches": 1})
        self.assertEqual(resp.status_code, 200)
        data = resp.get_json()
        self.assertTrue(data["ok"])
        self.assertEqual(data["batches_added"], 1)
        self.assertEqual(server.STATE["extend_batches"], 1)

    def test_run_rejected_while_running(self):
        self._mark_running()
        resp = self.client.post("/run-demo", json={})
        self.assertEqual(resp.status_code, 409)
        self.assertEqual(resp.get_json()["message"], "demo already running")

    def test_demo_thread_should_exit_on_stop_and_supersede(self):
        self._mark_running("a")
        self.assertFalse(server._demo_thread_should_exit("a"))
        with server.LOCK:
            server.STATE["stop_requested"] = True
            server.STATE["stopping"] = True
        self.assertTrue(server._demo_thread_should_exit("a"))
        with server.LOCK:
            server.STATE["stop_requested"] = False
            server.STATE["stopping"] = False
            server.STATE["active_demo_id"] = "b"
        self.assertTrue(server._demo_thread_should_exit("a"))
        self.assertFalse(server._demo_thread_should_exit("b"))

    def test_progress_ignores_traffic_updates_while_stopping(self):
        self._mark_running()
        with server.LOCK:
            server.STATE["stopping"] = True
            server.STATE["stop_requested"] = True
            server.STATE["demo_progress"]["phase"] = "stopping"
            server.STATE["demo_progress"]["message"] = server.STOPPING_MESSAGE
        server._set_demo_progress(
            phase="submitting_traffic", message="batch 2")
        self.assertEqual(
            server.STATE["demo_progress"]["phase"], "stopping")
        self.assertEqual(
            server.STATE["demo_progress"]["message"],
            server.STOPPING_MESSAGE)

    @mock.patch("server._fetch_tenant_status")
    @mock.patch("server.wait_for_empty_queues")
    def test_stop_running_demo_state_machine(self, wait_empty, fetch_status):
        fetch_status.return_value = {"pipelines": []}
        started = threading.Event()
        released = threading.Event()

        def slow_wait(*_args, **kwargs):
            started.set()
            released.wait(timeout=2)
            self.assertFalse(kwargs.get("require_gate_only", False))
            return True, 7, 0.4

        wait_empty.side_effect = slow_wait
        self._mark_running("abc")

        resp = self.client.post("/stop-demo")
        self.assertEqual(resp.status_code, 200)
        data = resp.get_json()
        self.assertTrue(data["ok"])
        self.assertTrue(data["stopped"])
        self.assertEqual(data["phase"], "stopping")
        self.assertEqual(data["message"], server.STOPPING_MESSAGE)

        self.assertTrue(started.wait(timeout=1.5))
        with server.LOCK:
            self.assertTrue(server.STATE["running"])
            self.assertTrue(server.STATE["stopping"])
            self.assertTrue(server.STATE["stop_requested"])
            self.assertTrue(server.STATE["traffic_stop"])
            self.assertEqual(server.STATE["extend_batches"], 0)
            self.assertEqual(
                server.STATE["demo_progress"]["phase"], "stopping")

        run_resp = self.client.post("/run-demo", json={})
        self.assertEqual(run_resp.status_code, 409)
        self.assertTrue(run_resp.get_json()["stopping"])

        again = self.client.post("/stop-demo")
        self.assertEqual(again.status_code, 200)
        self.assertEqual(again.get_json()["phase"], "stopping")
        self.assertEqual(wait_empty.call_count, 1)

        released.set()
        self._wait_until(lambda: not server.STATE["running"])
        with server.LOCK:
            self.assertFalse(server.STATE["stopping"])
            self.assertFalse(server.STATE["stop_requested"])
            self.assertFalse(server.STATE["traffic_stop"])
            self.assertIsNone(server.STATE["last_error"])
            self.assertEqual(
                server.STATE["demo_progress"]["phase"], "cancelled")
        prog = self.client.get("/demo-progress").get_json()
        self.assertFalse(prog["running"])
        self.assertFalse(prog["stop_available"])
        self.assertIn("queues cleared", prog["message"])

    @mock.patch("server.threading.Thread")
    @mock.patch("server._request_demo_reset", return_value=1.0)
    @mock.patch("server._reset_audit_reader")
    @mock.patch("server._invalidate_live_metrics_cache")
    def test_run_demo_works_after_stop(
            self, _cache, _audit, _reset, mock_thread):
        mock_thread.return_value = mock.Mock()
        with server.LOCK:
            server.STATE["running"] = False
            server.STATE["stopping"] = False
            server.STATE["stop_requested"] = False
            server.STATE["active_demo_id"] = None
            server.STATE["last_error"] = None
            server.STATE["demo_progress"]["phase"] = "cancelled"
        resp = self.client.post("/run-demo", json={})
        self.assertEqual(resp.status_code, 200)
        data = resp.get_json()
        self.assertTrue(data["ok"])
        self.assertEqual(data["phase"], "starting")
        self.assertTrue(server.STATE["running"])
        self.assertFalse(server.STATE["stopping"])
        self.assertFalse(server.STATE["stop_requested"])
        mock_thread.assert_called()
        self.assertEqual(
            mock_thread.call_args.kwargs.get("target"),
            server._run_demo_background)

    @mock.patch("server._fetch_tenant_status", return_value={"pipelines": []})
    @mock.patch(
        "server.wait_for_empty_queues",
        return_value=(False, 3, 1.2),
    )
    def test_stop_enables_run_even_if_queues_still_draining(
            self, wait_empty, _fetch):
        self._mark_running("drain")
        resp = self.client.post("/stop-demo")
        self.assertEqual(resp.status_code, 200)
        self._wait_until(lambda: not server.STATE["running"])
        wait_empty.assert_called()
        kwargs = wait_empty.call_args.kwargs
        self.assertFalse(kwargs.get("require_gate_only", False))
        prog = self.client.get("/demo-progress").get_json()
        self.assertEqual(prog["phase"], "cancelled")
        self.assertIn("draining", prog["message"])
        self.assertFalse(prog["running"])


if __name__ == "__main__":
    unittest.main()
