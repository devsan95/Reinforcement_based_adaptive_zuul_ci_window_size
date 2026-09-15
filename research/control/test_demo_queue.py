#!/usr/bin/env python3
"""Unit tests for demo gate-queue clearing helpers."""

from __future__ import annotations

import sys
import unittest
import json
from pathlib import Path
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parent))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "analysis"))

from server import (  # noqa: E402
    DEFAULT_INITIAL_WINDOW,
    DEMO_QUEUE_CLEAR_MAX_TIMEOUT,
    DEMO_QUEUE_CLEAR_TIMEOUT,
    _all_queue_depth,
    _all_queue_item_count,
    _audit_contains_demo_reset,
    _describe_queue_backlog,
    _effective_queue_clear_timeout,
    _extend_clear_deadline,
    _format_queue_change_label,
    _gate_queue_depth,
    _iter_all_queue_items,
    _iter_dequeue_targets,
    _iter_gate_queue_items,
    _item_from_queue_head,
    _normalize_dequeue_change,
    _queue_clear_rounds_needed,
    _residual_queue_ok,
    _windows_at_baseline,
    wait_for_empty_queues,
)


SAMPLE_STATUS = {
    "pipelines": [
        {
            "name": "check",
            "change_queues": [{
                "heads": [[{
                    "live": False,
                    "refs": [{"id": "9,1"}],
                }]],
            }],
        },
        {
            "name": "gate",
            "change_queues": [
                {
                    "heads": [
                        [
                            {
                                "live": True,
                                "refs": [
                                    {
                                        "id": "1,1",
                                        "project": "test1",
                                        "project_canonical": "test1",
                                    }
                                ],
                            },
                            {
                                "live": True,
                                "refs": [
                                    {
                                        "id": "2,1",
                                        "project": "test1",
                                        "project_canonical": "test1",
                                    }
                                ],
                            },
                        ],
                        [
                            {
                                "live": True,
                                "refs": [
                                    {
                                        "id": "3,1",
                                        "project": "test1",
                                        "project_canonical": "test1",
                                    }
                                ],
                            },
                        ],
                    ]
                }
            ],
        },
    ]
}


SAMPLE_ONE_ITEM = {
    "pipelines": [{
        "name": "gate",
        "change_queues": [{
            "heads": [[{
                "live": True,
                "refs": [{
                    "id": "1,1",
                    "project": "test1",
                    "project_canonical": "test1",
                }],
            }]],
        }],
    }],
}

SAMPLE_CHECK_AND_GATE = {
    "pipelines": [
        {
            "name": "check",
            "change_queues": [{
                "heads": [[{
                    "live": True,
                    "refs": [{
                        "id": "10,1",
                        "project": "test1",
                        "project_canonical": "test1",
                    }],
                }]],
            }],
        },
        {
            "name": "gate",
            "change_queues": [{
                "heads": [[{
                    "live": True,
                    "refs": [{
                        "id": "20,1",
                        "project": "test1",
                        "project_canonical": "test1",
                    }],
                }]],
            }],
        },
    ]
}


class DemoQueueTests(unittest.TestCase):
    def test_gate_queue_depth_counts_heads_not_dependents(self):
        self.assertEqual(_gate_queue_depth(SAMPLE_STATUS), 2)

    def test_iter_gate_queue_items_yields_queue_heads_only(self):
        items = list(_iter_gate_queue_items(SAMPLE_STATUS))
        self.assertEqual(items, [
            ("test1", "1,1"),
            ("test1", "2,1"),
            ("test1", "3,1"),
        ])

    def test_all_queue_depth_ignores_non_live_heads(self):
        self.assertEqual(_all_queue_depth(SAMPLE_STATUS), 2)

    def test_all_queue_item_count_includes_dependents(self):
        self.assertEqual(_all_queue_item_count(SAMPLE_STATUS), 3)

    def test_iter_all_queue_items_yields_every_live_change(self):
        items = list(_iter_all_queue_items(SAMPLE_STATUS))
        self.assertEqual(len(items), 3)
        self.assertIn(("gate", "test1", "1,1"), items)
        self.assertIn(("gate", "test1", "2,1"), items)
        self.assertIn(("gate", "test1", "3,1"), items)

    def test_describe_queue_backlog_groups_by_pipeline(self):
        backlog = _describe_queue_backlog(SAMPLE_STATUS, sample_limit=2)
        self.assertEqual(len(backlog), 1)
        self.assertEqual(backlog[0]["pipeline"], "gate")
        self.assertEqual(backlog[0]["count"], 3)
        self.assertEqual(len(backlog[0]["sample_changes"]), 2)
        self.assertEqual(
            backlog[0]["sample_changes"][0],
            _format_queue_change_label("test1", "1,1"))

    def test_iter_dequeue_targets_yields_tail_per_head_only(self):
        targets = list(_iter_dequeue_targets(SAMPLE_STATUS))
        self.assertEqual(targets, [
            ("gate", "test1", "3,1"),
            ("gate", "test1", "2,1"),
        ])

    def test_normalize_dequeue_change_adds_patchset(self):
        self.assertEqual(
            _normalize_dequeue_change({"id": "405"}),
            "405,1")
        self.assertEqual(
            _normalize_dequeue_change({
                "id": "405",
                "patchset": "2",
            }),
            "405,2")
        self.assertEqual(
            _normalize_dequeue_change({"id": "405,1"}),
            "405,1")

    def test_item_from_queue_head_normalizes_project(self):
        item = {
            "refs": [{
                "id": "9,1",
                "project_canonical": "gerrit/test1",
                "project": "test1",
            }],
        }
        self.assertEqual(_item_from_queue_head(item), ("test1", "9,1"))

    def test_iter_dequeue_targets_check_before_gate(self):
        targets = list(_iter_dequeue_targets(SAMPLE_CHECK_AND_GATE))
        self.assertEqual(targets, [
            ("check", "test1", "10,1"),
            ("gate", "test1", "20,1"),
        ])

    def test_queue_clear_rounds_follow_chain_depth(self):
        with mock.patch("server.DEMO_DEQUEUE_WORKERS", 24):
            # One long dependent chain: one tail per round.
            self.assertEqual(_queue_clear_rounds_needed(337, 1), 337)
            # Wide independent check: worker-limited HTTP batches.
            self.assertEqual(_queue_clear_rounds_needed(337, 337), 15)
            self.assertEqual(_queue_clear_rounds_needed(0, 0), 0)

    def test_effective_queue_clear_timeout_scales_with_backlog(self):
        small = _effective_queue_clear_timeout(10, 5)
        large = _effective_queue_clear_timeout(199, 159)
        leftover = _effective_queue_clear_timeout(337, 40)
        long_chain = _effective_queue_clear_timeout(337, 5)
        self.assertGreater(large, small)
        self.assertGreaterEqual(large, 25)
        self.assertGreaterEqual(small, 10)
        # The failing demo run aborted at ~37s with 337 leftovers.
        self.assertGreater(leftover, 37.0)
        self.assertGreater(long_chain, leftover)
        self.assertLessEqual(leftover, DEMO_QUEUE_CLEAR_MAX_TIMEOUT)
        self.assertEqual(
            _effective_queue_clear_timeout(0, 0),
            DEMO_QUEUE_CLEAR_TIMEOUT)

    def test_extend_clear_deadline_grows_for_remaining_depth(self):
        start = 1000.0
        deadline = 1037.0  # 37s budget like the failed run
        with mock.patch("server.time.time", return_value=1036.0):
            grown = _extend_clear_deadline(start, deadline, 337, 40)
        self.assertGreater(grown, deadline)
        self.assertLessEqual(grown, start + DEMO_QUEUE_CLEAR_MAX_TIMEOUT)
        with mock.patch("server.time.time", return_value=1036.0):
            unchanged = _extend_clear_deadline(start, deadline, 0, 0)
        self.assertEqual(unchanged, deadline)

    def test_residual_queue_ok_only_for_small_leftovers(self):
        with mock.patch("server.DEMO_QUEUE_CLEAR_STRAY_LIMIT", 2):
            self.assertFalse(_residual_queue_ok(0))
            self.assertTrue(_residual_queue_ok(1))
            self.assertFalse(_residual_queue_ok(37))
            self.assertFalse(_residual_queue_ok(337))

    @mock.patch("server.time.sleep")
    @mock.patch("server._batch_dequeue_items", return_value=3)
    @mock.patch("server._fetch_tenant_status")
    def test_wait_for_empty_queues_aborts_on_should_stop(
            self, fetch_status, _batch_dequeue, _sleep):
        fetch_status.return_value = SAMPLE_STATUS
        ok, removed, elapsed = wait_for_empty_queues(
            "http://web:9000",
            timeout=5,
            initial_scheduler_wait=0,
            should_stop=lambda: True,
        )
        self.assertFalse(ok)
        self.assertEqual(removed, 0)
        fetch_status.assert_not_called()

    @mock.patch("server.time.sleep")
    @mock.patch("server._batch_dequeue_items", return_value=3)
    @mock.patch("server._fetch_tenant_status")
    def test_wait_for_empty_queues_single_status_per_pass(
            self, fetch_status, batch_dequeue, _sleep):
        fetch_status.side_effect = [
            SAMPLE_STATUS,
            SAMPLE_STATUS,
            {"pipelines": []},
        ]
        ok, removed, elapsed = wait_for_empty_queues(
            "http://web:9000",
            timeout=5,
            initial_scheduler_wait=0,
        )
        self.assertTrue(ok)
        self.assertEqual(removed, 3)
        self.assertGreaterEqual(elapsed, 0)
        self.assertEqual(fetch_status.call_count, 3)
        batch_dequeue.assert_called_once()

    @mock.patch("server.time.sleep")
    @mock.patch("server._batch_dequeue_items")
    @mock.patch("server._fetch_tenant_status")
    def test_wait_for_empty_queues_happy_path_already_empty(
            self, fetch_status, batch_dequeue, _sleep):
        fetch_status.return_value = {"pipelines": []}
        ok, removed, elapsed = wait_for_empty_queues(
            "http://web:9000",
            timeout=5,
            initial_scheduler_wait=0,
        )
        self.assertTrue(ok)
        self.assertEqual(removed, 0)
        self.assertGreaterEqual(elapsed, 0)
        batch_dequeue.assert_not_called()

    @mock.patch("server.DEMO_QUEUE_STUCK_ROUNDS", 2)
    @mock.patch("server.time.sleep")
    @mock.patch("server._batch_dequeue_items", return_value=1)
    @mock.patch("server._fetch_tenant_status")
    def test_wait_for_empty_queues_requests_scheduler_purge_when_stuck(
            self, fetch_status, _batch_dequeue, _sleep):
        fetch_status.return_value = SAMPLE_STATUS
        purge = mock.Mock()
        ok, removed, _elapsed = wait_for_empty_queues(
            "http://web:9000",
            timeout=5,
            initial_scheduler_wait=0,
            request_scheduler_purge=purge,
        )
        self.assertFalse(ok)
        self.assertEqual(removed, 0)
        purge.assert_called()

    @mock.patch("server.DEMO_QUEUE_CLEAR_STRAY_LIMIT", 2)
    @mock.patch("server.time.sleep")
    @mock.patch("server._batch_dequeue_items", return_value=0)
    @mock.patch("server._fetch_tenant_status")
    def test_wait_for_empty_queues_accepts_small_residual(
            self, fetch_status, _batch_dequeue, _sleep):
        fetch_status.return_value = SAMPLE_ONE_ITEM
        ok, removed, elapsed = wait_for_empty_queues(
            "http://web:9000",
            timeout=0.4,
            initial_scheduler_wait=0,
        )
        self.assertTrue(ok)
        self.assertEqual(removed, 0)
        self.assertGreaterEqual(elapsed, 0)

    @mock.patch("server.DEMO_QUEUE_CLEAR_STRAY_LIMIT", 2)
    @mock.patch("server.time.sleep")
    @mock.patch("server._batch_dequeue_items", return_value=0)
    @mock.patch("server._fetch_tenant_status")
    def test_wait_for_empty_queues_fails_large_residual(
            self, fetch_status, _batch_dequeue, _sleep):
        fetch_status.return_value = SAMPLE_STATUS
        ok, removed, _elapsed = wait_for_empty_queues(
            "http://web:9000",
            timeout=0.4,
            initial_scheduler_wait=0,
        )
        self.assertFalse(ok)
        self.assertEqual(removed, 0)

    @mock.patch("server.time.sleep")
    @mock.patch("server._batch_dequeue_items", return_value=2)
    @mock.patch("server._fetch_tenant_status")
    def test_wait_for_empty_queues_drains_check_and_gate(
            self, fetch_status, batch_dequeue, _sleep):
        fetch_status.side_effect = [
            SAMPLE_CHECK_AND_GATE,
            SAMPLE_CHECK_AND_GATE,
            {"pipelines": []},
        ]
        ok, removed, _elapsed = wait_for_empty_queues(
            "http://web:9000",
            timeout=5,
            initial_scheduler_wait=0,
        )
        self.assertTrue(ok)
        self.assertEqual(removed, 2)
        batch_dequeue.assert_called()
        queued = batch_dequeue.call_args[0][1]
        self.assertEqual(queued[0][0], "check")


class AuditResetTests(unittest.TestCase):
    def test_audit_contains_demo_reset_filters_session(self):
        import tempfile
        with tempfile.NamedTemporaryFile(
                mode="w", suffix=".jsonl", delete=False, encoding="utf-8") as f:
            f.write(json.dumps({
                "timestamp": 100.0,
                "event": "demo_session_reset",
            }) + "\n")
            f.write(json.dumps({
                "timestamp": 200.0,
                "event": "demo_session_reset",
            }) + "\n")
            path = Path(f.name)
        try:
            with mock.patch("server.AUDIT_PATH", path):
                self.assertTrue(_audit_contains_demo_reset(150.0))
                self.assertFalse(_audit_contains_demo_reset(250.0))
        finally:
            path.unlink(missing_ok=True)

    def test_windows_at_baseline_from_rl_status(self):
        status = {
            "pipelines": [{
                "name": "gate",
                "rl_window": {
                    "queues": [{
                        "current_window": DEFAULT_INITIAL_WINDOW,
                        "tcp_shadow_window": DEFAULT_INITIAL_WINDOW,
                    }],
                },
            }],
        }
        with mock.patch("server._fetch_tenant_status", return_value=status):
            self.assertTrue(_windows_at_baseline())


if __name__ == "__main__":
    unittest.main()
