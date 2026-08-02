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
    _all_queue_depth,
    _all_queue_item_count,
    _audit_contains_demo_reset,
    _describe_queue_backlog,
    _effective_queue_clear_timeout,
    _format_queue_change_label,
    _gate_queue_depth,
    _iter_all_queue_items,
    _iter_dequeue_targets,
    _iter_gate_queue_items,
    _item_from_queue_head,
    _normalize_dequeue_change,
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

    def test_effective_queue_clear_timeout_scales_with_backlog(self):
        small = _effective_queue_clear_timeout(10, 5)
        large = _effective_queue_clear_timeout(199, 159)
        self.assertGreater(large, small)
        self.assertGreaterEqual(large, 25)
        self.assertGreaterEqual(small, 10)

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
                        "current_window": 20,
                        "tcp_shadow_window": 20,
                    }],
                },
            }],
        }
        with mock.patch("server._fetch_tenant_status", return_value=status):
            self.assertTrue(_windows_at_baseline())


if __name__ == "__main__":
    unittest.main()
