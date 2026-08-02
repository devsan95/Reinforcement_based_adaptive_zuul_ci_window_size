#!/usr/bin/env python3
"""Unit tests for live-metrics failure count aggregation."""

from __future__ import annotations

import json
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "analysis"))

from server import (  # noqa: E402
    _compute_failure_counts,
    _is_gate_job_failure,
    _last_rl_shrink_ts,
    _last_tcp_shrink_ts,
)


class FailureCountTests(unittest.TestCase):
    def test_gate_job_failure_detection(self):
        self.assertFalse(_is_gate_job_failure({"result": "SUCCESS"}))
        self.assertFalse(_is_gate_job_failure({"result": "MERGE"}))
        self.assertTrue(_is_gate_job_failure({"result": "FAILURE"}))

    def test_tcp_shrink_detection(self):
        events = [
            {"event": "tcp_shadow", "timestamp": 10.0,
             "window_before": 21, "window_after": 10, "succeeded": False},
            {"event": "tcp_shadow", "timestamp": 11.0,
             "window_before": 10, "window_after": 5, "succeeded": False},
        ]
        self.assertEqual(_last_tcp_shrink_ts(events), 11.0)

    def test_rl_shrink_detection(self):
        ticks = [
            {"timestamp": 1.0, "actual_window": 20},
            {"timestamp": 2.0, "actual_window": 18},
            {"timestamp": 3.0, "actual_window": 16},
        ]
        self.assertEqual(_last_rl_shrink_ts(ticks), 3.0)

    def test_compute_failure_counts_from_fixture(self):
        audit_path = (
            Path(__file__).resolve().parents[1]
            / "results" / "20260617-122109" / "audit.jsonl"
        )
        if not audit_path.is_file():
            self.skipTest("fixture audit not available")
        events = [
            json.loads(line)
            for line in audit_path.read_text(encoding="utf-8").splitlines()
            if line.strip()
        ]
        session_start = min(
            float(e["timestamp"]) for e in events if e.get("event") == "agent_started"
        )
        ticks = [
            e for e in events
            if e.get("event") == "agent_tick"
            and float(e["timestamp"]) >= session_start
        ]
        builds = [
            {
                "_ts": 1781690341.5,
                "result": "FAILURE",
                "job_name": "gate-job",
                "pipeline": "gate",
            },
            {
                "_ts": 1781690400.0,
                "result": "SUCCESS",
                "job_name": "gate-job",
                "pipeline": "gate",
            },
        ]
        counts = _compute_failure_counts(
            events, builds, ticks, session_start)
        self.assertGreaterEqual(counts["gate_cycles_total"], 1)
        self.assertGreaterEqual(counts["tcp_shrinks_total"], 1)
        self.assertEqual(counts["gate_jobs_total"], 1)
        self.assertIn("gate_cycles_since_tcp_shrink", counts)
        self.assertIn("gate_jobs_since_rl_shrink", counts)


if __name__ == "__main__":
    unittest.main()
