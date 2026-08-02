#!/usr/bin/env python3
"""Unit tests for gate build activity and session scoping."""

from __future__ import annotations

import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "analysis"))

from server import (  # noqa: E402
    STATE,
    _effective_session_start,
    _gate_build_activity,
    _is_gate_pipeline_build,
)


class GateBuildActivityTests(unittest.TestCase):
    def test_is_gate_pipeline_build(self):
        self.assertTrue(_is_gate_pipeline_build({
            "pipeline": "gate", "job_name": "research-gate-job"}))
        self.assertFalse(_is_gate_pipeline_build({
            "pipeline": "check", "job_name": "noop"}))

    def test_gate_build_activity_counts_failures(self):
        session = 1783742500.0
        builds = [
            {
                "pipeline": "gate",
                "job_name": "research-gate-job",
                "result": "FAILURE",
                "end_time": "2026-07-11T04:10:00+00:00",
            },
            {
                "pipeline": "gate",
                "job_name": "research-gate-job",
                "result": "RUNNING",
                "start_time": "2026-07-11T04:10:05+00:00",
            },
            {
                "pipeline": "gate",
                "job_name": "research-gate-job",
                "result": "SUCCESS",
                "end_time": "2026-06-01T04:10:00+00:00",
            },
        ]
        stats = _gate_build_activity(builds, session)
        self.assertEqual(stats["total"], 2)
        self.assertEqual(stats["failed"], 1)
        self.assertEqual(stats["running"], 1)

    def test_effective_session_start_prefers_demo_marker(self):
        events = [
            {"event": "agent_started", "timestamp": 2000.0},
        ]
        STATE["demo_session_start"] = 1000.0
        try:
            self.assertEqual(_effective_session_start(events), 1000.0)
        finally:
            STATE["demo_session_start"] = None


if __name__ == "__main__":
    unittest.main()
