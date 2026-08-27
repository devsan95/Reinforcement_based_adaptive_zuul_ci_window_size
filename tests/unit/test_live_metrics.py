#!/usr/bin/env python3
# Copyright 2026 Santoshkumar Vagga
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import json
import sys
import time
import unittest
from pathlib import Path
from unittest import mock

RESEARCH_DIR = Path(__file__).resolve().parents[2] / "research" / "control"
ANALYSIS_DIR = Path(__file__).resolve().parents[2] / "research" / "analysis"
sys.path.insert(0, str(ANALYSIS_DIR))
sys.path.insert(0, str(RESEARCH_DIR))

import server  # noqa: E402


class LiveMetricsTests(unittest.TestCase):
  def test_tcp_series_follows_gate_cycle_shadow_events(self):
    events = [
      {
        "timestamp": 100.0,
        "event": "agent_started",
      },
      {
        "timestamp": 101.0,
        "event": "agent_tick",
        "actual_window": 20,
        "recommended_window": 18,
        "tcp_shadow_window": 20,
        "mode": "active",
      },
      {
        "timestamp": 110.0,
        "event": "tcp_shadow",
        "succeeded": True,
        "window_before": 20,
        "window_after": 21,
      },
      {
        "timestamp": 115.0,
        "event": "tcp_shadow",
        "succeeded": False,
        "window_before": 21,
        "window_after": 10,
      },
      {
        "timestamp": 160.0,
        "event": "agent_tick",
        "actual_window": 18,
        "recommended_window": 16,
        "tcp_shadow_window": 10,
        "mode": "active",
      },
    ]
    ticks = server._agent_ticks(events, session_start=100.0)
    tcp_events = server._tcp_shadow_events(events, session_start=100.0)
    timestamps, rl_series, tcp_series, _, _ = server._build_window_series(
      ticks, tcp_events, builds=[], session_start=100.0)

    self.assertIn(21.0, tcp_series)
    self.assertIn(10.0, tcp_series)
    self.assertNotEqual(tcp_series, [20.0] * len(tcp_series))
    self.assertEqual(rl_series[-1], 18.0)
    self.assertEqual(tcp_series[-1], 10.0)
    self.assertGreaterEqual(rl_series[-1], tcp_series[-1])
    self.assertGreater(len(timestamps), 3)

  def test_build_live_metrics_uses_audit_tcp_shadow(self):
    base = time.time() - 120.0
    audit_lines = [
      json.dumps({
        "timestamp": base,
        "event": "agent_started",
      }),
      json.dumps({
        "timestamp": base + 1.0,
        "event": "agent_tick",
        "actual_window": 20,
        "recommended_window": 18,
        "tcp_shadow_window": 20,
        "mode": "active",
      }),
      json.dumps({
        "timestamp": base + 5.0,
        "event": "tcp_shadow",
        "succeeded": False,
        "window_before": 20,
        "window_after": 10,
      }),
    ]
    with mock.patch.object(server, "_load_audit_events", return_value=[
      json.loads(line) for line in audit_lines
    ]), \
         mock.patch.object(server, "fetch_builds", return_value=[]), \
         mock.patch.object(server, "_effective_session_start",
                           return_value=base):
      metrics = server.build_live_metrics()

    self.assertIn(10.0, metrics["tcp_window"])
    self.assertEqual(metrics["latest"]["tcp_window"], 10.0)
    self.assertGreaterEqual(
        metrics["latest"]["rl_window"], metrics["latest"]["tcp_window"])

  def test_live_metrics_uses_applied_rl_window(self):
    base = time.time() - 60.0
    audit_lines = [
      json.dumps({
        "timestamp": base,
        "event": "agent_started",
      }),
      json.dumps({
        "timestamp": base + 1.0,
        "event": "agent_tick",
        "actual_window": 20,
        "recommended_window": 18,
        "tcp_shadow_window": 20,
        "mode": "active",
      }),
      json.dumps({
        "timestamp": base + 5.0,
        "event": "tcp_shadow",
        "succeeded": False,
        "window_before": 20,
        "window_after": 10,
        "failures": [{
          "project": "test1",
          "change": "200,1",
          "job_name": "research-gate-job",
          "result": "FAILURE",
        }],
      }),
      json.dumps({
        "timestamp": base + 6.0,
        "event": "agent_tick",
        "actual_window": 18,
        "recommended_window": 10,
        "tcp_shadow_window": 10,
        "mode": "active",
      }),
    ]
    with mock.patch.object(server, "_load_audit_events", return_value=[
      json.loads(line) for line in audit_lines
    ]), \
         mock.patch.object(server, "fetch_builds", return_value=[]), \
         mock.patch.object(server, "_effective_session_start",
                           return_value=base), \
         mock.patch.object(server, "_fetch_live_gate_state",
                           return_value=None):
      metrics = server.build_live_metrics()

    self.assertEqual(metrics["latest"]["tcp_window"], 10.0)
    self.assertEqual(metrics["latest"]["rl_window"], 18.0)
    self.assertEqual(metrics["latest"]["parallelism_gain"], 8.0)
    # At failure time RL was still 20; TCP after shrink = 10 → 10 extras.
    self.assertEqual(metrics["latest"]["extra_changes_total"], 10)
    self.assertAlmostEqual(metrics["latest"]["throughput_efficiency_pct"], 100.0)
    self.assertGreater(metrics["effectiveness"]["minutes_saved"], 0.0)

  def test_live_metrics_from_zuul_status_windows(self):
    base = time.time() - 30.0
    with mock.patch.object(server, "_load_audit_events", return_value=[
      {"timestamp": base, "event": "agent_started"},
    ]), \
         mock.patch.object(server, "fetch_builds", return_value=[]), \
         mock.patch.object(server, "_effective_session_start",
                           return_value=base), \
         mock.patch.object(server, "_fetch_live_gate_state", return_value={
           "rl_window": 20.0,
           "tcp_window": 18.0,
           "gate_queue_count": 25,
           "changes_in_window_rl": 20,
           "changes_in_window_tcp": 18,
         }):
      metrics = server.build_live_metrics()

    self.assertEqual(metrics["latest"]["rl_window"], 20.0)
    self.assertEqual(metrics["latest"]["tcp_window"], 18.0)
    self.assertEqual(metrics["latest"]["parallelism_gain"], 2.0)
    self.assertEqual(metrics["latest"]["changes_in_window_rl"], 20)
    self.assertEqual(metrics["latest"]["changes_in_window_tcp"], 18)
    self.assertEqual(metrics["latest"]["gate_queue_count"], 25)
    self.assertEqual(metrics["latest"]["extra_in_flight"], 2)
    # No gate failures yet → session total extras is 0; live in-flight is 2.
    self.assertEqual(metrics["latest"]["extra_changes_total"], 0)
    self.assertAlmostEqual(
        metrics["latest"]["throughput_efficiency_pct"], 11.1)
    self.assertNotIn(
        "est_changes_per_hour_rl", metrics["effectiveness"])


  def test_live_metrics_session_extras_survive_drained_queue(self):
    """After failures, drained queue must not zero session extras / minutes."""
    base = time.time() - 180.0
    audit = [
      {"timestamp": base, "event": "agent_started"},
      {
        "timestamp": base + 10.0,
        "event": "agent_tick",
        "actual_window": 18,
        "tcp_shadow_window": 10,
        "mode": "active",
      },
      {
        "timestamp": base + 12.0,
        "event": "tcp_shadow",
        "succeeded": False,
        "window_before": 20,
        "window_after": 10,
        "failures": [{
          "project": "test1",
          "change": "100,1",
          "job_name": "research-gate-job",
          "result": "FAILURE",
        }],
      },
      {
        "timestamp": base + 40.0,
        "event": "tcp_shadow",
        "succeeded": False,
        "window_before": 10,
        "window_after": 5,
        "failures": [{
          "project": "test1",
          "change": "101,1",
          "job_name": "research-gate-job",
          "result": "FAILURE",
        }],
      },
      {
        "timestamp": base + 90.0,
        "event": "agent_tick",
        "actual_window": 20,
        "tcp_shadow_window": 20,
        "mode": "active",
      },
    ]
    builds = [{
      "pipeline": "gate",
      "job_name": "research-gate-job",
      "result": "SUCCESS",
      "start_time": base + 15.0,
      "end_time": base + 35.0,
    }]
    with mock.patch.object(server, "_load_audit_events", return_value=audit), \
         mock.patch.object(server, "fetch_builds", return_value=builds), \
         mock.patch.object(server, "_effective_session_start",
                           return_value=base), \
         mock.patch.object(server, "_fetch_live_gate_state", return_value={
           "rl_window": 20.0,
           "tcp_window": 20.0,
           "gate_queue_count": 0,
           "changes_in_window_rl": 0,
           "changes_in_window_tcp": 0,
         }):
      metrics = server.build_live_metrics()

    latest = metrics["latest"]
    adv = metrics["advantage"]
    summary = metrics["session_summary"]
    self.assertEqual(latest["extra_in_flight"], 0)
    self.assertGreater(latest["extra_changes_total"], 0)
    self.assertEqual(
        latest["extra_changes_total"], adv["extra_changes_total"])
    self.assertEqual(
        summary["extra_changes_total"], latest["extra_changes_total"])
    self.assertEqual(summary["live"]["extra_in_flight"], 0)
    self.assertGreater(latest["minutes_saved"], 0.0)
    self.assertEqual(summary["minutes_saved"], latest["minutes_saved"])
    self.assertGreater(latest["rl_advantage_pct"], 0.0)
    self.assertEqual(
        summary["rl_advantage_pct"], latest["rl_advantage_pct"])
    self.assertEqual(
        summary["gate_failures"], metrics["effectiveness"]["session_gate_failures"])
    self.assertEqual(
        summary["job_runs_saved"], summary["extra_changes_total"])
    self.assertEqual(
        latest["job_runs_saved"], summary["job_runs_saved"])
    self.assertEqual(summary["merged"], summary["session_changes_merged"])
    self.assertEqual(summary["submitted"], summary["changes_submitted"])
    self.assertIn("changes_submitted", summary)
    self.assertIn("merged", summary)
    self.assertGreaterEqual(summary["changes_submitted"], 0)
    self.assertIn("expected_failures", metrics)
    self.assertIn("total extra", metrics["effectiveness"]["impact_summary"])
    self.assertNotRegex(
        metrics["effectiveness"]["impact_summary"],
        r"\b0 total extra")
    from server import session_summary_invariants
    self.assertEqual(session_summary_invariants(summary), [])
    # Held windows come from failures, not drained live 20/20.
    self.assertIsNotNone(summary["rl_held_peak"])
    self.assertIsNotNone(summary["tcp_after_floor"])
    self.assertLess(summary["tcp_after_floor"], summary["rl_held_peak"])

  def test_session_summary_submitted_from_demo_progress(self):
    """session_summary.changes_submitted follows demo_progress STATE."""
    import server
    from unittest import mock
    base = 1_700_000_000.0
    with mock.patch.object(server, "_load_audit_events", return_value=[]), \
         mock.patch.object(server, "fetch_builds", return_value=[]), \
         mock.patch.object(server, "_effective_session_start",
                           return_value=base), \
         mock.patch.object(server, "_fetch_live_gate_state", return_value=None):
      with server.LOCK:
        prev_prog = dict(server.STATE.get("demo_progress") or {})
        prog = dict(prev_prog)
        prog["changes_submitted"] = 37
        server.STATE["demo_progress"] = prog
      try:
        metrics = server.build_live_metrics()
      finally:
        with server.LOCK:
          server.STATE["demo_progress"] = prev_prog
    summary = metrics["session_summary"]
    self.assertEqual(summary["changes_submitted"], 37)
    self.assertEqual(summary["submitted"], 37)
    self.assertEqual(summary["merged"], summary["session_changes_merged"])
    from server import session_summary_invariants
    self.assertEqual(session_summary_invariants(summary), [])

  def test_live_metrics_demo_change_count_follows_state(self):
    """/live-metrics must surface run-demo total_changes, not only the default."""
    import server
    from unittest import mock
    base = 1_700_000_000.0
    with mock.patch.object(server, "_load_audit_events", return_value=[]), \
         mock.patch.object(server, "fetch_builds", return_value=[]), \
         mock.patch.object(server, "_effective_session_start",
                           return_value=base), \
         mock.patch.object(server, "_fetch_live_gate_state", return_value=None):
      with server.LOCK:
        prev_total = server.STATE.get("demo_total_changes")
        prev_exp = server.STATE.get("demo_expected_failures")
        server.STATE["demo_total_changes"] = 42
        server.STATE["demo_expected_failures"] = 7
      try:
        metrics = server.build_live_metrics()
      finally:
        with server.LOCK:
          server.STATE["demo_total_changes"] = prev_total
          server.STATE["demo_expected_failures"] = prev_exp
    self.assertEqual(metrics["demo_change_count"], 42)
    self.assertEqual(metrics["expected_failures"], 7)


if __name__ == "__main__":
  unittest.main()
