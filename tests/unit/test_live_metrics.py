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
    self.assertEqual(metrics["latest"]["throughput_efficiency_pct"], 80.0)
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
    self.assertAlmostEqual(
        metrics["latest"]["throughput_efficiency_pct"], 11.1)
    self.assertGreater(metrics["effectiveness"]["minutes_saved"], 0.0)
    self.assertNotIn(
        "est_changes_per_hour_rl", metrics["effectiveness"])


if __name__ == "__main__":
  unittest.main()
