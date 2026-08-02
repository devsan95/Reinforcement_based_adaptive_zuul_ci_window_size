#!/usr/bin/env python3
"""Unit tests for RL effectiveness / minutes-saved calculations."""

from __future__ import annotations

import os
import sys
import time
import unittest
from pathlib import Path
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parent))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "analysis"))

import server  # noqa: E402
from server import (  # noqa: E402
    MINUTES_SAVED_FORMULA,
    _avg_gate_job_duration_sec,
    _changes_in_window_counts,
    _compute_effectiveness_metrics,
    _effective_demo_gate_wait_sec,
    _failure_impact_text,
    _format_build_change_id,
    _gate_queue_item_count,
    _integrate_parallel_change_seconds,
    _integrate_window_delta,
    _minutes_saved_from_change_seconds,
    _minutes_saved_from_failures,
    _parse_build_duration,
    adaptive_batch_size,
    extra_changes_in_flight,
    queue_is_saturated,
    queue_saturation_target,
)


class EffectivenessMetricsTests(unittest.TestCase):
    def test_integrate_window_delta_constant_parallelism_gain(self):
        slot_seconds = _integrate_window_delta(
            timestamps=[0.0, 60.0],
            rl_series=[25.0, 25.0],
            tcp_series=[20.0, 20.0],
            end_ts=120.0,
        )
        self.assertAlmostEqual(slot_seconds, 5.0 * 120.0)

    def test_integrate_window_delta_zero_when_rl_equals_tcp(self):
        slot_seconds = _integrate_window_delta(
            timestamps=[0.0, 100.0],
            rl_series=[20.0, 20.0],
            tcp_series=[20.0, 20.0],
            end_ts=200.0,
        )
        self.assertEqual(slot_seconds, 0.0)

    def test_minutes_saved_uses_job_duration(self):
        # Without failures: busy-only integral; idle queue → 0.
        idle = _compute_effectiveness_metrics(
            timestamps=[1000.0, 1060.0],
            rl_series=[24.0, 24.0],
            tcp_series=[20.0, 20.0],
            builds=[],
            failure_counts={"gate_jobs_total": 0, "gate_cycles_total": 0},
            session_start=1000.0,
            latest_rl_window=24.0,
            latest_tcp_window=20.0,
            latest_efficiency_pct=20.0,
            gate_queue_count=0,
            end_ts=1120.0,
        )
        self.assertAlmostEqual(idle["minutes_saved"], 0.0)

        # Discrete failure formula: 2 failures × extra 8 and 13 × 2s / 60
        failures = [
            {"rl_window": 18.0, "tcp_window_after": 10, "extra_changes": 8},
            {"rl_window": 18.0, "tcp_window_after": 5, "extra_changes": 13},
        ]
        with_fails = _compute_effectiveness_metrics(
            timestamps=[1000.0, 1060.0],
            rl_series=[24.0, 24.0],
            tcp_series=[20.0, 20.0],
            builds=[{
                "result": "SUCCESS",
                "start_time": 1000.0,
                "end_time": 1003.0,
            }, {
                "result": "FAILURE",
                "start_time": 1000.0,
                "end_time": 1001.0,
            }],
            failure_counts={"gate_jobs_total": 2, "gate_cycles_total": 0},
            session_start=1000.0,
            latest_rl_window=24.0,
            latest_tcp_window=20.0,
            latest_efficiency_pct=20.0,
            end_ts=1120.0,
            failures=failures,
        )
        # avg job (3+1)/2 = 2s; extras 8+13=21; 21*2/60 = 0.7; session=2min cap
        self.assertAlmostEqual(with_fails["minutes_saved"], 0.7)
        self.assertEqual(with_fails["job_duration_source"], "measured")
        self.assertIn("sum_over_failures", with_fails["minutes_saved_formula"])
        self.assertIn("impact_summary", with_fails)

    def test_minutes_saved_capped_at_session_wall_clock(self):
        failures = [
            {"extra_changes": 50},
            {"extra_changes": 50},
            {"extra_changes": 50},
        ]
        # 150 extras × 20s / 60 = 50 min raw, but session only 3 min → cap 3
        minutes = _minutes_saved_from_failures(
            failures, avg_job_duration_sec=20.0, session_duration_min=3.0)
        self.assertAlmostEqual(minutes, 3.0)

    def test_minutes_saved_zero_when_no_extra(self):
        minutes = _minutes_saved_from_failures(
            [{"extra_changes": 0}, {"rl_window": 10, "tcp_window_after": 10}],
            avg_job_duration_sec=15.0,
            session_duration_min=10.0,
        )
        self.assertEqual(minutes, 0.0)

    def test_queue_saturation_target_is_max_window_plus_margin(self):
        # margin default 4 (DEMO_QUEUE_SATURATION_MARGIN)
        self.assertEqual(queue_saturation_target(8.0, 4.0), 12)
        self.assertEqual(queue_saturation_target(3.0, 8.0), 12)

    def test_queue_is_saturated(self):
        self.assertTrue(queue_is_saturated(10, 8.0, 4.0))
        self.assertTrue(queue_is_saturated(8, 8.0, 4.0))
        self.assertFalse(queue_is_saturated(7, 8.0, 4.0))
        # Zero windows can never claim saturation.
        self.assertFalse(queue_is_saturated(5, 0.0, 0.0))

    def test_adaptive_batch_size_tops_up_shallow_queue(self):
        # depth 2, target 8+4=12 → shortfall 10 → base 10 + 10 capped at 20
        self.assertEqual(
            adaptive_batch_size(2, 8.0, 4.0, base_size=10, max_size=20), 20)
        # depth 9, target 12 → shortfall 3 → 13
        self.assertEqual(
            adaptive_batch_size(9, 8.0, 4.0, base_size=10, max_size=20), 13)
        # saturated queue → base batch unchanged
        self.assertEqual(
            adaptive_batch_size(15, 8.0, 4.0, base_size=10, max_size=20), 10)

    def test_extra_changes_in_flight(self):
        # Queue deeper than both windows → extra = RL − TCP
        self.assertEqual(extra_changes_in_flight(30, 20.0, 10.0), 10)
        # Queue between TCP and RL → extra = queue − TCP
        self.assertEqual(extra_changes_in_flight(15, 20.0, 10.0), 5)
        # Queue shallower than TCP → extra = 0 (root cause of old demo bug)
        self.assertEqual(extra_changes_in_flight(7, 20.0, 10.0), 0)
        self.assertEqual(extra_changes_in_flight(0, 20.0, 10.0), 0)

    def test_integrate_parallel_change_seconds_busy_only(self):
        # Idle queue → 0 (no runaway after drain)
        idle = _integrate_parallel_change_seconds(
            timestamps=[0.0, 60.0],
            rl_series=[25.0, 25.0],
            tcp_series=[20.0, 20.0],
            gate_queue_count=0,
            end_ts=120.0,
            busy_only=True,
        )
        self.assertEqual(idle, 0.0)

        # Busy saturated queue: only last segment integrates
        busy = _integrate_parallel_change_seconds(
            timestamps=[0.0, 60.0],
            rl_series=[25.0, 25.0],
            tcp_series=[20.0, 20.0],
            gate_queue_count=30,
            end_ts=120.0,
            busy_only=True,
        )
        # last segment 60→120: delta 5 * 60s
        self.assertAlmostEqual(busy, 5.0 * 60.0)

    def test_integrate_parallel_change_seconds_uses_live_queue(self):
        with_queue = _integrate_parallel_change_seconds(
            timestamps=[0.0, 60.0],
            rl_series=[25.0, 25.0],
            tcp_series=[20.0, 20.0],
            gate_queue_count=22,
            end_ts=120.0,
            busy_only=True,
        )
        # last segment only: min(22,25)-min(22,20) = 22-20 = 2 × 60s
        self.assertAlmostEqual(with_queue, 2.0 * 60.0)

    def test_format_build_change_id(self):
        build = {
            "ref": {"project": "test1", "change": "2263", "patchset": "1"},
            "job_name": "research-gate-job",
        }
        self.assertEqual(_format_build_change_id(build), "test1 2263,1")

    def test_failure_impact_text(self):
        text = _failure_impact_text(
            change_id="test1 2263,1",
            job_name="research-gate-job",
            tcp_before=20,
            tcp_after=10,
            rl_window=18.0,
            tcp_window=10.0,
            changes_in_window_delta=8,
            minutes_saved_so_far=4.5,
        )
        self.assertIn("test1 2263,1", text)
        self.assertIn("20→10", text)
        self.assertIn("RL held 18", text)
        self.assertIn("+8 extra", text)
        self.assertIn("Change", text)

    def test_session_advantage_not_zero_when_live_windows_reconverge(self):
        from server import (
            _compute_session_advantage,
            _failure_extra_changes,
            _build_comparison_table,
        )
        self.assertEqual(_failure_extra_changes(18, 10, 10), 8)
        failures = [
            {
                "rl_window": 18.0,
                "tcp_window": 10.0,
                "tcp_window_after": 10,
                "extra_changes": 8,
            },
            {
                "rl_window": 18.0,
                "tcp_window": 5.0,
                "tcp_window_after": 5,
                "extra_changes": 13,
            },
        ]
        # Live windows reconverged to 20/20 (the bug that made UI show 0%).
        adv = _compute_session_advantage(
            failures,
            rl_series=[20.0, 18.0, 18.0, 20.0],
            tcp_series=[20.0, 10.0, 5.0, 20.0],
            latest_rl=20.0,
            latest_tcp=20.0,
        )
        self.assertEqual(adv["extra_changes_total"], 21)
        # (8+13) / (10+5) * 100 = 140%
        self.assertAlmostEqual(adv["rl_advantage_pct"], 140.0)
        self.assertGreater(adv["peak_rl_advantage_pct"], 0)
        self.assertEqual(adv["advantage_source"], "failure_extra_over_tcp")

        cmp = _build_comparison_table(
            failures=failures,
            advantage=adv,
            effectiveness={"minutes_saved": 2.5},
            latest_rl=20.0,
            latest_tcp=20.0,
            baseline=20,
            failure_count=2,
        )
        self.assertEqual(cmp["extra_changes_total"], 21)
        self.assertIn("140.0%", cmp["summary"])
        self.assertEqual(len(cmp["rows"]), 5)

    def test_changes_in_window_counts_caps_at_window_size(self):
        self.assertEqual(_changes_in_window_counts(30, 24.0, 20.0), (24, 20))
        self.assertEqual(_changes_in_window_counts(15, 24.0, 20.0), (15, 15))
        self.assertEqual(_changes_in_window_counts(0, 24.0, 20.0), (0, 0))

    def test_gate_queue_item_count_includes_dependents(self):
        from test_demo_queue import SAMPLE_STATUS  # noqa: E402

        self.assertEqual(_gate_queue_item_count(SAMPLE_STATUS), 3)

    def test_effectiveness_includes_in_window_change_counts(self):
        effectiveness = _compute_effectiveness_metrics(
            timestamps=[1000.0],
            rl_series=[24.0],
            tcp_series=[20.0],
            builds=[],
            failure_counts={"gate_jobs_total": 0, "gate_cycles_total": 0},
            session_start=1000.0,
            latest_rl_window=24.0,
            latest_tcp_window=20.0,
            latest_efficiency_pct=20.0,
            changes_in_window_rl=18,
            changes_in_window_tcp=16,
            gate_queue_count=18,
            end_ts=1100.0,
        )
        self.assertEqual(effectiveness["changes_in_window_rl"], 18)
        self.assertEqual(effectiveness["changes_in_window_tcp"], 16)
        self.assertEqual(effectiveness["gate_queue_count"], 18)
        self.assertNotIn("est_changes_per_hour_rl", effectiveness)

    def test_avg_gate_job_duration_from_builds(self):
        builds = [{
            "result": "SUCCESS",
            "start_time": 1000.0,
            "end_time": 1002.0,
        }, {
            "result": "FAILURE",
            "duration": 4.0,
        }]
        avg, source = _avg_gate_job_duration_sec(builds)
        self.assertEqual(source, "measured")
        self.assertAlmostEqual(avg, 3.0)
        self.assertAlmostEqual(_parse_build_duration(builds[0]), 2.0)

        effectiveness = _compute_effectiveness_metrics(
            timestamps=[1000.0],
            rl_series=[20.0],
            tcp_series=[10.0],
            builds=builds,
            failure_counts={"gate_jobs_total": 1, "gate_cycles_total": 0},
            session_start=1000.0,
            latest_rl_window=20.0,
            latest_tcp_window=10.0,
            latest_efficiency_pct=100.0,
            end_ts=1100.0,
        )
        self.assertEqual(effectiveness["session_changes_merged"], 1)
        self.assertEqual(effectiveness["session_gate_failures"], 1)

    def test_demo_gate_wait_scales_with_change_count(self):
        with mock.patch.object(server, "DEMO_BATCH_INTERVAL_SEC", 30.0), \
             mock.patch.dict(os.environ, {}, clear=True):
            # Continuous default: max(90, 30*2+60) = 120
            self.assertEqual(_effective_demo_gate_wait_sec(), 120.0)
        with mock.patch.dict(os.environ, {"DEMO_GATE_WAIT_SEC": "200"}):
            self.assertEqual(_effective_demo_gate_wait_sec(), 200.0)

    def test_build_live_metrics_failure_details(self):
        base = time.time() - 120.0
        audit = [
            {"timestamp": base, "event": "agent_started"},
            {
                "timestamp": base + 10.0,
                "event": "agent_tick",
                "actual_window": 18,
                "tcp_shadow_window": 10,
            },
            {
                "timestamp": base + 12.0,
                "event": "tcp_shadow",
                "succeeded": False,
                "window_before": 20,
                "window_after": 10,
                "failures": [{
                    "project": "test1",
                    "change": "2263,1",
                    "job_name": "research-gate-job",
                    "result": "FAILURE",
                    "uuid": "abc-123",
                }],
            },
        ]
        builds = [{
            "pipeline": "gate",
            "job_name": "research-gate-job",
            "result": "FAILURE",
            "uuid": "build-uuid-1",
            "start_time": base + 11.0,
            "end_time": base + 12.5,
            "ref": {"project": "test1", "change": "2264", "patchset": "1"},
        }]
        with mock.patch.object(server, "_load_audit_events", return_value=audit), \
             mock.patch.object(server, "fetch_builds", return_value=builds), \
             mock.patch.object(server, "_effective_session_start",
                               return_value=base), \
             mock.patch.object(server, "_fetch_live_gate_state",
                               return_value=None):
            metrics = server.build_live_metrics()

        self.assertTrue(metrics["failures"])
        build_fail = [f for f in metrics["failures"] if f["source"] == "build"]
        self.assertEqual(len(build_fail), 1)
        self.assertEqual(build_fail[0]["change"], "test1 2264,1")
        self.assertEqual(build_fail[0]["uuid"], "build-uuid-1")
        self.assertEqual(build_fail[0]["result"], "FAILURE")
        self.assertIn("impact_text", build_fail[0])
        self.assertIn("test1 2264,1", build_fail[0]["impact_text"])
        self.assertIn("impact_summary", metrics["effectiveness"])


if __name__ == "__main__":
    unittest.main()
