#!/usr/bin/env python3
"""Unit tests for /run-demo param parsing and fail-stamp distribution."""

from __future__ import annotations

import sys
import unittest
from pathlib import Path
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parent))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "analysis"))

import server  # noqa: E402
from server import (  # noqa: E402
    DEMO_CHANGE_COUNT,
    DEMO_EXPECTED_FAILURES,
    DEMO_MAX_GATE_FAILURES,
    DEMO_MAX_TOTAL_CHANGES,
    default_demo_targets,
    downsample_keep_span,
    fails_for_batch,
    fails_for_duration_batch,
    job_runs_saved_est,
    parse_run_demo_params,
)


class ParseRunDemoParamsTests(unittest.TestCase):
    def test_empty_body_uses_defaults(self):
        params, err = parse_run_demo_params({})
        self.assertIsNone(err)
        self.assertIsNone(params["total_changes"])
        self.assertIsNone(params["gate_failures"])
        self.assertEqual(params["expected_failures"], DEMO_EXPECTED_FAILURES)

    def test_none_body_uses_defaults(self):
        params, err = parse_run_demo_params(None)
        self.assertIsNone(err)
        self.assertEqual(params["expected_failures"], DEMO_EXPECTED_FAILURES)

    def test_absolute_targets(self):
        params, err = parse_run_demo_params({
            "total_changes": 100,
            "gate_failures": 10,
        })
        self.assertIsNone(err)
        self.assertEqual(params["total_changes"], 100)
        self.assertEqual(params["gate_failures"], 10)
        self.assertEqual(params["expected_failures"], 10)

    def test_fail_count_alias(self):
        params, err = parse_run_demo_params({
            "total_changes": 50,
            "fail_count": 5,
        })
        self.assertIsNone(err)
        self.assertEqual(params["gate_failures"], 5)
        self.assertEqual(params["expected_failures"], 5)

    def test_total_only_derives_expected_from_ratio(self):
        with mock.patch.object(server, "DEMO_BATCH_SIZE", 10), \
                mock.patch.object(server, "DEMO_FAIL_PER_BATCH", 5):
            params, err = parse_run_demo_params({"total_changes": 100})
        self.assertIsNone(err)
        self.assertEqual(params["total_changes"], 100)
        self.assertIsNone(params["gate_failures"])
        self.assertEqual(params["expected_failures"], 50)

    def test_failures_only_sets_expected(self):
        params, err = parse_run_demo_params({"gate_failures": 12})
        self.assertIsNone(err)
        self.assertIsNone(params["total_changes"])
        self.assertEqual(params["gate_failures"], 12)
        self.assertEqual(params["expected_failures"], 12)

    def test_rejects_failures_gt_total(self):
        params, err = parse_run_demo_params({
            "total_changes": 10,
            "gate_failures": 11,
        })
        self.assertIsNone(params)
        self.assertIn("<=", err)

    def test_rejects_negative(self):
        params, err = parse_run_demo_params({"total_changes": -1})
        self.assertIsNone(params)
        self.assertIn(">= 0", err)

    def test_rejects_non_integer(self):
        params, err = parse_run_demo_params({"gate_failures": "abc"})
        self.assertIsNone(params)
        self.assertIn("integer", err)

    def test_rejects_over_cap(self):
        params, err = parse_run_demo_params({
            "total_changes": DEMO_MAX_TOTAL_CHANGES + 1,
        })
        self.assertIsNone(params)
        self.assertIn("<=", err)
        params, err = parse_run_demo_params({
            "gate_failures": DEMO_MAX_GATE_FAILURES + 1,
        })
        self.assertIsNone(params)
        self.assertIn("<=", err)

    def test_zero_targets_allowed(self):
        params, err = parse_run_demo_params({
            "total_changes": 0,
            "gate_failures": 0,
        })
        self.assertIsNone(err)
        self.assertEqual(params["total_changes"], 0)
        self.assertEqual(params["gate_failures"], 0)
        self.assertEqual(params["expected_failures"], 0)


class FailsForBatchTests(unittest.TestCase):
    def test_proportional_distribution(self):
        # 100 remaining, 10 fails, batch 10 → 1 fail
        self.assertEqual(fails_for_batch(10, 100, 10), 1)

    def test_exact_total_across_batches(self):
        remaining_changes = 100
        remaining_fails = 10
        stamped = 0
        while remaining_changes > 0:
            size = min(10, remaining_changes)
            n = fails_for_batch(size, remaining_changes, remaining_fails)
            stamped += n
            remaining_fails -= n
            remaining_changes -= size
        self.assertEqual(stamped, 10)
        self.assertEqual(remaining_fails, 0)

    def test_exact_total_adaptive_batch_sizes(self):
        """Variable batch sizes (adaptive saturation) still hit exact N."""
        remaining_changes = 47
        remaining_fails = 13
        stamped = 0
        sizes = [8, 12, 15, 10, 2]
        for size in sizes:
            if remaining_changes <= 0:
                break
            size = min(size, remaining_changes)
            n = fails_for_batch(size, remaining_changes, remaining_fails)
            self.assertGreaterEqual(n, 0)
            self.assertLessEqual(n, size)
            stamped += n
            remaining_fails -= n
            remaining_changes -= size
        self.assertEqual(stamped, 13)
        self.assertEqual(remaining_fails, 0)

    def test_forces_late_fails_when_needed(self):
        # 5 left, 5 fails left, batch 5 → must stamp all 5
        self.assertEqual(fails_for_batch(5, 5, 5), 5)

    def test_zero_when_no_fails_left(self):
        self.assertEqual(fails_for_batch(10, 50, 0), 0)

    def test_never_exceeds_batch(self):
        self.assertEqual(fails_for_batch(3, 3, 10), 3)

    def test_all_fails_request(self):
        remaining_changes = 20
        remaining_fails = 20
        stamped = 0
        while remaining_changes > 0:
            size = min(7, remaining_changes)
            n = fails_for_batch(size, remaining_changes, remaining_fails)
            stamped += n
            remaining_fails -= n
            remaining_changes -= size
        self.assertEqual(stamped, 20)


class FailsForDurationBatchTests(unittest.TestCase):
    def test_spreads_and_finishes_exact(self):
        remaining = 11
        stamped = 0
        for left in (4, 3, 2, 1):
            n = fails_for_duration_batch(10, remaining, left)
            stamped += n
            remaining -= n
        self.assertEqual(stamped, 11)
        self.assertEqual(remaining, 0)

    def test_last_batch_takes_remainder(self):
        self.assertEqual(fails_for_duration_batch(10, 7, 1), 7)

    def test_never_exceeds_batch(self):
        self.assertEqual(fails_for_duration_batch(3, 20, 2), 3)


class DownsampleKeepSpanTests(unittest.TestCase):
    def test_keeps_first_and_last(self):
        items = list(range(1000))
        out = downsample_keep_span(items, 50)
        self.assertEqual(out[0], 0)
        self.assertEqual(out[-1], 999)
        self.assertLessEqual(len(out), 50)

    def test_short_series_unchanged(self):
        items = [1, 2, 3]
        self.assertEqual(downsample_keep_span(items, 100), items)


class DefaultDemoTargetsTests(unittest.TestCase):
    def test_matches_module_defaults(self):
        d = default_demo_targets()
        self.assertEqual(d["total_changes"], DEMO_CHANGE_COUNT)
        self.assertEqual(d["gate_failures"], DEMO_EXPECTED_FAILURES)
        self.assertEqual(d["expected_failures"], DEMO_EXPECTED_FAILURES)


class BatchesPlanTipTests(unittest.TestCase):
    def test_duration_mode_tip(self):
        tip = server._batches_plan_tip(15, duration_sec=300)
        self.assertIn("Y = 15", tip)
        self.assertIn("min session", tip)
        self.assertIn("interval", tip)

    def test_changes_mode_tip(self):
        tip = server._batches_plan_tip(15, target_total=150)
        self.assertIn("Y = 15", tip)
        self.assertIn("150 changes", tip)
        self.assertIn("per batch", tip)


class JobRunsSavedTests(unittest.TestCase):
    def test_equals_extras(self):
        self.assertEqual(job_runs_saved_est(21), 21)
        self.assertEqual(job_runs_saved_est(0), 0)
        self.assertEqual(job_runs_saved_est(-3), 0)


class SessionSummaryJobRunsInvariantTests(unittest.TestCase):
    def test_mismatch_flagged(self):
        from server import session_summary_invariants
        bad = {
            "extra_changes_total": 10,
            "job_runs_saved": 9,
            "minutes_saved": 0,
            "rl_advantage_pct": 50,
            "gate_failures": 2,
            "rl_held_peak": 18,
            "tcp_after_floor": 8,
            "advantage_source": "failure_extra_over_tcp",
            "changes_submitted": 20,
            "submitted": 20,
            "merged": 5,
            "session_changes_merged": 5,
        }
        violations = session_summary_invariants(bad)
        self.assertTrue(any("job_runs_saved" in v for v in violations))

    def test_aligned_ok(self):
        from server import session_summary_invariants
        good = {
            "extra_changes_total": 10,
            "job_runs_saved": 10,
            "minutes_saved": 0.2,
            "rl_advantage_pct": 50,
            "gate_failures": 2,
            "rl_held_peak": 18,
            "tcp_after_floor": 8,
            "advantage_source": "failure_extra_over_tcp",
            "changes_submitted": 20,
            "submitted": 20,
            "merged": 5,
            "session_changes_merged": 5,
        }
        self.assertEqual(session_summary_invariants(good), [])

    def test_alias_mismatch_flagged(self):
        from server import session_summary_invariants
        bad = {
            "extra_changes_total": 0,
            "job_runs_saved": 0,
            "minutes_saved": 0,
            "rl_advantage_pct": 0,
            "gate_failures": 0,
            "changes_submitted": 10,
            "submitted": 9,
            "merged": 3,
            "session_changes_merged": 3,
        }
        violations = session_summary_invariants(bad)
        self.assertTrue(any("submitted" in v for v in violations))

    def test_merged_alias_mismatch_flagged(self):
        from server import session_summary_invariants
        bad = {
            "extra_changes_total": 0,
            "job_runs_saved": 0,
            "minutes_saved": 0,
            "rl_advantage_pct": 0,
            "gate_failures": 0,
            "changes_submitted": 10,
            "submitted": 10,
            "merged": 3,
            "session_changes_merged": 4,
        }
        violations = session_summary_invariants(bad)
        self.assertTrue(any("merged" in v for v in violations))


if __name__ == "__main__":
    unittest.main()
