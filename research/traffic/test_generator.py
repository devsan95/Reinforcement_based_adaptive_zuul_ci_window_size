#!/usr/bin/env python3
"""Unit tests for traffic generator fail-stamp planning."""

from __future__ import annotations

import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from generator import (  # noqa: E402
    INDEPENDENT_TOPICS,
    TOPICS,
    batch_should_fail,
    plan_batch_specs,
)


class BatchShouldFailTests(unittest.TestCase):
    def test_first_n_fail(self):
        fails = [i for i in range(10) if batch_should_fail(i, 3)]
        self.assertEqual(fails, [0, 1, 2])

    def test_zero_fail(self):
        self.assertFalse(any(batch_should_fail(i, 0) for i in range(10)))

    def test_all_fail(self):
        self.assertTrue(all(batch_should_fail(i, 10) for i in range(10)))


class PlanBatchSpecsTests(unittest.TestCase):
    def test_exact_fail_count_disables_depend(self):
        for batch_num in range(6):
            topic, specs = plan_batch_specs(
                batch_num, 10, 5,
                prior_change_ids=["I" + "a" * 40],
                enable_depend=False,
            )
            self.assertIn(topic, INDEPENDENT_TOPICS)
            self.assertNotEqual(topic, "demo-depend")
            self.assertEqual(sum(1 for _i, fail, _d in specs if fail), 5)
            self.assertTrue(all(dep is None for _i, _f, dep in specs))

    def test_depend_never_on_fail_stamps(self):
        topic, specs = plan_batch_specs(
            2, 10, 8,
            prior_change_ids=["I" + "b" * 40],
            enable_depend=True,
        )
        self.assertEqual(topic, "demo-depend")
        for _i, should_fail, depends_on in specs:
            if should_fail:
                self.assertIsNone(depends_on)
        # Pass indices in the later half may depend.
        pass_deps = [
            dep for i, fail, dep in specs if not fail and i >= 5]
        self.assertTrue(any(d is not None for d in pass_deps))

    def test_exact_fail_count_all_fails_no_depend(self):
        topic, specs = plan_batch_specs(
            2, 10, 10,
            prior_change_ids=["I" + "c" * 40],
            enable_depend=True,
        )
        self.assertEqual(sum(1 for _i, fail, _d in specs if fail), 10)
        self.assertTrue(all(dep is None for _i, _f, dep in specs))

    def test_topic_rotation_with_depend(self):
        topics = [
            plan_batch_specs(n, 10, 5, enable_depend=True)[0]
            for n in range(3)
        ]
        self.assertEqual(topics, list(TOPICS))


if __name__ == "__main__":
    unittest.main()
