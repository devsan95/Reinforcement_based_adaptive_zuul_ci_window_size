"""Unit tests for deterministic every_nth gate failure pattern."""

from __future__ import annotations

import unittest


def should_fail_every_nth(change: int, every: int = 5) -> bool:
    """Mirror research gate-job every_nth logic."""
    every = max(1, int(every))
    return change > 0 and (change % every) == 0


class TestEveryNthFailures(unittest.TestCase):
    def test_exactly_ten_failures_in_fifty(self):
        fails = [n for n in range(1, 51) if should_fail_every_nth(n, 5)]
        self.assertEqual(fails, [5, 10, 15, 20, 25, 30, 35, 40, 45, 50])
        self.assertGreaterEqual(len(fails), 5)
        self.assertEqual(len(fails), 10)

    def test_deterministic_across_reruns(self):
        a = [should_fail_every_nth(n, 5) for n in range(1, 51)]
        b = [should_fail_every_nth(n, 5) for n in range(1, 51)]
        self.assertEqual(a, b)

    def test_non_multiples_succeed(self):
        for n in (1, 2, 3, 4, 6, 49):
            self.assertFalse(should_fail_every_nth(n, 5))


if __name__ == "__main__":
    unittest.main()
