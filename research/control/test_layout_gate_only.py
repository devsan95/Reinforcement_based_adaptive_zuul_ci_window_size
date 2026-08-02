#!/usr/bin/env python3
"""Unit tests for traditional check → gate layout detection."""

from __future__ import annotations

import sys
import unittest
from pathlib import Path
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "analysis"))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from server import (  # noqa: E402
    _layout_is_check_then_gate,
    _layout_is_gate_only,
    _pipeline_job_names,
)


CHECK_THEN_GATE_LAYOUT = {
    "configs": [
        {
            "pipelines": [
                {
                    "name": "check",
                    "jobs": [[{"name": "research-check-job"}]],
                },
                {
                    "name": "gate",
                    "jobs": [[{"name": "research-gate-job"}]],
                },
            ],
        },
    ],
}

GATE_ONLY_LAYOUT = {
    "configs": [
        {
            "pipelines": [
                {"name": "check", "jobs": []},
                {
                    "name": "gate",
                    "jobs": [[{"name": "research-gate-job"}]],
                },
            ],
        },
    ],
}

STALE_LAYOUT = {
    "configs": [
        {
            "pipelines": [
                {
                    "name": "check",
                    "jobs": [[{"name": "research-check-job"}]],
                },
                {
                    "name": "gate",
                    "jobs": [[{"name": "research-gate-job"}]],
                },
            ],
        },
        {
            "pipelines": [
                {"name": "check", "jobs": []},
                {"name": "gate", "jobs": []},
            ],
        },
    ],
}


class LayoutCheckThenGateTests(unittest.TestCase):
    def test_pipeline_job_names(self):
        self.assertEqual(
            _pipeline_job_names(CHECK_THEN_GATE_LAYOUT, "gate"),
            ["research-gate-job"],
        )
        self.assertEqual(
            _pipeline_job_names(CHECK_THEN_GATE_LAYOUT, "check"),
            ["research-check-job"],
        )

    def test_check_then_gate_layout_true(self):
        with mock.patch.object(
                __import__("server"), "_fetch_project_layout",
                return_value=CHECK_THEN_GATE_LAYOUT):
            self.assertTrue(_layout_is_check_then_gate())
            self.assertTrue(_layout_is_gate_only())  # alias

    def test_gate_only_layout_false(self):
        with mock.patch.object(
                __import__("server"), "_fetch_project_layout",
                return_value=GATE_ONLY_LAYOUT):
            self.assertFalse(_layout_is_check_then_gate())

    def test_merged_configs_still_find_jobs(self):
        # Job names are collected across config entries.
        with mock.patch.object(
                __import__("server"), "_fetch_project_layout",
                return_value=STALE_LAYOUT):
            self.assertTrue(_layout_is_check_then_gate())


if __name__ == "__main__":
    unittest.main()
