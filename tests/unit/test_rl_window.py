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

import math
import os
import unittest
from pathlib import Path
from unittest import mock

import time

from zuul import model
from zuul.rl_window import (
    ACTION_DELTAS,
    DEFAULT_INITIAL_WINDOW,
    KNN_MAX_DISTANCE,
    STATE_LABELS,
    STATE_SIZE,
    ExecutorSnapshot,
    QueueMetrics,
    WindowController,
    _executor_audit_fields,
    adjust_window_after_cycle,
    get_rl_state,
    set_window_from_api,
)


class FakeZKContext:
    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False


class FakeChangeQueue(model.ChangeQueue):
    # ZKObject blocks attribute writes outside an active ZK context; the
    # fake behaves like a plain object so tests can poke window sizes.
    def __setattr__(self, name, value):
        object.__setattr__(self, name, value)

    @property
    def zk_context(self):
        return self._fake_zk_context

    def __init__(self):
        super().__init__()
        self._fake_zk_context = mock.Mock()
        self._set(
            uuid="test-queue",
            manager=mock.Mock(pipeline=mock.Mock(name="gate")),
            name="shared",
            project_branches=[],
            _jobs=set(),
            queue=[],
            window=20,
            window_floor=3,
            window_ceiling=50,
            window_increase_type="linear",
            window_increase_factor=1,
            window_decrease_type="exponential",
            window_decrease_factor=2,
            dynamic=False,
        )

    def getPath(self):
        return "/tenant/gate/queue/test-queue"

    def activeContext(self, context):
        return FakeZKContext()


class RLWindowTests(unittest.TestCase):
    def setUp(self):
        self.controller = WindowController()
        self.queue = FakeChangeQueue()

    def test_state_vector_size(self):
        scheduler = mock.Mock(executor=mock.Mock(running_builds={}))
        state = self.controller.get_rl_state(scheduler, self.queue)
        self.assertEqual(len(state), STATE_SIZE)
        self.assertTrue(all(math.isfinite(v) for v in state))

    def test_set_window_from_api_clamps(self):
        size = self.controller.set_window_from_api(self.queue, 999)
        self.assertEqual(size, 50)
        self.assertEqual(self.queue.window, 50)

    def test_tcp_increase_on_success(self):
        self.controller.adjust_window_after_cycle(self.queue, succeeded=True)
        self.assertEqual(self.queue.window, 21)

    def test_tcp_decrease_on_failure(self):
        self.controller.adjust_window_after_cycle(self.queue, succeeded=False)
        self.assertEqual(self.queue.window, 10)

    def test_active_override_blocks_tcp(self):
        self.controller._mode = "active"
        # Seed a low TCP shadow so the RL>=TCP floor does not lift the
        # override (the floor is tested separately below).
        key = self.controller._queue_key(self.queue)
        self.controller._tcp_shadow[key] = 5
        self.controller.set_window_from_api(self.queue, 8)
        self.controller.adjust_window_after_cycle(self.queue, succeeded=True)
        # TCP would have grown the window to 21; the RL override holds 8.
        self.assertEqual(self.queue.window, 8)

    def test_action_deltas(self):
        self.assertEqual(ACTION_DELTAS, (-2, -1, 0, 1, 2))

    def test_tcp_shadow_tracks_independently(self):
        self.queue.window = 20
        key = self.controller._queue_key(self.queue)
        # Seed above DEFAULT_INITIAL_WINDOW so the test exercises grow/shrink
        # from a known value rather than the demo baseline.
        self.controller._tcp_shadow[key] = 20
        self.controller._advance_tcp_shadow(self.queue, succeeded=True)
        self.assertEqual(self.controller._tcp_shadow[key], 21)
        self.controller._advance_tcp_shadow(self.queue, succeeded=False)
        self.assertEqual(self.controller._tcp_shadow[key], 10)

    def test_module_helpers(self):
        scheduler = mock.Mock(executor=mock.Mock(running_builds={"a": 1, "b": 2}))
        # Pin the TCP shadow below the requested size so the RL>=TCP
        # floor does not lift the API override.
        key = self.controller._queue_key(self.queue)
        self.controller._tcp_shadow[key] = 5
        with mock.patch("zuul.rl_window.CONTROLLER", self.controller):
            state = get_rl_state(scheduler, self.queue)
            self.assertEqual(len(state), STATE_SIZE)
            set_window_from_api(self.queue, 12)
            self.assertEqual(self.queue.window, 12)
            adjust_window_after_cycle(self.queue, succeeded=False)
            self.assertEqual(self.queue.window, 6)

    def test_tcp_shadow_updates_recommendation(self):
        self.controller._mode = "active"
        self.controller._enabled = True
        key = self.controller._queue_key(self.queue)
        self.controller._tcp_shadow[key] = 20
        self.controller._recommendations[key] = {
            "queue_uuid": self.queue.uuid,
            "queue_name": "shared",
            "recommended_window": 18,
            "current_window": 18,
            "tcp_shadow_window": 20,
            "action_delta": -2,
            "mode": "active",
            "updated_at": 0.0,
        }
        self.controller.adjust_window_after_cycle(self.queue, succeeded=False)
        self.assertEqual(self.controller._recommendations[key][
            "tcp_shadow_window"], 10)
        status = self.controller.get_queue_status_json(
            "example-tenant", "gate", self.queue)
        self.assertEqual(status["rl_tcp_shadow_window"], 10)

    def test_begin_demo_session_resets_windows(self):
        self.controller._mode = "active"
        self.controller._enabled = True
        self.controller._tcp_shadow[self.controller._queue_key(self.queue)] = 35
        self.controller.set_window_from_api(self.queue, 30)
        self.queue.window = 30

        tenant = mock.Mock()
        pipeline_manager = mock.Mock()
        pipeline_manager.state.queues = [self.queue]
        tenant.layout.pipeline_managers = {"gate": pipeline_manager}
        scheduler = mock.Mock()
        scheduler.abide.tenants = {"example-tenant": tenant}
        scheduler.createZKContext.return_value = FakeZKContext()
        pipeline_manager.currentContext.return_value = FakeZKContext()
        self.queue.queue = []

        with mock.patch("zuul.rl_window.os.makedirs"), \
             mock.patch("builtins.open", mock.mock_open()):
            self.controller.begin_demo_session(scheduler)

        key = self.controller._queue_key(self.queue)
        self.assertEqual(self.queue.window, DEFAULT_INITIAL_WINDOW)
        self.assertEqual(
            self.controller._tcp_shadow[key], DEFAULT_INITIAL_WINDOW)
        self.assertNotIn(key, self.controller._overrides)
        self.assertTrue(self.controller._session_baseline_set)

    def test_rl_never_below_tcp_shadow(self):
        key = self.controller._queue_key(self.queue)
        self.controller._tcp_shadow[key] = 15
        size = self.controller.set_window_from_api(self.queue, 8)
        self.assertEqual(size, 15)
        self.assertEqual(self.queue.window, 15)
        self.assertGreaterEqual(size, self.controller._tcp_shadow[key])

    def test_rl_shadow_recommendation_floored_at_tcp(self):
        key = self.controller._queue_key(self.queue)
        self.queue.window = 20
        self.controller._tcp_shadow[key] = 18
        self.controller._mode = "shadow"
        recommended = self.controller._apply_action(
            self.queue, 0, apply=False)  # delta -2 -> 18, not 18 floored
        self.assertEqual(recommended, 18)
        self.controller._tcp_shadow[key] = 19
        recommended = self.controller._apply_action(
            self.queue, 0, apply=False)  # 20-2=18, floored to 19
        self.assertEqual(recommended, 19)

    def test_rl_floor_after_tcp_shrink_in_active_mode(self):
        self.controller._mode = "active"
        key = self.controller._queue_key(self.queue)
        self.queue.window = 20
        self.controller._tcp_shadow[key] = 20
        self.controller.set_window_from_api(self.queue, 20)
        self.controller.adjust_window_after_cycle(self.queue, succeeded=False)
        self.assertEqual(self.controller._tcp_shadow[key], 10)
        self.assertEqual(self.queue.window, 20)
        self.controller.set_window_from_api(self.queue, 8)
        self.assertEqual(self.queue.window, 10)
        self.assertGreaterEqual(
            self.queue.window, self.controller._tcp_shadow[key])

    def test_demo_reset_trigger_file(self):
        import tempfile
        self.controller._enabled = True

        tenant = mock.Mock()
        pipeline_manager = mock.Mock()
        pipeline_manager.state.queues = [self.queue]
        tenant.layout.pipeline_managers = {"gate": pipeline_manager}
        scheduler = mock.Mock()
        scheduler.abide.tenants = {"example-tenant": tenant}
        scheduler.createZKContext.return_value = FakeZKContext()
        pipeline_manager.currentContext.return_value = FakeZKContext()
        self.controller._scheduler = scheduler

        with tempfile.TemporaryDirectory() as tmpdir:
            trigger = os.path.join(tmpdir, "rl_demo_reset.request")
            self.controller._demo_reset_path = trigger
            Path(trigger).write_text('{"timestamp": 1}\n', encoding="utf-8")
            self.queue.window = 40
            with mock.patch("zuul.rl_window.os.makedirs"), \
                 mock.patch("builtins.open", mock.mock_open()):
                self.assertTrue(
                    self.controller._check_demo_reset_request(scheduler))
            self.assertFalse(os.path.exists(trigger))
            self.assertEqual(self.queue.window, DEFAULT_INITIAL_WINDOW)

    def test_pending_demo_baseline_retry(self):
        self.controller._enabled = True
        self.controller._pending_demo_baseline = True

        tenant = mock.Mock()
        pipeline_manager = mock.Mock()
        pipeline_manager.state.queues = [self.queue]
        tenant.layout.pipeline_managers = {"gate": pipeline_manager}
        scheduler = mock.Mock()
        scheduler.abide.tenants = {"example-tenant": tenant}
        scheduler.createZKContext.return_value = FakeZKContext()
        pipeline_manager.currentContext.return_value = FakeZKContext()

        self.queue.window = 3
        with mock.patch("zuul.rl_window.os.makedirs"), \
             mock.patch("builtins.open", mock.mock_open()):
            self.assertTrue(
                self.controller._retry_pending_demo_baseline(scheduler))
        self.assertFalse(self.controller._pending_demo_baseline)
        self.assertEqual(self.queue.window, DEFAULT_INITIAL_WINDOW)


class RLStatePolicyTests(unittest.TestCase):
    """New input → prediction pipeline: state bounds, kNN, guardrails."""

    def setUp(self):
        self.controller = WindowController()
        self.queue = FakeChangeQueue()

    def test_state_labels_match_state_size(self):
        self.assertEqual(len(STATE_LABELS), STATE_SIZE)

    def test_state_features_all_clipped_to_unit_interval(self):
        # Pathological inputs: queue far deeper than 2×ceiling and more
        # running builds than executor capacity must still yield [0, 1].
        self.queue.queue = [mock.Mock()] * 500
        scheduler = mock.Mock(executor=mock.Mock(
            running_builds={i: i for i in range(500)}))
        key = self.controller._queue_key(self.queue)
        metrics = self.controller._metrics_for(key)
        now = time.time()
        for i in range(20):
            metrics.outcomes.append((now - i, i % 2 == 0))
        state = self.controller.get_rl_state(scheduler, self.queue)
        self.assertEqual(len(state), STATE_SIZE)
        for value in state:
            self.assertGreaterEqual(value, 0.0)
            self.assertLessEqual(value, 1.0)

    def test_recent_failure_rate_ignores_stale_outcomes(self):
        metrics = QueueMetrics()
        now = time.time()
        # Failures older than the window must not count at all.
        metrics.outcomes.append((now - 1000, False))
        self.assertEqual(
            self.controller._recent_failure_rate(metrics, now), 0.0)

    def test_recent_failure_rate_recency_weighted(self):
        metrics = QueueMetrics()
        now = time.time()
        # An old success and a fresh failure: the failure should dominate.
        metrics.outcomes.append((now - 170, True))
        metrics.outcomes.append((now - 1, False))
        rate = self.controller._recent_failure_rate(metrics, now)
        self.assertGreater(rate, 0.5)
        self.assertLessEqual(rate, 1.0)

    def test_success_streak_counts_back_from_newest(self):
        metrics = QueueMetrics()
        now = time.time()
        for ok in (False, True, True, True):
            metrics.outcomes.append((now, ok))
        self.assertEqual(self.controller._success_streak(metrics), 3)
        metrics.outcomes.append((now, False))
        self.assertEqual(self.controller._success_streak(metrics), 0)

    def test_table_query_maps_live_state_to_table_space(self):
        # live: [norm_w, queue/(2*ceiling), fail, streak, available, pressure]
        live = [0.4, 0.25, 0.15, 0.3, 0.7, 0.9]
        query = WindowController._table_query(live)
        # table: [norm_w, queue/ceiling, fail, hour_sin, hour_cos, pressure]
        # executor_available (0.7) is dropped; table 6th dim is queue/window.
        self.assertEqual(query, [0.4, 0.5, 0.15, 0.0, 1.0, 0.9])

    def test_knn_vote_prefers_nearby_majority(self):
        self.controller._policy_kind = "ppo_table"
        self.controller._policy_table = {}
        self.controller._policy_entries = [
            {"state": [0.40, 0.50, 0.00, 0.0, 1.0, 0.50], "action_idx": 4},
            {"state": [0.42, 0.50, 0.02, 0.0, 1.0, 0.50], "action_idx": 4},
            {"state": [0.38, 0.48, 0.00, 0.0, 1.0, 0.52], "action_idx": 3},
            {"state": [0.90, 0.90, 0.50, 0.0, 1.0, 1.00], "action_idx": 0},
        ]
        live = [0.4, 0.25, 0.0, 0.0, 0.0, 0.5]
        action, dist, detail = self.controller._lookup_table_action(live)
        self.assertEqual(action, 4)
        self.assertLessEqual(dist, KNN_MAX_DISTANCE)
        self.assertIn("kNN", detail)

    def test_knn_out_of_distribution_falls_back_to_heuristic(self):
        self.controller._policy_kind = "ppo_table"
        self.controller._policy_table = {}
        self.controller._policy_entries = [
            {"state": [1.0, 1.0, 1.0, 0.0, 1.0, 1.0], "action_idx": 0},
        ]
        live = [0.1, 0.1, 0.0, 0.0, 0.0, 0.0]
        action, dist, detail = self.controller._lookup_table_action(live)
        self.assertIsNone(action)
        self.assertGreater(dist, KNN_MAX_DISTANCE)
        chosen, reason, meta = self.controller._choose_action(live)
        self.assertEqual(meta["source"], "heuristic")
        self.assertNotIn("heuristic (", reason)
        self.assertIn("out of table distribution", meta.get("policy_detail", ""))
        self.assertIn(ACTION_DELTAS[chosen], ACTION_DELTAS)

    def test_guardrail_holds_shrink_during_failure_burst(self):
        self.controller._policy_kind = "ppo_table"
        self.controller._policy_table = {}
        # Nearest table entry says shrink −2 at 50% failure rate.
        self.controller._policy_entries = [
            {"state": [0.4, 0.5, 0.5, 0.0, 1.0, 0.5], "action_idx": 0},
        ]
        live = [0.4, 0.25, 0.5, 0.0, 1.0, 0.5]
        action, reason, meta = self.controller._choose_action(live)
        self.assertEqual(ACTION_DELTAS[action], 0)
        self.assertIn("Held the window", reason)
        self.assertIn("holding through failures", reason)
        self.assertNotIn("kNN", reason)
        self.assertEqual(meta.get("guardrail"), "hold_on_failure_burst")

    def test_guardrail_ramps_hold_on_success_streak(self):
        self.controller._policy_kind = "ppo_table"
        self.controller._policy_table = {}
        # Nearest table entry holds, but a long healthy streak upgrades.
        self.controller._policy_entries = [
            {"state": [0.4, 0.5, 0.0, 0.0, 1.0, 0.5], "action_idx": 2},
        ]
        live = [0.4, 0.25, 0.0, 0.8, 1.0, 0.5]
        action, reason, meta = self.controller._choose_action(live)
        self.assertEqual(ACTION_DELTAS[action], 2)
        self.assertIn("Increased the window by 2", reason)
        self.assertIn("consecutive successes", reason)
        self.assertEqual(meta.get("guardrail"), "ramp_on_success_streak")

    def test_heuristic_reason_strings(self):
        # No policy loaded at all → pure heuristic with clear reasons.
        burst = [0.4, 0.25, 0.5, 0.0, 1.0, 0.5]
        action, reason, meta = self.controller._choose_action(burst)
        self.assertEqual(ACTION_DELTAS[action], 0)
        self.assertIn("Held the window", reason)
        self.assertIn("holding through failures", reason)
        self.assertEqual(meta["source"], "heuristic")
        healthy = [0.4, 0.25, 0.0, 0.5, 1.0, 0.5]
        action, reason, _ = self.controller._choose_action(healthy)
        self.assertEqual(ACTION_DELTAS[action], 2)
        self.assertIn("Increased the window by 2", reason)
        self.assertIn("consecutive successes", reason)

    def test_policy_reason_strings_are_plain_english(self):
        self.controller._policy_kind = "ppo_table"
        self.controller._policy_table = {}
        # Quiet state → table votes trim −2.
        self.controller._policy_entries = [
            {"state": [0.4, 0.5, 0.0, 0.0, 1.0, 0.5], "action_idx": 0},
        ]
        live = [0.4, 0.25, 0.0, 0.0, 1.0, 0.5]
        action, reason, meta = self.controller._choose_action(live)
        self.assertEqual(ACTION_DELTAS[action], -2)
        self.assertIn("Reduced the window by 2", reason)
        self.assertIn("sharp cut", reason)
        self.assertNotIn("by -2", reason)
        self.assertNotIn("kNN", reason)
        self.assertNotIn("nearest", reason)
        self.assertEqual(meta["source"], "knn")
        self.assertIn("kNN", meta.get("policy_detail", ""))

        # Healthy grow +1
        self.controller._policy_entries = [
            {"state": [0.4, 0.5, 0.0, 0.0, 1.0, 0.5], "action_idx": 3},
        ]
        action, reason, meta = self.controller._choose_action(live)
        self.assertEqual(ACTION_DELTAS[action], 1)
        self.assertIn("Increased the window by 1", reason)
        self.assertNotIn("kNN", reason)

        # Hold
        self.controller._policy_entries = [
            {"state": [0.4, 0.5, 0.0, 0.0, 1.0, 0.5], "action_idx": 2},
        ]
        action, reason, meta = self.controller._choose_action(live)
        self.assertEqual(ACTION_DELTAS[action], 0)
        self.assertIn("Held the window", reason)
        self.assertNotIn("kNN", reason)


class _FakePPOModel:
    """Stand-in for a loaded stable_baselines3.PPO model's .predict()."""

    def __init__(self, action_idx):
        self.action_idx = action_idx
        self.calls = []

    def predict(self, obs, deterministic=True):
        self.calls.append((tuple(obs), deterministic))
        return self.action_idx, None


class RLPurePPOTests(unittest.TestCase):
    """A loaded PPO network's action must be used as-is: no guardrails,
    no kNN table, no TCP-shadow floor. Required for RQ2 — the
    statistical comparison must evaluate the learned policy on its own
    merits (thesis Section 3.2.3 / 6.4)."""

    def setUp(self):
        self.controller = WindowController()
        self.queue = FakeChangeQueue()

    def test_ppo_takes_priority_over_policy_table(self):
        # Even with a kNN table loaded, a loaded PPO model wins.
        self.controller._policy_table = {}
        self.controller._policy_entries = [
            {"state": [0.4, 0.5, 0.0, 0.0, 1.0, 0.5], "action_idx": 4},
        ]
        self.controller._policy = _FakePPOModel(action_idx=0)  # shrink −2
        live = [0.4, 0.25, 0.0, 0.0, 1.0, 0.5]
        action, reason, meta = self.controller._choose_action(live)
        self.assertEqual(action, 0)
        self.assertEqual(meta["source"], "ppo")

    def test_ppo_action_not_wrapped_in_guardrails(self):
        # Failure burst (>DEMO_HOLD_FAILURE_RATE) would normally force a
        # hold via _guarded()/hold_on_failure_burst; pure PPO must be
        # free to shrink anyway.
        self.controller._policy = _FakePPOModel(action_idx=0)  # −2
        burst = [0.4, 0.25, 0.5, 0.0, 1.0, 0.5]
        action, reason, meta = self.controller._choose_action(burst)
        self.assertEqual(action, 0)
        self.assertEqual(ACTION_DELTAS[action], -2)
        self.assertEqual(meta["source"], "ppo")
        self.assertNotIn("guardrail", meta)
        self.assertNotIn("holding through failures", reason)

    def test_ppo_action_ignores_scarce_executor_hold(self):
        # Guardrail would normally hold/trim growth when executors are
        # scarce; pure PPO's chosen growth must still apply.
        self.controller._policy = _FakePPOModel(action_idx=4)  # +2
        live = [0.4, 0.25, 0.0, 0.0, 0.04, 0.5]
        snap = ExecutorSnapshot(
            capacity=50, available=2, in_use=48,
            available_ratio=0.04, occupancy=0.96, source="launcher")
        action, reason, meta = self.controller._choose_action(
            live, snapshot=snap, current_window=20)
        self.assertEqual(action, 4)
        self.assertEqual(meta["source"], "ppo")
        self.assertNotIn("guardrail", meta)

    def test_ppo_window_not_floored_at_tcp_shadow(self):
        # TCP shadow sits at 15; PPO recommends shrinking to 8. The old
        # demo behaviour would clamp the applied window up to 15
        # (self._enforce_rl_tcp_floor's max(requested, tcp_shadow));
        # pure PPO must be allowed to go lower.
        key = self.controller._queue_key(self.queue)
        self.queue.window = 10
        self.controller._tcp_shadow[key] = 15
        self.controller._policy = _FakePPOModel(action_idx=0)  # −2
        recommended = self.controller._apply_action(
            self.queue, 0, apply=False, decision_source="ppo")
        self.assertEqual(recommended, 8)  # 10 - 2, not floored to 15
        self.assertLess(recommended, self.controller._tcp_shadow[key])

    def test_non_ppo_source_still_gets_tcp_floor(self):
        # Sanity check that the default (table/heuristic) behaviour is
        # unchanged when decision_source is not "ppo".
        key = self.controller._queue_key(self.queue)
        self.queue.window = 10
        self.controller._tcp_shadow[key] = 15
        recommended = self.controller._apply_action(
            self.queue, 0, apply=False, decision_source="heuristic")
        self.assertEqual(recommended, 15)  # floored up to TCP shadow

    def test_ppo_still_clamped_to_pipeline_bounds(self):
        # Physical validity (floor/ceiling) always applies, even to a
        # pure PPO decision — this is not a "guardrail", it is what
        # keeps the window a legal value.
        key = self.controller._queue_key(self.queue)
        self.queue.window = 4
        self.controller._tcp_shadow[key] = 3
        self.controller._policy = _FakePPOModel(action_idx=0)  # −2 -> 2
        recommended = self.controller._apply_action(
            self.queue, 0, apply=False, decision_source="ppo")
        self.assertEqual(recommended, self.queue.window_floor)  # clamped to 3

    def test_ppo_predict_called_with_state_vector(self):
        model = _FakePPOModel(action_idx=2)
        self.controller._policy = model
        live = [0.4, 0.25, 0.1, 0.2, 0.8, 0.3]
        self.controller._choose_action(live)
        self.assertEqual(len(model.calls), 1)
        obs, deterministic = model.calls[0]
        self.assertEqual(list(obs), live)
        self.assertTrue(deterministic)


class RLExecutorAvailabilityTests(unittest.TestCase):
    """Window must move with launcher/nodepool slot availability."""

    def setUp(self):
        self.controller = WindowController()
        self.queue = FakeChangeQueue()

    def _snapshot(self, available, capacity=50, source="launcher"):
        in_use = max(0, capacity - available)
        return ExecutorSnapshot(
            capacity=capacity,
            available=available,
            in_use=in_use,
            available_ratio=_clip_ratio(available, capacity),
            occupancy=_clip_ratio(in_use, capacity),
            source=source,
        )

    def test_state_label_is_executor_available(self):
        self.assertEqual(STATE_LABELS[4], "executor_available")

    def test_launcher_ready_slots_feed_state(self):
        summaries = [
            {"uuid": "host", "state": "slot-host", "main_node_id": None,
             "subnodes": ["a", "b", "c", "d"], "slot": None},
            {"uuid": "a", "state": "ready", "main_node_id": "host",
             "subnodes": [], "slot": 0},
            {"uuid": "b", "state": "ready", "main_node_id": "host",
             "subnodes": [], "slot": 1},
            {"uuid": "c", "state": "in-use", "main_node_id": "host",
             "subnodes": [], "slot": 2},
            {"uuid": "d", "state": "in-use", "main_node_id": "host",
             "subnodes": [], "slot": 3},
        ]
        scheduler = mock.Mock()
        scheduler.launcher.listProviderNodeSummaries.return_value = summaries
        scheduler.abide.tenants = {}
        with mock.patch.dict(os.environ, {"RL_WINDOW_EXECUTOR_CAPACITY": "50"}):
            state = self.controller.get_rl_state(scheduler, self.queue)
        self.assertEqual(len(state), STATE_SIZE)
        # 2 ready / capacity 50 (env) → 0.04 available_ratio
        self.assertAlmostEqual(state[4], 2 / 50, places=4)
        snap = self.controller._executor_snapshot
        self.assertEqual(snap.source, "launcher")
        self.assertEqual(snap.available, 2)
        self.assertEqual(snap.in_use, 2)
        self.assertEqual(snap.capacity, 50)

    def test_count_launcher_slots_skips_slot_host(self):
        counted = WindowController._count_launcher_slots([
            {"state": "slot-host"},
            {"state": "ready"},
            {"state": "ready"},
            {"state": "in-use"},
            {"state": "building"},
        ])
        self.assertEqual(counted, (2, 1, 1))

    def test_high_occupancy_holds_instead_of_ramping(self):
        # Success streak would otherwise +2; few free slots must hold/trim.
        live = [0.4, 0.4, 0.0, 0.8, 0.15, 1.0]
        snap = self._snapshot(available=2, capacity=50)
        action, reason, meta = self.controller._choose_action(
            live, snapshot=snap, current_window=20)
        self.assertLessEqual(ACTION_DELTAS[action], 0)
        self.assertEqual(ACTION_DELTAS[action], 0)
        self.assertIn("only 2 executors free", reason)
        self.assertIn("Held the window", reason)

    def test_extreme_occupancy_trims_window(self):
        live = [0.4, 0.4, 0.0, 0.0, 0.04, 1.0]
        snap = self._snapshot(available=2, capacity=50)
        action, reason, _ = self.controller._choose_action(
            live, snapshot=snap, current_window=20)
        self.assertEqual(ACTION_DELTAS[action], -1)
        self.assertIn("Reduced the window", reason)
        self.assertIn("only 2 executors free", reason)

    def test_high_occupancy_clamps_window_to_free_slots(self):
        key = self.controller._queue_key(self.queue)
        self.queue.window = 20
        self.controller._tcp_shadow[key] = 5
        self.controller._executor_snapshot = self._snapshot(
            available=4, capacity=50)
        # Policy +2 would be 22; cap at max(floor=3, available=4).
        recommended = self.controller._apply_action(
            self.queue, 4, apply=False)
        self.assertEqual(recommended, 4)
        self.assertGreaterEqual(recommended, self.queue.window_floor)

    def test_capacity_clamp_can_win_over_tcp_floor(self):
        key = self.controller._queue_key(self.queue)
        self.queue.window = 20
        self.controller._tcp_shadow[key] = 10
        self.controller._executor_snapshot = self._snapshot(
            available=4, capacity=50)
        recommended = self.controller._apply_action(
            self.queue, 4, apply=False)
        self.assertEqual(recommended, 4)
        self.assertLess(recommended, self.controller._tcp_shadow[key])

    def test_capacity_clamp_does_not_go_below_floor(self):
        key = self.controller._queue_key(self.queue)
        self.queue.window = 8
        self.controller._tcp_shadow[key] = 3
        self.controller._executor_snapshot = self._snapshot(
            available=1, capacity=50)
        recommended = self.controller._apply_action(
            self.queue, 2, apply=False)  # hold at 8 → cap to floor 3
        self.assertEqual(recommended, self.queue.window_floor)

    def test_plenty_free_and_healthy_can_grow(self):
        # No success streak; deep queue + low failures + many free slots.
        live = [0.16, 0.4, 0.0, 0.0, 0.9, 1.0]
        snap = self._snapshot(available=45, capacity=50)
        action, reason, meta = self.controller._choose_action(
            live, snapshot=snap, current_window=8)
        self.assertGreater(ACTION_DELTAS[action], 0)
        self.assertIn("executors free", reason)
        self.assertIn("queue is deep", reason)

    def test_table_grow_blocked_when_slots_scarce(self):
        self.controller._policy_kind = "ppo_table"
        self.controller._policy_table = {}
        self.controller._policy_entries = [
            {"state": [0.4, 0.5, 0.0, 0.0, 1.0, 0.5], "action_idx": 4},
        ]
        live = [0.4, 0.25, 0.0, 0.0, 0.04, 0.5]
        snap = self._snapshot(available=2, capacity=50)
        action, reason, meta = self.controller._choose_action(
            live, snapshot=snap, current_window=20)
        self.assertEqual(ACTION_DELTAS[action], 0)
        self.assertEqual(meta.get("guardrail"), "hold_on_low_executors")
        self.assertIn("only 2 executors free", reason)

    def test_tcp_floor_kept_when_capacity_is_plenty(self):
        key = self.controller._queue_key(self.queue)
        self.queue.window = 8
        self.controller._tcp_shadow[key] = 15
        self.controller._executor_snapshot = self._snapshot(
            available=40, capacity=50)
        size = self.controller.set_window_from_api(
            self.queue, 8, source="agent")
        self.assertEqual(size, 15)

    def test_running_builds_fallback_occupancy(self):
        scheduler = mock.Mock(
            executor=mock.Mock(
                running_builds={"a": 1, "b": 2, "c": 3},
                executor_api=None),
            launcher=mock.Mock(spec=[]),
            nodepool=mock.Mock(spec=[]),
        )
        scheduler.abide.tenants = {}
        with mock.patch.dict(os.environ, {"RL_WINDOW_EXECUTOR_CAPACITY": "4"}):
            self.controller._executor_snapshot = None
            self.controller._executor_snapshot_at = 0.0
            state = self.controller.get_rl_state(scheduler, self.queue)
        # 3 running / 4 capacity → 1 free → available_ratio 0.25
        self.assertAlmostEqual(state[4], 0.25, places=4)
        self.assertEqual(self.controller._executor_snapshot.source,
                         "running_builds")

    def test_executor_audit_fields_named_slots_and_ratio(self):
        snap = self._snapshot(available=2, capacity=50)
        fields = _executor_audit_fields(snap)
        self.assertEqual(fields["available_slots"], 2)
        self.assertEqual(fields["executor_capacity"], 50)
        self.assertEqual(fields["executor_in_use"], 48)
        self.assertAlmostEqual(fields["executor_available"], 2 / 50)
        self.assertAlmostEqual(fields["executor_occupancy"], 48 / 50)
        self.assertEqual(fields["executor_source"], "launcher")
        empty = _executor_audit_fields(None)
        self.assertIsNone(empty["available_slots"])
        self.assertIsNone(empty["executor_available"])


def _clip_ratio(part, whole):
    whole = max(int(whole), 1)
    return max(0.0, min(1.0, float(part) / whole))


if __name__ == "__main__":
    unittest.main()
