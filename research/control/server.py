#!/usr/bin/env python3
"""Demo control API for running RL scenarios from the UI."""

from __future__ import annotations

import json
import logging
import math
import os
import shutil
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from bisect import bisect_right
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
from subprocess import CalledProcessError, run
from typing import Dict, List, Optional, Sequence, Tuple

import jwt
from flask import Flask, jsonify, request

log = logging.getLogger("rl-control")

from compare_report import build_report, fetch_builds, parse_build_time

APP = Flask(__name__)

BASE_DIR = Path(os.environ.get("RL_CONTROL_BASE_DIR", "/results"))
PUBLISH_DIR = Path(os.environ.get("RL_CONTROL_PUBLISH_DIR", "/published"))
AUDIT_PATH = Path(os.environ.get("RL_CONTROL_AUDIT_PATH", "/var/lib/zuul/rl_window_audit.jsonl"))
ZUUL_API = os.environ.get("RL_CONTROL_ZUUL_API", "http://web:9000")
TRAFFIC_DIR = Path("/app")
# Cap chart samples; downsample uniformly across the full session (never a
# trailing sliding window that drops early demo points).
LIVE_METRICS_MAX_POINTS = int(os.environ.get("RL_LIVE_METRICS_MAX_POINTS", "2400"))
LIVE_METRICS_LOOKBACK_SEC = float(
    os.environ.get("RL_LIVE_METRICS_LOOKBACK_SEC", "3600"))
LIVE_METRICS_CACHE_SEC = float(
    os.environ.get("LIVE_METRICS_CACHE_SEC", "2"))
BUILDS_CACHE_SEC = float(os.environ.get("BUILDS_CACHE_SEC", "5"))
THROUGHPUT_WINDOW_SEC = float(
    os.environ.get("RL_THROUGHPUT_WINDOW_SEC", "60"))
# Small demo baseline: a 10-change batch immediately exceeds the window so
# min(queue, window) differentiates RL vs TCP as soon as TCP shrinks.
DEFAULT_INITIAL_WINDOW = int(os.environ.get("RL_DEFAULT_INITIAL_WINDOW", "8"))
DEFAULT_GATE_JOB_DURATION_SEC = float(
    os.environ.get("RL_DEFAULT_GATE_JOB_DURATION_SEC", "1.0"))
DEMO_CHANGE_COUNT = int(os.environ.get("DEMO_CHANGE_COUNT", "100"))
DEMO_BATCH_SIZE = int(os.environ.get("DEMO_BATCH_SIZE", "10"))
# Adaptive saturation: keep gate queue depth > max(RL, TCP) + margin so both
# windows are fully utilized and in-flight delta == window delta exactly.
DEMO_QUEUE_SATURATION_MARGIN = int(
    os.environ.get("DEMO_QUEUE_SATURATION_MARGIN", "4"))
DEMO_MAX_BATCH_SIZE = int(os.environ.get("DEMO_MAX_BATCH_SIZE", "20"))
DEMO_BATCH_INTERVAL_SEC = float(
    os.environ.get("DEMO_BATCH_INTERVAL_SEC", "30"))
DEMO_DURATION_SEC = float(os.environ.get("DEMO_DURATION_SEC", "300"))
DEMO_FAIL_PER_BATCH = int(os.environ.get("DEMO_FAIL_PER_BATCH", "5"))
DEMO_PASS_PER_BATCH = int(os.environ.get("DEMO_PASS_PER_BATCH", "5"))
DEMO_QUEUE_CLEAR_TIMEOUT = float(
    os.environ.get("DEMO_QUEUE_CLEAR_TIMEOUT", "30"))
DEMO_QUEUE_CLEAR_TIMEOUT_PER_ITEM = float(
    os.environ.get("DEMO_QUEUE_CLEAR_TIMEOUT_PER_ITEM", "0.12"))
DEMO_QUEUE_CLEAR_MAX_TIMEOUT = float(
    os.environ.get("DEMO_QUEUE_CLEAR_MAX_TIMEOUT", "600"))
DEMO_QUEUE_POLL_INTERVAL = float(
    os.environ.get("DEMO_QUEUE_POLL_INTERVAL", "0.5"))
DEMO_QUEUE_PROCESSING_WAIT = float(
    os.environ.get("DEMO_QUEUE_PROCESSING_WAIT", "0.75"))
DEMO_QUEUE_STUCK_ROUNDS = int(
    os.environ.get("DEMO_QUEUE_STUCK_ROUNDS", "4"))
DEMO_SCHEDULER_PURGE_WAIT = float(
    os.environ.get("DEMO_SCHEDULER_PURGE_WAIT", "1.5"))
DEMO_AUDIT_WAIT = float(os.environ.get("DEMO_AUDIT_WAIT", "3"))
DEMO_RESET_POLL_INTERVAL = float(
    os.environ.get("DEMO_RESET_POLL_INTERVAL", "0.5"))
DEMO_RESET_GRACE_SEC = float(os.environ.get("DEMO_RESET_GRACE_SEC", "2"))
DEMO_DEQUEUE_HTTP_TIMEOUT = float(
    os.environ.get("DEMO_DEQUEUE_HTTP_TIMEOUT", "10"))
DEMO_GATE_WAIT_BASE_SEC = float(os.environ.get("DEMO_GATE_WAIT_BASE_SEC", "120"))
DEMO_GATE_WAIT_BASE_COUNT = int(os.environ.get("DEMO_GATE_WAIT_BASE_COUNT", "7"))
DEMO_GATE_BUILD_START_TIMEOUT = float(
    os.environ.get("DEMO_GATE_BUILD_START_TIMEOUT", "120"))
# Expected failures across default 5-min continuous run (~10 batches × 5).
DEMO_EXPECTED_FAILURES = int(os.environ.get(
    "DEMO_EXPECTED_FAILURES",
    str(max(1, int(DEMO_DURATION_SEC / DEMO_BATCH_INTERVAL_SEC)
            * DEMO_FAIL_PER_BATCH))))
# Caps for optional /run-demo body params (total_changes / gate_failures).
DEMO_MAX_TOTAL_CHANGES = int(os.environ.get("DEMO_MAX_TOTAL_CHANGES", "500"))
DEMO_MAX_GATE_FAILURES = int(os.environ.get("DEMO_MAX_GATE_FAILURES", "500"))
DEMO_GATE_BUILD_POLL_SEC = float(
    os.environ.get("DEMO_GATE_BUILD_POLL_SEC", "5"))


def _effective_demo_gate_wait_sec() -> float:
    """Post-traffic wait; continuous demo uses duration + one drain window."""
    explicit = os.environ.get("DEMO_GATE_WAIT_SEC")
    if explicit is not None:
        return float(explicit)
    # Continuous: wait for last batch to drain after traffic stops.
    return max(90.0, DEMO_BATCH_INTERVAL_SEC * 2 + 60.0)


DEMO_DEQUEUE_WORKERS = int(os.environ.get("DEMO_DEQUEUE_WORKERS", "24"))
TRAFFIC_WORKERS = int(os.environ.get("TRAFFIC_WORKERS", "10"))
DEMO_RESET_PATH = Path(
    os.environ.get("RL_DEMO_RESET_PATH", "/var/lib/zuul/rl_demo_reset.request"))
DEMO_TENANT = os.environ.get("RL_DEMO_TENANT", "example-tenant")
DEMO_PIPELINE = os.environ.get("RL_DEMO_PIPELINE", "gate")
GERRIT_BASE = os.environ.get("GERRIT_URL", "http://gerrit:8080").rstrip("/")
GERRIT_AUTH = os.environ.get("GERRIT_AUTH", "admin:secret")
CONFIG_BUNDLE = Path(
    os.environ.get("RL_CONFIG_BUNDLE", "/bootstrap/zuul-config"))
SCHEDULER_CONTAINER = os.environ.get("RL_SCHEDULER_CONTAINER", "")
CONFIG_SYNC_TIMEOUT = float(os.environ.get("RL_CONFIG_SYNC_TIMEOUT", "120"))
CONFIG_LAYOUT_POLL = float(os.environ.get("RL_CONFIG_LAYOUT_POLL", "3"))
ZUUL_ADMIN_JWT_SECRET = os.environ.get(
    "ZUUL_ADMIN_JWT_SECRET", "exampleSecret")
ZUUL_ADMIN_JWT_ISSUER = os.environ.get(
    "ZUUL_ADMIN_JWT_ISSUER", "zuul_operator")
ZUUL_ADMIN_JWT_AUDIENCE = os.environ.get(
    "ZUUL_ADMIN_JWT_AUDIENCE", "zuul.example.com")

DEMO_PHASES = (
    "idle",
    "starting",
    "waiting_audit",
    "resetting_windows",
    "clearing_queues",
    "submitting_traffic",
    "waiting_gate_cycles",
    "generating_report",
    "publishing",
    "done",
    "error",
)


def _planned_batches(duration_sec: float = DEMO_DURATION_SEC) -> int:
    """How many batches fit in the primary continuous window."""
    if DEMO_BATCH_INTERVAL_SEC <= 0:
        return 1
    return max(1, int(duration_sec // DEMO_BATCH_INTERVAL_SEC) + (
        1 if duration_sec % DEMO_BATCH_INTERVAL_SEC else 0
    ))


def _batches_plan_tip(
        planned: int,
        *,
        target_total: Optional[int] = None,
        duration_sec: float = DEMO_DURATION_SEC) -> str:
    """One-line why batches_planned equals Y (for the demo progress tip)."""
    planned = max(1, int(planned))
    interval = max(1, int(DEMO_BATCH_INTERVAL_SEC))
    if target_total is not None and int(target_total) > 0:
        return (
            f"Y = {planned}: {int(target_total)} changes "
            f"÷ {DEMO_BATCH_SIZE} per batch"
        )
    dur_min = max(1, int(round(float(duration_sec) / 60.0)))
    return (
        f"Y = {planned}: ~{dur_min} min session ÷ {interval}s interval"
    )

def _parse_optional_nonneg_int(
        value, *, field: str, max_value: int) -> Tuple[Optional[int], Optional[str]]:
    """Parse an optional non-negative int; empty/None → (None, None)."""
    if value is None or value == "":
        return None, None
    try:
        n = int(value)
    except (TypeError, ValueError):
        return None, f"{field} must be an integer"
    if n < 0:
        return None, f"{field} must be >= 0"
    if n > max_value:
        return None, f"{field} must be <= {max_value}"
    return n, None


def parse_run_demo_params(
        body: Optional[dict] = None) -> Tuple[Optional[dict], Optional[str]]:
    """Validate optional /run-demo body params.

    Accepted keys:
      total_changes — stop after this many submitted changes
      gate_failures / fail_count — stamp this many fails across the session

    Empty body keeps duration-based defaults. Returns (params, error).
    """
    body = body or {}
    total, err = _parse_optional_nonneg_int(
        body.get("total_changes"),
        field="total_changes",
        max_value=DEMO_MAX_TOTAL_CHANGES)
    if err:
        return None, err
    fails, err = _parse_optional_nonneg_int(
        body.get("gate_failures"),
        field="gate_failures",
        max_value=DEMO_MAX_GATE_FAILURES)
    if err:
        return None, err
    if fails is None and "fail_count" in body:
        fails, err = _parse_optional_nonneg_int(
            body.get("fail_count"),
            field="fail_count",
            max_value=DEMO_MAX_GATE_FAILURES)
        if err:
            return None, err
    if total is not None and fails is not None and fails > total:
        return None, "gate_failures must be <= total_changes"

    if fails is not None:
        expected = fails
    elif total is not None:
        ratio = DEMO_FAIL_PER_BATCH / max(DEMO_BATCH_SIZE, 1)
        expected = max(0, int(round(total * ratio)))
    else:
        expected = DEMO_EXPECTED_FAILURES

    return {
        "total_changes": total,
        "gate_failures": fails,
        "expected_failures": int(expected),
    }, None


def fails_for_batch(
        batch_size: int,
        remaining_changes: int,
        remaining_fails: int) -> int:
    """Distribute remaining fail stamps across remaining changes.

    Ensures the fail target remains reachable after this batch and never
    exceeds batch_size or remaining_fails.
    """
    batch_size = max(0, int(batch_size))
    remaining_changes = max(0, int(remaining_changes))
    remaining_fails = max(0, int(remaining_fails))
    if batch_size <= 0 or remaining_changes <= 0 or remaining_fails <= 0:
        return 0
    size = min(batch_size, remaining_changes)
    after = remaining_changes - size
    hi = min(size, remaining_fails)
    # Must stamp at least this many now so the target stays reachable;
    # clamp to hi when remaining_fails somehow exceeds remaining_changes.
    lo = min(hi, max(0, remaining_fails - after))
    prop = int(round(remaining_fails * size / float(remaining_changes)))
    return max(lo, min(hi, prop))


def fails_for_duration_batch(
        batch_size: int,
        remaining_fails: int,
        batches_left: int) -> int:
    """Spread remaining fail stamps across estimated remaining duration batches.

    Last estimated batch takes the remainder so the absolute fail target is
    reachable before the continuous window ends.
    """
    batch_size = max(0, int(batch_size))
    remaining_fails = max(0, int(remaining_fails))
    batches_left = max(1, int(batches_left))
    if batch_size <= 0 or remaining_fails <= 0:
        return 0
    if batches_left <= 1:
        return min(batch_size, remaining_fails)
    # Keep the target reachable: leave at most batch_size per future batch.
    lo = min(batch_size, max(0, remaining_fails - (batches_left - 1) * batch_size))
    prop = int(math.ceil(remaining_fails / float(batches_left)))
    return max(lo, min(batch_size, remaining_fails, prop))


def downsample_keep_span(items: Sequence, max_points: int) -> List:
    """Uniformly sample across the full series, always keeping first + last."""
    if max_points <= 0 or len(items) <= max_points:
        return list(items)
    n = len(items)
    if max_points == 1:
        return [items[-1]]
    out: List = []
    last_idx = -1
    for i in range(max_points):
        idx = int(round(i * (n - 1) / float(max_points - 1)))
        if idx == last_idx:
            continue
        out.append(items[idx])
        last_idx = idx
    if out and out[-1] is not items[-1]:
        out[-1] = items[-1]
    if out and out[0] is not items[0]:
        out[0] = items[0]
    return out


def default_demo_targets() -> dict:
    """Visible UI / empty-/run-demo defaults for Changes and Fails."""
    return {
        "total_changes": DEMO_CHANGE_COUNT,
        "gate_failures": DEMO_EXPECTED_FAILURES,
        "expected_failures": DEMO_EXPECTED_FAILURES,
        "duration_sec": DEMO_DURATION_SEC,
        "batch_size": DEMO_BATCH_SIZE,
        "fail_per_batch": DEMO_FAIL_PER_BATCH,
    }


def job_runs_saved_est(extra_changes_total: int) -> int:
    """Session extras ≈ estimated job-runs saved (demo gate jobs ~1s)."""
    return max(0, int(extra_changes_total or 0))


def _session_expected_failures() -> int:
    with LOCK:
        value = STATE.get("demo_expected_failures")
    if value is None:
        return DEMO_EXPECTED_FAILURES
    try:
        return max(0, int(value))
    except (TypeError, ValueError):
        return DEMO_EXPECTED_FAILURES


def _demo_progress_idle() -> dict:
    planned = _planned_batches()
    return {
        "demo_id": None,
        "phase": "idle",
        "message": "",
        "queues_cleared": 0,
        "queue_depth_remaining": 0,
        "changes_submitted": 0,
        "failures_so_far": 0,
        "elapsed_s": 0.0,
        "percent": 0,
        "wait_elapsed_s": 0.0,
        "wait_total_s": _effective_demo_gate_wait_sec(),
        "started_at": None,
        "batches_completed": 0,
        "batches_planned": planned,
        "batches_remaining": planned,
        "batch_current": 0,
        "batches_plan_tip": _batches_plan_tip(planned),
        "extend_available": False,
        "traffic_active": False,
        "time_remaining_s": DEMO_DURATION_SEC,
        "fail_per_batch": DEMO_FAIL_PER_BATCH,
        "pass_per_batch": DEMO_PASS_PER_BATCH,
        "batch_size": DEMO_BATCH_SIZE,
        "batch_interval_sec": DEMO_BATCH_INTERVAL_SEC,
        "duration_sec": DEMO_DURATION_SEC,
        "gate_queue_depth": 0,
        "rl_window": None,
        "tcp_window": None,
        "extra_in_flight": 0,
        "total_changes": None,
        "gate_failures_target": None,
        "expected_failures": DEMO_EXPECTED_FAILURES,
    }

STATE: Dict[str, object] = {
    "running": False,
    "started_at": None,
    "finished_at": None,
    "last_error": None,
    "latest_run": None,
    "demo_session_start": None,
    "demo_progress": _demo_progress_idle(),
    # Continuous traffic control (extend does not cancel in-flight builds).
    "traffic_stop": False,
    "extend_batches": 0,
    "traffic_deadline": None,
    "batches_completed": 0,
    # Optional /run-demo targets (None = duration / default-ratio mode).
    "demo_total_changes": None,
    "demo_gate_failures": None,
    "demo_expected_failures": DEMO_EXPECTED_FAILURES,
    "demo_fail_stamped": 0,
}

LOCK = threading.Lock()
_METRICS_LOCK = threading.Lock()


class _IncrementalAuditReader:
    """Append-only audit log reader — re-reads only new bytes."""

    def __init__(self):
        self._events: List[dict] = []
        self._offset = 0
        self._inode: Optional[int] = None

    def read(self, path: Path) -> List[dict]:
        cutoff = time.time() - LIVE_METRICS_LOOKBACK_SEC
        if not path.is_file():
            self._events = []
            self._offset = 0
            self._inode = None
            return []
        stat = path.stat()
        if self._inode != stat.st_ino or stat.st_size < self._offset:
            self._events = []
            self._offset = 0
            self._inode = stat.st_ino
        with path.open(encoding="utf-8") as handle:
            handle.seek(self._offset)
            chunk = handle.read()
            self._offset = handle.tell()
        if chunk:
            for line in chunk.splitlines():
                line = line.strip()
                if not line:
                    continue
                try:
                    record = json.loads(line)
                except json.JSONDecodeError:
                    continue
                self._events.append(record)
        if self._events:
            self._events = [
                e for e in self._events
                if float(e.get("timestamp", 0)) >= cutoff
            ]
        return list(self._events)


_AUDIT_READER = _IncrementalAuditReader()
_BUILDS_CACHE: Dict[str, object] = {"ts": 0.0, "data": []}
_LIVE_METRICS_CACHE: Dict[str, object] = {"ts": 0.0, "data": None}


def _set_demo_progress(**kwargs) -> dict:
    with LOCK:
        prog = dict(STATE.get("demo_progress") or _demo_progress_idle())
        prog.update(kwargs)
        started = prog.get("started_at")
        if started:
            prog["elapsed_s"] = round(time.time() - float(started), 1)
        # Watchdog heartbeat: the UI warns when this stops advancing while
        # a demo is running (stale > ~30s means the worker is stuck).
        prog["progress_updated_at"] = time.time()
        STATE["demo_progress"] = prog
        return dict(prog)


class _PhaseHeartbeat:
    """Background progress heartbeat for long-running demo phases.

    Guarantees the demo-progress message/timestamp advances every few
    seconds even while a blocking call (layout sync, batch push, docker
    restart) is in flight, so the UI never looks frozen.
    """

    def __init__(self, message_fn, interval: float = 3.0, **progress_kwargs):
        self._message_fn = message_fn
        self._interval = interval
        self._progress_kwargs = progress_kwargs
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._started_at = time.time()

    def __enter__(self):
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()
        return self

    def __exit__(self, *exc_info):
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=self._interval + 1)
        return False

    def _run(self):
        while not self._stop.wait(self._interval):
            elapsed = time.time() - self._started_at
            try:
                message = self._message_fn(elapsed)
            except Exception:
                message = None
            kwargs = dict(self._progress_kwargs)
            if message:
                kwargs["message"] = message
            try:
                _set_demo_progress(**kwargs)
            except Exception:
                log.debug("phase heartbeat update failed", exc_info=True)


def _demo_progress_snapshot() -> dict:
    with LOCK:
        prog = dict(STATE.get("demo_progress") or _demo_progress_idle())
        started = prog.get("started_at")
        if started:
            prog["elapsed_s"] = round(time.time() - float(started), 1)
        return prog


def _audit_contains_demo_reset(
        session_start: Optional[float] = None) -> bool:
    """True once the scheduler has processed a demo session reset."""
    if not AUDIT_PATH.is_file():
        return False
    try:
        tail = AUDIT_PATH.read_text(encoding="utf-8")[-65536:]
    except OSError:
        return False
    for line in tail.splitlines():
        try:
            event = json.loads(line)
        except json.JSONDecodeError:
            continue
        ts = float(event.get("timestamp", 0))
        if session_start is not None and ts < session_start - 0.5:
            continue
        if event.get("event") in ("demo_session_reset", "demo_queue_purged"):
            return True
        if (event.get("event") == "agent_tick"
                and event.get("baseline")):
            return True
    return False


def _reset_file_pending() -> bool:
    return DEMO_RESET_PATH.is_file()


def _fetch_gate_rl_windows() -> Tuple[Optional[float], Optional[float]]:
    """Return (rl_window, tcp_shadow) for the gate pipeline when exposed."""
    try:
        status = _fetch_tenant_status(ZUUL_API)
        for pipeline in status.get("pipelines", []):
            if pipeline.get("name") != DEMO_PIPELINE:
                continue
            rl_info = pipeline.get("rl_window") or {}
            queues = rl_info.get("queues") or []
            if queues:
                q = queues[0]
                rl = q.get("current_window")
                tcp = q.get("tcp_shadow_window")
                if rl is not None:
                    return float(rl), (
                        float(tcp) if tcp is not None else float(rl))
            for change_queue in pipeline.get("change_queues", []):
                rl = (change_queue.get("rl_current_window")
                      or change_queue.get("window"))
                tcp = change_queue.get("rl_tcp_shadow_window")
                if rl is not None:
                    return float(rl), (
                        float(tcp) if tcp is not None else float(rl))
    except Exception:
        pass
    return None, None


def _windows_at_baseline() -> bool:
    rl, tcp = _fetch_gate_rl_windows()
    if rl is None:
        return False
    baseline = float(DEFAULT_INITIAL_WINDOW)
    return rl == baseline and (tcp is None or tcp == baseline)


def _wait_for_demo_reset(
        session_start: float,
        timeout: float = DEMO_AUDIT_WAIT,
        interval: float = DEMO_RESET_POLL_INTERVAL,
        *,
        update_progress: bool = True):
    """Wait briefly for scheduler demo reset; proceed after grace if consumed."""
    deadline = time.time() + timeout
    wait_started = time.time()
    reset_consumed_at: Optional[float] = None
    if not _reset_file_pending():
        reset_consumed_at = wait_started

    while time.time() < deadline:
        now = time.time()
        if _audit_contains_demo_reset(session_start):
            return
        if _windows_at_baseline():
            return

        if not _reset_file_pending():
            if reset_consumed_at is None:
                reset_consumed_at = now
            if now - reset_consumed_at >= DEMO_RESET_GRACE_SEC:
                log.info(
                    "demo reset trigger consumed; proceeding after %.1fs grace",
                    DEMO_RESET_GRACE_SEC)
                return
        else:
            reset_consumed_at = None

        if update_progress:
            elapsed = now - wait_started
            rl, tcp = _fetch_gate_rl_windows()
            if rl is not None:
                window_msg = (
                    f"RL={int(rl)} TCP={int(tcp if tcp is not None else rl)}"
                )
            else:
                window_msg = "waiting for scheduler"
            _set_demo_progress(
                phase="resetting_windows",
                message=(
                    f"Resetting RL/TCP windows to {DEFAULT_INITIAL_WINDOW} "
                    f"({window_msg}) · {elapsed:.0f}s"
                ),
                percent=min(9, 3 + int(6 * elapsed / max(timeout, 1))),
            )
        time.sleep(interval)

    log.warning(
        "demo reset not confirmed in audit within %.1fs; proceeding to "
        "queue clear",
        timeout)


def _demo_reset_ready(session_start: float) -> bool:
    """True once scheduler has processed this session's demo reset."""
    return _audit_contains_demo_reset(session_start)


def _cors(resp):
    resp.headers["Access-Control-Allow-Origin"] = "*"
    resp.headers["Access-Control-Allow-Methods"] = "GET, POST, OPTIONS"
    resp.headers["Access-Control-Allow-Headers"] = "Content-Type"
    return resp


@APP.after_request
def after_request(response):
    return _cors(response)


@APP.route("/health", methods=["GET"])
def health():
    return jsonify({"ok": True, "running": bool(STATE["running"])})


@APP.route("/status", methods=["GET"])
def status():
    with LOCK:
        payload = {
            "running": STATE["running"],
            "started_at": STATE["started_at"],
            "finished_at": STATE["finished_at"],
            "last_error": STATE["last_error"],
            "latest_run": STATE["latest_run"],
            "demo_session_start": STATE["demo_session_start"],
        }
    payload["demo_progress"] = _demo_progress_snapshot()
    if payload.get("latest_run"):
        payload["report_url"] = "/rl-report/report.html"
        payload["throughput_graph_url"] = "/rl-report/throughput_graph.png"
        payload["window_delta_graph_url"] = "/rl-report/window_delta_graph.png"
        payload["summary_url"] = "/rl-report/summary.json"
    cached = _get_cached_live_metrics()
    payload["latest"] = cached.get("latest")
    payload["effectiveness"] = cached.get("effectiveness")
    payload["advantage"] = cached.get("advantage")
    payload["session_summary"] = cached.get("session_summary")
    counts = cached.get("failure_counts", {})
    payload["failure_counts"] = counts
    # Prefer session_summary gate_failures (aligned with extras) when present.
    session = cached.get("session_summary") or {}
    payload["failure_count"] = session.get(
        "gate_failures", counts.get("gate_jobs_total", 0))
    payload["demo_change_count"] = DEMO_CHANGE_COUNT
    payload["baseline_window"] = DEFAULT_INITIAL_WINDOW
    payload["expected_failures"] = _session_expected_failures()
    defaults = default_demo_targets()
    payload["default_total_changes"] = defaults["total_changes"]
    payload["default_gate_failures"] = defaults["gate_failures"]
    payload["default_expected_failures"] = defaults["expected_failures"]
    with LOCK:
        if STATE.get("demo_total_changes") is not None:
            payload["demo_change_count"] = STATE["demo_total_changes"]
        if STATE.get("demo_gate_failures") is not None:
            payload["gate_failures_target"] = STATE["demo_gate_failures"]
    return jsonify(payload)


@APP.route("/demo-progress", methods=["GET"])
def demo_progress():
    prog = _demo_progress_snapshot()
    with LOCK:
        running = bool(STATE["running"])
        last_error = STATE.get("last_error")
        latest_run = STATE.get("latest_run")
    return jsonify({
        "ok": True,
        "running": running,
        "last_error": last_error,
        "latest_run": latest_run,
        **prog,
    })


def _load_audit_events(path: Path) -> List[dict]:
    return _AUDIT_READER.read(path)


def _cached_builds(api_url: str) -> List[dict]:
    now = time.time()
    with _METRICS_LOCK:
        age = now - float(_BUILDS_CACHE["ts"])
        if age < BUILDS_CACHE_SEC and _BUILDS_CACHE["data"]:
            return list(_BUILDS_CACHE["data"])
    builds = fetch_builds(api_url)
    with _METRICS_LOCK:
        _BUILDS_CACHE["ts"] = now
        _BUILDS_CACHE["data"] = builds
    return builds


def _invalidate_live_metrics_cache():
    with _METRICS_LOCK:
        _LIVE_METRICS_CACHE["ts"] = 0.0
        _LIVE_METRICS_CACHE["data"] = None
        _BUILDS_CACHE["ts"] = 0.0
        _BUILDS_CACHE["data"] = []


def _reset_audit_reader():
    _AUDIT_READER._events = []
    _AUDIT_READER._offset = 0
    _AUDIT_READER._inode = None


def _gerrit_auth_header() -> str:
    import base64
    return "Basic " + base64.b64encode(GERRIT_AUTH.encode()).decode()


def _fetch_project_layout(project: str) -> dict:
    url = (
        f"{ZUUL_API.rstrip('/')}/api/tenant/"
        f"{urllib.parse.quote(DEMO_TENANT)}/project/"
        f"{urllib.parse.quote(project)}"
    )
    with urllib.request.urlopen(url, timeout=30) as resp:
        return json.load(resp)


def _pipeline_job_names(layout: dict, pipeline: str) -> List[str]:
    names: List[str] = []
    for config in layout.get("configs", []):
        for pipe in config.get("pipelines", []):
            if pipe.get("name") != pipeline:
                continue
            for job_group in pipe.get("jobs", []):
                for job in job_group:
                    name = job.get("name")
                    if name:
                        names.append(name)
    return names


def _fetch_components() -> object:
    try:
        url = f"{ZUUL_API.rstrip('/')}/api/components"
        with urllib.request.urlopen(url, timeout=10) as resp:
            return json.load(resp)
    except (urllib.error.URLError, TimeoutError, OSError, json.JSONDecodeError):
        return None


def _component_is_running(kind: str) -> bool:
    """True when at least one Zuul component of kind reports running state."""
    data = _fetch_components()
    if data is None:
        return False
    if isinstance(data, dict):
        entries = data.get(kind) or []
        return any(
            isinstance(entry, dict)
            and (entry.get("state") or "").lower() == "running"
            for entry in entries
        )
    if isinstance(data, list):
        return any(
            isinstance(entry, dict)
            and entry.get("kind") == kind
            and (entry.get("state") or "").lower() == "running"
            for entry in data
        )
    return False


def _executor_is_running() -> bool:
    """True when at least one Zuul executor reports running state."""
    return _component_is_running("executor")


def _stack_service_container(service: str) -> str:
    """Map logical service name to compose container (e.g. zuul-rl-executor-1)."""
    if SCHEDULER_CONTAINER and "-scheduler-" in SCHEDULER_CONTAINER:
        prefix = SCHEDULER_CONTAINER.split("-scheduler-")[0]
        return f"{prefix}-{service}-1"
    return service


def _start_stack_workers() -> None:
    """Best-effort start of executor/launcher/node via docker start or compose."""
    services = ("executor", "launcher", "node")
    if SCHEDULER_CONTAINER:
        for service in services:
            try:
                run(
                    ["docker", "start", _stack_service_container(service)],
                    check=False,
                    timeout=30,
                    capture_output=True,
                    text=True,
                )
            except (CalledProcessError, FileNotFoundError, OSError):
                pass
    compose_dir = os.environ.get("RL_COMPOSE_DIR", "").strip()
    compose_project = os.environ.get("RL_COMPOSE_PROJECT", "").strip()
    compose_file = os.environ.get(
        "RL_COMPOSE_FILE", "docker-compose.rl-app.yaml")
    if compose_dir and compose_project:
        compose_path = Path(compose_dir) / compose_file
        if compose_path.is_file():
            try:
                # rl-app compose already includes the base file; pass base
                # first only when using both so overlays win on merge.
                base_compose = Path(compose_dir) / "docker-compose.yaml"
                compose_args = ["docker", "compose"]
                if base_compose.is_file() and compose_path.name != base_compose.name:
                    compose_args.extend(["-f", str(base_compose)])
                # Prefer `start` over `up -d` so a readiness retry never
                # recreates dependent services (scheduler/web/control) and
                # kills the in-flight demo.
                compose_args.extend([
                    "-f", str(compose_path),
                    "-p", compose_project,
                    "start", *services, "logs",
                ])
                run(
                    compose_args,
                    check=False,
                    timeout=180,
                    capture_output=True,
                    text=True,
                )
            except (CalledProcessError, FileNotFoundError, OSError) as exc:
                log.warning("compose up for stack workers failed: %s", exc)


def _ensure_executor_ready(*, update_progress: bool = False) -> None:
    """Fail fast when gate jobs cannot run (executor/launcher down)."""
    if _executor_is_running() and _component_is_running("launcher"):
        return
    log.warning(
        "stack workers not ready (executor=%s launcher=%s); starting",
        _executor_is_running(), _component_is_running("launcher"))
    _start_stack_workers()
    start = time.time()
    deadline = start + 90
    while time.time() < deadline:
        if _executor_is_running() and _component_is_running("launcher"):
            return
        if update_progress:
            _set_demo_progress(
                message=(
                    "Starting Zuul executor/launcher "
                    f"({time.time() - start:.0f}s / 90s max)…"
                ),
            )
        time.sleep(2)
    raise RuntimeError(
        "Zuul executor/launcher is not running — gate jobs will not execute. "
        "Run: docker compose -f docker-compose.yaml "
        "-f docker-compose.rl-app.yaml -p zuul-rl "
        "up -d executor launcher node logs")


def _layout_is_check_then_gate(project: str = "test1") -> bool:
    """True when test1 has check → gate jobs (traditional Zuul flow)."""
    try:
        layout = _fetch_project_layout(project)
    except (urllib.error.URLError, TimeoutError, OSError, json.JSONDecodeError):
        return False
    check_jobs = _pipeline_job_names(layout, "check")
    gate_jobs = _pipeline_job_names(layout, "gate")
    return (
        "research-check-job" in check_jobs
        and "research-gate-job" in gate_jobs
    )


def _layout_is_gate_only(project: str = "test1") -> bool:
    """Deprecated alias — prefer _layout_is_check_then_gate."""
    return _layout_is_check_then_gate(project)


def _restart_scheduler_container() -> bool:
    if not SCHEDULER_CONTAINER:
        return False
    try:
        run(
            ["docker", "restart", SCHEDULER_CONTAINER],
            check=True,
            timeout=120,
            capture_output=True,
            text=True,
        )
        time.sleep(30)
        return True
    except (CalledProcessError, FileNotFoundError, OSError) as exc:
        log.warning("scheduler restart failed: %s", exc)
        return False


def _full_reconfigure_scheduler() -> bool:
    """Reload tenant config from disk into ZK (fixes stale main.yaml includes)."""
    if not SCHEDULER_CONTAINER:
        return False
    try:
        run(
            ["docker", "exec", SCHEDULER_CONTAINER,
             "zuul-scheduler", "full-reconfigure"],
            check=True,
            timeout=180,
            capture_output=True,
            text=True,
        )
        log.info("scheduler full-reconfigure completed")
        time.sleep(8)
        return True
    except (CalledProcessError, FileNotFoundError, OSError) as exc:
        log.warning("scheduler full-reconfigure failed: %s", exc)
        return False


def _sync_zuul_config_from_bundle() -> bool:
    """Push bundled zuul-config to Gerrit and merge (idempotent)."""
    bundle = CONFIG_BUNDLE
    compose_bundle = Path("/compose/zuul-config")
    if compose_bundle.is_dir():
        shutil.rmtree(bundle, ignore_errors=True)
        shutil.copytree(compose_bundle, bundle)
    if not bundle.is_dir():
        log.warning("config bundle missing: %s", bundle)
        return False
    workdir = Path("/tmp/rl-config-sync")
    shutil.rmtree(workdir, ignore_errors=True)
    workdir.mkdir(parents=True, exist_ok=True)
    repo = workdir / "zuul-config"
    gerrit_host = GERRIT_BASE.removeprefix("http://").removeprefix("https://")
    run(
        ["git", "clone",
         f"http://{GERRIT_AUTH}@{gerrit_host}/zuul-config", str(repo)],
        check=True,
        capture_output=True,
        text=True,
        timeout=120,
    )
    shutil.rmtree(repo / "zuul.d", ignore_errors=True)
    shutil.rmtree(repo / "playbooks", ignore_errors=True)
    shutil.copytree(bundle / "zuul.d", repo / "zuul.d")
    shutil.copytree(bundle / "playbooks", repo / "playbooks")
    zuul_yaml = bundle / ".zuul.yaml"
    if zuul_yaml.is_file():
        shutil.copy2(zuul_yaml, repo / ".zuul.yaml")
    elif (repo / ".zuul.yaml").exists():
        (repo / ".zuul.yaml").unlink()
    run(["git", "-C", str(repo), "config", "user.email", "admin@example.com"],
        check=True, timeout=30)
    run(["git", "-C", str(repo), "config", "user.name", "Admin"], check=True,
        timeout=30)
    hook = repo / ".git" / "hooks" / "commit-msg"
    run(
        ["curl", "-sf", "--max-time", "60", "-o", str(hook),
         f"{GERRIT_BASE}/tools/hooks/commit-msg"],
        check=True,
        timeout=90,
    )
    hook.chmod(0o755)
    add_paths = [".zuul.yaml", "zuul.d", "playbooks"]
    if not (bundle / ".zuul.yaml").is_file():
        add_paths = ["zuul.d", "playbooks"]
    run(["git", "-C", str(repo), "add", "-A"],
        check=True, timeout=60)
    diff = run(
        ["git", "-C", str(repo), "diff", "--cached", "--quiet"],
        check=False,
        timeout=60,
    )
    if diff.returncode == 0:
        log.info("zuul-config already matches bundle")
        return True
    run(
        ["git", "-C", str(repo), "commit", "-m",
         "Sync RL research Zuul configuration"],
        check=True,
        timeout=60,
    )
    push = run(
        ["git", "-C", str(repo), "push",
         f"http://{GERRIT_AUTH}@{gerrit_host}/zuul-config",
         "HEAD:refs/for/master"],
        check=True,
        capture_output=True,
        text=True,
        timeout=120,
    )
    change = None
    for line in (push.stdout or "").splitlines() + (push.stderr or "").splitlines():
        if "/+/" in line:
            change = int(line.split("/+/")[-1].split()[0])
            break
    if change is None:
        query = urllib.request.Request(
            f"{GERRIT_BASE}/a/changes/?q=project:zuul-config+status:open&n=1",
            headers={"Authorization": _gerrit_auth_header()},
        )
        with urllib.request.urlopen(query, timeout=30) as resp:
            raw = resp.read().decode()
        if raw.startswith(")]}'"):
            raw = raw[4:]
        items = json.loads(raw or "[]")
        if not items:
            raise RuntimeError("zuul-config push succeeded but no open change found")
        change = int(items[0]["_number"])
    review_body = json.dumps({
        "labels": {"Code-Review": 2, "Verified": 2, "Workflow": 1},
    }).encode()
    review_req = urllib.request.Request(
        f"{GERRIT_BASE}/a/changes/zuul-config~{change}/revisions/current/review",
        data=review_body,
        method="POST",
        headers={
            "Content-Type": "application/json",
            "Authorization": _gerrit_auth_header(),
        },
    )
    with urllib.request.urlopen(review_req, timeout=30) as resp:
        resp.read()
    submit_req = urllib.request.Request(
        f"{GERRIT_BASE}/a/changes/zuul-config~{change}/submit",
        method="POST",
        headers={"Authorization": _gerrit_auth_header()},
    )
    with urllib.request.urlopen(submit_req, timeout=30) as resp:
        resp.read()
    log.info("merged zuul-config change %s", change)
    return True


def _wait_for_check_then_gate_layout(
        timeout: float = CONFIG_SYNC_TIMEOUT) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        if _layout_is_check_then_gate():
            return True
        time.sleep(CONFIG_LAYOUT_POLL)
    return _layout_is_check_then_gate()


def _ensure_check_then_gate_layout() -> None:
    """Sync zuul-config until traditional check → gate layout is active."""
    if _layout_is_check_then_gate():
        return
    log.warning(
        "layout is not check→gate; syncing zuul-config from bundle")
    if not _sync_zuul_config_from_bundle():
        raise RuntimeError(
            f"unable to sync zuul-config from {CONFIG_BUNDLE}")
    if _wait_for_check_then_gate_layout(timeout=45):
        return
    # Reload tenant config from disk (ZK may still hold a stale main.yaml
    # that omitted provider/section classes and broke static nodes).
    if _full_reconfigure_scheduler():
        if _wait_for_check_then_gate_layout(timeout=60):
            return
    if _restart_scheduler_container():
        _full_reconfigure_scheduler()
        if _wait_for_check_then_gate_layout(timeout=CONFIG_SYNC_TIMEOUT):
            return
    raise RuntimeError(
        "Zuul layout still missing traditional check→gate for test1 "
        "(need research-check-job on check and research-gate-job on gate). "
        "Restart the scheduler container and retry.")


def _ensure_gate_only_layout() -> None:
    """Deprecated alias — prefer _ensure_check_then_gate_layout."""
    _ensure_check_then_gate_layout()


def _get_cached_live_metrics() -> dict:
    now = time.time()
    with _METRICS_LOCK:
        cached = _LIVE_METRICS_CACHE["data"]
        age = now - float(_LIVE_METRICS_CACHE["ts"])
        if cached is not None and age < LIVE_METRICS_CACHE_SEC:
            return dict(cached)
    metrics = build_live_metrics()
    with _METRICS_LOCK:
        _LIVE_METRICS_CACHE["ts"] = now
        _LIVE_METRICS_CACHE["data"] = metrics
    return metrics


def _session_start_ts(events: Sequence[dict]) -> Optional[float]:
    """Return timestamp of the latest scheduler agent_started event."""
    started = [
        float(e["timestamp"])
        for e in events
        if e.get("event") == "agent_started"
    ]
    return max(started) if started else None


def _effective_session_start(events: Sequence[dict]) -> Optional[float]:
    """Return demo session start when a demo is active, else latest agent_started."""
    with LOCK:
        demo_start = STATE.get("demo_session_start")
    if demo_start is not None:
        return float(demo_start)
    return _session_start_ts(events)


def _make_zuul_admin_token() -> str:
    """JWT for Zuul dequeue API (HS256 zuul_operator authenticator)."""
    now = int(time.time())
    payload = {
        "iss": ZUUL_ADMIN_JWT_ISSUER,
        "aud": ZUUL_ADMIN_JWT_AUDIENCE,
        "sub": "rl-control",
        "iat": now,
        "exp": now + 600,
        "zuul": {"admin": [DEMO_TENANT]},
    }
    return jwt.encode(payload, ZUUL_ADMIN_JWT_SECRET, algorithm="HS256")


def _fetch_tenant_status(api_url: str) -> dict:
    url = (
        f"{api_url.rstrip('/')}/api/tenant/"
        f"{urllib.parse.quote(DEMO_TENANT)}/status"
    )
    with urllib.request.urlopen(url, timeout=30) as resp:
        return json.load(resp)


def _head_is_live(head: Sequence[dict]) -> bool:
    if not head:
        return False
    return head[0].get("live") is not False


def _all_queue_depth(status: dict) -> int:
    """Count live queue head groups across all pipelines."""
    depth = 0
    for pipeline in status.get("pipelines", []):
        for change_queue in pipeline.get("change_queues", []):
            for head in change_queue.get("heads", []):
                if _head_is_live(head):
                    depth += 1
    return depth


def _all_queue_item_count(status: dict) -> int:
    """Count every live queued change across all pipeline head groups."""
    return sum(1 for _ in _iter_all_queue_items(status))


def _effective_queue_clear_timeout(item_count: int, head_count: int) -> float:
    """Scale clear timeout for large backlogs and async dequeue processing."""
    if item_count <= 0 and head_count <= 0:
        return DEMO_QUEUE_CLEAR_TIMEOUT
    items = max(item_count, head_count)
    heads = max(head_count, 1)
    workers = max(DEMO_DEQUEUE_WORKERS, 1)
    # One REST round dequeues at most one tail per head group.
    rounds = max(1, (heads + workers - 1) // workers)
    per_round = (
        DEMO_QUEUE_CLEAR_TIMEOUT_PER_ITEM
        + DEMO_QUEUE_PROCESSING_WAIT
        + DEMO_QUEUE_POLL_INTERVAL
    )
    estimated = rounds * per_round
    # Large saturated backlogs need extra wall time for scheduler purge + ZK.
    estimated += min(120.0, items * 0.08)
    return min(
        DEMO_QUEUE_CLEAR_MAX_TIMEOUT,
        max(DEMO_QUEUE_CLEAR_TIMEOUT, estimated + 15.0),
    )


def _format_queue_change_label(project: str, change: str) -> str:
    """Human-readable change label, e.g. test1 405,1."""
    return f"{project} {change}"


def _describe_queue_backlog(status: dict, sample_limit: int = 5) -> List[dict]:
    """Summarize remaining queue depth per pipeline for logging/errors."""
    by_pipeline: Dict[str, List[str]] = {}
    for pipeline_name, project, change in _iter_all_queue_items(status):
        by_pipeline.setdefault(pipeline_name, []).append(
            _format_queue_change_label(project, change))
    return [
        {
            "pipeline": pipeline,
            "count": len(changes),
            "sample_changes": changes[:sample_limit],
        }
        for pipeline, changes in sorted(by_pipeline.items())
    ]


def _gate_queue_depth(status: dict) -> int:
    depth = 0
    for pipeline in status.get("pipelines", []):
        if pipeline.get("name") != DEMO_PIPELINE:
            continue
        for change_queue in pipeline.get("change_queues", []):
            for head in change_queue.get("heads", []):
                if _head_is_live(head):
                    depth += 1
    return depth


def _gate_queue_item_count(status: dict) -> int:
    """Count every live queued change in the gate pipeline."""
    return sum(1 for _ in _iter_gate_queue_items(status))


def _changes_in_window_counts(
        gate_queue_count: int,
        rl_window: float,
        tcp_window: float) -> Tuple[int, int]:
    """Parallel changes active under RL vs TCP window caps."""
    rl_cap = max(int(round(rl_window)), 0)
    tcp_cap = max(int(round(tcp_window)), 0)
    rl_in = min(gate_queue_count, rl_cap) if rl_cap > 0 else 0
    tcp_in = min(gate_queue_count, tcp_cap) if tcp_cap > 0 else 0
    return rl_in, tcp_in


def _normalize_dequeue_project(project: str) -> str:
    if not project:
        return project
    if "/" in project:
        project = project.split("/", 1)[-1]
    return project


def _normalize_dequeue_change(ref: dict) -> Optional[str]:
    """Return Zuul dequeue change id (number,patchset) from a status ref."""
    change = ref.get("id")
    patchset = ref.get("patchset")
    nested = ref.get("ref")
    if isinstance(nested, dict):
        if not change or change == nested.get("ref"):
            change = nested.get("change") or change
        patchset = patchset or nested.get("patchset")
    if change is None:
        return None
    change = str(change)
    if "," in change:
        return change
    if patchset is not None:
        return f"{change},{patchset}"
    return f"{change},1"


def _item_from_queue_head(item: dict):
    refs = item.get("refs") or []
    if not refs:
        return None
    ref = refs[0]
    project = _normalize_dequeue_project(
        ref.get("project") or ref.get("project_canonical") or "")
    change = _normalize_dequeue_change(ref)
    if not project or not change:
        return None
    return project, change


def _iter_all_queue_items(status: dict):
    """Yield (pipeline_name, project, change_id) for every queued change."""
    seen = set()
    for pipeline in status.get("pipelines", []):
        pipeline_name = pipeline.get("name")
        if not pipeline_name:
            continue
        for change_queue in pipeline.get("change_queues", []):
            for head in change_queue.get("heads", []):
                if not _head_is_live(head):
                    continue
                for item in head:
                    parsed = _item_from_queue_head(item)
                    if parsed is None:
                        continue
                    project, change = parsed
                    key = (pipeline_name, project, change)
                    if key in seen:
                        continue
                    seen.add(key)
                    yield pipeline_name, project, change


def _iter_gate_queue_items(status: dict):
    """Yield (project, change_id) for each queued gate item."""
    for pipeline_name, project, change in _iter_all_queue_items(status):
        if pipeline_name == DEMO_PIPELINE:
            yield project, change


def _iter_dequeue_targets(status: dict):
    """Yield (pipeline, project, change) tail-first, one per head group.

    REST dequeue only succeeds reliably for tail items in a dependent chain;
    dequeuing every dependent in parallel causes silent scheduler failures.
    """
    targets: List[Tuple[str, str, str]] = []
    for pipeline in status.get("pipelines", []):
        pipeline_name = pipeline.get("name")
        if not pipeline_name:
            continue
        for change_queue in pipeline.get("change_queues", []):
            for head in change_queue.get("heads", []):
                if not _head_is_live(head):
                    continue
                parsed = _item_from_queue_head(head[-1])
                if parsed is None:
                    continue
                project, change = parsed
                targets.append((pipeline_name, project, change))
    for target in reversed(targets):
        yield target


def _dequeue_change(
        api_url: str,
        token: str,
        pipeline: str,
        project: str,
        change: str) -> bool:
    if "/" in project:
        project = project.split("/", 1)[-1]
    path = urllib.parse.quote(project, safe="")
    url = (
        f"{api_url.rstrip('/')}/api/tenant/"
        f"{urllib.parse.quote(DEMO_TENANT)}/project/{path}/dequeue"
    )
    body = json.dumps({
        "pipeline": pipeline,
        "change": change,
    }).encode()
    req = urllib.request.Request(
        url,
        data=body,
        method="POST",
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Bearer {token}",
        },
    )
    try:
        with urllib.request.urlopen(
                req, timeout=DEMO_DEQUEUE_HTTP_TIMEOUT) as resp:
            resp.read()
        return True
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        log.warning(
            "dequeue %s %s %s failed: HTTP %s %s",
            pipeline, project, change, exc.code, body[:200])
        return False
    except (urllib.error.URLError, TimeoutError, OSError) as exc:
        log.warning(
            "dequeue %s %s %s failed: %s",
            pipeline, project, change, exc)
        return False


def _batch_dequeue_items(
        api_url: str,
        items: Sequence[Tuple[str, str, str]],
        *,
        wait_for_results: bool = True) -> int:
    """Fire parallel REST dequeue requests for many queued items."""
    if not items:
        return 0
    token = _make_zuul_admin_token()
    workers = min(DEMO_DEQUEUE_WORKERS, max(1, len(items)))
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = [
            pool.submit(
                _dequeue_change, api_url, token, pipeline, project, change)
            for pipeline, project, change in items
        ]
        if not wait_for_results:
            return len(items)
        removed = 0
        failed = 0
        for future in as_completed(futures):
            try:
                if future.result():
                    removed += 1
                else:
                    failed += 1
            except Exception:
                failed += 1
        if failed:
            log.warning(
                "batch dequeue: %d ok, %d failed of %d",
                removed, failed, len(items))
        return removed


def clear_all_queue_backlog(
        api_url: str, status: Optional[dict] = None) -> int:
    """Dequeue every item waiting in any pipeline (check, gate, etc.)."""
    if status is None:
        status = _fetch_tenant_status(api_url)
    items = list(_iter_all_queue_items(status))
    return _batch_dequeue_items(api_url, items)


def _queue_counts_by_pipeline(status: dict) -> Dict[str, int]:
    """Count live queued changes per pipeline name."""
    counts: Dict[str, int] = {}
    for pipeline_name, _project, _change in _iter_all_queue_items(status):
        counts[pipeline_name] = counts.get(pipeline_name, 0) + 1
    return counts


def clear_check_queue_backlog(
        api_url: str, status: Optional[dict] = None) -> int:
    """Dequeue every item currently waiting in the check pipeline."""
    if status is None:
        status = _fetch_tenant_status(api_url)
    items = [
        ("check", project, change)
        for pipeline_name, project, change in _iter_all_queue_items(status)
        if pipeline_name == "check"
    ]
    return _batch_dequeue_items(api_url, items)


def clear_gate_queue_backlog(
        api_url: str, status: Optional[dict] = None) -> int:
    """Dequeue every item currently waiting in the gate pipeline."""
    if status is None:
        status = _fetch_tenant_status(api_url)
    items = [
        (DEMO_PIPELINE, project, change)
        for project, change in _iter_gate_queue_items(status)
    ]
    return _batch_dequeue_items(api_url, items)


def _audit_purge_removed_since(session_start: float) -> Optional[int]:
    """Return items removed by the latest scheduler purge since session_start."""
    if not AUDIT_PATH.is_file():
        return None
    try:
        tail = AUDIT_PATH.read_text(encoding="utf-8")[-65536:]
    except OSError:
        return None
    latest: Optional[int] = None
    for line in tail.splitlines():
        try:
            event = json.loads(line)
        except json.JSONDecodeError:
            continue
        ts = float(event.get("timestamp", 0))
        if ts < session_start - 0.5:
            continue
        if event.get("event") != "demo_queue_purged":
            continue
        latest = int(event.get("removed", 0))
    return latest


def _wait_for_scheduler_purge(
        session_start: float,
        timeout: float = DEMO_SCHEDULER_PURGE_WAIT) -> int:
    """Wait for scheduler purge audit after a reset/purge request."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        removed = _audit_purge_removed_since(session_start)
        if removed is not None:
            return removed
        if not _reset_file_pending():
            # Reset file consumed; allow a short grace for audit write.
            time.sleep(0.25)
            removed = _audit_purge_removed_since(session_start)
            if removed is not None:
                return removed
        time.sleep(DEMO_RESET_POLL_INTERVAL)
    return _audit_purge_removed_since(session_start) or 0


def wait_for_empty_queues(
        api_url: str,
        timeout: Optional[float] = None,
        on_poll=None,
        *,
        initial_scheduler_wait: float = DEMO_SCHEDULER_PURGE_WAIT,
        request_scheduler_purge=None,
        require_gate_only: bool = False,
) -> Tuple[bool, int, float]:
    """Poll until queues are empty, dequeuing stragglers.

    When require_gate_only is True (RL demo), success requires an empty gate
    pipeline only; check stragglers are dequeued best-effort but do not fail
    the demo. Check jobs are required for Verified+1 before gate promotion —
    do not dequeue check during traffic submission.
    """
    start = time.time()
    purge_session_start: Optional[float] = None
    purge_pending_deadline = 0.0

    status = _fetch_tenant_status(api_url)
    initial_items = _all_queue_item_count(status)
    initial_heads = _all_queue_depth(status)
    if require_gate_only:
        initial_items = _gate_queue_item_count(status)
        initial_heads = _gate_queue_depth(status)
    if timeout is None:
        timeout = _effective_queue_clear_timeout(initial_items, initial_heads)
    deadline = start + timeout
    items_at_round_start = initial_items
    total_cleared = 0
    stuck_rounds = 0
    log.info(
        "queue_clear_start items=%d heads=%d timeout=%.1fs gate_only=%s",
        initial_items, initial_heads, timeout, require_gate_only)

    if initial_scheduler_wait > 0:
        time.sleep(initial_scheduler_wait)

    if request_scheduler_purge is not None and initial_items > 0:
        purge_session_start = request_scheduler_purge()
        purge_pending_deadline = time.time() + max(
            DEMO_SCHEDULER_PURGE_WAIT * 4, 8.0)
        purged = _wait_for_scheduler_purge(
            purge_session_start,
            timeout=max(DEMO_SCHEDULER_PURGE_WAIT * 3, 6.0))
        log.info("scheduler purge removed %d item(s)", purged)
        time.sleep(DEMO_QUEUE_PROCESSING_WAIT)
        status = _fetch_tenant_status(api_url)
        item_count = (
            _gate_queue_item_count(status) if require_gate_only
            else _all_queue_item_count(status))
        if item_count < items_at_round_start:
            items_at_round_start = item_count

    def _dequeue_visible_tails(status: dict) -> int:
        items = list(_iter_dequeue_targets(status))
        if not items:
            return 0
        return _batch_dequeue_items(api_url, items, wait_for_results=True)

    def _current_item_count(status: dict) -> int:
        if require_gate_only:
            return _gate_queue_item_count(status)
        return _all_queue_item_count(status)

    def _queue_is_clear(status: dict) -> bool:
        if require_gate_only:
            return (
                _gate_queue_depth(status) == 0
                and _gate_queue_item_count(status) == 0
            )
        return (
            _all_queue_depth(status) == 0
            and _all_queue_item_count(status) == 0
        )

    while time.time() < deadline:
        status = _fetch_tenant_status(api_url)
        depth = _all_queue_depth(status)
        item_count = _current_item_count(status)
        if _queue_is_clear(status):
            if require_gate_only:
                clear_check_queue_backlog(api_url, status)
            elapsed = time.time() - start
            cleared_so_far = max(0, initial_items - item_count)
            if on_poll:
                on_poll(item_count, cleared_so_far)
            return True, cleared_so_far, elapsed

        if item_count >= items_at_round_start and item_count > 0:
            stuck_rounds += 1
        else:
            stuck_rounds = 0
        items_at_round_start = item_count

        if (stuck_rounds >= DEMO_QUEUE_STUCK_ROUNDS
                and request_scheduler_purge is not None):
            log.warning(
                "queue clear stuck at %d item(s) across %d head(s); "
                "requesting scheduler purge",
                item_count, depth)
            purge_session_start = request_scheduler_purge()
            purge_pending_deadline = time.time() + max(
                DEMO_SCHEDULER_PURGE_WAIT * 4, 8.0)
            purged = _wait_for_scheduler_purge(
                purge_session_start,
                timeout=max(DEMO_SCHEDULER_PURGE_WAIT * 3, 6.0))
            log.info("scheduler purge (stuck) removed %d item(s)", purged)
            stuck_rounds = 0
            time.sleep(DEMO_SCHEDULER_PURGE_WAIT)

        dispatched = _dequeue_visible_tails(status)
        if dispatched > 0:
            time.sleep(DEMO_QUEUE_PROCESSING_WAIT)

        status = _fetch_tenant_status(api_url)
        item_count = _current_item_count(status)
        cleared_so_far = max(0, initial_items - item_count)
        total_cleared = max(total_cleared, cleared_so_far)
        if on_poll:
            on_poll(item_count, cleared_so_far)
        if _queue_is_clear(status):
            if require_gate_only:
                clear_check_queue_backlog(api_url, status)
            elapsed = time.time() - start
            return True, cleared_so_far, elapsed
        time.sleep(DEMO_QUEUE_POLL_INTERVAL)

    if purge_pending_deadline > time.time():
        extra_deadline = purge_pending_deadline + timeout * 0.5
        log.info(
            "queue clear extending deadline for pending scheduler purge "
            "(until %.1fs)",
            extra_deadline - start)
        while time.time() < extra_deadline:
            status = _fetch_tenant_status(api_url)
            item_count = _current_item_count(status)
            if _queue_is_clear(status):
                if require_gate_only:
                    clear_check_queue_backlog(api_url, status)
                elapsed = time.time() - start
                cleared = max(0, initial_items - item_count)
                return True, cleared, elapsed
            _dequeue_visible_tails(status)
            time.sleep(DEMO_QUEUE_PROCESSING_WAIT)
            status = _fetch_tenant_status(api_url)
            item_count = _current_item_count(status)
            if _queue_is_clear(status):
                if require_gate_only:
                    clear_check_queue_backlog(api_url, status)
                elapsed = time.time() - start
                cleared = max(0, initial_items - item_count)
                return True, cleared, elapsed
            time.sleep(DEMO_QUEUE_POLL_INTERVAL)

    final_status = _fetch_tenant_status(api_url)
    if _queue_is_clear(final_status):
        if require_gate_only:
            clear_check_queue_backlog(api_url, final_status)
        elapsed = time.time() - start
        cleared = max(0, initial_items - _current_item_count(final_status))
        return True, cleared, elapsed
    final_depth = _all_queue_depth(final_status)
    final_items = _current_item_count(final_status)
    backlog = _describe_queue_backlog(final_status)
    log.error(
        "queue_clear_timeout items_remaining=%d heads_remaining=%d "
        "cleared=%d elapsed=%.1fs backlog=%s",
        final_items, final_depth,
        max(0, initial_items - final_items),
        time.time() - start,
        json.dumps(backlog))
    elapsed = time.time() - start
    return (
        False,
        max(0, initial_items - final_items),
        elapsed,
    )


def wait_for_empty_gate_queue(
        api_url: str,
        timeout: float = DEMO_QUEUE_CLEAR_TIMEOUT) -> bool:
    """Poll until gate queue is empty, dequeuing stragglers as needed."""
    ok, _removed, _elapsed = wait_for_empty_queues(api_url, timeout=timeout)
    return ok


def _request_demo_reset(*, purge_only: bool = False) -> float:
    """Signal scheduler to reset windows and/or purge pipeline queues."""
    ts = time.time()
    STATE["demo_session_start"] = ts
    payload = {"timestamp": ts}
    if purge_only:
        payload["purge_only"] = True
    try:
        DEMO_RESET_PATH.parent.mkdir(parents=True, exist_ok=True)
        DEMO_RESET_PATH.write_text(
            json.dumps(payload) + "\n", encoding="utf-8")
    except OSError:
        pass
    return ts


def _request_scheduler_purge() -> float:
    """Request scheduler queue purge without resetting RL/TCP windows."""
    return _request_demo_reset(purge_only=True)


def _agent_ticks(events: Sequence[dict],
                 session_start: Optional[float] = None) -> List[dict]:
    if session_start is None:
        session_start = _effective_session_start(events)
    ticks = [
        e for e in events
        if e.get("event") == "agent_tick"
        and (session_start is None or float(e["timestamp"]) >= session_start)
        and e.get("tcp_shadow_window") is not None
    ]
    ticks.sort(key=lambda e: float(e["timestamp"]))
    return downsample_keep_span(ticks, LIVE_METRICS_MAX_POINTS)


def _tcp_shadow_events(events: Sequence[dict],
                       session_start: Optional[float] = None) -> List[dict]:
    """Gate-cycle TCP shadow updates (independent of RL agent ticks)."""
    if session_start is None:
        session_start = _effective_session_start(events)
    shadows = [
        e for e in events
        if e.get("event") == "tcp_shadow"
        and (session_start is None or float(e["timestamp"]) >= session_start)
        and e.get("window_after") is not None
    ]
    shadows.sort(key=lambda e: float(e["timestamp"]))
    return downsample_keep_span(shadows, LIVE_METRICS_MAX_POINTS)


def _windows_at(ticks: Sequence[dict], tcp_events: Sequence[dict],
                ts: float) -> Tuple[Optional[float], Optional[float]]:
    rl = float(DEFAULT_INITIAL_WINDOW)
    tcp = float(DEFAULT_INITIAL_WINDOW)
    if ticks:
        stamps = [float(t["timestamp"]) for t in ticks]
        idx = bisect_right(stamps, ts) - 1
        if idx < 0:
            idx = 0
        tick = ticks[idx]
        rl = float(tick.get("actual_window",
                            tick.get("recommended_window", 0)))
    if tcp_events:
        stamps = [float(e["timestamp"]) for e in tcp_events]
        idx = bisect_right(stamps, ts) - 1
        if idx < 0:
            idx = 0
        tcp = float(tcp_events[idx]["window_after"])
    elif ticks:
        stamps = [float(t["timestamp"]) for t in ticks]
        idx = bisect_right(stamps, ts) - 1
        if idx < 0:
            idx = 0
        tick = ticks[idx]
        tcp = float(tick.get("tcp_shadow_window", DEFAULT_INITIAL_WINDOW))
    return rl, tcp


def _append_series_point(
        timestamps: List[float],
        rl_series: List[float],
        tcp_series: List[float],
        throughput_series: List[float],
        efficiency_series: List[Optional[float]],
        *,
        ts: float,
        rl_window: float,
        tcp_window: float,
        ticks: Sequence[dict],
        builds: Sequence[dict],
) -> None:
    efficiency = _throughput_efficiency_pct(rl_window, tcp_window)
    if (
        timestamps
        and rl_window == rl_series[-1]
        and tcp_window == tcp_series[-1]
        and efficiency == efficiency_series[-1]
    ):
        return
    timestamps.append(ts)
    rl_series.append(rl_window)
    tcp_series.append(tcp_window)
    throughput_series.append(
        _throughput_at(ticks, builds, ts, THROUGHPUT_WINDOW_SEC))
    efficiency_series.append(efficiency)


def _build_window_series(
        ticks: Sequence[dict],
        tcp_events: Sequence[dict],
        builds: Sequence[dict],
        session_start: Optional[float],
) -> Tuple[List[float], List[float], List[float], List[float],
           List[Optional[float]]]:
    """Merge RL agent ticks and per-cycle TCP shadow events for the chart."""
    timestamps: List[float] = []
    rl_series: List[float] = []
    tcp_series: List[float] = []
    throughput_series: List[float] = []
    efficiency_series: List[Optional[float]] = []

    baseline = _seed_baseline_point(session_start)
    timestamps.append(baseline["timestamp"])
    rl_series.append(baseline["rl_window"])
    tcp_series.append(baseline["tcp_window"])
    throughput_series.append(baseline["throughput"])
    efficiency_series.append(baseline["throughput_efficiency"])

    current_rl = baseline["rl_window"]
    current_tcp = baseline["tcp_window"]

    merged: List[Tuple[str, float, dict]] = []
    for tick in ticks:
        merged.append(("rl", float(tick["timestamp"]), tick))
    for event in tcp_events:
        merged.append(("tcp", float(event["timestamp"]), event))
    merged.sort(key=lambda item: item[1])

    for kind, ts, payload in merged:
        if kind == "rl":
            current_rl = float(
                payload.get("actual_window",
                            payload.get("recommended_window", current_rl)))
        else:
            current_tcp = float(payload["window_after"])
        _append_series_point(
            timestamps, rl_series, tcp_series, throughput_series,
            efficiency_series,
            ts=ts,
            rl_window=current_rl,
            tcp_window=current_tcp,
            ticks=ticks,
            builds=builds,
        )

    return (timestamps, rl_series, tcp_series, throughput_series,
            efficiency_series)


def _throughput_efficiency_pct(rl_size: float, tcp_size: float) -> Optional[float]:
    if tcp_size <= 0:
        return None
    return round(((rl_size - tcp_size) / tcp_size) * 100.0, 1)


def _effective_rl_window(rl_size: float, tcp_size: float) -> float:
    """RL active window is always >= TCP shadow in the demo."""
    return max(rl_size, tcp_size)


def _seed_baseline_point(session_start: Optional[float]) -> dict:
    ts = session_start if session_start is not None else time.time()
    return {
        "timestamp": ts,
        "rl_window": float(DEFAULT_INITIAL_WINDOW),
        "tcp_window": float(DEFAULT_INITIAL_WINDOW),
        "throughput": 0.0,
        "throughput_efficiency": 0.0,
    }


def _is_gate_pipeline_build(build: dict) -> bool:
    pipeline = (build.get("pipeline") or "").lower()
    job = (build.get("job_name") or "").lower()
    return pipeline == DEMO_PIPELINE or "gate" in job


def _gate_build_activity(
        builds: Sequence[dict],
        session_start: Optional[float]) -> dict:
    """Count session gate builds by lifecycle for demo health checks."""
    stats = {
        "running": 0,
        "finished": 0,
        "failed": 0,
        "total": 0,
    }
    for build in builds:
        if not _is_gate_pipeline_build(build):
            continue
        ts = parse_build_time(build)
        if session_start is not None and (ts is None or ts < session_start):
            continue
        stats["total"] += 1
        result = (build.get("result") or "").upper()
        if result in ("NEW", "RUNNING", ""):
            stats["running"] += 1
        elif result in ("SUCCESS", "MERGE"):
            stats["finished"] += 1
        else:
            stats["failed"] += 1
            stats["finished"] += 1
    return stats


def _audit_tcp_shadow_count(session_start: Optional[float]) -> int:
    if session_start is None or not AUDIT_PATH.is_file():
        return 0
    count = 0
    for event in _load_audit_events(AUDIT_PATH):
        if float(event.get("timestamp", 0)) < session_start:
            continue
        if event.get("event") == "tcp_shadow":
            count += 1
    return count


def _wait_for_gate_build_activity(
        session_start: float,
        *,
        timeout: Optional[float] = None,
        on_poll=None) -> dict:
    """Block until research-gate-job builds start, restarting workers if needed."""
    wait_timeout = (
        timeout if timeout is not None else DEMO_GATE_BUILD_START_TIMEOUT)
    start = time.time()
    deadline = start + wait_timeout
    last_stats = _gate_build_activity([], session_start)
    while time.time() < deadline:
        with _METRICS_LOCK:
            _BUILDS_CACHE["ts"] = 0.0
            _BUILDS_CACHE["data"] = []
        builds = fetch_builds(ZUUL_API)
        last_stats = _gate_build_activity(builds, session_start)
        cycles = _audit_tcp_shadow_count(session_start)
        if last_stats["running"] > 0 or last_stats["finished"] > 0:
            log.info(
                "gate build activity detected running=%d finished=%d "
                "failed=%d tcp_shadow=%d",
                last_stats["running"], last_stats["finished"],
                last_stats["failed"], cycles)
            return last_stats
        if not (_executor_is_running() and _component_is_running("launcher")):
            log.warning("gate builds idle; restarting stack workers")
            _start_stack_workers()
        if on_poll is not None:
            on_poll(last_stats, cycles, time.time() - start)
        time.sleep(DEMO_GATE_BUILD_POLL_SEC)
    raise RuntimeError(
        "No research-gate-job builds started within "
        f"{wait_timeout:.0f}s "
        f"(executor={_executor_is_running()}, "
        f"launcher={_component_is_running('launcher')}). "
        "Gate queue has changes but jobs are not executing — run: "
        "docker compose -f docker-compose.yaml "
        "-f docker-compose.rl-app.yaml -p zuul-rl "
        "up -d executor launcher node logs")


def _gate_builds(builds: Sequence[dict],
                 session_start: Optional[float] = None) -> List[dict]:
    gate_builds = []
    cutoff = time.time() - LIVE_METRICS_LOOKBACK_SEC
    for build in builds:
        if not _is_gate_pipeline_build(build):
            continue
        ts = parse_build_time(build)
        if ts is None or ts < cutoff:
            continue
        if session_start is not None and ts < session_start:
            continue
        gate_builds.append({**build, "_ts": ts})
    gate_builds.sort(key=lambda b: b["_ts"])
    return gate_builds


def _session_scoped_events(events: Sequence[dict],
                           session_start: Optional[float]) -> List[dict]:
    if session_start is None:
        return list(events)
    return [
        e for e in events
        if float(e.get("timestamp", 0)) >= session_start
    ]


def _is_gate_job_failure(build: dict) -> bool:
    result = (build.get("result") or "").upper()
    return result not in ("SUCCESS", "MERGE", "NEW", None, "")


def _last_rl_shrink_ts(ticks: Sequence[dict]) -> Optional[float]:
    """Timestamp of the most recent RL active-window decrease (agent_tick)."""
    last: Optional[float] = None
    prev_window: Optional[int] = None
    for tick in ticks:
        window = tick.get("actual_window")
        if window is None:
            continue
        current = int(window)
        if prev_window is not None and current < prev_window:
            last = float(tick["timestamp"])
        prev_window = current
    return last


def _last_tcp_shrink_ts(events: Sequence[dict]) -> Optional[float]:
    """Timestamp of the most recent TCP shadow window decrease."""
    last: Optional[float] = None
    for event in events:
        if event.get("event") != "tcp_shadow":
            continue
        before = event.get("window_before")
        after = event.get("window_after")
        if before is None or after is None:
            continue
        if int(after) < int(before):
            last = float(event["timestamp"])
    return last


def _count_since(items: Sequence[dict], ts_key: str,
                 since: Optional[float]) -> int:
    if since is None:
        return len(items)
    return sum(1 for item in items if float(item[ts_key]) > since)


def _compute_failure_counts(
        events: Sequence[dict],
        builds: Sequence[dict],
        ticks: Sequence[dict],
        session_start: Optional[float]) -> dict:
    """Aggregate gate job and cycle failure counts for the demo session."""
    session_events = _session_scoped_events(events, session_start)
    gate_failures = [
        b for b in builds if _is_gate_job_failure(b)
    ]
    cycle_failures = [
        e for e in session_events
        if e.get("event") == "tcp_shadow" and e.get("succeeded") is False
    ]
    tcp_shrinks = sum(
        1 for e in session_events
        if e.get("event") == "tcp_shadow"
        and e.get("window_before") is not None
        and e.get("window_after") is not None
        and int(e["window_after"]) < int(e["window_before"])
    )
    rl_shrinks = 0
    prev_window: Optional[int] = None
    for tick in ticks:
        window = tick.get("actual_window")
        if window is None:
            continue
        current = int(window)
        if prev_window is not None and current < prev_window:
            rl_shrinks += 1
        prev_window = current

    last_rl_shrink = _last_rl_shrink_ts(ticks)
    last_tcp_shrink = _last_tcp_shrink_ts(session_events)

    return {
        "gate_jobs_total": len(gate_failures),
        "gate_cycles_total": len(cycle_failures),
        "tcp_shrinks_total": tcp_shrinks,
        "rl_shrinks_total": rl_shrinks,
        "gate_jobs_since_rl_shrink": _count_since(
            gate_failures, "_ts", last_rl_shrink),
        "gate_cycles_since_tcp_shrink": _count_since(
            [{**e, "_ts": float(e["timestamp"])} for e in cycle_failures],
            "_ts", last_tcp_shrink),
        "last_rl_shrink_at": last_rl_shrink,
        "last_tcp_shrink_at": last_tcp_shrink,
    }


def _throughput_at(ticks: Sequence[dict], builds: Sequence[dict],
                   ts: float, window_sec: float) -> float:
    start = ts - window_sec
    completed = 0
    for build in builds:
        if build.get("result") not in ("SUCCESS", "MERGE"):
            continue
        bts = build.get("_ts")
        if bts is None:
            continue
        if start <= bts <= ts:
            completed += 1
    return round(completed / max(window_sec / 60.0, 1 / 60.0), 3)


def _parse_timestamp(value: object) -> Optional[float]:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value) / 1000.0 if value > 1e12 else float(value)
    if isinstance(value, str):
        try:
            return datetime.fromisoformat(
                value.replace("Z", "+00:00")).timestamp()
        except ValueError:
            return None
    return None


def _parse_build_duration(build: dict) -> Optional[float]:
    start = None
    for key in ("start_time", "enqueue_time"):
        start = _parse_timestamp(build.get(key))
        if start is not None:
            break
    end = _parse_timestamp(build.get("end_time"))
    if start is not None and end is not None and end > start:
        return end - start
    return None


def _build_duration_sec(build: dict) -> Optional[float]:
    """Measured gate job duration from API duration field or start/end times."""
    duration = build.get("duration")
    if duration is not None:
        try:
            parsed = float(duration)
            if parsed > 0:
                return parsed
        except (TypeError, ValueError):
            pass
    return _parse_build_duration(build)


def _avg_gate_job_duration_sec(builds: Sequence[dict]) -> Tuple[float, str]:
    """Return (avg_seconds, source) from finished gate builds or demo default."""
    durations = []
    for build in builds:
        result = (build.get("result") or "").upper()
        if result in ("NEW", "RUNNING", ""):
            continue
        duration = _build_duration_sec(build)
        if duration is not None and duration > 0:
            durations.append(duration)
    if durations:
        return round(sum(durations) / len(durations), 3), "measured"
    return DEFAULT_GATE_JOB_DURATION_SEC, "default"


def _format_build_change_id(build: dict) -> Optional[str]:
    """Human-readable change id, e.g. test1 2263,1 from builds API ref."""
    ref = build.get("ref") or {}
    project = ref.get("project") or ref.get("project_canonical")
    change = ref.get("change")
    patchset = ref.get("patchset")
    if not project:
        buildset = build.get("buildset") or {}
        refs = buildset.get("refs") or []
        if refs:
            ref = refs[0]
            project = ref.get("project") or ref.get("project_canonical")
            change = ref.get("change")
            patchset = ref.get("patchset")
    if not project:
        return None
    if change and patchset:
        return f"{project} {change},{patchset}"
    if change:
        return f"{project} {change}"
    return str(project)


MINUTES_SAVED_FORMULA = (
    "minutes_saved = sum_over_failures(max(0, RL_held − TCP_after)) "
    "× avg_gate_job_duration_sec / 60, "
    "capped at session wall-clock minutes "
    "(only counts real failure events; idle time does not accumulate)"
)
JOB_RUNS_SAVED_FORMULA = (
    "job_runs_saved (est.) = extra_changes_total "
    "= Σ max(0, RL_held − TCP_after) over gate failures "
    "(≈ serial gate job-runs saved when jobs are ~1s; "
    "minutes_saved = job_runs_saved × avg_job_duration_sec / 60)"
)
# Display cap so tiny TCP_after denominators cannot produce absurd % next to
# modest extras/minutes (raw uncapped value is still exposed).
ADVANTAGE_PCT_DISPLAY_CAP = float(
    os.environ.get("RL_ADVANTAGE_PCT_DISPLAY_CAP", "200"))


def _count_merged_gate_builds(builds: Sequence[dict]) -> int:
    return sum(
        1 for build in builds
        if build.get("result") in ("SUCCESS", "MERGE")
    )


def _integrate_window_delta(
        timestamps: Sequence[float],
        rl_series: Sequence[float],
        tcp_series: Sequence[float],
        end_ts: Optional[float] = None) -> float:
    """Extra parallel slot-seconds: integral of max(0, RL window − TCP window).

    Diagnostic only — display minutes_saved uses the discrete failure formula.
    """
    if not timestamps or not rl_series or not tcp_series:
        return 0.0
    end = end_ts if end_ts is not None else time.time()
    total = 0.0
    for i, t0 in enumerate(timestamps):
        t1 = timestamps[i + 1] if i + 1 < len(timestamps) else end
        if t1 <= t0:
            continue
        idx = i + 1 if i + 1 < len(rl_series) else i
        delta = max(0.0, float(rl_series[idx]) - float(tcp_series[idx]))
        total += delta * (t1 - t0)
    return total


def _integrate_parallel_change_seconds(
        timestamps: Sequence[float],
        rl_series: Sequence[float],
        tcp_series: Sequence[float],
        *,
        gate_queue_count: Optional[int] = None,
        end_ts: Optional[float] = None,
        busy_only: bool = True) -> float:
    """Extra parallel change-seconds while the gate is busy.

    When busy_only is True (default), only the final segment accumulates if
    gate_queue_count > 0; historical segments use window delta only when the
    live queue is known busy. Idle / drained queues contribute 0 so the
    estimate cannot run away after traffic stops.
    """
    if not timestamps or not rl_series or not tcp_series:
        return 0.0
    end = end_ts if end_ts is not None else time.time()
    queue = int(gate_queue_count or 0)
    if busy_only and queue <= 0:
        return 0.0
    total = 0.0
    last_idx = len(timestamps) - 1
    for i, t0 in enumerate(timestamps):
        t1 = timestamps[i + 1] if i + 1 < len(timestamps) else end
        if t1 <= t0:
            continue
        idx = i + 1 if i + 1 < len(rl_series) else i
        rl_w = float(rl_series[idx])
        tcp_w = float(tcp_series[idx])
        if queue > 0:
            rl_in, tcp_in = _changes_in_window_counts(queue, rl_w, tcp_w)
            delta = max(0, rl_in - tcp_in)
        else:
            delta = 0.0
        # Only integrate the most recent segment when queue depth is live;
        # older segments would otherwise assume eternal saturation.
        if busy_only and i < last_idx:
            continue
        total += delta * (t1 - t0)
    return total


def _minutes_saved_from_change_seconds(
        change_slot_seconds: float,
        avg_job_duration_sec: float) -> float:
    return round(change_slot_seconds * avg_job_duration_sec / 60.0, 2)


def _extras_total_from_failures(failures: Sequence[dict]) -> int:
    """Session-total extras vs TCP: Σ max(0, RL_held − TCP_after) over failures.

    Same discrete sum that drives minutes_saved — not a live queue snapshot.
    """
    total_extra = 0
    for fail in failures:
        extra = fail.get("extra_changes")
        if extra is None:
            extra = _failure_extra_changes(
                fail.get("rl_window"),
                fail.get("tcp_window"),
                fail.get("tcp_window_after"),
            )
        total_extra += max(0, int(extra or 0))
    return total_extra


def _minutes_saved_from_failures(
        failures: Sequence[dict],
        avg_job_duration_sec: float,
        *,
        session_duration_min: float = 0.0) -> float:
    """Bounded, intuitive estimate from discrete gate failures.

    minutes_saved = Σ max(0, RL_held − TCP_after) × avg_job_duration / 60
    capped at session wall-clock minutes so idle time cannot inflate the
    number into hundreds of minutes.
    """
    total_extra = _extras_total_from_failures(failures)
    raw = total_extra * float(avg_job_duration_sec) / 60.0
    if session_duration_min > 0:
        raw = min(raw, float(session_duration_min))
    return round(max(0.0, raw), 2)


def extra_changes_in_flight(
        gate_queue_depth: int,
        rl_window: float,
        tcp_window: float) -> int:
    """Live extra = max(0, min(depth, RL) − min(depth, TCP))."""
    rl_in, tcp_in = _changes_in_window_counts(
        int(gate_queue_depth), float(rl_window), float(tcp_window))
    return max(0, rl_in - tcp_in)


def queue_saturation_target(rl_window: float, tcp_window: float) -> int:
    """Queue depth needed for a fully valid window comparison."""
    return int(round(max(float(rl_window), float(tcp_window)))) + \
        DEMO_QUEUE_SATURATION_MARGIN


def queue_is_saturated(
        gate_queue_depth: int,
        rl_window: float,
        tcp_window: float) -> bool:
    """True when depth >= max(RL, TCP): both windows fully utilized."""
    return int(gate_queue_depth) >= int(
        round(max(float(rl_window), float(tcp_window)))) > 0


def adaptive_batch_size(
        gate_queue_depth: int,
        rl_window: float,
        tcp_window: float,
        *,
        base_size: int = None,
        max_size: int = None) -> int:
    """Batch size that keeps the gate queue above max(RL, TCP) + margin.

    Rule: shortfall = target − depth; batch = clamp(base + shortfall,
    base, max). Deterministic fail ratio is preserved by scaling
    fail_per_batch with the same proportion (half the batch fails).
    """
    base = base_size if base_size is not None else DEMO_BATCH_SIZE
    cap = max_size if max_size is not None else DEMO_MAX_BATCH_SIZE
    target = queue_saturation_target(rl_window, tcp_window)
    shortfall = max(0, target - int(gate_queue_depth))
    return max(base, min(cap, base + shortfall))


def _tcp_shrink_near_failure(
        tcp_events: Sequence[dict],
        fail_ts: float) -> Tuple[Optional[int], Optional[int]]:
    """TCP window before/after from the nearest failed gate-cycle shadow event."""
    best: Optional[Tuple[float, int, int]] = None
    for event in tcp_events:
        if event.get("event") != "tcp_shadow":
            continue
        if event.get("succeeded") is not False:
            continue
        before = event.get("window_before")
        after = event.get("window_after")
        if before is None or after is None:
            continue
        if int(after) >= int(before):
            continue
        ts = float(event["timestamp"])
        if ts < fail_ts - 30.0:
            continue
        if best is None or abs(ts - fail_ts) < abs(best[0] - fail_ts):
            best = (ts, int(before), int(after))
    if best is None:
        return None, None
    return best[1], best[2]


def _session_held_windows(failures: Sequence[dict]) -> dict:
    """Peak RL / floor TCP (and means) at gate-failure moments this session.

    These are the windows that justify session extras/minutes — not the live
    end-of-run values, which often reconverge after the queue drains.
    """
    rl_vals: List[float] = []
    tcp_vals: List[float] = []
    for fail in failures:
        rl_at = fail.get("rl_window")
        tcp_at = fail.get("tcp_window_after")
        if tcp_at is None:
            tcp_at = fail.get("tcp_window")
        if rl_at is not None:
            rl_vals.append(float(rl_at))
        if tcp_at is not None:
            tcp_vals.append(float(tcp_at))
    if not rl_vals and not tcp_vals:
        return {
            "rl_held_peak": None,
            "tcp_after_floor": None,
            "rl_held_mean": None,
            "tcp_after_mean": None,
        }
    return {
        "rl_held_peak": max(rl_vals) if rl_vals else None,
        "tcp_after_floor": min(tcp_vals) if tcp_vals else None,
        "rl_held_mean": round(sum(rl_vals) / len(rl_vals), 1) if rl_vals else None,
        "tcp_after_mean": (
            round(sum(tcp_vals) / len(tcp_vals), 1) if tcp_vals else None),
    }


def _session_impact_summary(
        *,
        minutes_saved: float,
        changes_in_window_delta: int,
        avg_job_duration_sec: float,
        session_gate_failures: int,
        latest_rl_window: float,
        latest_tcp_window: float,
        changes_in_window_rl: int,
        changes_in_window_tcp: int,
        job_duration_source: str,
        extra_changes_total: int = 0,
        session_rl_held: Optional[float] = None,
        session_tcp_floor: Optional[float] = None) -> str:
    if minutes_saved < 0.1:
        return (
            "No measurable RL advantage yet this session "
            f"(live RL window {latest_rl_window:.0f}, TCP shadow "
            f"{latest_tcp_window:.0f}; "
            f"{changes_in_window_rl} vs {changes_in_window_tcp} changes in flight)."
        )
    dur_note = (
        f"measured avg gate job {avg_job_duration_sec:.1f}s"
        if job_duration_source == "measured"
        else f"demo default gate job {avg_job_duration_sec:.1f}s")
    # Prefer session-total extras (same sum as minutes_saved). Live in-flight
    # can be 0 after the queue drains even when the run saved real time.
    session_extras = max(0, int(extra_changes_total or 0))
    if session_extras <= 0:
        session_extras = max(0, int(changes_in_window_delta or 0))
    # Describe failure-time held windows so summary cannot claim live
    # reconverged 20/20 while also reporting positive session extras.
    if session_rl_held is not None and session_tcp_floor is not None:
        window_clause = (
            f"At gate failures RL held up to {session_rl_held:.0f} while "
            f"TCP shrank as low as {session_tcp_floor:.0f}"
        )
    else:
        window_clause = (
            f"TCP shadow shrank to {latest_tcp_window:.0f} while RL held "
            f"{latest_rl_window:.0f}"
        )
    text = (
        f"{window_clause} — {session_extras} total extra change-slot(s) "
        f"vs TCP this session — estimated "
        f"{session_extras} job-runs saved "
        f"(~{minutes_saved:.1f} min at {avg_job_duration_sec:.1f}s/job; "
        f"{dur_note})"
    )
    if session_gate_failures:
        text += f" across {session_gate_failures} gate failure(s)"
    return text + "."


def _failure_extra_changes(
        rl_window: Optional[float],
        tcp_window: Optional[float],
        tcp_after: Optional[int] = None) -> int:
    """Extra parallel slots RL kept vs TCP at a failure (R − T, floored at 0)."""
    if rl_window is None:
        return 0
    tcp = tcp_after if tcp_after is not None else tcp_window
    if tcp is None:
        return 0
    return max(0, int(round(float(rl_window))) - int(round(float(tcp))))


def _failure_impact_text(
        *,
        change_id: Optional[str],
        job_name: str,
        tcp_before: Optional[int],
        tcp_after: Optional[int],
        rl_window: Optional[float],
        tcp_window: Optional[float],
        changes_in_window_delta: int,
        minutes_saved_so_far: float) -> str:
    change_label = change_id or "unknown"
    rl_val = int(rl_window) if rl_window is not None else "?"
    tcp_val = int(tcp_window) if tcp_window is not None else "?"
    extra = changes_in_window_delta
    if tcp_before is not None and tcp_after is not None:
        base = (
            f"Change {change_label} failed → TCP shrunk "
            f"{tcp_before}→{tcp_after}; RL held {rl_val}"
        )
    else:
        base = (
            f"Change {change_label} failed → TCP {tcp_val}; RL held {rl_val}"
        )
    if extra > 0:
        base += f" (+{extra} extra vs TCP)"
    return base


def _cap_advantage_pct(raw_pct: Optional[float]) -> Tuple[float, bool, Optional[float]]:
    """Return (display_pct, was_capped, raw_pct)."""
    if raw_pct is None:
        return 0.0, False, None
    raw = float(raw_pct)
    cap = float(ADVANTAGE_PCT_DISPLAY_CAP)
    if raw > cap:
        return round(cap, 1), True, round(raw, 1)
    return round(raw, 1), False, round(raw, 1)


def _compute_session_advantage(
        failures: Sequence[dict],
        rl_series: Sequence[float],
        tcp_series: Sequence[float],
        *,
        latest_rl: float,
        latest_tcp: float) -> dict:
    """Session RL advantage that stays meaningful after the queue drains.

    Instantaneous (RL−TCP)/TCP often returns to 0% when the gate empties or
    windows reconverge. Prefer failure-time extras:

      extra_i = max(0, RL_held − TCP_after)_i
      rl_advantage_pct = sum(extra_i) / sum(TCP_after_i) × 100
      (display-capped at ADVANTAGE_PCT_DISPLAY_CAP)

    Equivalent to (sum RL_held / sum TCP_after − 1) × 100 when extras are
    exactly RL−TCP. Fallback: peak (RL−TCP)/TCP over the window series.
    """
    extras: List[int] = []
    tcp_denoms: List[int] = []
    for fail in failures:
        tcp_at = fail.get("tcp_window_after")
        if tcp_at is None:
            tcp_at = fail.get("tcp_window")
        rl_at = fail.get("rl_window")
        extra = fail.get("extra_changes")
        if extra is None:
            extra = _failure_extra_changes(rl_at, tcp_at, None)
        extras.append(max(0, int(extra or 0)))
        if tcp_at is not None:
            tcp_denoms.append(max(1, int(round(float(tcp_at)))))

    total_extra = _extras_total_from_failures(failures)
    held = _session_held_windows(failures)
    if tcp_denoms and total_extra > 0:
        cum_pct = round(100.0 * total_extra / sum(tcp_denoms), 1)
    else:
        cum_pct = None

    peak = 0.0
    for rl, tcp in zip(rl_series, tcp_series):
        if tcp and float(tcp) > 0:
            peak = max(peak, ((float(rl) - float(tcp)) / float(tcp)) * 100.0)
    peak = round(peak, 1)

    instant = _throughput_efficiency_pct(latest_rl, latest_tcp)
    if cum_pct is not None:
        raw_advantage = cum_pct
        source = "failure_extra_over_tcp"
    elif peak > 0:
        raw_advantage = peak
        source = "series_peak"
    elif instant is not None:
        raw_advantage = instant
        source = "instantaneous"
    else:
        raw_advantage = 0.0
        source = "none"

    advantage, capped, raw = _cap_advantage_pct(raw_advantage)
    formula = (
        "sum(extra_changes at each gate failure) / "
        "sum(TCP_after at those failures) × 100"
    )
    if capped and raw is not None:
        formula += (
            f" (display capped at {ADVANTAGE_PCT_DISPLAY_CAP:g}%; "
            f"raw {raw}%)"
        )

    return {
        "extra_changes_total": total_extra,
        "rl_advantage_pct": advantage,
        "rl_advantage_pct_raw": raw if raw is not None else advantage,
        "advantage_capped": capped,
        "peak_rl_advantage_pct": peak,
        "instant_rl_advantage_pct": instant,
        "advantage_source": source,
        "extra_changes_per_failure": extras,
        "rl_held_peak": held["rl_held_peak"],
        "tcp_after_floor": held["tcp_after_floor"],
        "rl_held_mean": held["rl_held_mean"],
        "tcp_after_mean": held["tcp_after_mean"],
        "advantage_formula": formula,
    }


def _build_session_summary(
        *,
        failures: Sequence[dict],
        advantage: dict,
        effectiveness: dict,
        failure_counts: dict,
        latest: dict,
        changes_submitted: Optional[int] = None) -> dict:
    """Single coherent session model for demo summaries / popups.

    Session aggregates (extras, minutes, advantage, held windows, failure
    count) all derive from the same failures list. Live fields are nested
    under ``live`` so UI cannot confuse drained-queue snapshots with
    session totals.

    Headline demo counts (UI Session summary):
      - changes_submitted / submitted — demo traffic submitted this session
      - merged / session_changes_merged — successful gate merges
      - gate_failures — stamped / speculative gate fails ("in conflict" in UI)
      - extra_changes_total — session extras accommodated vs TCP
    """
    fail_n = len(failures)
    # Prefer failures-list length (extras/minutes basis). Fall back to
    # build-API count when the list is empty.
    if fail_n <= 0:
        fail_n = int(failure_counts.get("gate_jobs_total", 0) or 0)
    extras = int(advantage.get("extra_changes_total")
                 if advantage.get("extra_changes_total") is not None
                 else effectiveness.get("extra_changes_total") or 0)
    minutes = float(effectiveness.get("minutes_saved") or 0.0)
    # Keep minutes/extras coherent: positive minutes imply positive extras
    # (same discrete sum). Cap can shrink minutes but never invent extras.
    if minutes >= 0.1 and extras <= 0:
        minutes = 0.0
    adv_pct = float(advantage.get("rl_advantage_pct") or 0.0)
    adv_raw = advantage.get("rl_advantage_pct_raw")
    if adv_raw is None:
        adv_raw = adv_pct
    held_rl = advantage.get("rl_held_peak")
    held_tcp = advantage.get("tcp_after_floor")
    if held_rl is None or held_tcp is None:
        held = _session_held_windows(failures)
        held_rl = held["rl_held_peak"] if held_rl is None else held_rl
        held_tcp = held["tcp_after_floor"] if held_tcp is None else held_tcp

    merged = int(effectiveness.get("session_changes_merged") or 0)
    if changes_submitted is None:
        with LOCK:
            prog = STATE.get("demo_progress") or {}
            changes_submitted = int(prog.get("changes_submitted") or 0)
    else:
        changes_submitted = max(0, int(changes_submitted or 0))

    return {
        # Headline demo counts (keep aliases in sync — one model).
        "changes_submitted": changes_submitted,
        "submitted": changes_submitted,
        "merged": merged,
        "session_changes_merged": merged,
        # Gate stamped / speculative fails (UI: "Failed (gate)" / in-conflict).
        "gate_failures": fail_n,
        "extra_changes_total": extras,
        "job_runs_saved": job_runs_saved_est(extras),
        "minutes_saved": minutes,
        "rl_advantage_pct": adv_pct,
        "rl_advantage_pct_raw": float(adv_raw),
        "advantage_capped": bool(advantage.get("advantage_capped")),
        "advantage_source": advantage.get("advantage_source") or "none",
        "advantage_formula": advantage.get("advantage_formula") or "",
        "rl_held_peak": held_rl,
        "tcp_after_floor": held_tcp,
        "rl_held_mean": advantage.get("rl_held_mean"),
        "tcp_after_mean": advantage.get("tcp_after_mean"),
        "avg_gate_job_duration_sec": float(
            effectiveness.get("avg_gate_job_duration_sec")
            or DEFAULT_GATE_JOB_DURATION_SEC),
        "minutes_saved_formula": (
            effectiveness.get("minutes_saved_formula") or MINUTES_SAVED_FORMULA),
        "job_runs_saved_formula": (
            effectiveness.get("job_runs_saved_formula")
            or JOB_RUNS_SAVED_FORMULA),
        "impact_summary": effectiveness.get("impact_summary") or "",
        "live": {
            "rl_window": latest.get("rl_window"),
            "tcp_window": latest.get("tcp_window"),
            "extra_in_flight": int(latest.get("extra_in_flight") or 0),
            "gate_queue_count": int(latest.get("gate_queue_count") or 0),
            "changes_in_window_rl": int(
                latest.get("changes_in_window_rl") or 0),
            "changes_in_window_tcp": int(
                latest.get("changes_in_window_tcp") or 0),
        },
    }


def session_summary_invariants(summary: dict) -> List[str]:
    """Return human-readable invariant violations (empty list = consistent)."""
    violations: List[str] = []
    extras = int(summary.get("extra_changes_total") or 0)
    minutes = float(summary.get("minutes_saved") or 0.0)
    adv = float(summary.get("rl_advantage_pct") or 0.0)
    fail_n = int(summary.get("gate_failures") or 0)
    job_runs = summary.get("job_runs_saved")
    if job_runs is not None and int(job_runs) != extras:
        violations.append(
            f"job_runs_saved ({job_runs}) must equal "
            f"extra_changes_total ({extras})")
    if minutes >= 0.1 and extras <= 0:
        violations.append(
            "minutes_saved > 0 requires extra_changes_total > 0")
    if extras > 0 and fail_n <= 0:
        violations.append(
            "extra_changes_total > 0 requires gate_failures > 0")
    if adv > float(ADVANTAGE_PCT_DISPLAY_CAP) + 0.05:
        violations.append(
            f"rl_advantage_pct {adv} exceeds display cap "
            f"{ADVANTAGE_PCT_DISPLAY_CAP}")
    if extras > 0 and adv <= 0 and summary.get("advantage_source") == (
            "failure_extra_over_tcp"):
        violations.append(
            "failure_extra_over_tcp source with extras > 0 must have "
            "advantage > 0")
    # Held windows must exist when we claim session extras from failures.
    if extras > 0:
        if summary.get("rl_held_peak") is None:
            violations.append("extras > 0 but rl_held_peak missing")
        if summary.get("tcp_after_floor") is None:
            violations.append("extras > 0 but tcp_after_floor missing")
    # Alias pairs must stay coherent (UI may read either name).
    submitted = summary.get("changes_submitted")
    if submitted is not None and summary.get("submitted") is not None:
        if int(submitted) != int(summary["submitted"]):
            violations.append(
                "submitted alias must equal changes_submitted")
    merged = summary.get("session_changes_merged")
    if merged is not None and summary.get("merged") is not None:
        if int(merged) != int(summary["merged"]):
            violations.append(
                "merged alias must equal session_changes_merged")
    if summary.get("changes_submitted") is not None:
        if int(summary["changes_submitted"]) < 0:
            violations.append("changes_submitted must be >= 0")
    return violations


def _build_comparison_table(
        *,
        failures: Sequence[dict],
        advantage: dict,
        effectiveness: dict,
        latest_rl: float,
        latest_tcp: float,
        baseline: int,
        failure_count: int,
        expected_failures: Optional[int] = None,
        session_summary: Optional[dict] = None) -> dict:
    """Post-run TCP-only vs RL agent comparison for the UI table.

    Prefers session_summary fields so the block never contradicts the
    Session summary cards / charts.
    """
    ss = session_summary or {}
    total_extra = int(
        ss.get("extra_changes_total")
        if ss.get("extra_changes_total") is not None
        else advantage.get("extra_changes_total") or 0)
    minutes = float(
        ss.get("minutes_saved")
        if ss.get("minutes_saved") is not None
        else effectiveness.get("minutes_saved") or 0)
    fail_n = int(
        ss.get("gate_failures")
        if ss.get("gate_failures") is not None
        else failure_count)
    expected_n = expected_failures
    if expected_n is None:
        expected_n = _session_expected_failures()
    expected_n = max(0, int(expected_n))
    merged = int(
        ss.get("merged")
        if ss.get("merged") is not None
        else ss.get("session_changes_merged")
        if ss.get("session_changes_merged") is not None
        else effectiveness.get("session_changes_merged") or 0)
    submitted = int(
        ss.get("changes_submitted")
        if ss.get("changes_submitted") is not None
        else ss.get("submitted") or 0)
    rl_peak = ss.get("rl_held_peak")
    tcp_floor = ss.get("tcp_after_floor")
    if rl_peak is None or tcp_floor is None:
        held = _session_held_windows(failures)
        if rl_peak is None:
            rl_peak = held.get("rl_held_peak")
        if tcp_floor is None:
            tcp_floor = held.get("tcp_after_floor")
    # Prefer failure-time held windows. Live latest often reconverges after
    # drain and must not overwrite the session story.
    if rl_peak is None:
        rl_peak = float(latest_rl)
    if tcp_floor is None:
        tcp_floor = float(latest_tcp)
    rl_peak = float(rl_peak)
    tcp_floor = float(tcp_floor)

    window_benefit = round(rl_peak - tcp_floor, 1)
    adv_pct = ss.get("rl_advantage_pct")
    if adv_pct is None:
        adv_pct = advantage.get("rl_advantage_pct", 0)
    job_runs = int(
        ss.get("job_runs_saved")
        if ss.get("job_runs_saved") is not None
        else job_runs_saved_est(total_extra))
    if job_runs != total_extra:
        job_runs = total_extra
    rows = [
        {
            "metric": "Changes submitted (session)",
            "tcp_only": submitted,
            "with_rl": submitted,
            "benefit": "same load",
        },
        {
            "metric": "Merged (successful gate)",
            "tcp_only": merged,
            "with_rl": merged,
            "benefit": "same merges",
        },
        {
            "metric": "Failed (gate) / in conflict (actual / expected)",
            "tcp_only": f"{fail_n}/{expected_n}",
            "with_rl": f"{fail_n}/{expected_n}",
            "benefit": "same load",
        },
        {
            "metric": "Extra accommodated vs TCP (session total)",
            "tcp_only": 0,
            "with_rl": total_extra,
            "benefit": total_extra,
        },
        {
            "metric": "Window at failures (RL peak / TCP floor)",
            "tcp_only": int(round(tcp_floor)),
            "with_rl": int(round(rl_peak)),
            "benefit": f"+{int(round(window_benefit))}" if window_benefit > 0
            else str(int(round(window_benefit))),
        },
        {
            "metric": "Est. job-runs saved (session)",
            "tcp_only": 0,
            "with_rl": job_runs,
            "benefit": job_runs,
        },
        {
            "metric": "RL advantage vs TCP (%)",
            "tcp_only": "0%",
            "with_rl": f"{adv_pct}%",
            "benefit": f"+{adv_pct}%",
        },
    ]
    if total_extra > 0 or job_runs > 0:
        mins_note = (
            f" (~{minutes} min at ~1s jobs)" if minutes >= 0.1 else "")
        summary = (
            f"After run: {submitted} submitted, {merged} merged, "
            f"{fail_n}/{expected_n} failed (gate), "
            f"TCP floor {int(round(tcp_floor))} vs RL peak "
            f"{int(round(rl_peak))} — {total_extra} extra accommodated vs "
            f"TCP / {job_runs} job-runs saved{mins_note}, "
            f"{adv_pct}% advantage."
        )
    else:
        summary = (
            f"After run: {submitted} submitted, {merged} merged, "
            f"{fail_n}/{expected_n} failed (gate) "
            f"(baseline window {baseline}). "
            "No measurable RL>TCP extras yet this session."
        )
    return {
        "columns": ["Metric", "If TCP only", "With RL agent", "Benefit"],
        "rows": rows,
        "summary": summary,
        "changes_submitted": submitted,
        "submitted": submitted,
        "extra_changes_total": total_extra,
        "job_runs_saved": job_runs,
        "rl_advantage_pct": adv_pct,
        "rl_held_peak": rl_peak,
        "tcp_after_floor": tcp_floor,
        "gate_failures": fail_n,
        "expected_failures": expected_n,
        "merged": merged,
        "session_changes_merged": merged,
    }


def _compute_effectiveness_metrics(
        *,
        timestamps: Sequence[float],
        rl_series: Sequence[float],
        tcp_series: Sequence[float],
        builds: Sequence[dict],
        failure_counts: dict,
        session_start: Optional[float],
        latest_rl_window: float,
        latest_tcp_window: float,
        latest_efficiency_pct: Optional[float],
        changes_in_window_rl: int = 0,
        changes_in_window_tcp: int = 0,
        gate_queue_count: int = 0,
        end_ts: Optional[float] = None,
        failures: Optional[Sequence[dict]] = None,
) -> dict:
    """Session RL vs TCP effectiveness estimates for the live monitor."""
    now = end_ts if end_ts is not None else time.time()
    slot_seconds = _integrate_window_delta(
        timestamps, rl_series, tcp_series, end_ts=now)
    change_slot_seconds = _integrate_parallel_change_seconds(
        timestamps, rl_series, tcp_series,
        gate_queue_count=gate_queue_count, end_ts=now, busy_only=True)
    avg_job_duration_sec, job_duration_source = _avg_gate_job_duration_sec(builds)
    session_duration_min = 0.0
    if session_start is not None:
        session_duration_min = round(
            max(now - float(session_start), 0.0) / 60.0, 2)
    # Primary: discrete per-failure estimate (bounded, no idle runaway).
    fail_list = list(failures or [])
    extra_changes_total = _extras_total_from_failures(fail_list)
    if fail_list:
        minutes_saved = _minutes_saved_from_failures(
            fail_list, avg_job_duration_sec,
            session_duration_min=session_duration_min)
    else:
        # Before any failures land, busy-only integral (0 when queue idle).
        minutes_saved = _minutes_saved_from_change_seconds(
            change_slot_seconds, avg_job_duration_sec)
        if session_duration_min > 0:
            minutes_saved = round(
                min(minutes_saved, session_duration_min), 2)
    window_delta = round(latest_rl_window - latest_tcp_window, 1)
    changes_in_window_delta = max(0, changes_in_window_rl - changes_in_window_tcp)
    # Also expose window gap when queue is saturated enough that extra==window_delta
    if (gate_queue_count >= int(round(latest_rl_window))
            and latest_rl_window > latest_tcp_window):
        changes_in_window_delta = max(
            changes_in_window_delta,
            int(round(latest_rl_window)) - int(round(latest_tcp_window)))
    # Failures list is the canonical basis for extras/minutes — keep the
    # displayed failure count aligned with that list.
    if fail_list:
        session_gate_failures = len(fail_list)
    else:
        session_gate_failures = int(
            failure_counts.get("gate_jobs_total", 0) or 0)
    held = _session_held_windows(fail_list)
    impact_summary = _session_impact_summary(
        minutes_saved=minutes_saved,
        changes_in_window_delta=changes_in_window_delta,
        avg_job_duration_sec=avg_job_duration_sec,
        session_gate_failures=session_gate_failures,
        latest_rl_window=latest_rl_window,
        latest_tcp_window=latest_tcp_window,
        changes_in_window_rl=changes_in_window_rl,
        changes_in_window_tcp=changes_in_window_tcp,
        job_duration_source=job_duration_source,
        extra_changes_total=extra_changes_total,
        session_rl_held=held.get("rl_held_peak"),
        session_tcp_floor=held.get("tcp_after_floor"),
    )

    return {
        "minutes_saved": minutes_saved,
        "minutes_saved_formula": MINUTES_SAVED_FORMULA,
        "job_runs_saved": job_runs_saved_est(extra_changes_total),
        "job_runs_saved_formula": JOB_RUNS_SAVED_FORMULA,
        "cumulative_slot_seconds": round(slot_seconds, 1),
        "cumulative_change_slot_seconds": round(change_slot_seconds, 1),
        "window_delta": window_delta,
        "parallelism_gain": window_delta,
        "changes_in_window_delta": changes_in_window_delta,
        "extra_changes_total": extra_changes_total,
        "throughput_efficiency_pct": latest_efficiency_pct,
        "changes_in_window_rl": changes_in_window_rl,
        "changes_in_window_tcp": changes_in_window_tcp,
        "gate_queue_count": gate_queue_count,
        "avg_gate_job_duration_sec": avg_job_duration_sec,
        "job_duration_source": job_duration_source,
        "session_changes_merged": _count_merged_gate_builds(builds),
        "session_changes_merged_note": (
            f"Successful gate merges; expect ~{DEMO_PASS_PER_BATCH}/"
            f"{DEMO_BATCH_SIZE} pass per batch"
        ),
        "session_gate_failures": session_gate_failures,
        "session_gate_cycle_failures": failure_counts.get(
            "gate_cycles_total", 0),
        "session_duration_min": session_duration_min,
        "rl_held_peak": held.get("rl_held_peak"),
        "tcp_after_floor": held.get("tcp_after_floor"),
        "impact_summary": impact_summary,
    }


def _fetch_live_gate_state(api_url: str) -> Optional[dict]:
    """Live gate RL/TCP windows and in-window change counts from Zuul status."""
    try:
        status = _fetch_tenant_status(api_url)
    except (urllib.error.URLError, TimeoutError, OSError, json.JSONDecodeError,
            ValueError):
        return None
    gate_count = _gate_queue_item_count(status)
    for pipeline in status.get("pipelines", []):
        if pipeline.get("name") != DEMO_PIPELINE:
            continue
        rl_block = pipeline.get("rl_window") or {}
        queues = rl_block.get("queues") or []
        if not queues:
            continue
        queue = queues[0]
        rl_win = queue.get("current_window")
        tcp_win = queue.get("tcp_shadow_window")
        if rl_win is None or tcp_win is None:
            continue
        rl_f = float(rl_win)
        tcp_f = float(tcp_win)
        rl_in, tcp_in = _changes_in_window_counts(gate_count, rl_f, tcp_f)
        return {
            "rl_window": rl_f,
            "tcp_window": tcp_f,
            "gate_queue_count": gate_count,
            "changes_in_window_rl": rl_in,
            "changes_in_window_tcp": tcp_in,
            "decision_reason": queue.get("decision_reason") or "",
            "decision_source": queue.get("decision_source") or "",
            "decision_detail": queue.get("decision_detail") or "",
            "queue_saturated": queue_is_saturated(gate_count, rl_f, tcp_f),
            "queue_target": queue_saturation_target(rl_f, tcp_f),
        }
    return None


def _fetch_live_gate_windows(api_url: str) -> Optional[Tuple[float, float]]:
    """Current applied RL window and TCP shadow from Zuul status API."""
    state = _fetch_live_gate_state(api_url)
    if state is None:
        return None
    return state["rl_window"], state["tcp_window"]


def _extend_series_to_now(
        timestamps: List[float],
        rl_series: List[float],
        tcp_series: List[float],
        throughput_series: List[float],
        efficiency_series: List[Optional[float]],
        *,
        ticks: Sequence[dict],
        builds: Sequence[dict],
        fallback_rl: float,
        fallback_tcp: float,
        live_windows: Optional[Tuple[float, float]] = None,
) -> Tuple[float, float]:
    """Append a live point at now so integrals and latest windows stay current."""
    now = time.time()
    if live_windows is not None:
        rl_now, tcp_now = live_windows
    else:
        rl_now = float(fallback_rl)
        tcp_now = float(fallback_tcp)
    _append_series_point(
        timestamps, rl_series, tcp_series, throughput_series,
        efficiency_series,
        ts=now,
        rl_window=rl_now,
        tcp_window=tcp_now,
        ticks=ticks,
        builds=builds,
    )
    return rl_now, tcp_now


def build_live_metrics() -> dict:
    events = _load_audit_events(AUDIT_PATH)
    session_start = _effective_session_start(events)
    ticks = _agent_ticks(events, session_start)
    tcp_events = _tcp_shadow_events(events, session_start)
    builds = _gate_builds(_cached_builds(ZUUL_API), session_start)

    (timestamps, rl_series, tcp_series, throughput_series,
     efficiency_series) = _build_window_series(
        ticks, tcp_events, builds, session_start)

    fallback_rl = rl_series[-1] if rl_series else float(DEFAULT_INITIAL_WINDOW)
    fallback_tcp = tcp_series[-1] if tcp_series else float(
        DEFAULT_INITIAL_WINDOW)
    now = time.time()
    live_state = _fetch_live_gate_state(ZUUL_API)
    live_windows = None
    if live_state is not None:
        live_windows = (live_state["rl_window"], live_state["tcp_window"])
    last_rl_window, last_tcp_window = _extend_series_to_now(
        timestamps, rl_series, tcp_series, throughput_series,
        efficiency_series,
        ticks=ticks,
        builds=builds,
        fallback_rl=fallback_rl,
        fallback_tcp=fallback_tcp,
        live_windows=live_windows,
    )

    failure_counts = _compute_failure_counts(
        events, builds, ticks, session_start)

    last_efficiency = _throughput_efficiency_pct(
        last_rl_window, last_tcp_window)
    gate_queue_count = live_state["gate_queue_count"] if live_state else 0
    changes_in_window_rl = live_state["changes_in_window_rl"] if live_state else 0
    changes_in_window_tcp = (
        live_state["changes_in_window_tcp"] if live_state else 0)
    live_extra = extra_changes_in_flight(
        gate_queue_count, last_rl_window, last_tcp_window)

    failures = []
    session_index = 0
    seen_failure_keys: set = set()
    avg_dur_early, _ = _avg_gate_job_duration_sec(builds)
    for build in builds:
        if not _is_gate_job_failure(build):
            continue
        session_index += 1
        ts = build["_ts"]
        rl, tcp = _windows_at(ticks, tcp_events, ts)
        change_id = _format_build_change_id(build)
        duration = _build_duration_sec(build)
        tcp_before, tcp_after = _tcp_shrink_near_failure(tcp_events, ts)
        par_gain = round((rl or 0) - (tcp or 0), 1)
        extra_changes = _failure_extra_changes(rl, tcp, tcp_after)
        in_window_delta = extra_changes
        impact_text = _failure_impact_text(
            change_id=change_id,
            job_name=build.get("job_name") or build.get("name") or "unknown",
            tcp_before=tcp_before,
            tcp_after=tcp_after,
            rl_window=rl,
            tcp_window=tcp,
            changes_in_window_delta=in_window_delta,
            minutes_saved_so_far=0.0,
        )
        key = build.get("uuid") or f"{change_id}:{ts}"
        seen_failure_keys.add(key)
        failures.append({
            "timestamp": ts,
            "change": change_id,
            "job_name": build.get("job_name") or build.get("name") or "unknown",
            "pipeline": build.get("pipeline") or "gate",
            "result": (build.get("result") or "").upper(),
            "duration_sec": round(duration, 2) if duration is not None else None,
            "rl_window": rl,
            "tcp_window": tcp,
            "tcp_window_before": tcp_before,
            "tcp_window_after": tcp_after,
            "parallelism_gain": par_gain,
            "extra_changes": extra_changes,
            "changes_in_window_delta": in_window_delta,
            "minutes_saved_so_far": 0.0,
            "uuid": build.get("uuid"),
            "session_index": session_index,
            "impact_text": impact_text,
            "source": "build",
        })

    # Enrich from audit tcp_shadow failures[] when scheduler logs per-change detail.
    session_events = _session_scoped_events(events, session_start)
    for event in session_events:
        if event.get("event") != "tcp_shadow" or event.get("succeeded") is not False:
            continue
        audit_failures = event.get("failures") or []
        evt_ts = float(event["timestamp"])
        tcp_before = event.get("window_before")
        tcp_after = event.get("window_after")
        rl_at, tcp_at = _windows_at(ticks, tcp_events, evt_ts)
        for entry in audit_failures:
            change_id = entry.get("change") or entry.get("change_id")
            project = entry.get("project")
            if project and change_id and " " not in str(change_id):
                change_id = f"{project} {change_id}"
            job_name = entry.get("job_name") or entry.get("job") or "gate cycle"
            build_uuid = entry.get("uuid") or entry.get("build_uuid")
            dedupe = build_uuid or f"audit:{change_id}:{evt_ts}"
            if dedupe in seen_failure_keys:
                continue
            seen_failure_keys.add(dedupe)
            session_index += 1
            par_gain = round((rl_at or 0) - (tcp_at or 0), 1)
            tcp_after_i = int(tcp_after) if tcp_after is not None else None
            extra_changes = _failure_extra_changes(rl_at, tcp_at, tcp_after_i)
            in_window_delta = extra_changes
            impact_text = _failure_impact_text(
                change_id=change_id,
                job_name=job_name,
                tcp_before=int(tcp_before) if tcp_before is not None else None,
                tcp_after=tcp_after_i,
                rl_window=rl_at,
                tcp_window=tcp_at,
                changes_in_window_delta=in_window_delta,
                minutes_saved_so_far=0.0,
            )
            failures.append({
                "timestamp": evt_ts,
                "change": change_id,
                "job_name": job_name,
                "pipeline": DEMO_PIPELINE,
                "result": (entry.get("result") or "FAILURE").upper(),
                "duration_sec": entry.get("duration_sec"),
                "rl_window": rl_at,
                "tcp_window": tcp_at,
                "tcp_window_before": (
                    int(tcp_before) if tcp_before is not None else None),
                "tcp_window_after": tcp_after_i,
                "parallelism_gain": par_gain,
                "extra_changes": extra_changes,
                "changes_in_window_delta": in_window_delta,
                "minutes_saved_so_far": 0.0,
                "uuid": build_uuid,
                "session_index": session_index,
                "impact_text": impact_text,
                "source": "audit",
            })

    failures.sort(key=lambda item: float(item["timestamp"]))
    for idx, fail in enumerate(failures, start=1):
        fail["session_index"] = idx
        # Running discrete minutes-saved through this failure.
        fail["minutes_saved_so_far"] = _minutes_saved_from_failures(
            failures[:idx], avg_dur_early,
            session_duration_min=max(
                0.0,
                (float(fail["timestamp"]) - float(session_start or fail["timestamp"]))
                / 60.0,
            ) if session_start else 0.0,
        )

    effectiveness = _compute_effectiveness_metrics(
        timestamps=timestamps,
        rl_series=rl_series,
        tcp_series=tcp_series,
        builds=builds,
        failure_counts=failure_counts,
        session_start=session_start,
        latest_rl_window=last_rl_window,
        latest_tcp_window=last_tcp_window,
        latest_efficiency_pct=last_efficiency,
        changes_in_window_rl=changes_in_window_rl,
        changes_in_window_tcp=changes_in_window_tcp,
        gate_queue_count=gate_queue_count,
        end_ts=now,
        failures=failures,
    )
    # Prefer live in-flight extra when queue is deep enough.
    if live_extra > 0:
        effectiveness["changes_in_window_delta"] = live_extra

    advantage = _compute_session_advantage(
        failures, rl_series, tcp_series,
        latest_rl=last_rl_window, latest_tcp=last_tcp_window)
    # Prefer session advantage so post-run UI is never stuck at 0% when
    # RL held higher than TCP at failures (even if live windows reconverged).
    display_efficiency = advantage["rl_advantage_pct"]
    effectiveness["throughput_efficiency_pct"] = display_efficiency
    effectiveness["rl_advantage_pct"] = display_efficiency
    effectiveness["extra_changes_total"] = advantage["extra_changes_total"]
    effectiveness["peak_rl_advantage_pct"] = advantage["peak_rl_advantage_pct"]
    effectiveness["advantage_source"] = advantage["advantage_source"]
    effectiveness["advantage_formula"] = advantage["advantage_formula"]
    effectiveness["rl_advantage_pct_raw"] = advantage.get("rl_advantage_pct_raw")
    effectiveness["advantage_capped"] = advantage.get("advantage_capped", False)
    effectiveness["rl_held_peak"] = advantage.get("rl_held_peak")
    effectiveness["tcp_after_floor"] = advantage.get("tcp_after_floor")

    # Canonical failure count matches the failures list used for extras.
    session_fail_n = len(failures) if failures else int(
        failure_counts.get("gate_jobs_total", 0) or 0)
    effectiveness["session_gate_failures"] = session_fail_n

    demo_phase = None
    session_submitted = 0
    with LOCK:
        prog = STATE.get("demo_progress") or {}
        demo_phase = prog.get("phase")
        session_submitted = int(prog.get("changes_submitted") or 0)
        demo_done = (demo_phase == "done") or (
            not STATE.get("running") and bool(STATE.get("latest_run")))

    mode = ticks[-1].get("mode") if ticks else None
    latest = {
        "timestamp": now,
        "rl_window": last_rl_window,
        "tcp_window": last_tcp_window,
        "throughput_per_min": _throughput_at(
            ticks, builds, now, THROUGHPUT_WINDOW_SEC),
        "throughput_efficiency_pct": display_efficiency,
        "rl_advantage_pct": display_efficiency,
        "rl_at_tcp_floor": last_rl_window <= last_tcp_window,
        "mode": mode,
        "minutes_saved": effectiveness["minutes_saved"],
        "impact_summary": effectiveness.get("impact_summary"),
        "parallelism_gain": effectiveness["parallelism_gain"],
        "changes_in_window_delta": effectiveness.get("changes_in_window_delta"),
        "changes_in_window_rl": changes_in_window_rl,
        "changes_in_window_tcp": changes_in_window_tcp,
        "extra_in_flight": live_extra,
        "extra_changes_total": advantage["extra_changes_total"],
        "job_runs_saved": effectiveness.get(
            "job_runs_saved",
            job_runs_saved_est(advantage["extra_changes_total"])),
        "rl_held_peak": advantage.get("rl_held_peak"),
        "tcp_after_floor": advantage.get("tcp_after_floor"),
        "gate_queue_count": gate_queue_count,
        "queue_depth": gate_queue_count,
        "queue_saturated": queue_is_saturated(
            gate_queue_count, last_rl_window, last_tcp_window),
        "queue_target": queue_saturation_target(
            last_rl_window, last_tcp_window),
        "rl_decision_reason": (
            live_state.get("decision_reason") if live_state else None),
        "rl_decision_source": (
            live_state.get("decision_source") if live_state else None),
        "rl_decision_detail": (
            live_state.get("decision_detail") if live_state else None),
        "avg_gate_job_duration_sec": effectiveness[
            "avg_gate_job_duration_sec"],
        "minutes_saved_formula": MINUTES_SAVED_FORMULA,
        "job_runs_saved_formula": JOB_RUNS_SAVED_FORMULA,
    }

    session_summary = _build_session_summary(
        failures=failures,
        advantage=advantage,
        effectiveness=effectiveness,
        failure_counts=failure_counts,
        latest=latest,
        changes_submitted=session_submitted,
    )

    comparison = None
    if demo_done or failures:
        comparison = _build_comparison_table(
            failures=failures,
            advantage=advantage,
            effectiveness=effectiveness,
            latest_rl=last_rl_window,
            latest_tcp=last_tcp_window,
            baseline=DEFAULT_INITIAL_WINDOW,
            failure_count=session_fail_n,
            expected_failures=_session_expected_failures(),
            session_summary=session_summary,
        )

    # Rebuild efficiency series from window series so the chart shows
    # meaningful divergence (not a flat 0% after reconvergence).
    rebuilt_eff: List[Optional[float]] = []
    for rl_v, tcp_v in zip(rl_series, tcp_series):
        rebuilt_eff.append(_throughput_efficiency_pct(float(rl_v), float(tcp_v)))
    if rebuilt_eff:
        rebuilt_eff[-1] = display_efficiency

    defaults = default_demo_targets()
    with LOCK:
        demo_change = STATE.get("demo_total_changes")
    return {
        "timestamps": timestamps,
        "rl_window": rl_series,
        "tcp_window": tcp_series,
        "throughput": throughput_series,
        "throughput_efficiency": rebuilt_eff or efficiency_series,
        "failures": failures[-20:],
        "failure_counts": failure_counts,
        "effectiveness": effectiveness,
        "latest": latest,
        "comparison": comparison,
        "advantage": advantage,
        "session_summary": session_summary,
        "demo_phase": demo_phase,
        "updated_at": time.time(),
        "session_start": session_start,
        "baseline_window": DEFAULT_INITIAL_WINDOW,
        "demo_change_count": (
            demo_change if demo_change is not None else DEMO_CHANGE_COUNT),
        "demo_batch_size": DEMO_BATCH_SIZE,
        "demo_duration_sec": DEMO_DURATION_SEC,
        "expected_failures_min": DEMO_FAIL_PER_BATCH,
        "expected_failures": _session_expected_failures(),
        "default_total_changes": defaults["total_changes"],
        "default_gate_failures": defaults["gate_failures"],
        "fail_per_batch": DEMO_FAIL_PER_BATCH,
        "pass_per_batch": DEMO_PASS_PER_BATCH,
    }


@APP.route("/live-metrics", methods=["GET"])
def live_metrics():
    return jsonify(_get_cached_live_metrics())


@APP.route("/run-demo", methods=["POST", "OPTIONS"])
def run_demo():
    if request.method == "OPTIONS":
        return ("", 204)
    body = request.get_json(silent=True) or {}
    params, err = parse_run_demo_params(body)
    if err:
        return jsonify({"ok": False, "message": err}), 400
    assert params is not None
    with LOCK:
        if STATE["running"]:
            prog = _demo_progress_snapshot()
            return jsonify({
                "ok": False,
                "message": "demo already running",
                "demo_id": prog.get("demo_id"),
                "phase": prog.get("phase"),
            }), 409
        demo_id = datetime.utcnow().strftime("%Y%m%d-%H%M%S")
        started = time.time()
        STATE["running"] = True
        STATE["started_at"] = started
        STATE["finished_at"] = None
        STATE["last_error"] = None
        STATE["demo_total_changes"] = params["total_changes"]
        STATE["demo_gate_failures"] = params["gate_failures"]
        STATE["demo_expected_failures"] = params["expected_failures"]
        STATE["demo_fail_stamped"] = 0
    session_start = _request_demo_reset()
    _reset_audit_reader()
    _invalidate_live_metrics_cache()
    target_msg = ""
    if params["total_changes"] is not None or params["gate_failures"] is not None:
        bits = []
        if params["total_changes"] is not None:
            bits.append(f"{params['total_changes']} changes")
        if params["gate_failures"] is not None:
            bits.append(f"{params['gate_failures']} gate failures")
        target_msg = " — " + ", ".join(bits)
    _set_demo_progress(
        demo_id=demo_id,
        phase="starting",
        message=(
            "Demo session starting — resetting RL/TCP windows to baseline"
            + target_msg
        ),
        started_at=started,
        percent=2,
        queues_cleared=0,
        queue_depth_remaining=0,
        changes_submitted=0,
        failures_so_far=0,
        wait_elapsed_s=0.0,
        wait_total_s=_effective_demo_gate_wait_sec(),
        total_changes=params["total_changes"],
        gate_failures_target=params["gate_failures"],
        expected_failures=params["expected_failures"],
    )
    thread = threading.Thread(
        target=_run_demo_background, args=(demo_id,), daemon=True)
    thread.start()
    return jsonify({
        "ok": True,
        "message": "demo started",
        "demo_id": demo_id,
        "phase": "starting",
        "session_start": session_start,
        "baseline_window": DEFAULT_INITIAL_WINDOW,
        "total_changes": params["total_changes"],
        "gate_failures": params["gate_failures"],
        "expected_failures": params["expected_failures"],
    })


def _mark(path: Path, name: str):
    ts = time.time()
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps({"scenario": name, "timestamp": ts}) + "\n")


def _run_traffic(*args):
    cmd = [
        "python", str(TRAFFIC_DIR / "traffic" / "generator.py"),
        "--workers", str(TRAFFIC_WORKERS),
        *args,
    ]
    env = os.environ.copy()
    env.setdefault("GERRIT_URL", "http://gerrit:8080")
    env.setdefault("DEMO_BATCH_SIZE", str(DEMO_BATCH_SIZE))
    env.setdefault("DEMO_FAIL_PER_BATCH", str(DEMO_FAIL_PER_BATCH))
    env.setdefault("DEMO_PASS_PER_BATCH", str(DEMO_PASS_PER_BATCH))
    env.setdefault("DEMO_BATCH_INTERVAL_SEC", str(DEMO_BATCH_INTERVAL_SEC))
    env.setdefault("DEMO_DURATION_SEC", str(DEMO_DURATION_SEC))
    # Bounded: a full continuous run plus generous drain margin.
    run(cmd, check=True, env=env,
        timeout=DEMO_DURATION_SEC + DEMO_BATCH_INTERVAL_SEC * 4 + 600)


def _live_failure_count() -> int:
    try:
        return int(
            _get_cached_live_metrics()
            .get("failure_counts", {})
            .get("gate_jobs_total", 0))
    except Exception:
        return 0


def _traffic_should_stop() -> bool:
    with LOCK:
        return bool(STATE.get("traffic_stop"))


def _pop_extend_batches() -> int:
    with LOCK:
        n = int(STATE.get("extend_batches") or 0)
        STATE["extend_batches"] = 0
        return n


def _run_continuous_traffic_loop(markers: Path) -> dict:
    """Push batches every DEMO_BATCH_INTERVAL_SEC for DEMO_DURATION_SEC.

    Honours extend_batches (does not cancel in-flight builds) and traffic_stop.
    When demo_total_changes / demo_gate_failures are set (via /run-demo),
    stops after the change target and stamps fails to hit the fail target.
    """
    sys_path_traffic = str(TRAFFIC_DIR / "traffic")
    if sys_path_traffic not in sys.path:
        sys.path.insert(0, sys_path_traffic)
    import generator as traffic_gen  # type: ignore

    with LOCK:
        target_total = STATE.get("demo_total_changes")
        target_fails = STATE.get("demo_gate_failures")
        expected_fails = int(
            STATE.get("demo_expected_failures") or DEMO_EXPECTED_FAILURES)
        STATE["demo_fail_stamped"] = 0

    if target_total is not None:
        target_total = max(0, int(target_total))
    if target_fails is not None:
        target_fails = max(0, int(target_fails))

    started = time.time()
    # Size the deadline so duration alone cannot strand change/fail targets.
    duration = DEMO_DURATION_SEC
    if target_total is not None and target_total > 0:
        est_batches = max(
            1, int(math.ceil(target_total / max(DEMO_BATCH_SIZE, 1))))
        duration = max(
            duration,
            est_batches * DEMO_BATCH_INTERVAL_SEC + 120.0)
    if target_fails is not None and target_fails > 0:
        est_fail_batches = max(
            1, int(math.ceil(target_fails / max(DEMO_BATCH_SIZE, 1))))
        # Extra slack for check timeouts / skips that need catch-up batches.
        duration = max(
            duration,
            est_fail_batches * DEMO_BATCH_INTERVAL_SEC + 180.0)
    with LOCK:
        # Full duration of traffic measured from the first batch —
        # setup phases (layout sync, queue clear) must not eat into it.
        deadline = started + duration
        STATE["traffic_deadline"] = deadline
        STATE["batches_completed"] = 0
        STATE["traffic_stop"] = False

    planned = _planned_batches(max(0.0, deadline - started))
    if target_total is not None and target_total > 0:
        planned = max(
            planned,
            int(math.ceil(target_total / max(DEMO_BATCH_SIZE, 1))))
    plan_tip = _batches_plan_tip(
        planned,
        target_total=target_total,
        duration_sec=max(0.0, deadline - started),
    )
    fail_note = (
        f"{target_fails} fail stamps target"
        if target_fails is not None
        else f"{DEMO_FAIL_PER_BATCH} fail + {DEMO_PASS_PER_BATCH} pass / batch")
    change_note = (
        f"{target_total} changes"
        if target_total is not None
        else f"~{int(duration / 60)} min")
    _set_demo_progress(
        phase="submitting_traffic",
        traffic_active=True,
        batches_planned=planned,
        batches_completed=0,
        batches_remaining=planned,
        batch_current=0,
        batches_plan_tip=plan_tip,
        extend_available=True,
        time_remaining_s=round(max(0.0, deadline - time.time()), 1),
        total_changes=target_total,
        gate_failures_target=target_fails,
        expected_failures=expected_fails,
        message=(
            f"Continuous traffic: {DEMO_BATCH_SIZE} changes every "
            f"{int(DEMO_BATCH_INTERVAL_SEC)}s for {change_note} "
            f"({fail_note})"
        ),
        percent=22,
    )
    _mark(markers, "baseline")

    batch_num = 0
    total_submitted = 0
    total_fail_stamped = 0
    catch_up_rounds = 0
    max_catch_up_rounds = 8
    all_change_ids: List[str] = []

    while True:
        if _traffic_should_stop():
            break
        remaining_fails_now = (
            max(0, target_fails - total_fail_stamped)
            if target_fails is not None else 0)
        hit_change_cap = (
            target_total is not None and total_submitted >= target_total)
        # Hit change cap with fail budget still open → catch-up batch of
        # remaining fail stamps (does not count against the change cap).
        catch_up_fails = bool(
            hit_change_cap and target_fails is not None
            and remaining_fails_now > 0
            and catch_up_rounds < max_catch_up_rounds)
        if hit_change_cap and not catch_up_fails:
            break
        if (target_fails is not None and target_fails > 0
                and total_fail_stamped >= target_fails
                and (target_total is None or hit_change_cap)):
            break
        now = time.time()
        extend_n = _pop_extend_batches()
        with LOCK:
            deadline = float(STATE.get("traffic_deadline") or deadline)
            if extend_n > 0:
                # Keep session live; extend does not cancel in-flight builds.
                STATE["traffic_deadline"] = max(deadline, now) + (
                    extend_n * DEMO_BATCH_INTERVAL_SEC)
                deadline = float(STATE["traffic_deadline"])

        needs_fail_catchup = (
            target_fails is not None
            and total_fail_stamped < target_fails
            and catch_up_rounds < max_catch_up_rounds)
        if batch_num > 0 and now >= deadline and extend_n <= 0:
            if needs_fail_catchup or catch_up_fails:
                # Extend just enough for one more catch-up batch.
                with LOCK:
                    STATE["traffic_deadline"] = (
                        now + DEMO_BATCH_INTERVAL_SEC + 30.0)
                    deadline = float(STATE["traffic_deadline"])
            else:
                # Allow one more batch if extend just arrived.
                extend_n = _pop_extend_batches()
                if extend_n <= 0:
                    break
                with LOCK:
                    STATE["traffic_deadline"] = now + (
                        extend_n * DEMO_BATCH_INTERVAL_SEC)
                    deadline = float(STATE["traffic_deadline"])

        failures = _live_failure_count()
        remaining = max(0.0, deadline - time.time())

        # Adaptive saturation: size this batch so gate queue depth stays
        # above max(RL, TCP) + margin — both windows fully utilized, so
        # the in-flight difference equals the window difference exactly.
        pre_live = _fetch_live_gate_state(ZUUL_API) or {}
        pre_depth = int(pre_live.get("gate_queue_count") or 0)
        pre_rl = float(pre_live.get("rl_window") or DEFAULT_INITIAL_WINDOW)
        pre_tcp = float(pre_live.get("tcp_window") or DEFAULT_INITIAL_WINDOW)
        batch_size = adaptive_batch_size(pre_depth, pre_rl, pre_tcp)
        if catch_up_fails:
            # Dedicated fail-stamp top-up after the change cap.
            batch_size = min(
                max(1, remaining_fails_now), DEMO_MAX_BATCH_SIZE)
        elif target_total is not None:
            remaining_changes = max(0, target_total - total_submitted)
            if remaining_changes <= 0 and not needs_fail_catchup:
                break
            if remaining_changes > 0:
                batch_size = min(batch_size, remaining_changes)

        if target_fails is not None:
            remaining_fails = max(0, target_fails - total_fail_stamped)
            if remaining_fails <= 0 and (
                    target_total is None or total_submitted >= target_total):
                break
            if catch_up_fails:
                fail_n = min(batch_size, remaining_fails)
            elif target_total is not None:
                remaining_changes = max(0, target_total - total_submitted)
                if remaining_changes <= 0:
                    fail_n = min(batch_size, remaining_fails)
                else:
                    fail_n = fails_for_batch(
                        batch_size, remaining_changes, remaining_fails)
            else:
                # Duration mode with an absolute fail target.
                batches_left = max(
                    1, int(remaining // DEMO_BATCH_INTERVAL_SEC) + 1)
                fail_n = fails_for_duration_batch(
                    batch_size, remaining_fails, batches_left)
        elif batch_size > 0 and DEMO_FAIL_PER_BATCH > 0:
            # Deterministic fail ratio scales with batch size (half fail).
            fail_n = max(1, round(
                batch_size * DEMO_FAIL_PER_BATCH / max(DEMO_BATCH_SIZE, 1)))
        else:
            fail_n = 0
        fail_n = min(int(fail_n), batch_size)
        if batch_size <= 0:
            break
        target = queue_saturation_target(pre_rl, pre_tcp)
        topup_note = (
            f" · queue {pre_depth} < target {target} — topping up"
            if batch_size > DEMO_BATCH_SIZE and not catch_up_fails else ""
        )
        if catch_up_fails:
            topup_note = (
                f" · fail catch-up ({remaining_fails_now} stamps left)"
            )
        _set_demo_progress(
            phase="submitting_traffic",
            traffic_active=True,
            batches_completed=batch_num,
            batches_planned=max(planned, batch_num + 1),
            batches_remaining=max(0, int(remaining // DEMO_BATCH_INTERVAL_SEC)),
            batch_current=batch_num + 1,
            batches_plan_tip=_batches_plan_tip(
                max(planned, batch_num + 1),
                target_total=target_total,
                duration_sec=max(0.0, deadline - started),
            ),
            changes_submitted=total_submitted,
            failures_so_far=failures,
            time_remaining_s=round(remaining, 1),
            extend_available=True,
            gate_queue_depth=pre_depth,
            queue_saturated=queue_is_saturated(pre_depth, pre_rl, pre_tcp),
            queue_target=target,
            expected_failures=expected_fails,
            message=(
                f"Batch {batch_num + 1} — pushing {batch_size} changes "
                f"({fail_n} fail + {batch_size - fail_n} pass)"
                f"{topup_note} · {total_submitted} submitted · "
                f"{failures} gate failures · "
                f"~{int(remaining // 60)}:{int(remaining % 60):02d} left"
            ),
            percent=min(70, 22 + batch_num * 4),
        )

        def _on_change_progress(done: int, size: int,
                                _batch=batch_num, _base=total_submitted):
            # Heartbeat during pushes + Verified waits — a batch can take
            # minutes; without this the progress panel would look frozen.
            _set_demo_progress(
                changes_submitted=_base,
                message=(
                    f"Batch {_batch + 1} — {done}/{size} changes pushed "
                    f"(check → Verified+1 → gate)"
                ),
            )

        def _batch_heartbeat_message(elapsed: float,
                                     _batch=batch_num,
                                     _size=batch_size) -> str:
            live = _fetch_live_gate_state(ZUUL_API) or {}
            return (
                f"Batch {_batch + 1} — pushing {_size} changes, "
                f"waiting for check Verified+1… {elapsed:.0f}s · gate queue "
                f"{live.get('gate_queue_count', 0)} · RL "
                f"{int(live.get('rl_window') or 0)} / TCP "
                f"{int(live.get('tcp_window') or 0)}"
            )

        try:
            # Heartbeat covers the whole push + Verified wait (can take
            # minutes); per-change on_progress adds completion counts.
            with _PhaseHeartbeat(_batch_heartbeat_message, interval=4.0):
                result = traffic_gen.submit_batch(
                    project="test1",
                    batch_num=batch_num,
                    batch_size=batch_size,
                    fail_per_batch=fail_n,
                    promote_to_gate=True,
                    workers=TRAFFIC_WORKERS,
                    start_index=total_submitted,
                    prior_change_ids=all_change_ids,
                    on_progress=_on_change_progress,
                )
        except Exception as exc:  # noqa: BLE001 — batch skip, keep demo alive
            log.exception("batch %d submission failed; skipping", batch_num)
            _set_demo_progress(
                message=(
                    f"Batch {batch_num + 1} failed "
                    f"({type(exc).__name__}) — continuing with next batch"
                ),
            )
            result = {"submitted": 0, "change_ids": [], "fail_stamped": 0}
        total_submitted += int(result.get("submitted") or 0)
        stamped = int(result.get("fail_stamped") or 0)
        total_fail_stamped += stamped
        if catch_up_fails:
            catch_up_rounds += 1
        with LOCK:
            STATE["demo_fail_stamped"] = total_fail_stamped
        all_change_ids.extend(result.get("change_ids") or [])
        batch_num += 1
        with LOCK:
            STATE["batches_completed"] = batch_num

        live = _fetch_live_gate_state(ZUUL_API) or {}
        _set_demo_progress(
            batches_completed=batch_num,
            batch_current=batch_num,
            batches_planned=max(planned, batch_num),
            batches_plan_tip=_batches_plan_tip(
                max(planned, batch_num),
                target_total=target_total,
                duration_sec=max(0.0, deadline - started),
            ),
            changes_submitted=total_submitted,
            failures_so_far=_live_failure_count(),
            gate_queue_depth=live.get("gate_queue_count", 0),
            rl_window=live.get("rl_window"),
            tcp_window=live.get("tcp_window"),
            extra_in_flight=extra_changes_in_flight(
                int(live.get("gate_queue_count") or 0),
                float(live.get("rl_window") or DEFAULT_INITIAL_WINDOW),
                float(live.get("tcp_window") or DEFAULT_INITIAL_WINDOW),
            ),
            time_remaining_s=round(max(0.0, deadline - time.time()), 1),
            expected_failures=expected_fails,
        )

        if target_total is not None and total_submitted >= target_total:
            break

        # Sleep until next batch interval (interruptible for extend/stop).
        batch_started = time.time()
        last_live_update = 0.0
        while True:
            if _traffic_should_stop():
                break
            if target_total is not None and total_submitted >= target_total:
                # Keep sleeping only if fail catch-up is still needed; otherwise
                # exit the interval wait and let the outer loop catch up / stop.
                if (target_fails is None
                        or total_fail_stamped >= target_fails):
                    break
            extend_n = _pop_extend_batches()
            if extend_n > 0:
                with LOCK:
                    cur = float(STATE.get("traffic_deadline") or time.time())
                    STATE["traffic_deadline"] = cur + (
                        extend_n * DEMO_BATCH_INTERVAL_SEC)
                    deadline = float(STATE["traffic_deadline"])
            elapsed = time.time() - batch_started
            if elapsed >= DEMO_BATCH_INTERVAL_SEC:
                break
            if time.time() >= deadline and not int(STATE.get("extend_batches") or 0):
                # Finish interval only if no pending extend and no fail shortfall.
                with LOCK:
                    pending = int(STATE.get("extend_batches") or 0)
                if pending <= 0 and time.time() >= deadline:
                    if (target_fails is not None
                            and total_fail_stamped < target_fails):
                        break  # outer loop will catch up
                    break
            # Live heartbeat between batches (~every 3s): windows, queue
            # depth and countdown keep moving so the panel never idles.
            if time.time() - last_live_update >= 3.0:
                last_live_update = time.time()
                live = _fetch_live_gate_state(ZUUL_API) or {}
                remaining = max(0.0, deadline - time.time())
                until_next = max(
                    0.0, DEMO_BATCH_INTERVAL_SEC - (time.time() - batch_started))
                live_depth = int(live.get("gate_queue_count") or 0)
                live_rl = float(
                    live.get("rl_window") or DEFAULT_INITIAL_WINDOW)
                live_tcp = float(
                    live.get("tcp_window") or DEFAULT_INITIAL_WINDOW)
                saturated = queue_is_saturated(live_depth, live_rl, live_tcp)
                _set_demo_progress(
                    failures_so_far=_live_failure_count(),
                    gate_queue_depth=live_depth,
                    rl_window=live.get("rl_window"),
                    tcp_window=live.get("tcp_window"),
                    queue_saturated=saturated,
                    queue_target=queue_saturation_target(live_rl, live_tcp),
                    extra_in_flight=extra_changes_in_flight(
                        live_depth, live_rl, live_tcp),
                    time_remaining_s=round(remaining, 1),
                    expected_failures=expected_fails,
                    message=(
                        f"Batch {batch_num} done — next batch in "
                        f"{until_next:.0f}s · gate queue {live_depth} "
                        f"({'saturated' if saturated else 'draining'}) · RL "
                        f"{int(live_rl)} / TCP {int(live_tcp)} · "
                        f"~{int(remaining // 60)}:{int(remaining % 60):02d} left"
                    ),
                )
            time.sleep(0.5)

        if _traffic_should_stop():
            break
        if (target_total is not None and total_submitted >= target_total
                and (target_fails is None
                     or total_fail_stamped >= target_fails)):
            break
        if (target_fails is not None and target_fails > 0
                and total_fail_stamped >= target_fails
                and target_total is None):
            break
        if time.time() >= deadline:
            if (target_fails is not None
                    and total_fail_stamped < target_fails):
                # Outer loop starts another catch-up batch.
                continue
            extend_n = _pop_extend_batches()
            if extend_n <= 0:
                break
            with LOCK:
                STATE["traffic_deadline"] = time.time() + (
                    extend_n * DEMO_BATCH_INTERVAL_SEC)

    _set_demo_progress(traffic_active=False)
    return {
        "batches": batch_num,
        "submitted": total_submitted,
        "fail_stamped": total_fail_stamped,
        "duration_sec": round(time.time() - started, 1),
    }


@APP.route("/extend-demo", methods=["POST", "OPTIONS"])
def extend_demo():
    """Continue continuous traffic without cancelling in-flight builds.

    Body (any one):
      {"batches": N}  — push N more batches of DEMO_BATCH_SIZE
      {"minutes": M}  — continue for M more minutes
      {"count": N}    — push N more changes (rounded up to whole batches)
    """
    if request.method == "OPTIONS":
        return ("", 204)
    body = request.get_json(silent=True) or {}
    batches = body.get("batches")
    minutes = body.get("minutes")
    count = body.get("count")
    add_batches = 0
    if batches is not None:
        add_batches = max(1, int(batches))
    elif minutes is not None:
        add_batches = max(
            1, int(math.ceil(float(minutes) * 60.0 / DEMO_BATCH_INTERVAL_SEC)))
    elif count is not None:
        add_batches = max(
            1, int(math.ceil(float(count) / max(DEMO_BATCH_SIZE, 1))))
    else:
        add_batches = 1  # default: +1 batch of 10

    with LOCK:
        running = bool(STATE["running"])
        if not running:
            return jsonify({
                "ok": False,
                "message": "no active demo session — start with /run-demo first",
            }), 409
        STATE["extend_batches"] = int(STATE.get("extend_batches") or 0) + add_batches
        cur_deadline = STATE.get("traffic_deadline")
        now = time.time()
        base = float(cur_deadline) if cur_deadline else now
        if base < now:
            base = now
        STATE["traffic_deadline"] = base + add_batches * DEMO_BATCH_INTERVAL_SEC
        new_deadline = STATE["traffic_deadline"]
        pending = STATE["extend_batches"]

    _set_demo_progress(
        extend_available=True,
        time_remaining_s=round(max(0.0, float(new_deadline) - time.time()), 1),
        message=(
            f"Session extended by {add_batches} batch(es) "
            f"(+{add_batches * DEMO_BATCH_SIZE} changes) — "
            f"in-flight builds continue"
        ),
    )
    return jsonify({
        "ok": True,
        "batches_added": add_batches,
        "changes_added": add_batches * DEMO_BATCH_SIZE,
        "extend_pending": pending,
        "traffic_deadline": new_deadline,
        "message": "extend queued; in-flight builds not cancelled",
    })


def _run_demo_background(demo_id: str):
    run_id = demo_id
    out = BASE_DIR / run_id
    out.mkdir(parents=True, exist_ok=True)
    markers = out / "markers.jsonl"
    try:
        _set_demo_progress(
            phase="starting",
            message=(
                "Syncing zuul-config "
                "(check → Verified+1 → gate) and verifying layout"
            ),
            percent=3,
            batches_planned=_planned_batches(),
            extend_available=False,
        )
        # Layout sync may restart / full-reconfigure the scheduler; keep the
        # progress heartbeat alive so the UI never shows a frozen phase.
        with _PhaseHeartbeat(
                lambda elapsed: (
                    "Syncing zuul-config (check → Verified+1 → gate) "
                    f"and verifying layout… {elapsed:.0f}s"
                ),
                percent=4):
            _ensure_check_then_gate_layout()
        _ensure_executor_ready(update_progress=True)

        with LOCK:
            session_start = STATE.get("demo_session_start")
            STATE["traffic_stop"] = False
            STATE["extend_batches"] = 0
            STATE["traffic_deadline"] = time.time() + DEMO_DURATION_SEC
            STATE["batches_completed"] = 0
        if session_start is None:
            session_start = time.time()

        if not _demo_reset_ready(session_start):
            _set_demo_progress(
                phase="resetting_windows",
                message=(
                    f"Scheduler resetting RL/TCP windows to "
                    f"{DEFAULT_INITIAL_WINDOW}"
                ),
                percent=5,
            )
            _wait_for_demo_reset(session_start)

        _set_demo_progress(
            phase="clearing_queues",
            message="Clearing old changes...",
            percent=10,
        )

        clear_start = time.time()

        def _on_queue_poll(depth: int, removed: int):
            elapsed = time.time() - clear_start
            try:
                status = _fetch_tenant_status(ZUUL_API)
                by_pipe = _queue_counts_by_pipeline(status)
            except Exception:
                by_pipe = {}
            gate_n = by_pipe.get(DEMO_PIPELINE, 0)
            check_n = by_pipe.get("check", 0)
            rate = removed / elapsed if elapsed > 0 and removed > 0 else 0.0
            if rate > 0 and depth > 0:
                eta_s = int(depth / rate)
                timing = f"~{eta_s}s remaining"
            else:
                timing = f"{elapsed:.0f}s elapsed"
            _set_demo_progress(
                queues_cleared=removed,
                queue_depth_remaining=depth,
                gate_queue_remaining=gate_n,
                check_queue_remaining=check_n,
                clear_elapsed_s=round(elapsed, 1),
                message=(
                    f"Clearing old changes... {depth} left "
                    f"(gate {gate_n}, check {check_n}) — {timing}"
                ),
                percent=10 + min(
                    10,
                    int(10 * removed / max(depth + removed, 1)),
                ),
            )

        initial_status = _fetch_tenant_status(ZUUL_API)
        initial_depth = _all_queue_depth(initial_status)
        initial_items = _all_queue_item_count(initial_status)
        clear_timeout = _effective_queue_clear_timeout(
            _gate_queue_item_count(initial_status),
            _gate_queue_depth(initial_status))
        _set_demo_progress(queue_depth_remaining=initial_items)
        log.info(
            "queue_depth_before_clear items=%d heads=%d timeout=%.1fs gate_heads=%d",
            initial_items,
            initial_depth,
            clear_timeout,
            _gate_queue_depth(initial_status),
        )
        queues_empty, total_cleared, dequeue_sec = wait_for_empty_queues(
            ZUUL_API,
            timeout=clear_timeout,
            on_poll=_on_queue_poll,
            initial_scheduler_wait=DEMO_SCHEDULER_PURGE_WAIT,
            request_scheduler_purge=_request_scheduler_purge,
            require_gate_only=True,
        )
        log.info(
            "dequeue_completed_in_sec=%.2f removed=%d empty=%s",
            dequeue_sec, total_cleared, queues_empty)
        if not queues_empty:
            final_status = _fetch_tenant_status(ZUUL_API)
            backlog = _describe_queue_backlog(final_status)
            raise RuntimeError(
                "gate pipeline still has backlog after clear timeout "
                f"({total_cleared} cleared, "
                f"{_gate_queue_item_count(final_status)} gate remaining, "
                f"{dequeue_sec:.1f}s): {json.dumps(backlog)}")
        _set_demo_progress(
            phase="clearing_queues",
            queues_cleared=total_cleared,
            queue_depth_remaining=0,
            message=(
                f"Gate queue empty "
                f"({total_cleared} dequeued in {dequeue_sec:.1f}s)"
            ),
            percent=20,
            dequeue_completed_in_sec=round(dequeue_sec, 2),
        )

        _ensure_executor_ready(update_progress=True)

        traffic_summary = _run_continuous_traffic_loop(markers)
        time.sleep(2.0)
        post_status = _fetch_tenant_status(ZUUL_API)
        by_pipe = _queue_counts_by_pipeline(post_status)
        gate_n = by_pipe.get(DEMO_PIPELINE, 0)
        check_n = by_pipe.get("check", 0)
        _set_demo_progress(
            changes_submitted=traffic_summary.get("submitted", 0),
            batches_completed=traffic_summary.get("batches", 0),
            gate_queue_remaining=gate_n,
            check_queue_remaining=check_n,
            traffic_active=False,
            extend_available=False,
            message=(
                f"Traffic complete — {traffic_summary.get('batches', 0)} batches, "
                f"{traffic_summary.get('submitted', 0)} changes "
                f"({gate_n} in gate, {check_n} still in check)"
            ),
            percent=72,
        )

        def _on_build_poll(stats: dict, cycles: int, elapsed: float):
            _set_demo_progress(
                phase="waiting_gate_cycles",
                message=(
                    f"Waiting for gate jobs to start "
                    f"({elapsed:.0f}s) — builds={stats['total']} "
                    f"tcp_cycles={cycles}"
                ),
                percent=74,
            )

        build_stats = _wait_for_gate_build_activity(
            session_start, on_poll=_on_build_poll)
        _set_demo_progress(
            phase="waiting_gate_cycles",
            message=(
                f"Gate jobs running "
                f"({build_stats['running']} active, "
                f"{build_stats['finished']} finished, "
                f"{build_stats['failed']} failed)"
            ),
            percent=76,
            gate_builds_running=build_stats["running"],
            gate_builds_finished=build_stats["finished"],
            gate_builds_failed=build_stats["failed"],
        )

        wait_total = int(_effective_demo_gate_wait_sec())
        interval = 2
        steps = max(1, wait_total // interval)
        for step in range(steps):
            elapsed = (step + 1) * interval
            pct = 76 + int(12 * elapsed / wait_total)
            failures = _live_failure_count()
            cycles = _audit_tcp_shadow_count(session_start)
            live = _fetch_live_gate_state(ZUUL_API) or {}
            _set_demo_progress(
                phase="waiting_gate_cycles",
                message=(
                    f"Draining gate cycles "
                    f"({elapsed}s / {wait_total}s) — "
                    f"{failures} gate failure(s), {cycles} merge cycle(s)"
                ),
                wait_elapsed_s=float(elapsed),
                wait_total_s=float(wait_total),
                failures_so_far=failures,
                gate_cycles_so_far=cycles,
                gate_queue_depth=live.get("gate_queue_count", 0),
                rl_window=live.get("rl_window"),
                tcp_window=live.get("tcp_window"),
                extra_in_flight=extra_changes_in_flight(
                    int(live.get("gate_queue_count") or 0),
                    float(live.get("rl_window") or DEFAULT_INITIAL_WINDOW),
                    float(live.get("tcp_window") or DEFAULT_INITIAL_WINDOW),
                ),
                percent=min(88, pct),
            )
            time.sleep(interval)

        _mark(markers, "after-burst")
        _set_demo_progress(
            phase="generating_report",
            message="Collecting audit data and generating comparison report",
            percent=90,
            failures_so_far=_live_failure_count(),
            extend_available=False,
        )

        audit_out = out / "audit.jsonl"
        shutil.copy2(AUDIT_PATH, audit_out)

        with _PhaseHeartbeat(
                lambda elapsed: (
                    "Collecting audit data and generating comparison "
                    f"report… {elapsed:.0f}s"
                ),
                percent=91):
            build_report(audit_out, markers, out, ZUUL_API)

        _set_demo_progress(
            phase="publishing",
            message="Publishing report and graphs",
            percent=95,
        )
        _publish(out)
        _invalidate_live_metrics_cache()

        with LOCK:
            STATE["running"] = False
            STATE["finished_at"] = time.time()
            STATE["latest_run"] = run_id
            STATE["traffic_stop"] = False
        _set_demo_progress(
            phase="done",
            message=f"Demo complete — report {run_id} published",
            percent=100,
            failures_so_far=_live_failure_count(),
            extend_available=False,
            traffic_active=False,
        )
    except (CalledProcessError, RuntimeError, Exception) as exc:
        with LOCK:
            STATE["running"] = False
            STATE["finished_at"] = time.time()
            STATE["last_error"] = str(exc)
            STATE["traffic_stop"] = True
        _set_demo_progress(
            phase="error",
            message=str(exc),
            failures_so_far=_live_failure_count(),
            extend_available=False,
            traffic_active=False,
        )


def _publish(out: Path):
    PUBLISH_DIR.mkdir(parents=True, exist_ok=True)
    for name in (
        "report.html",
        "throughput_graph.png",
        "window_delta_graph.png",
        "comparison_table.csv",
        "summary.json",
    ):
        src = out / name
        if src.is_file():
            shutil.copy2(src, PUBLISH_DIR / name)
    (PUBLISH_DIR / "version.txt").write_text(str(int(time.time())), encoding="utf-8")


if __name__ == "__main__":
    APP.run(host="0.0.0.0", port=19100)
