#!/usr/bin/env python3
"""Submit synthetic Gerrit changes for Zuul RL demos.

Traditional flow (--gate):
  1. Upload change → enters check (patchset-created)
  2. Wait for Verified +1 from check success
  3. Apply Workflow +1 → enters gate

Continuous mode (--continuous):
  Every DEMO_BATCH_INTERVAL_SEC (default 30s), submit DEMO_BATCH_SIZE
  (default 10) changes for DEMO_DURATION_SEC (default 300s).

Fail/pass rule (deterministic, stamped in demo-meta.json):
  Exactly DEMO_FAIL_PER_BATCH fails and DEMO_PASS_PER_BATCH passes
  per batch of 10 (defaults: 5 fail + 5 pass).
  Batch indices 0 .. FAIL-1 → gate fail; FAIL .. SIZE-1 → pass.
  Satisfies ≥3 fail and ≤5 pass per batch of 10.
"""

from __future__ import annotations

import argparse
import base64
import json
import os
import subprocess
import tempfile
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import List, Optional, Sequence, Tuple

GERRIT_BASE = os.environ.get("GERRIT_URL", "http://localhost:8080").rstrip("/")
GERRIT_AUTH = os.environ.get("GERRIT_AUTH", "admin:secret")
GERRIT_HOST = GERRIT_BASE.removeprefix("http://").removeprefix("https://")
VERIFIED_TIMEOUT = float(os.environ.get("TRAFFIC_VERIFIED_TIMEOUT", "180"))
VERIFIED_POLL = float(os.environ.get("TRAFFIC_VERIFIED_POLL", "2"))
# Hard ceiling for every git / curl subprocess so a wedged network call can
# never hang a submit worker (and therefore the whole batch) forever.
GIT_TIMEOUT = float(os.environ.get("TRAFFIC_GIT_TIMEOUT", "90"))

DEMO_BATCH_SIZE = int(os.environ.get("DEMO_BATCH_SIZE", "10"))
DEMO_BATCH_INTERVAL_SEC = float(os.environ.get("DEMO_BATCH_INTERVAL_SEC", "30"))
DEMO_DURATION_SEC = float(os.environ.get("DEMO_DURATION_SEC", "300"))
DEMO_FAIL_PER_BATCH = int(os.environ.get("DEMO_FAIL_PER_BATCH", "5"))
DEMO_PASS_PER_BATCH = int(os.environ.get("DEMO_PASS_PER_BATCH", "5"))

# Rotate scenarios across batches.
TOPICS = ("demo-burst", "demo-steady", "demo-depend")

# Fail rule: indices [0, FAIL) fail; [FAIL, SIZE) pass.
# Default 5+5 → ≥3 fail and ≤5 pass per batch of 10.


def batch_should_fail(batch_index: int,
                      fail_per_batch: int = DEMO_FAIL_PER_BATCH) -> bool:
    """Deterministic per-batch fail rule used by generator + gate job."""
    return int(batch_index) < int(fail_per_batch)


def _auth_header() -> str:
    return "Basic " + base64.b64encode(GERRIT_AUTH.encode()).decode()


def gerrit_review(project: str, change_num: int, workflow: int = 1):
    payload = json.dumps({
        "labels": {"Workflow": workflow, "Code-Review": 2},
        "message": "research traffic generator approval",
    }).encode()
    url = (
        f"{GERRIT_BASE}/a/changes/"
        f"{project}~{change_num}/revisions/current/review"
    )
    req = urllib.request.Request(
        url,
        data=payload,
        headers={
            "Content-Type": "application/json",
            "Authorization": _auth_header(),
        },
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=30) as resp:
        resp.read()


def _gerrit_json(url: str) -> dict | list:
    req = urllib.request.Request(
        url, headers={"Authorization": _auth_header()})
    with urllib.request.urlopen(req, timeout=30) as resp:
        raw = resp.read().decode()
    if raw.startswith(")]}'"):
        raw = raw[4:]
    return json.loads(raw or "{}")


def gerrit_verified_value(project: str, change_num: int) -> int | None:
    """Return the highest Verified vote on the current patchset, or None."""
    url = (
        f"{GERRIT_BASE}/a/changes/{project}~{change_num}"
        f"?o=LABELS&o=DETAILED_LABELS"
    )
    try:
        data = _gerrit_json(url)
    except (urllib.error.URLError, TimeoutError, OSError, json.JSONDecodeError):
        return None
    if not isinstance(data, dict):
        return None
    verified = (data.get("labels") or {}).get("Verified") or {}
    values = []
    for vote in verified.get("all") or []:
        if isinstance(vote, dict) and vote.get("value") is not None:
            values.append(int(vote["value"]))
    if values:
        return max(values)
    if verified.get("approved"):
        return 1
    if verified.get("rejected"):
        return -1
    if verified.get("value") is not None:
        return int(verified["value"])
    return None


def wait_for_verified(
        project: str,
        change_num: int,
        min_value: int = 1,
        timeout: float = VERIFIED_TIMEOUT) -> bool:
    """Poll Gerrit until Verified >= min_value (check success)."""
    deadline = time.time() + timeout
    last = None
    while time.time() < deadline:
        last = gerrit_verified_value(project, change_num)
        if last is not None and last >= min_value:
            return True
        if last is not None and last < 0:
            return False
        time.sleep(VERIFIED_POLL)
    print(
        f"WARNING: change {change_num} Verified timeout "
        f"(last={last}, need>={min_value})",
        flush=True,
    )
    return False


def _setup_repo(repo: Path, project: str):
    subprocess.check_call([
        "git", "clone", "--depth", "1",
        f"http://{GERRIT_AUTH}@{GERRIT_HOST}/{project}",
        str(repo),
    ], timeout=GIT_TIMEOUT)
    subprocess.check_call(
        ["git", "config", "user.email", "research@example.com"], cwd=repo,
        timeout=GIT_TIMEOUT)
    subprocess.check_call(
        ["git", "config", "user.name", "Research Bot"], cwd=repo,
        timeout=GIT_TIMEOUT)
    hook = repo / ".git" / "hooks" / "commit-msg"
    subprocess.check_call([
        "curl", "-sf", "--max-time", str(int(GIT_TIMEOUT)), "-o", str(hook),
        f"{GERRIT_BASE}/tools/hooks/commit-msg"], timeout=GIT_TIMEOUT + 10)
    hook.chmod(0o755)


def _read_change_id(repo_dir: Path) -> Optional[str]:
    msg = subprocess.check_output(
        ["git", "log", "-1", "--format=%B"], cwd=repo_dir, text=True,
        timeout=GIT_TIMEOUT)
    for line in msg.splitlines():
        if line.startswith("Change-Id:"):
            return line.split(":", 1)[1].strip()
    return None


def submit_change(
        repo_dir: Path,
        project: str,
        message: str,
        *,
        topic: Optional[str] = None) -> Tuple[Optional[int], Optional[str]]:
    """Commit + push; return (gerrit_change_num, change_id)."""
    subprocess.check_call(["git", "add", "-A"], cwd=repo_dir,
                          timeout=GIT_TIMEOUT)
    subprocess.check_call(
        ["git", "commit", "-m", message], cwd=repo_dir, timeout=GIT_TIMEOUT)
    change_id = _read_change_id(repo_dir)
    ref = "HEAD:refs/for/master"
    if topic:
        ref = f"HEAD:refs/for/master%topic={topic}"
    push = subprocess.check_output(
        ["git", "push",
         f"http://{GERRIT_AUTH}@{GERRIT_HOST}/{project}",
         ref],
        cwd=repo_dir,
        text=True,
        stderr=subprocess.STDOUT,
        timeout=GIT_TIMEOUT,
    )
    for line in push.splitlines():
        if "/+/" in line:
            num = line.split("/+/")[-1].split()[0]
            return int(num), change_id
    return None, change_id


def _build_commit_message(
        *,
        global_index: int,
        batch_num: int,
        batch_index: int,
        should_fail: bool,
        topic: str,
        depends_on: Optional[str],
) -> str:
    fail_flag = 1 if should_fail else 0
    lines = [
        f"Research continuous demo b{batch_num} i{batch_index} "
        f"fail={fail_flag} n={global_index}",
        "",
        f"Demo-Batch: {batch_num}",
        f"Demo-Batch-Index: {batch_index}",
        f"Demo-Should-Fail: {fail_flag}",
        f"Demo-Topic: {topic}",
    ]
    if depends_on:
        lines.append(f"Depends-On: {depends_on}")
    return "\n".join(lines) + "\n"


def _submit_one(
        workdir: Path,
        project: str,
        global_index: int,
        *,
        promote_to_gate: bool,
        immediate_gate: bool,
        batch_num: int = 0,
        batch_index: int = 0,
        should_fail: bool = False,
        topic: Optional[str] = None,
        depends_on: Optional[str] = None,
) -> Tuple[int, Optional[int], str, Optional[str]]:
    """Clone, commit once, push — isolated per worker for parallelism.

    Returns (global_index, change_num, phase, change_id).
    """
    repo = workdir / f"repo-{global_index}-{int(time.time() * 1000)}"
    _setup_repo(repo, project)
    unique = repo / f"research-{int(time.time())}-{global_index}.txt"
    unique.write_text(
        f"synthetic change {global_index} at {time.time()}\n"
        f"batch={batch_num} index={batch_index} fail={int(should_fail)}\n"
    )
    # Stamped metadata for gate-job fail rule (read from workspace).
    meta = {
        "batch": batch_num,
        "batch_index": batch_index,
        "should_fail": bool(should_fail),
        "topic": topic or "",
        "global_index": global_index,
        "fail_rule": (
            f"batch_index < {DEMO_FAIL_PER_BATCH} "
            f"→ fail ({DEMO_FAIL_PER_BATCH} fail / "
            f"{DEMO_PASS_PER_BATCH} pass per {DEMO_BATCH_SIZE})"
        ),
    }
    (repo / "demo-meta.json").write_text(
        json.dumps(meta, indent=2) + "\n", encoding="utf-8")

    message = _build_commit_message(
        global_index=global_index,
        batch_num=batch_num,
        batch_index=batch_index,
        should_fail=should_fail,
        topic=topic or "demo",
        depends_on=depends_on,
    )
    change, change_id = submit_change(
        repo, project, message, topic=topic)
    phase = "uploaded"
    if change is None:
        return global_index, None, phase, change_id
    if immediate_gate:
        gerrit_review(project, change, workflow=1)
        phase = "gate-immediate"
    elif promote_to_gate:
        print(
            f"Waiting for check Verified+1 on {project} {change}…",
            flush=True,
        )
        if wait_for_verified(project, change, min_value=1):
            gerrit_review(project, change, workflow=1)
            phase = "gate-after-check"
        else:
            phase = "check-failed-or-timeout"
    return global_index, change, phase, change_id


def submit_batch(
        *,
        project: str,
        batch_num: int,
        batch_size: int = DEMO_BATCH_SIZE,
        fail_per_batch: int = DEMO_FAIL_PER_BATCH,
        promote_to_gate: bool = True,
        immediate_gate: bool = False,
        workers: int = 8,
        start_index: int = 0,
        prior_change_ids: Optional[Sequence[str]] = None,
        on_progress=None,
) -> dict:
    """Submit one batch of changes with topic + optional Depends-On mix.

    Per-change failures (push error, Verified timeout, git hang) are logged
    and skipped — one bad change can never block the batch. on_progress, when
    given, is called as on_progress(done_count, batch_size) after each change.

    Scenario rotation by batch_num % 3:
      0 demo-burst  — independent changes, shared topic
      1 demo-steady — independent changes, shared topic
      2 demo-depend — half the batch Depends-On a prior Change-Id
    """
    topic = TOPICS[batch_num % len(TOPICS)]
    workdir = Path(tempfile.mkdtemp(prefix=f"zuul-traffic-b{batch_num}-"))
    workers = max(1, min(workers, batch_size))
    prior = list(prior_change_ids or [])
    anchor = prior[-1] if prior else None

    specs = []
    for i in range(batch_size):
        should_fail = batch_should_fail(i, fail_per_batch)
        depends_on = None
        # demo-depend: later half of batch depends on a previous Change-Id
        if topic == "demo-depend" and anchor and i >= batch_size // 2:
            depends_on = anchor
        specs.append((i, should_fail, depends_on))

    submitted = 0
    gated = 0
    fail_stamped = 0
    skipped = 0
    done = 0
    change_ids: List[str] = []
    change_nums: List[int] = []

    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = [
            pool.submit(
                _submit_one,
                workdir,
                project,
                start_index + i,
                promote_to_gate=promote_to_gate,
                immediate_gate=immediate_gate,
                batch_num=batch_num,
                batch_index=i,
                should_fail=should_fail,
                topic=topic,
                depends_on=depends_on,
            )
            for i, should_fail, depends_on in specs
        ]
        for future in as_completed(futures):
            done += 1
            try:
                index, change, phase, change_id = future.result()
            except Exception as exc:  # noqa: BLE001 — skip, never block batch
                skipped += 1
                print(
                    f"WARNING: batch {batch_num} change skipped "
                    f"({type(exc).__name__}: {exc})",
                    flush=True,
                )
                if on_progress is not None:
                    try:
                        on_progress(done, batch_size)
                    except Exception:
                        pass
                continue
            submitted += 1
            if phase.startswith("gate"):
                gated += 1
            batch_index = index - start_index
            if batch_should_fail(batch_index, fail_per_batch):
                fail_stamped += 1
            if change_id:
                change_ids.append(change_id)
            if change is not None:
                change_nums.append(change)
            print(
                f"Uploaded {project} change {change} "
                f"batch={batch_num} idx={batch_index} "
                f"topic={topic} [{phase}]",
                flush=True,
            )
            if on_progress is not None:
                try:
                    on_progress(done, batch_size)
                except Exception:
                    pass

    result = {
        "batch_num": batch_num,
        "topic": topic,
        "submitted": submitted,
        "gated": gated,
        "skipped": skipped,
        "fail_stamped": fail_stamped,
        "pass_stamped": submitted - fail_stamped,
        "change_ids": change_ids,
        "change_nums": change_nums,
        "start_index": start_index,
    }
    print(
        f"Batch {batch_num} done — {submitted} submitted, {gated} gated, "
        f"{skipped} skipped, {fail_stamped} stamped-to-fail, topic={topic}",
        flush=True,
    )
    return result


def run_continuous(
        *,
        project: str,
        duration_sec: float = DEMO_DURATION_SEC,
        batch_size: int = DEMO_BATCH_SIZE,
        interval_sec: float = DEMO_BATCH_INTERVAL_SEC,
        fail_per_batch: int = DEMO_FAIL_PER_BATCH,
        promote_to_gate: bool = True,
        workers: int = 8,
        max_batches: Optional[int] = None,
        stop_check=None,
) -> dict:
    """Push batches every interval_sec until duration or max_batches.

    stop_check: optional callable() -> bool; when True, stop after current batch.
    Does not cancel in-flight Gerrit/Zuul work.
    """
    started = time.time()
    deadline = started + max(0.0, float(duration_sec))
    batch_num = 0
    total_submitted = 0
    total_fail_stamped = 0
    all_change_ids: List[str] = []
    batches: List[dict] = []

    while True:
        if stop_check is not None and stop_check():
            print("Continuous traffic stop requested", flush=True)
            break
        if max_batches is not None and batch_num >= max_batches:
            break
        if batch_num > 0 and time.time() >= deadline and max_batches is None:
            # Allow completing batches started before deadline via extend.
            break
        # First batch always runs; later batches respect deadline unless
        # max_batches was set (extend path).
        if batch_num > 0 and max_batches is None and time.time() >= deadline:
            break

        batch_started = time.time()
        result = submit_batch(
            project=project,
            batch_num=batch_num,
            batch_size=batch_size,
            fail_per_batch=fail_per_batch,
            promote_to_gate=promote_to_gate,
            workers=workers,
            start_index=total_submitted,
            prior_change_ids=all_change_ids,
        )
        batches.append(result)
        total_submitted += result["submitted"]
        total_fail_stamped += result["fail_stamped"]
        all_change_ids.extend(result["change_ids"])
        batch_num += 1

        if max_batches is not None and batch_num >= max_batches:
            break
        if time.time() >= deadline and max_batches is None:
            break

        elapsed_batch = time.time() - batch_started
        sleep_for = max(0.0, interval_sec - elapsed_batch)
        # Wake periodically so stop_check / deadline can interrupt sleep.
        woke = 0.0
        while woke < sleep_for:
            if stop_check is not None and stop_check():
                break
            if max_batches is None and time.time() >= deadline:
                break
            step = min(1.0, sleep_for - woke)
            time.sleep(step)
            woke += step
        if stop_check is not None and stop_check():
            break
        if max_batches is None and time.time() >= deadline:
            break

    summary = {
        "batches": batch_num,
        "submitted": total_submitted,
        "fail_stamped": total_fail_stamped,
        "duration_sec": round(time.time() - started, 1),
        "batch_results": batches,
    }
    print(
        f"Continuous done — {batch_num} batches, {total_submitted} changes, "
        f"{total_fail_stamped} stamped-to-fail in {summary['duration_sec']}s",
        flush=True,
    )
    return summary


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--project", default="test1")
    parser.add_argument("--count", type=int, default=5)
    parser.add_argument(
        "--gate", action="store_true",
        help=(
            "Traditional flow: wait for check Verified+1, then Workflow+1 "
            "so the change enters gate"
        ),
    )
    parser.add_argument(
        "--gate-only", action="store_true",
        help="Alias for --gate (traditional check → Verified+1 → gate)",
    )
    parser.add_argument(
        "--immediate-gate", action="store_true",
        help=(
            "Legacy: approve Workflow+1 immediately on upload "
            "(skips waiting for check Verified+1)"
        ),
    )
    parser.add_argument(
        "--workers", type=int,
        default=int(os.environ.get("TRAFFIC_WORKERS", "8")),
        help="Parallel Gerrit submit workers")
    parser.add_argument(
        "--continuous", action="store_true",
        help="Push batches every --interval for --duration seconds",
    )
    parser.add_argument(
        "--duration", type=float, default=DEMO_DURATION_SEC,
        help="Continuous mode duration seconds (default 300)")
    parser.add_argument(
        "--interval", type=float, default=DEMO_BATCH_INTERVAL_SEC,
        help="Seconds between batch starts (default 30)")
    parser.add_argument(
        "--batch-size", type=int, default=DEMO_BATCH_SIZE,
        help="Changes per batch (default 10)")
    parser.add_argument(
        "--fail-per-batch", type=int, default=DEMO_FAIL_PER_BATCH,
        help="How many of each batch are stamped to fail at gate (default 5)")
    parser.add_argument(
        "--batches", type=int, default=None,
        help="Submit exactly N batches (extend / one-shot), ignore duration",
    )
    args = parser.parse_args()
    promote = args.gate or args.gate_only
    immediate = args.immediate_gate
    workers = max(1, args.workers)

    if args.continuous or args.batches is not None:
        run_continuous(
            project=args.project,
            duration_sec=args.duration,
            batch_size=args.batch_size,
            interval_sec=args.interval,
            fail_per_batch=args.fail_per_batch,
            promote_to_gate=promote or True,
            workers=workers,
            max_batches=args.batches,
        )
        return

    # Legacy one-shot: still stamp demo-meta for consistent fail rule.
    batch_size = args.count
    fail_n = min(DEMO_FAIL_PER_BATCH, batch_size)
    result = submit_batch(
        project=args.project,
        batch_num=0,
        batch_size=batch_size,
        fail_per_batch=fail_n,
        promote_to_gate=promote,
        immediate_gate=immediate,
        workers=min(workers, batch_size),
        start_index=0,
    )
    print(
        f"Done — {result['submitted']} change(s) submitted, "
        f"{result['gated']} promoted to gate with {workers} worker(s)"
    )


if __name__ == "__main__":
    main()
