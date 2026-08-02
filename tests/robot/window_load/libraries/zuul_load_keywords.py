"""Robot Framework keywords for Zuul RL/TCP load generation."""

from __future__ import annotations

import base64
import json
import os
import random
import re
import shutil
import subprocess
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Dict, List, Optional, Tuple


class ZuulLoadKeywords:
    """Generate mixed Gerrit/Zuul traffic for RL window-size experiments."""

    ROBOT_LIBRARY_SCOPE = "TEST SUITE"

    def __init__(self) -> None:
        self._repos: Dict[Tuple[str, str], Path] = {}
        self._changes: List[dict] = []
        self._rng = random.Random()
        self._workdir: Optional[Path] = None
        self._artifact_dir: Optional[Path] = None
        self._status_sample_interval = 25
        self._status_samples = 0
        self._last_status_sample = 0.0

    def prepare_workspace(
        self,
        gerrit_url: str = "http://localhost:8080",
        gerrit_auth: str = "admin:secret",
        projects: str = "test1",
        branches: str = "master",
        workspace_dir: str = "",
    ) -> str:
        """Clone repositories and install commit-msg hook."""
        self.gerrit_url = gerrit_url.rstrip("/")
        self.gerrit_auth = gerrit_auth
        self.projects = [p.strip() for p in projects.split(",") if p.strip()]
        self.branches = [b.strip() for b in branches.split(",") if b.strip()]
        if not self.projects:
            raise RuntimeError("At least one project is required")
        if not self.branches:
            raise RuntimeError("At least one branch is required")

        if workspace_dir:
            self._workdir = Path(workspace_dir).resolve()
            self._workdir.mkdir(parents=True, exist_ok=True)
        else:
            self._workdir = Path(tempfile.mkdtemp(prefix="zuul-robot-load-"))

        for project in self.projects:
            repo_dir = self._workdir / project
            if repo_dir.exists():
                shutil.rmtree(repo_dir)
            clone_url = self._project_url(project)
            self._run_git(["clone", clone_url, str(repo_dir)], cwd=self._workdir)
            self._run_git(["config", "user.email", "robot@example.com"], cwd=repo_dir)
            self._run_git(["config", "user.name", "Robot Load Bot"], cwd=repo_dir)
            hook_url = f"{self.gerrit_url}/tools/hooks/commit-msg"
            hook_path = repo_dir / ".git" / "hooks" / "commit-msg"
            self._download_file(hook_url, hook_path)
            hook_path.chmod(0o755)
            for branch in self.branches:
                self._repos[(project, branch)] = repo_dir
        return str(self._workdir)

    def run_mixed_load(
        self,
        total_count: int = 300,
        pace_ms: int = 150,
        scenario_mix: str = "burst=45,trickle=35,patchset=15,recheck=5",
        tenant: str = "example-tenant",
        check_pipeline: str = "check",
        gate_pipeline: str = "gate",
        gate_ratio: float = 0.35,
        artifact_dir: str = "",
        seed: int = 101,
        status_sample_interval: int = 25,
    ) -> str:
        """Generate weighted mixed traffic and emit JSONL artifacts."""
        if not self._repos:
            raise RuntimeError("Call prepare_workspace before run_mixed_load")
        if total_count < 1:
            raise RuntimeError("total_count must be positive")

        self._rng.seed(seed)
        self.tenant = tenant
        self.check_pipeline = check_pipeline
        self.gate_pipeline = gate_pipeline
        self.gate_ratio = max(0.0, min(1.0, gate_ratio))
        self._status_sample_interval = max(1, status_sample_interval)
        self._status_samples = 0
        self._last_status_sample = time.time()

        if artifact_dir:
            self._artifact_dir = Path(artifact_dir).resolve()
            self._artifact_dir.mkdir(parents=True, exist_ok=True)
        else:
            stamp = time.strftime("%Y%m%d-%H%M%S")
            self._artifact_dir = Path("robot-artifacts") / f"window-load-{stamp}"
            self._artifact_dir.mkdir(parents=True, exist_ok=True)

        events_path = self._artifact_dir / "events.jsonl"
        status_path = self._artifact_dir / "status_samples.jsonl"
        summary_path = self._artifact_dir / "summary.json"

        scenarios = self._parse_scenario_mix(scenario_mix)
        counters = {name: 0 for name in scenarios}
        submitted_changes = 0
        start_ts = time.time()

        while submitted_changes < total_count:
            scenario = self._pick_weighted_scenario(scenarios)
            if scenario == "burst":
                burst = min(total_count - submitted_changes, self._rng.randint(6, 18))
                for _ in range(burst):
                    event = self._submit_one_change("burst")
                    self._append_jsonl(events_path, event)
                    submitted_changes += 1
                    counters["burst"] += 1
                    self._sample_status_if_needed(status_path, submitted_changes)
            elif scenario == "trickle":
                event = self._submit_one_change("trickle")
                self._append_jsonl(events_path, event)
                submitted_changes += 1
                counters["trickle"] += 1
                self._sample_status_if_needed(status_path, submitted_changes)
                time.sleep(max(0.01, pace_ms / 1000.0))
            elif scenario == "patchset":
                event = self._submit_patchset_event()
                self._append_jsonl(events_path, event)
                if event.get("event_type") == "new_change":
                    submitted_changes += 1
                counters["patchset"] += 1
                self._sample_status_if_needed(status_path, submitted_changes)
                time.sleep(max(0.005, pace_ms / 2000.0))
            elif scenario == "recheck":
                event = self._submit_recheck_event()
                self._append_jsonl(events_path, event)
                if event.get("event_type") == "new_change":
                    submitted_changes += 1
                counters["recheck"] += 1
                self._sample_status_if_needed(status_path, submitted_changes)
                time.sleep(max(0.005, pace_ms / 2000.0))
            else:
                raise RuntimeError(f"Unsupported scenario: {scenario}")

        duration = round(time.time() - start_ts, 3)
        summary = {
            "submitted_changes": submitted_changes,
            "duration_seconds": duration,
            "events_written": self._count_jsonl_lines(events_path),
            "status_samples_written": self._status_samples,
            "scenario_counts": counters,
            "tenant": tenant,
            "pipelines": {
                "check_pipeline": check_pipeline,
                "gate_pipeline": gate_pipeline,
            },
            "gate_ratio": self.gate_ratio,
            "projects": self.projects,
            "branches": self.branches,
            "artifacts": {
                "events_jsonl": str(events_path),
                "status_samples_jsonl": str(status_path),
                "summary_json": str(summary_path),
            },
        }
        summary_path.write_text(json.dumps(summary, indent=2) + "\n", encoding="utf-8")
        return str(summary_path)

    def cleanup_workspace(self) -> None:
        """Delete temporary workspace created by prepare_workspace."""
        if self._workdir and self._workdir.exists():
            shutil.rmtree(self._workdir)
        self._workdir = None

    def _submit_one_change(self, scenario: str) -> dict:
        project = self._rng.choice(self.projects)
        branch = self._rng.choice(self.branches)
        repo = self._repos[(project, branch)]
        stamp = int(time.time() * 1000)
        fname = f"robot-load-{scenario}-{stamp}-{self._rng.randint(1000, 9999)}.txt"
        file_path = repo / fname
        file_path.write_text(
            f"scenario={scenario}\nproject={project}\nbranch={branch}\nts={stamp}\n",
            encoding="utf-8",
        )
        self._run_git(["add", "-A"], cwd=repo)
        self._run_git(
            ["commit", "-m", f"Robot load ({scenario}) {project}/{branch} {stamp}"],
            cwd=repo,
        )
        change_num = self._push_for_review(repo, project, branch)
        gate_target = self._rng.random() <= self.gate_ratio
        if gate_target:
            self._set_review_labels(project, change_num)
        record = {
            "event_type": "new_change",
            "scenario": scenario,
            "project": project,
            "branch": branch,
            "change": change_num,
            "gate_target": gate_target,
            "timestamp": int(time.time()),
        }
        self._changes.append(record)
        return record

    def _submit_patchset_event(self) -> dict:
        if not self._changes:
            return self._submit_one_change("patchset-seed")
        base = self._rng.choice(self._changes)
        repo = self._repos[(base["project"], base["branch"])]
        stamp = int(time.time() * 1000)
        fname = f"robot-patchset-{base['change']}-{stamp}.txt"
        (repo / fname).write_text(
            f"patchset update for change {base['change']} at {stamp}\n",
            encoding="utf-8",
        )
        self._run_git(["add", "-A"], cwd=repo)
        self._run_git(["commit", "--amend", "--no-edit"], cwd=repo)
        change_num = self._push_for_review(repo, base["project"], base["branch"])
        return {
            "event_type": "patchset_update",
            "scenario": "patchset",
            "project": base["project"],
            "branch": base["branch"],
            "change": change_num,
            "base_change": base["change"],
            "timestamp": int(time.time()),
        }

    def _submit_recheck_event(self) -> dict:
        if not self._changes:
            return self._submit_one_change("recheck-seed")
        base = self._rng.choice(self._changes)
        self._post_change_message(base["project"], base["change"], "recheck")
        return {
            "event_type": "recheck",
            "scenario": "recheck",
            "project": base["project"],
            "branch": base["branch"],
            "change": base["change"],
            "timestamp": int(time.time()),
        }

    def _sample_status_if_needed(self, status_path: Path, submitted_changes: int) -> None:
        now = time.time()
        if submitted_changes % self._status_sample_interval != 0 and (now - self._last_status_sample) < 20:
            return
        status = self._fetch_json(
            f"{self.zuul_url}/api/tenant/{urllib.parse.quote(self.tenant)}/status"
        )
        snapshot = {
            "timestamp": int(now),
            "submitted_changes": submitted_changes,
            "tenant": self.tenant,
            "pipelines": [],
        }
        for pipe in status.get("pipelines", []):
            if pipe.get("name") not in (self.check_pipeline, self.gate_pipeline):
                continue
            queues = []
            for queue in pipe.get("change_queues", []):
                queues.append(
                    {
                        "name": queue.get("name"),
                        "window": queue.get("window"),
                        "rl_recommended_window": queue.get("rl_recommended_window"),
                        "items": queue.get("_count"),
                    }
                )
            snapshot["pipelines"].append(
                {
                    "name": pipe.get("name"),
                    "heads": len(pipe.get("heads", [])),
                    "change_queues": queues,
                    "rl_window": pipe.get("rl_window"),
                }
            )
        self._append_jsonl(status_path, snapshot)
        self._status_samples += 1
        self._last_status_sample = now

    @property
    def zuul_url(self) -> str:
        return os.environ.get("ZUUL_URL", "http://localhost:19090").rstrip("/")

    def _project_url(self, project: str) -> str:
        host = self.gerrit_url.removeprefix("http://").removeprefix("https://")
        return f"http://{self.gerrit_auth}@{host}/{project}"

    def _push_for_review(self, repo: Path, project: str, branch: str) -> int:
        output = self._run_git(
            ["push", self._project_url(project), f"HEAD:refs/for/{branch}"],
            cwd=repo,
            capture=True,
        )
        match = re.search(r"/\+/(\d+)", output)
        if not match:
            raise RuntimeError(f"Unable to parse change number from push output:\n{output}")
        return int(match.group(1))

    def _set_review_labels(self, project: str, change_num: int) -> None:
        payload = {
            "labels": {"Workflow": 1, "Code-Review": 2},
            "message": "Robot load generator gate-target review",
        }
        self._api_post(
            f"{self.gerrit_url}/a/changes/{project}~{change_num}/revisions/current/review",
            payload,
        )

    def _post_change_message(self, project: str, change_num: int, message: str) -> None:
        payload = {"message": message}
        self._api_post(
            f"{self.gerrit_url}/a/changes/{project}~{change_num}/revisions/current/review",
            payload,
        )

    def _api_post(self, url: str, payload: dict) -> dict:
        data = json.dumps(payload).encode("utf-8")
        req = urllib.request.Request(
            url,
            data=data,
            headers=self._auth_headers(),
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=30) as resp:
            raw = resp.read().decode("utf-8", errors="replace")
        return self._decode_gerrit_json(raw)

    def _fetch_json(self, url: str) -> dict:
        req = urllib.request.Request(url, headers={"Accept": "application/json"})
        with urllib.request.urlopen(req, timeout=30) as resp:
            text = resp.read().decode("utf-8", errors="replace")
        return json.loads(text)

    def _download_file(self, url: str, destination: Path) -> None:
        req = urllib.request.Request(url)
        with urllib.request.urlopen(req, timeout=30) as resp:
            destination.write_bytes(resp.read())

    def _decode_gerrit_json(self, text: str) -> dict:
        cleaned = text[4:] if text.startswith(")]}'") else text
        if not cleaned.strip():
            return {}
        return json.loads(cleaned)

    def _run_git(self, args: List[str], cwd: Path, capture: bool = False) -> str:
        cmd = ["git", *args]
        if capture:
            out = subprocess.check_output(
                cmd, cwd=cwd, stderr=subprocess.STDOUT, text=True
            )
            return out
        subprocess.check_call(cmd, cwd=cwd)
        return ""

    def _auth_headers(self) -> dict:
        token = base64.b64encode(self.gerrit_auth.encode("utf-8")).decode("ascii")
        return {
            "Authorization": f"Basic {token}",
            "Content-Type": "application/json",
            "Accept": "application/json",
        }

    def _parse_scenario_mix(self, scenario_mix: str) -> Dict[str, int]:
        parsed: Dict[str, int] = {}
        for entry in scenario_mix.split(","):
            entry = entry.strip()
            if not entry:
                continue
            if "=" not in entry:
                raise RuntimeError(
                    "scenario_mix must be comma separated key=value entries"
                )
            name, weight_raw = entry.split("=", 1)
            name = name.strip()
            weight = int(weight_raw.strip())
            if name not in {"burst", "trickle", "patchset", "recheck"}:
                raise RuntimeError(f"Unsupported scenario in mix: {name}")
            if weight <= 0:
                continue
            parsed[name] = weight
        if not parsed:
            raise RuntimeError("scenario_mix did not define any active scenario")
        return parsed

    def _pick_weighted_scenario(self, scenarios: Dict[str, int]) -> str:
        total = sum(scenarios.values())
        point = self._rng.uniform(0, total)
        cur = 0.0
        for name, weight in scenarios.items():
            cur += weight
            if point <= cur:
                return name
        return next(iter(scenarios))

    def _append_jsonl(self, path: Path, item: dict) -> None:
        with path.open("a", encoding="utf-8") as fh:
            fh.write(json.dumps(item, sort_keys=True) + "\n")

    def _count_jsonl_lines(self, path: Path) -> int:
        if not path.exists():
            return 0
        with path.open("r", encoding="utf-8") as fh:
            return sum(1 for _ in fh)


_INSTANCE = ZuulLoadKeywords()


def prepare_workspace(
    gerrit_url: str = "http://localhost:8080",
    gerrit_auth: str = "admin:secret",
    projects: str = "test1",
    branches: str = "master",
    workspace_dir: str = "",
) -> str:
    return _INSTANCE.prepare_workspace(
        gerrit_url=gerrit_url,
        gerrit_auth=gerrit_auth,
        projects=projects,
        branches=branches,
        workspace_dir=workspace_dir,
    )


def run_mixed_load(
    total_count: int = 300,
    pace_ms: int = 150,
    scenario_mix: str = "burst=45,trickle=35,patchset=15,recheck=5",
    tenant: str = "example-tenant",
    check_pipeline: str = "check",
    gate_pipeline: str = "gate",
    gate_ratio: float = 0.35,
    artifact_dir: str = "",
    seed: int = 101,
    status_sample_interval: int = 25,
) -> str:
    return _INSTANCE.run_mixed_load(
        total_count=total_count,
        pace_ms=pace_ms,
        scenario_mix=scenario_mix,
        tenant=tenant,
        check_pipeline=check_pipeline,
        gate_pipeline=gate_pipeline,
        gate_ratio=gate_ratio,
        artifact_dir=artifact_dir,
        seed=seed,
        status_sample_interval=status_sample_interval,
    )


def cleanup_workspace() -> None:
    _INSTANCE.cleanup_workspace()
