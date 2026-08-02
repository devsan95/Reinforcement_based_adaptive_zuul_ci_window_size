#!/usr/bin/env python3
"""Build RL vs TCP window comparison table and throughput graph from a run."""

from __future__ import annotations

import argparse
import csv
import json
import sys
import urllib.error
import urllib.request
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Sequence, Tuple


@dataclass
class ScenarioSegment:
    name: str
    start: float
    end: float


@dataclass
class ScenarioSummary:
    name: str
    rl_window: float
    tcp_window: float
    window_delta: float
    throughput_per_min: float
    completed_builds: int
    duration_min: float
    improvement_pct: Optional[float]


def load_jsonl(path: Path) -> List[dict]:
    rows: List[dict] = []
    if not path.is_file():
        return rows
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line:
            continue
        rows.append(json.loads(line))
    return rows


def load_markers(path: Path) -> List[ScenarioSegment]:
    raw = [m for m in load_jsonl(path)
           if m.get("scenario") and m.get("timestamp") is not None]
    if not raw:
        return []
    markers = sorted(raw, key=lambda m: float(m["timestamp"]))
    segments: List[ScenarioSegment] = []
    for idx, m in enumerate(markers):
        name = str(m["scenario"])
        start = float(m["timestamp"])
        if idx + 1 < len(markers):
            end = float(markers[idx + 1]["timestamp"])
        else:
            end = float("inf")
        segments.append(ScenarioSegment(name=name, start=start, end=end))
    return segments


def default_segments(events: Sequence[dict]) -> List[ScenarioSegment]:
    ticks = [e for e in events if e.get("event") == "agent_tick"]
    if not ticks:
        return []
    start = min(float(e["timestamp"]) for e in ticks)
    end = max(float(e["timestamp"]) for e in ticks) + 1
    return [ScenarioSegment(name="full_run", start=start, end=end)]


def fetch_builds(api_url: str, limit: int = 500) -> List[dict]:
    url = f"{api_url.rstrip('/')}/api/tenant/example-tenant/builds?limit={limit}"
    try:
        with urllib.request.urlopen(url, timeout=15) as resp:
            return json.load(resp)
    except (urllib.error.URLError, TimeoutError, json.JSONDecodeError) as exc:
        print(f"warning: could not fetch builds from {url}: {exc}", file=sys.stderr)
        return []


def parse_build_time(build: dict) -> Optional[float]:
    for key in ("end_time", "start_time", "enqueue_time"):
        value = build.get(key)
        if value is None:
            continue
        if isinstance(value, (int, float)):
            return float(value) / 1000.0 if value > 1e12 else float(value)
        if isinstance(value, str):
            try:
                return datetime.fromisoformat(
                    value.replace("Z", "+00:00")).timestamp()
            except ValueError:
                continue
    return None


def segment_events(events: Sequence[dict], seg: ScenarioSegment) -> List[dict]:
    return [
        e for e in events
        if seg.start <= float(e.get("timestamp", 0)) < seg.end
    ]


def summarize_segment(
    name: str,
    events: Sequence[dict],
    builds: Sequence[dict],
    seg: ScenarioSegment,
    baseline_throughput: Optional[float],
) -> ScenarioSummary:
    ticks = [e for e in events if e.get("event") == "agent_tick"]
    if ticks:
        rl_vals = [float(t["recommended_window"]) for t in ticks]
        tcp_vals = [
            float(t.get("tcp_shadow_window", t.get("actual_window", 0)))
            for t in ticks]
        rl_window = sum(rl_vals) / len(rl_vals)
        tcp_window = sum(tcp_vals) / len(tcp_vals)
    else:
        rl_window = tcp_window = 0.0

    duration_min = max((seg.end - seg.start) / 60.0, 1 / 60.0)
    if seg.end == float("inf") and ticks:
        duration_min = max(
            (float(ticks[-1]["timestamp"]) - seg.start) / 60.0, 1 / 60.0)

    completed = 0
    for build in builds:
        if build.get("result") not in ("SUCCESS", "MERGE"):
            continue
        ts = parse_build_time(build)
        if ts is None or not (seg.start <= ts < seg.end):
            continue
        pipeline = (build.get("pipeline") or "").lower()
        job = (build.get("job_name") or "").lower()
        if pipeline == "gate" or "gate" in job:
            completed += 1

    throughput = completed / duration_min
    improvement = None
    if baseline_throughput and baseline_throughput > 0:
        improvement = ((throughput - baseline_throughput)
                       / baseline_throughput) * 100.0

    return ScenarioSummary(
        name=name,
        rl_window=round(rl_window, 2),
        tcp_window=round(tcp_window, 2),
        window_delta=round(rl_window - tcp_window, 2),
        throughput_per_min=round(throughput, 3),
        completed_builds=completed,
        duration_min=round(duration_min, 2),
        improvement_pct=round(improvement, 1) if improvement is not None else None,
    )


def write_csv(path: Path, rows: Sequence[ScenarioSummary]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow([
            "scenario",
            "rl_window",
            "tcp_window",
            "rl_minus_tcp",
            "throughput_per_min",
            "completed_builds",
            "duration_min",
            "improvement_vs_baseline_pct",
        ])
        for row in rows:
            writer.writerow([
                row.name,
                row.rl_window,
                row.tcp_window,
                row.window_delta,
                row.throughput_per_min,
                row.completed_builds,
                row.duration_min,
                "" if row.improvement_pct is None else row.improvement_pct,
            ])


def write_summary_json(path: Path, rows: Sequence[ScenarioSummary]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "scenarios": [
            {
                "name": r.name,
                "rl_window": r.rl_window,
                "tcp_window": r.tcp_window,
                "window_delta": r.window_delta,
                "throughput_per_min": r.throughput_per_min,
                "completed_builds": r.completed_builds,
                "duration_min": r.duration_min,
                "improvement_pct": r.improvement_pct,
            }
            for r in rows
        ]
    }
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def print_table(rows: Sequence[ScenarioSummary]) -> None:
    header = (
        f"{'Scenario':<22} {'RL':>5} {'TCP':>5} {'Diff':>5} "
        f"{'Thr/min':>8} {'Builds':>7} {'Improv%':>8}")
    print(header)
    print("-" * len(header))
    for row in rows:
        imp = "" if row.improvement_pct is None else f"{row.improvement_pct:+.1f}"
        print(
            f"{row.name:<22} {row.rl_window:>5.1f} {row.tcp_window:>5.1f} "
            f"{row.window_delta:>+5.1f} {row.throughput_per_min:>8.3f} "
            f"{row.completed_builds:>7d} {imp:>8}")


def _import_plot():
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
        return plt
    except ImportError:
        return None


def write_throughput_graph(path: Path, rows: Sequence[ScenarioSummary]) -> bool:
    plt = _import_plot()
    if plt is None:
        print("warning: matplotlib not installed; skipping throughput graph",
              file=sys.stderr)
        return False

    if not rows:
        return False

    labels = [r.name for r in rows]
    x = list(range(len(labels)))
    throughput = [r.throughput_per_min for r in rows]

    fig, ax = plt.subplots(figsize=(9, 4.5))
    bars = ax.bar(x, throughput, color="#2a7ade", label="Throughput (builds/min)")
    ax.set_title("Throughput Change Per Scenario")
    ax.set_ylabel("Builds / min")
    ax.set_xticks(x)
    ax.set_xticklabels(labels, rotation=20, ha="right")
    ax.grid(axis="y", alpha=0.3)

    for bar, row in zip(bars, rows):
        if row.improvement_pct is not None:
            ax.text(
                bar.get_x() + bar.get_width() / 2,
                bar.get_height(),
                f"{row.improvement_pct:+.0f}%",
                ha="center", va="bottom", fontsize=9)

    ax.legend()
    fig.tight_layout()
    path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(path, dpi=120)
    plt.close(fig)
    return True


def write_window_delta_graph(path: Path, rows: Sequence[ScenarioSummary]) -> bool:
    plt = _import_plot()
    if plt is None:
        print("warning: matplotlib not installed; skipping window delta graph",
              file=sys.stderr)
        return False

    if not rows:
        return False

    labels = [r.name for r in rows]
    x = list(range(len(labels)))
    values = [r.window_delta for r in rows]
    colors = ["#3e8635" if v >= 0 else "#c9190b" for v in values]

    fig, ax = plt.subplots(figsize=(9, 4.5))
    bars = ax.bar(x, values, color=colors, label="RL - TCP window")
    ax.set_title("RL Agent Window Difference vs TCP Policy")
    ax.set_ylabel("Window Delta")
    ax.axhline(0, color="#6a6e73", linewidth=1)
    ax.set_xticks(x)
    ax.set_xticklabels(labels, rotation=20, ha="right")
    ax.grid(axis="y", alpha=0.3)
    for bar, value in zip(bars, values):
        ax.text(
            bar.get_x() + bar.get_width() / 2,
            bar.get_height(),
            f"{value:+.1f}",
            ha="center",
            va="bottom" if value >= 0 else "top",
            fontsize=9,
        )

    ax.legend()
    fig.tight_layout()
    path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(path, dpi=120)
    plt.close(fig)
    return True


def write_html(path: Path, rows: Sequence[ScenarioSummary],
               throughput_graph_name: str,
               delta_graph_name: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    table_rows = "\n".join(
        f"<tr><td>{r.name}</td><td>{r.rl_window}</td><td>{r.tcp_window}</td>"
        f"<td>{r.window_delta:+.1f}</td><td>{r.throughput_per_min}</td>"
        f"<td>{r.completed_builds}</td>"
        f"<td>{'' if r.improvement_pct is None else f'{r.improvement_pct:+.1f}%'}"
        f"</td></tr>"
        for r in rows)
    throughput_block = (
        f'<img src="{throughput_graph_name}" alt="throughput graph" style="max-width:100%"/>'
        if throughput_graph_name and (path.parent / throughput_graph_name).is_file()
        else "<p>No throughput graph generated.</p>")
    delta_block = (
        f'<img src="{delta_graph_name}" alt="window delta graph" style="max-width:100%"/>'
        if delta_graph_name and (path.parent / delta_graph_name).is_file()
        else "<p>No window delta graph generated.</p>")
    html = f"""<!DOCTYPE html>
<html><head><meta charset="utf-8">
<title>RL vs TCP Run Report</title>
<style>
body {{ font-family: system-ui, sans-serif; margin: 2rem; }}
table {{ border-collapse: collapse; margin: 1rem 0; }}
th, td {{ border: 1px solid #ccc; padding: 0.4rem 0.7rem; text-align: right; }}
th:first-child, td:first-child {{ text-align: left; }}
th {{ background: #f4f4f4; }}
</style></head><body>
<h1>RL vs TCP Window Comparison</h1>
<table>
<tr><th>Scenario</th><th>RL window</th><th>TCP window</th><th>RL−TCP</th>
<th>Throughput/min</th><th>Builds</th><th>vs baseline</th></tr>
{table_rows}
</table>
<h2>Throughput Change</h2>
{throughput_block}
<h2>RL vs TCP Window Delta</h2>
{delta_block}
</body></html>"""
    path.write_text(html, encoding="utf-8")


def build_report(
    audit_path: Path,
    markers_path: Optional[Path],
    output_dir: Path,
    api_url: str,
) -> List[ScenarioSummary]:
    events = load_jsonl(audit_path)
    segments = load_markers(markers_path) if markers_path else []
    if not segments:
        segments = default_segments(events)

    builds = fetch_builds(api_url)
    summaries: List[ScenarioSummary] = []
    baseline_tp: Optional[float] = None

    for seg in segments:
        seg_events = segment_events(events, seg)
        summary = summarize_segment(
            seg.name, seg_events, builds, seg, baseline_tp)
        summaries.append(summary)
        if baseline_tp is None:
            baseline_tp = summary.throughput_per_min

    write_csv(output_dir / "comparison_table.csv", summaries)
    write_summary_json(output_dir / "summary.json", summaries)
    print_table(summaries)
    wrote_throughput = write_throughput_graph(
        output_dir / "throughput_graph.png", summaries)
    wrote_delta = write_window_delta_graph(
        output_dir / "window_delta_graph.png", summaries)
    write_html(
        output_dir / "report.html",
        summaries,
        "throughput_graph.png" if wrote_throughput else "",
        "window_delta_graph.png" if wrote_delta else "",
    )
    return summaries


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        description="Compare RL and TCP window sizes with throughput per scenario.")
    parser.add_argument("--audit", required=True, type=Path,
                        help="Path to rl_window_audit.jsonl")
    parser.add_argument("--markers", type=Path,
                        help="Scenario markers JSONL (scenario, timestamp)")
    parser.add_argument("--output", type=Path, default=Path("research/results/latest"),
                        help="Output directory for CSV/HTML/PNG")
    parser.add_argument("--api-url", default="http://localhost:19090",
                        help="Zuul API base URL")
    args = parser.parse_args(argv)

    if not args.audit.is_file():
        print(f"error: audit file not found: {args.audit}", file=sys.stderr)
        return 1

    build_report(args.audit, args.markers, args.output, args.api_url)
    print(f"\nWrote: {args.output}/comparison_table.csv")
    print(f"       {args.output}/summary.json")
    print(f"       {args.output}/report.html")
    if (args.output / "throughput_graph.png").is_file():
        print(f"       {args.output}/throughput_graph.png")
    if (args.output / "window_delta_graph.png").is_file():
        print(f"       {args.output}/window_delta_graph.png")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
